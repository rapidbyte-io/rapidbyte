use std::collections::HashSet;
use std::io::Cursor;
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use tokio_postgres::{Client, NoTls};

use rapidbyte_sdk::errors::{CommitState, ConnectorError};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    DataErrorPolicy, Metric, MetricValue, SchemaEvolutionPolicy, StreamContext, WriteMode,
    WritePerf, WriteSummary,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

use crate::ddl::{prepare_staging, swap_staging_table};
use crate::loader::{write_batch, WriteContext};

/// Connect to PostgreSQL using the provided config.
pub(crate) async fn connect(
    config: &crate::config::Config,
) -> Result<tokio_postgres::Client, String> {
    let conn_str = config.connection_string();
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
}

/// Entry point for writing a single stream: validates identifiers, connects,
/// runs the pull loop through a WriteSession, and returns a WriteSummary.
pub fn write_stream(
    config: &crate::config::Config,
    ctx: &StreamContext,
) -> Result<WriteSummary, ConnectorError> {
    // Validate identifiers before interpolating into SQL
    validate_pg_identifier(&ctx.stream_name).map_err(|e| {
        ConnectorError::config(
            "INVALID_IDENTIFIER",
            format!("Invalid stream name: {}", e),
        )
    })?;
    validate_pg_identifier(&config.schema).map_err(|e| {
        ConnectorError::config(
            "INVALID_IDENTIFIER",
            format!("Invalid schema name: {}", e),
        )
    })?;

    let rt = crate::config::create_runtime();
    rt.block_on(async {
        // Phase 1: Connect
        let connect_start = Instant::now();
        let client = connect(config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        // Phase 2: Session lifecycle
        let mut session = WriteSession::begin(
            &client,
            &config.schema,
            SessionConfig {
                stream_name: ctx.stream_name.clone(),
                write_mode: ctx.write_mode.clone(),
                load_method: config.load_method.clone(),
                schema_policy: ctx.policies.schema_evolution,
                on_data_error: ctx.policies.on_data_error,
                checkpoint: CheckpointConfig {
                    interval_bytes: ctx.limits.checkpoint_interval_bytes,
                    interval_rows: ctx.limits.checkpoint_interval_rows,
                    interval_seconds: ctx.limits.checkpoint_interval_seconds,
                },
            },
        )
        .await
        .map_err(|e| ConnectorError::transient_db("SESSION_BEGIN_FAILED", e))?;

        // Phase 3: Pull loop — read batches from host
        let mut buf: Vec<u8> = Vec::new();
        let mut loop_error: Option<String> = None;

        loop {
            match host_ffi::next_batch(&mut buf, ctx.limits.max_batch_bytes) {
                Ok(None) => break,
                Ok(Some(n)) => {
                    if let Err(e) = session.process_batch(&buf[..n]).await {
                        loop_error = Some(e);
                        break;
                    }
                }
                Err(e) => {
                    loop_error = Some(format!("next_batch failed: {}", e));
                    break;
                }
            }
        }

        // Handle errors
        if let Some(err) = loop_error {
            session.rollback().await;
            return Err(
                ConnectorError::transient_db("WRITE_FAILED", err)
                    .with_commit_state(CommitState::BeforeCommit),
            );
        }

        // Phase 4: Commit
        let result = session.commit().await.map_err(|e| {
            ConnectorError::transient_db("COMMIT_FAILED", e)
                .with_commit_state(CommitState::AfterCommitUnknown)
        })?;

        Ok(WriteSummary {
            records_written: result.total_rows,
            bytes_written: result.total_bytes,
            batches_written: result.batches_written,
            checkpoint_count: result.checkpoint_count,
            records_failed: result.total_failed,
            perf: Some(WritePerf {
                connect_secs,
                flush_secs: result.flush_secs,
                commit_secs: result.commit_secs,
                arrow_decode_secs: result.arrow_decode_secs,
            }),
        })
    })
}

/// Configuration for a write session, bundling stream-level settings.
pub struct SessionConfig {
    pub stream_name: String,
    pub write_mode: Option<WriteMode>,
    pub load_method: String,
    pub schema_policy: SchemaEvolutionPolicy,
    pub on_data_error: DataErrorPolicy,
    pub checkpoint: CheckpointConfig,
}

/// Checkpoint threshold configuration extracted from StreamLimits.
pub struct CheckpointConfig {
    pub interval_bytes: u64,
    pub interval_rows: u64,
    pub interval_seconds: u64,
}

/// Result of a completed write session, used to build WriteSummary.
pub struct SessionResult {
    pub total_rows: u64,
    pub total_bytes: u64,
    pub total_failed: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
}

/// Manages the full lifecycle of writing a single stream to PostgreSQL.
///
/// Encapsulates Replace-mode staging, watermark-based resume, transaction
/// management, checkpoint emission, and load-method dispatch. The caller
/// only needs: `begin()` -> loop `process_batch()` -> `commit()`.
pub struct WriteSession<'a> {
    client: &'a Client,
    target_schema: &'a str,
    /// Original stream name (used for watermarks and checkpoints).
    stream_name: String,
    /// Effective stream name (differs from stream_name in Replace mode).
    effective_stream: String,
    effective_write_mode: Option<WriteMode>,
    load_method: String,
    schema_policy: SchemaEvolutionPolicy,
    on_data_error: DataErrorPolicy,
    checkpoint_config: CheckpointConfig,

    // Replace mode state
    is_replace: bool,

    // Watermark resume state
    watermark_records: u64,
    cumulative_records: u64,

    // Transaction / checkpoint tracking
    flush_start: Instant,
    total_rows: u64,
    total_bytes: u64,
    total_failed: u64,
    batches_written: u64,
    checkpoint_count: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
    last_checkpoint_time: Instant,

    // Arrow decode timing
    arrow_decode_nanos: u64,

    // Table creation tracking
    created_tables: HashSet<String>,

    /// Columns present in Arrow but excluded from writes (new_column: Ignore policy).
    ignored_columns: HashSet<String>,
    /// Columns with type mismatches written as NULL (type_change: Null policy).
    type_null_columns: HashSet<String>,
}

impl<'a> WriteSession<'a> {
    /// Open a write session: ensure watermarks table, set up Replace-mode
    /// staging, query watermark for resume, and BEGIN the first transaction.
    pub async fn begin(
        client: &'a Client,
        target_schema: &'a str,
        config: SessionConfig,
    ) -> Result<WriteSession<'a>, String> {
        let stream_name = &config.stream_name;
        let write_mode = config.write_mode;
        // Ensure watermarks table exists (non-fatal if it fails)
        if let Err(e) = ensure_watermarks_table(client, target_schema).await {
            host_ffi::log(
                1,
                &format!("dest-postgres: watermarks table creation failed (non-fatal): {}", e),
            );
        }

        // Replace mode: route writes to a staging table
        let is_replace = matches!(write_mode, Some(WriteMode::Replace));
        let (effective_stream, effective_write_mode) = if is_replace {
            let staging_name = prepare_staging(client, target_schema, stream_name).await?;
            host_ffi::log(
                2,
                &format!(
                    "dest-postgres: Replace mode — writing to staging table '{}'",
                    staging_name
                ),
            );
            (staging_name, Some(WriteMode::Append))
        } else {
            (stream_name.to_string(), write_mode.clone())
        };

        // Exactly-once: query watermark for resume position
        let watermark_records = if !is_replace {
            match get_watermark(client, target_schema, stream_name).await {
                Ok(w) => {
                    if w > 0 {
                        host_ffi::log(
                            2,
                            &format!(
                                "dest-postgres: resuming from watermark — {} records already committed for stream '{}'",
                                w, stream_name
                            ),
                        );
                    }
                    w
                }
                Err(e) => {
                    host_ffi::log(
                        1,
                        &format!("dest-postgres: watermark query failed (starting fresh): {}", e),
                    );
                    0
                }
            }
        } else {
            0
        };

        // BEGIN first transaction
        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("BEGIN failed: {}", e))?;

        let now = Instant::now();
        Ok(WriteSession {
            client,
            target_schema,
            stream_name: config.stream_name,
            effective_stream,
            effective_write_mode,
            load_method: config.load_method,
            schema_policy: config.schema_policy,
            on_data_error: config.on_data_error,
            checkpoint_config: config.checkpoint,
            is_replace,
            watermark_records,
            cumulative_records: 0,
            flush_start: now,
            total_rows: 0,
            total_bytes: 0,
            total_failed: 0,
            batches_written: 0,
            checkpoint_count: 0,
            bytes_since_commit: 0,
            rows_since_commit: 0,
            last_checkpoint_time: now,
            arrow_decode_nanos: 0,
            created_tables: HashSet::new(),
            ignored_columns: HashSet::new(),
            type_null_columns: HashSet::new(),
        })
    }

    /// Process a single IPC batch: skip if already committed (watermark),
    /// write via INSERT or COPY, accumulate stats, and checkpoint if thresholds
    /// are reached.
    pub async fn process_batch(&mut self, ipc_bytes: &[u8]) -> Result<(), String> {
        let n = ipc_bytes.len();

        // Exactly-once: skip already-committed batches
        if self.watermark_records > 0 && self.cumulative_records < self.watermark_records {
            let batch_rows = count_ipc_rows(ipc_bytes)?;
            self.cumulative_records += batch_rows;
            if self.cumulative_records <= self.watermark_records {
                host_ffi::log(
                    3,
                    &format!(
                        "dest-postgres: skipping batch ({}/{} records already committed)",
                        self.cumulative_records, self.watermark_records
                    ),
                );
                return Ok(());
            }
            host_ffi::log(
                2,
                &format!(
                    "dest-postgres: resuming writes at cumulative record {}",
                    self.cumulative_records
                ),
            );
        }

        // Write the batch
        let mut write_ctx = WriteContext {
            client: self.client,
            target_schema: self.target_schema,
            stream_name: &self.effective_stream,
            created_tables: &mut self.created_tables,
            write_mode: self.effective_write_mode.as_ref(),
            schema_policy: Some(&self.schema_policy),
            on_data_error: self.on_data_error,
            load_method: &self.load_method,
            ignored_columns: &mut self.ignored_columns,
            type_null_columns: &mut self.type_null_columns,
        };

        let (result, decode_nanos) = write_batch(&mut write_ctx, ipc_bytes).await?;

        // Accumulate stats
        self.arrow_decode_nanos += decode_nanos;
        self.total_rows += result.rows_written;
        self.total_failed += result.rows_failed;
        self.total_bytes += n as u64;
        self.bytes_since_commit += n as u64;
        self.rows_since_commit += result.rows_written;
        self.batches_written += 1;

        // Emit real-time metrics per spec § Standard Metrics
        let _ = host_ffi::metric("dest-postgres", &self.stream_name, &Metric {
            name: "records_written".to_string(),
            value: MetricValue::Counter(self.total_rows),
            labels: vec![],
        });
        let _ = host_ffi::metric("dest-postgres", &self.stream_name, &Metric {
            name: "bytes_written".to_string(),
            value: MetricValue::Counter(self.total_bytes),
            labels: vec![],
        });

        // Checkpoint if any threshold is reached
        self.maybe_checkpoint().await?;

        Ok(())
    }

    /// Check checkpoint thresholds and commit + re-open transaction if needed.
    async fn maybe_checkpoint(&mut self) -> Result<(), String> {
        let cfg = &self.checkpoint_config;
        let should_checkpoint = (cfg.interval_bytes > 0
            && self.bytes_since_commit >= cfg.interval_bytes)
            || (cfg.interval_rows > 0 && self.rows_since_commit >= cfg.interval_rows)
            || (cfg.interval_seconds > 0
                && self.last_checkpoint_time.elapsed().as_secs() >= cfg.interval_seconds);

        if !should_checkpoint {
            return Ok(());
        }

        // Update watermark before commit (atomic with data in same transaction)
        set_watermark(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.total_rows,
            self.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {}", e))?;

        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("Checkpoint COMMIT failed: {}", e))?;

        // Emit checkpoint so host can track progress
        let cp = rapidbyte_sdk::protocol::Checkpoint {
            id: self.checkpoint_count + 1,
            kind: rapidbyte_sdk::protocol::CheckpointKind::Dest,
            stream: self.stream_name.clone(),
            cursor_field: None,
            cursor_value: None,
            records_processed: self.total_rows,
            bytes_processed: self.total_bytes,
        };
        let _ = host_ffi::checkpoint("dest-postgres", &self.stream_name, &cp);
        self.checkpoint_count += 1;
        self.bytes_since_commit = 0;
        self.rows_since_commit = 0;
        self.last_checkpoint_time = Instant::now();

        host_ffi::log(
            3,
            &format!(
                "dest-postgres: checkpoint {} — committed {} rows, {} bytes so far",
                self.checkpoint_count, self.total_rows, self.total_bytes
            ),
        );

        // Begin new transaction for next chunk
        self.client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("Post-checkpoint BEGIN failed: {}", e))?;

        Ok(())
    }

    /// Finalize the session: set final watermark, COMMIT, emit final
    /// checkpoint, and perform Replace-mode swap if needed.
    ///
    /// Returns `SessionResult` with all accumulated stats.
    pub async fn commit(mut self) -> Result<SessionResult, String> {
        let flush_secs = self.flush_start.elapsed().as_secs_f64();

        // Update watermark before final commit (atomic with data)
        set_watermark(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.total_rows,
            self.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {}", e))?;

        let commit_start = Instant::now();
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {}", e))?;
        let commit_secs = commit_start.elapsed().as_secs_f64();

        // Final checkpoint — always emit so the host can correlate
        // source and dest checkpoints for cursor advancement.
        {
            let cp = rapidbyte_sdk::protocol::Checkpoint {
                id: self.checkpoint_count + 1,
                kind: rapidbyte_sdk::protocol::CheckpointKind::Dest,
                stream: self.stream_name.clone(),
                cursor_field: None,
                cursor_value: None,
                records_processed: self.total_rows,
                bytes_processed: self.total_bytes,
            };
            let _ = host_ffi::checkpoint("dest-postgres", &self.stream_name, &cp);
            self.checkpoint_count += 1;
        }

        // Replace mode: atomically swap staging table into target position
        if self.is_replace {
            swap_staging_table(self.client, self.target_schema, &self.stream_name).await?;
        }

        // Clear watermark after successful commit — the watermark is only
        // useful for crash recovery within a single run. Between runs
        // (especially incremental), a stale watermark would cause the next
        // run's batches to be skipped.
        let _ = clear_watermark(self.client, self.target_schema, &self.stream_name).await;

        host_ffi::log(
            2,
            &format!(
                "dest-postgres: flushed {} rows in {} batches via {} (flush={:.3}s commit={:.3}s)",
                self.total_rows, self.batches_written, self.load_method, flush_secs, commit_secs
            ),
        );

        Ok(SessionResult {
            total_rows: self.total_rows,
            total_bytes: self.total_bytes,
            total_failed: self.total_failed,
            batches_written: self.batches_written,
            checkpoint_count: self.checkpoint_count,
            flush_secs,
            commit_secs,
            arrow_decode_secs: self.arrow_decode_nanos as f64 / 1e9,
        })
    }

    /// Abort the session: ROLLBACK the current transaction.
    pub async fn rollback(self) {
        let _ = self.client.execute("ROLLBACK", &[]).await;
    }
}

/// Ensure the __rb_watermarks metadata table exists.
///
/// Also creates the target schema if it doesn't exist yet, since the watermark
/// table must live in the same schema as the data tables.
async fn ensure_watermarks_table(
    client: &Client,
    target_schema: &str,
) -> Result<(), String> {
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;

    let qualified = format!("\"{}\".__rb_watermarks", target_schema);
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            stream_name TEXT PRIMARY KEY,
            records_committed BIGINT NOT NULL DEFAULT 0,
            bytes_committed BIGINT NOT NULL DEFAULT 0,
            committed_at TIMESTAMP NOT NULL DEFAULT NOW()
        )",
        qualified
    );
    client
        .execute(&ddl, &[])
        .await
        .map_err(|e| format!("Failed to create watermarks table: {}", e))?;
    Ok(())
}

/// Get the watermark (records committed) for a stream. Returns 0 if none.
async fn get_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<u64, String> {
    let qualified = format!("\"{}\".__rb_watermarks", target_schema);
    let sql = format!(
        "SELECT records_committed FROM {} WHERE stream_name = $1",
        qualified
    );
    match client.query_opt(&sql, &[&stream_name]).await {
        Ok(Some(row)) => {
            let val: i64 = row.get(0);
            Ok(val as u64)
        }
        Ok(None) => Ok(0),
        Err(e) => Err(format!("Failed to get watermark: {}", e)),
    }
}

/// Update the watermark for a stream. Called inside the same transaction as the data COMMIT.
async fn set_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    records_committed: u64,
    bytes_committed: u64,
) -> Result<(), String> {
    let qualified = format!("\"{}\".__rb_watermarks", target_schema);
    let sql = format!(
        "INSERT INTO {} (stream_name, records_committed, bytes_committed, committed_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (stream_name)
         DO UPDATE SET records_committed = $2, bytes_committed = $3, committed_at = NOW()",
        qualified
    );
    client
        .execute(
            &sql,
            &[
                &stream_name,
                &(records_committed as i64),
                &(bytes_committed as i64),
            ],
        )
        .await
        .map_err(|e| format!("Failed to set watermark: {}", e))?;
    Ok(())
}

/// Count the number of rows in an Arrow IPC byte buffer without writing them.
fn count_ipc_rows(ipc_bytes: &[u8]) -> Result<u64, String> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("IPC decode failed: {}", e))?;
    let mut total = 0u64;
    for batch in reader {
        let batch = batch.map_err(|e| format!("IPC batch read failed: {}", e))?;
        total += batch.num_rows() as u64;
    }
    Ok(total)
}

/// Clear the watermark for a stream (used when Replace mode completes its swap).
async fn clear_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let qualified = format!("\"{}\".__rb_watermarks", target_schema);
    let sql = format!(
        "DELETE FROM {} WHERE stream_name = $1",
        qualified
    );
    client
        .execute(&sql, &[&stream_name])
        .await
        .map_err(|e| format!("Failed to clear watermark: {}", e))?;
    Ok(())
}
