//! Stream write session lifecycle for destination PostgreSQL connector.
//!
//! Owns connection/session orchestration around batch writes, checkpoints,
//! watermark-based resume, and Replace-mode staging swap.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::SchemaEvolutionPolicy;

use crate::batch::{write_batch, WriteContext};
use crate::config::LoadMethod;
use crate::ddl::{prepare_staging, swap_staging_table};

/// Entry point for writing a single stream.
pub async fn write_stream(
    config: &crate::config::Config,
    ctx: &Context,
    stream: &StreamContext,
) -> Result<WriteSummary, ConnectorError> {
    let connect_start = Instant::now();
    let client = crate::client::connect(config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
    let connect_secs = connect_start.elapsed().as_secs_f64();

    let mut session = WriteSession::begin(
        ctx,
        &client,
        &config.schema,
        SessionConfig {
            stream_name: stream.stream_name.clone(),
            write_mode: stream.write_mode.clone(),
            load_method: config.load_method,
            schema_policy: stream.policies.schema_evolution,
            checkpoint: CheckpointConfig {
                interval_bytes: stream.limits.checkpoint_interval_bytes,
                interval_rows: stream.limits.checkpoint_interval_rows,
                interval_seconds: stream.limits.checkpoint_interval_seconds,
            },
            copy_flush_bytes: config.copy_flush_bytes,
        },
    )
    .await
    .map_err(|e| ConnectorError::transient_db("SESSION_BEGIN_FAILED", e))?;

    let mut loop_error: Option<String> = None;

    loop {
        match ctx.next_batch(stream.limits.max_batch_bytes) {
            Ok(None) => break,
            Ok(Some((schema, batches))) => {
                if let Err(e) = session.process_batch(&schema, &batches).await {
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

    if let Some(err) = loop_error {
        session.rollback().await;
        return Err(
            ConnectorError::transient_db("WRITE_FAILED", err).with_commit_state(CommitState::BeforeCommit),
        );
    }

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
            arrow_decode_secs: 0.0,
        }),
    })
}

/// Configuration for a write session, bundling stream-level settings.
pub struct SessionConfig {
    pub stream_name: String,
    pub write_mode: Option<WriteMode>,
    pub load_method: LoadMethod,
    pub schema_policy: SchemaEvolutionPolicy,
    pub checkpoint: CheckpointConfig,
    pub copy_flush_bytes: Option<usize>,
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
}

struct WriteStats {
    total_rows: u64,
    total_bytes: u64,
    total_failed: u64,
    batches_written: u64,
    checkpoint_count: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
}

/// Manages lifecycle of writing a single stream to PostgreSQL.
pub struct WriteSession<'a> {
    ctx: &'a Context,
    client: &'a Client,
    target_schema: &'a str,

    // Stream identity
    stream_name: String,
    effective_stream: String,
    effective_write_mode: Option<WriteMode>,

    // Config
    load_method: LoadMethod,
    schema_policy: SchemaEvolutionPolicy,
    checkpoint_config: CheckpointConfig,
    copy_flush_bytes: Option<usize>,

    // Replace mode
    is_replace: bool,

    // Watermark resume
    watermark_records: u64,
    cumulative_records: u64,

    // Timing + stats
    flush_start: Instant,
    last_checkpoint_time: Instant,
    stats: WriteStats,

    // Schema tracking
    created_tables: HashSet<String>,
    ignored_columns: HashSet<String>,
    type_null_columns: HashSet<String>,
}

impl<'a> WriteSession<'a> {
    /// Open a write session and BEGIN the first transaction.
    pub async fn begin(
        ctx: &'a Context,
        client: &'a Client,
        target_schema: &'a str,
        config: SessionConfig,
    ) -> Result<WriteSession<'a>, String> {
        let stream_name = &config.stream_name;
        let write_mode = config.write_mode;

        if let Err(e) = ensure_watermarks_table(client, target_schema).await {
            ctx.log(
                LogLevel::Warn,
                &format!(
                    "dest-postgres: watermarks table creation failed (non-fatal): {}",
                    e
                ),
            );
        }

        let is_replace = matches!(write_mode, Some(WriteMode::Replace));
        let (effective_stream, effective_write_mode) = if is_replace {
            let staging_name = prepare_staging(ctx, client, target_schema, stream_name)
                .await
                .map_err(|e| format!("{:#}", e))?;
            ctx.log(
                LogLevel::Info,
                &format!(
                    "dest-postgres: Replace mode — writing to staging table '{}'",
                    staging_name
                ),
            );
            (staging_name, Some(WriteMode::Append))
        } else {
            (stream_name.to_string(), write_mode.clone())
        };

        let watermark_records = if !is_replace {
            match get_watermark(client, target_schema, stream_name).await {
                Ok(w) => {
                    if w > 0 {
                        ctx.log(
                            LogLevel::Info,
                            &format!(
                                "dest-postgres: resuming from watermark — {} records already committed for stream '{}'",
                                w, stream_name
                            ),
                        );
                    }
                    w
                }
                Err(e) => {
                    ctx.log(
                        LogLevel::Warn,
                        &format!(
                            "dest-postgres: watermark query failed (starting fresh): {}",
                            e
                        ),
                    );
                    0
                }
            }
        } else {
            0
        };

        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("BEGIN failed: {}", e))?;

        let now = Instant::now();
        Ok(WriteSession {
            ctx,
            client,
            target_schema,
            stream_name: config.stream_name,
            effective_stream,
            effective_write_mode,
            load_method: config.load_method,
            schema_policy: config.schema_policy,
            checkpoint_config: config.checkpoint,
            copy_flush_bytes: config.copy_flush_bytes,
            is_replace,
            watermark_records,
            cumulative_records: 0,
            flush_start: now,
            last_checkpoint_time: now,
            stats: WriteStats {
                total_rows: 0,
                total_bytes: 0,
                total_failed: 0,
                batches_written: 0,
                checkpoint_count: 0,
                bytes_since_commit: 0,
                rows_since_commit: 0,
            },
            created_tables: HashSet::new(),
            ignored_columns: HashSet::new(),
            type_null_columns: HashSet::new(),
        })
    }

    /// Process a decoded Arrow batch.
    pub async fn process_batch(
        &mut self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<(), String> {
        // Calculate byte size from batches
        let n: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        // Watermark resume: skip already-committed batches
        if self.watermark_records > 0 && self.cumulative_records < self.watermark_records {
            let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            self.cumulative_records += batch_rows;
            if self.cumulative_records <= self.watermark_records {
                self.ctx.log(
                    LogLevel::Debug,
                    &format!(
                        "dest-postgres: skipping batch ({}/{} records already committed)",
                        self.cumulative_records, self.watermark_records
                    ),
                );
                return Ok(());
            }
            self.ctx.log(
                LogLevel::Info,
                &format!(
                    "dest-postgres: resuming writes at cumulative record {}",
                    self.cumulative_records
                ),
            );
        }

        let mut write_ctx = WriteContext {
            client: self.client,
            target_schema: self.target_schema,
            stream_name: &self.effective_stream,
            created_tables: &mut self.created_tables,
            write_mode: self.effective_write_mode.as_ref(),
            schema_policy: Some(&self.schema_policy),
            load_method: self.load_method,
            ignored_columns: &mut self.ignored_columns,
            type_null_columns: &mut self.type_null_columns,
            copy_flush_bytes: self.copy_flush_bytes,
        };

        let result = write_batch(self.ctx, &mut write_ctx, schema, batches).await?;

        self.stats.total_rows += result.rows_written;
        self.stats.total_failed += result.rows_failed;
        self.stats.total_bytes += n as u64;
        self.stats.bytes_since_commit += n as u64;
        self.stats.rows_since_commit += result.rows_written;
        self.stats.batches_written += 1;

        let _ = self.ctx.metric(&Metric {
            name: "records_written".to_string(),
            value: MetricValue::Counter(self.stats.total_rows),
            labels: vec![],
        });
        let _ = self.ctx.metric(&Metric {
            name: "bytes_written".to_string(),
            value: MetricValue::Counter(self.stats.total_bytes),
            labels: vec![],
        });

        self.maybe_checkpoint().await?;

        Ok(())
    }

    /// Commit and reopen transaction when checkpoint thresholds are reached.
    async fn maybe_checkpoint(&mut self) -> Result<(), String> {
        let cfg = &self.checkpoint_config;
        let should_checkpoint = (cfg.interval_bytes > 0
            && self.stats.bytes_since_commit >= cfg.interval_bytes)
            || (cfg.interval_rows > 0 && self.stats.rows_since_commit >= cfg.interval_rows)
            || (cfg.interval_seconds > 0
                && self.last_checkpoint_time.elapsed().as_secs() >= cfg.interval_seconds);

        if !should_checkpoint {
            return Ok(());
        }

        set_watermark(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.stats.total_rows,
            self.stats.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {}", e))?;

        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("Checkpoint COMMIT failed: {}", e))?;

        let cp = Checkpoint {
            id: self.stats.checkpoint_count + 1,
            kind: CheckpointKind::Dest,
            stream: self.stream_name.clone(),
            cursor_field: None,
            cursor_value: None,
            records_processed: self.stats.total_rows,
            bytes_processed: self.stats.total_bytes,
        };
        let _ = self.ctx.checkpoint(&cp);
        self.stats.checkpoint_count += 1;
        self.stats.bytes_since_commit = 0;
        self.stats.rows_since_commit = 0;
        self.last_checkpoint_time = Instant::now();

        self.ctx.log(
            LogLevel::Debug,
            &format!(
                "dest-postgres: checkpoint {} — committed {} rows, {} bytes so far",
                self.stats.checkpoint_count, self.stats.total_rows, self.stats.total_bytes
            ),
        );

        self.client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("Post-checkpoint BEGIN failed: {}", e))?;

        Ok(())
    }

    /// Finalize the session.
    pub async fn commit(mut self) -> Result<SessionResult, String> {
        let flush_secs = self.flush_start.elapsed().as_secs_f64();

        set_watermark(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.stats.total_rows,
            self.stats.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {}", e))?;

        let commit_start = Instant::now();
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {}", e))?;
        let commit_secs = commit_start.elapsed().as_secs_f64();

        let cp = Checkpoint {
            id: self.stats.checkpoint_count + 1,
            kind: CheckpointKind::Dest,
            stream: self.stream_name.clone(),
            cursor_field: None,
            cursor_value: None,
            records_processed: self.stats.total_rows,
            bytes_processed: self.stats.total_bytes,
        };
        let _ = self.ctx.checkpoint(&cp);
        self.stats.checkpoint_count += 1;

        if self.is_replace {
            swap_staging_table(self.ctx, self.client, self.target_schema, &self.stream_name)
                .await
                .map_err(|e| format!("{:#}", e))?;
        }

        let _ = clear_watermark(self.client, self.target_schema, &self.stream_name).await;

        self.ctx.log(
            LogLevel::Info,
            &format!(
                "dest-postgres: flushed {} rows in {} batches via {} (flush={:.3}s commit={:.3}s)",
                self.stats.total_rows,
                self.stats.batches_written,
                self.load_method,
                flush_secs,
                commit_secs
            ),
        );

        Ok(SessionResult {
            total_rows: self.stats.total_rows,
            total_bytes: self.stats.total_bytes,
            total_failed: self.stats.total_failed,
            batches_written: self.stats.batches_written,
            checkpoint_count: self.stats.checkpoint_count,
            flush_secs,
            commit_secs,
        })
    }

    /// Abort the session with ROLLBACK.
    pub async fn rollback(self) {
        let _ = self.client.execute("ROLLBACK", &[]).await;
    }
}

/// Ensure the __rb_watermarks metadata table exists.
async fn ensure_watermarks_table(client: &Client, target_schema: &str) -> Result<(), String> {
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_identifier(target_schema));
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;

    let qualified = format!("{}.__rb_watermarks", quote_identifier(target_schema));
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

/// Get watermark (records committed) for a stream. Returns 0 if none.
async fn get_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<u64, String> {
    let qualified = format!("{}.__rb_watermarks", quote_identifier(target_schema));
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

/// Upsert watermark row inside the same transaction as data writes.
async fn set_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    records_committed: u64,
    bytes_committed: u64,
) -> Result<(), String> {
    let qualified = format!("{}.__rb_watermarks", quote_identifier(target_schema));
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

/// Clear watermark for a stream after successful completion.
async fn clear_watermark(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let qualified = format!("{}.__rb_watermarks", quote_identifier(target_schema));
    let sql = format!("DELETE FROM {} WHERE stream_name = $1", qualified);
    client
        .execute(&sql, &[&stream_name])
        .await
        .map_err(|e| format!("Failed to clear watermark: {}", e))?;
    Ok(())
}
