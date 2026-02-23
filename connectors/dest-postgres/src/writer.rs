//! Stream write session lifecycle for destination `PostgreSQL` connector.
//!
//! Owns connection/session orchestration around batch writes, checkpoints,
//! watermark-based resume, and Replace-mode staging swap.

use std::sync::Arc;
use std::time::Instant;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::ddl::{prepare_staging, swap_staging_table};
use crate::decode;

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
                bytes: stream.limits.checkpoint_interval_bytes,
                rows: stream.limits.checkpoint_interval_rows,
                seconds: stream.limits.checkpoint_interval_seconds,
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
                loop_error = Some(format!("next_batch failed: {e}"));
                break;
            }
        }
    }

    if let Some(err) = loop_error {
        session.rollback().await;
        return Err(ConnectorError::transient_db("WRITE_FAILED", err)
            .with_commit_state(CommitState::BeforeCommit));
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
        records_failed: 0,
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

/// Checkpoint threshold configuration extracted from `StreamLimits`.
pub struct CheckpointConfig {
    pub bytes: u64,
    pub rows: u64,
    pub seconds: u64,
}

/// Result of a completed write session, used to build `WriteSummary`.
pub struct SessionResult {
    pub total_rows: u64,
    pub total_bytes: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    pub flush_secs: f64,
    pub commit_secs: f64,
}

struct WriteStats {
    total_rows: u64,
    total_bytes: u64,
    batches_written: u64,
    checkpoint_count: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
}

/// Manages lifecycle of writing a single stream to `PostgreSQL`.
pub struct WriteSession<'a> {
    ctx: &'a Context,
    client: &'a Client,
    target_schema: &'a str,

    // Stream identity
    stream_name: String,
    effective_stream: String,
    qualified_table: String,
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
    schema_state: crate::ddl::SchemaState,
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

        if let Err(e) = crate::watermark::ensure_table(client, target_schema).await {
            ctx.log(
                LogLevel::Warn,
                &format!(
                    "dest-postgres: watermarks table creation failed (non-fatal): {e}"
                ),
            );
        }

        let is_replace = matches!(write_mode, Some(WriteMode::Replace));
        let (effective_stream, effective_write_mode) = if is_replace {
            let staging_name = prepare_staging(ctx, client, target_schema, stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
            ctx.log(
                LogLevel::Info,
                &format!(
                    "dest-postgres: Replace mode — writing to staging table '{staging_name}'"
                ),
            );
            (staging_name, Some(WriteMode::Append))
        } else {
            (stream_name.to_owned(), write_mode)
        };

        let watermark_records = if is_replace {
            0
        } else {
            match crate::watermark::get(client, target_schema, stream_name).await {
                Ok(w) => {
                    if w > 0 {
                        ctx.log(
                            LogLevel::Info,
                            &format!(
                                "dest-postgres: resuming from watermark — {w} records already committed for stream '{stream_name}'"
                            ),
                        );
                    }
                    w
                }
                Err(e) => {
                    ctx.log(
                        LogLevel::Warn,
                        &format!(
                            "dest-postgres: watermark query failed (starting fresh): {e}"
                        ),
                    );
                    0
                }
            }
        };

        let qualified_table = decode::qualified_name(target_schema, &effective_stream);

        client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| format!("BEGIN failed: {e}"))?;

        let now = Instant::now();
        Ok(WriteSession {
            ctx,
            client,
            target_schema,
            stream_name: config.stream_name,
            effective_stream,
            qualified_table,
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
                batches_written: 0,
                checkpoint_count: 0,
                bytes_since_commit: 0,
                rows_since_commit: 0,
            },
            schema_state: crate::ddl::SchemaState::new(),
        })
    }

    /// Process a decoded Arrow batch.
    pub async fn process_batch(
        &mut self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<(), String> {
        let n: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();

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

        // Ensure DDL (hoisted from insert/copy paths)
        self.schema_state
            .ensure_table(
                self.ctx,
                self.client,
                self.target_schema,
                &self.effective_stream,
                self.effective_write_mode.as_ref(),
                Some(&self.schema_policy),
                schema,
            )
            .await?;

        // Pre-compute column info
        let active_cols = decode::active_column_indices(schema, &self.schema_state.ignored_columns);
        if active_cols.is_empty() {
            self.ctx.log(
                LogLevel::Warn,
                "dest-postgres: all columns ignored, skipping batch",
            );
            return Ok(());
        }
        let type_null_flags =
            decode::type_null_flags(&active_cols, schema, &self.schema_state.type_null_columns);

        let target = decode::WriteTarget {
            table: &self.qualified_table,
            active_cols: &active_cols,
            schema,
            type_null_flags: &type_null_flags,
        };

        // Dispatch to write path
        let use_copy = self.load_method == LoadMethod::Copy
            && !matches!(self.effective_write_mode, Some(WriteMode::Upsert { .. }));

        let rows_written = if use_copy {
            crate::copy::write(self.ctx, self.client, &target, batches, self.copy_flush_bytes)
                .await?
        } else {
            let upsert_clause = decode::build_upsert_clause(
                self.effective_write_mode.as_ref(),
                schema,
                &active_cols,
            );
            crate::insert::write(self.ctx, self.client, &target, batches, upsert_clause.as_deref())
                .await?
        };

        self.stats.total_rows += rows_written;
        self.stats.total_bytes += n as u64;
        self.stats.bytes_since_commit += n as u64;
        self.stats.rows_since_commit += rows_written;
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

    /// Build a checkpoint struct from the current session state.
    fn build_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            id: self.stats.checkpoint_count + 1,
            kind: CheckpointKind::Dest,
            stream: self.stream_name.clone(),
            cursor_field: None,
            cursor_value: None,
            records_processed: self.stats.total_rows,
            bytes_processed: self.stats.total_bytes,
        }
    }

    /// Commit and reopen transaction when checkpoint thresholds are reached.
    async fn maybe_checkpoint(&mut self) -> Result<(), String> {
        let cfg = &self.checkpoint_config;
        let should_checkpoint = (cfg.bytes > 0
            && self.stats.bytes_since_commit >= cfg.bytes)
            || (cfg.rows > 0 && self.stats.rows_since_commit >= cfg.rows)
            || (cfg.seconds > 0
                && self.last_checkpoint_time.elapsed().as_secs() >= cfg.seconds);

        if !should_checkpoint {
            return Ok(());
        }

        crate::watermark::set(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.stats.total_rows,
            self.stats.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {e}"))?;

        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("Checkpoint COMMIT failed: {e}"))?;

        let _ = self.ctx.checkpoint(&self.build_checkpoint());
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
            .map_err(|e| format!("Post-checkpoint BEGIN failed: {e}"))?;

        Ok(())
    }

    /// Finalize the session.
    pub async fn commit(mut self) -> Result<SessionResult, String> {
        let flush_secs = self.flush_start.elapsed().as_secs_f64();

        crate::watermark::set(
            self.client,
            self.target_schema,
            &self.stream_name,
            self.stats.total_rows,
            self.stats.total_bytes,
        )
        .await
        .map_err(|e| format!("Watermark update failed: {e}"))?;

        let commit_start = Instant::now();
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {e}"))?;
        let commit_secs = commit_start.elapsed().as_secs_f64();

        let _ = self.ctx.checkpoint(&self.build_checkpoint());
        self.stats.checkpoint_count += 1;

        if self.is_replace {
            swap_staging_table(self.ctx, self.client, self.target_schema, &self.stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
        }

        let _ = crate::watermark::clear(self.client, self.target_schema, &self.stream_name).await;

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
