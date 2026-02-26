//! Stream write session lifecycle for destination `PostgreSQL` connector.
//!
//! Owns connection/session orchestration around batch writes, checkpoints,
//! watermark-based resume, and Replace-mode staging swap.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::catalog::SchemaHint;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::ddl::{prepare_staging, swap_staging_table};
use crate::decode;

const COPY_FLUSH_1MB: usize = 1024 * 1024;
const COPY_FLUSH_4MB: usize = 4 * 1024 * 1024;
const COPY_FLUSH_16MB: usize = 16 * 1024 * 1024;

fn bench_profile_copy_flush_bytes() -> Option<usize> {
    match std::env::var("BENCH_PROFILE") {
        Ok(value) if value.eq_ignore_ascii_case("small") => Some(COPY_FLUSH_1MB),
        Ok(value) if value.eq_ignore_ascii_case("medium") => Some(COPY_FLUSH_4MB),
        Ok(value) if value.eq_ignore_ascii_case("large") => Some(COPY_FLUSH_16MB),
        _ => None,
    }
}

fn adaptive_copy_flush_bytes(
    configured: Option<usize>,
    profile_default: Option<usize>,
    avg_row_bytes: Option<usize>,
) -> usize {
    if let Some(bytes) = configured {
        return bytes;
    }

    if let Some(bytes) = profile_default {
        return bytes;
    }

    match avg_row_bytes {
        Some(bytes) if bytes >= 64 * 1024 => COPY_FLUSH_16MB,
        Some(bytes) if bytes >= 8 * 1024 => COPY_FLUSH_4MB,
        _ => COPY_FLUSH_1MB,
    }
}

fn emit_write_perf_metrics(ctx: &Context, perf: &WritePerf) {
    let gauges = [
        ("dest_connect_secs", perf.connect_secs),
        ("dest_flush_secs", perf.flush_secs),
        ("dest_commit_secs", perf.commit_secs),
        ("dest_arrow_decode_secs", perf.arrow_decode_secs),
    ];

    for (name, value) in gauges {
        let _ = ctx.metric(&Metric {
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            labels: vec![],
        });
    }
}

fn preflight_schema_from_hint(schema_hint: &SchemaHint) -> Option<Arc<Schema>> {
    match schema_hint {
        SchemaHint::Columns(columns) => {
            if columns.is_empty() {
                None
            } else {
                Some(build_arrow_schema(columns))
            }
        }
        SchemaHint::ArrowIpc(ipc_bytes) => decode_ipc(ipc_bytes).ok().map(|(schema, _)| schema),
        _ => None,
    }
}

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

    let setup = prepare_stream_once(
        &config.schema,
        &stream.stream_name,
        stream.write_mode.clone(),
        &stream.schema,
        stream.policies.schema_evolution,
        CheckpointConfig {
            bytes: stream.limits.checkpoint_interval_bytes,
            rows: stream.limits.checkpoint_interval_rows,
            seconds: stream.limits.checkpoint_interval_seconds,
        },
        config.copy_flush_bytes,
        config.load_method,
    )
    .map_err(|e| ConnectorError::config("INVALID_STREAM_SETUP", e))?;
    let skip_mutable_setup = stream.partition_count.unwrap_or(1) > 1 && !setup.is_replace;
    let setup = if skip_mutable_setup {
        setup
    } else {
        async_prepare_stream_once(ctx, &client, &stream.schema, setup)
            .await
            .map_err(|e| ConnectorError::config("INVALID_STREAM_SETUP", e))?
    };

    let mut session = WriteSession::begin(ctx, &client, &config.schema, setup)
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

    let perf = WritePerf {
        connect_secs,
        flush_secs: result.flush_secs,
        commit_secs: result.commit_secs,
        arrow_decode_secs: 0.0,
    };
    emit_write_perf_metrics(ctx, &perf);

    Ok(WriteSummary {
        records_written: result.total_rows,
        bytes_written: result.total_bytes,
        batches_written: result.batches_written,
        checkpoint_count: result.checkpoint_count,
        records_failed: 0,
        perf: Some(perf),
    })
}

/// Configuration for a write session, bundling stream-level settings.
pub type SessionConfig = WriteContract;

/// Immutable setup output for destination worker execution.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WriteContract {
    pub target_schema: String,
    pub stream_name: String,
    pub effective_stream: String,
    pub qualified_table: String,
    pub effective_write_mode: Option<WriteMode>,
    pub schema_policy: SchemaEvolutionPolicy,
    pub checkpoint: CheckpointConfig,
    pub copy_flush_bytes: Option<usize>,
    pub load_method: LoadMethod,
    pub profile_copy_flush_bytes: Option<usize>,
    pub is_replace: bool,
    pub watermark_records: u64,
    pub ignored_columns: HashSet<String>,
    pub type_null_columns: HashSet<String>,
}

/// Build a destination write contract for a stream.
fn prepare_stream_once(
    target_schema: &str,
    stream_name: &str,
    write_mode: Option<WriteMode>,
    _schema_hint: &SchemaHint,
    schema_policy: SchemaEvolutionPolicy,
    checkpoint: CheckpointConfig,
    copy_flush_bytes: Option<usize>,
    load_method: LoadMethod,
) -> Result<WriteContract, String> {
    if stream_name.trim().is_empty() {
        return Err("stream name must not be empty".to_string());
    }

    let is_replace = matches!(write_mode, Some(WriteMode::Replace));
    let effective_write_mode = if is_replace {
        Some(WriteMode::Append)
    } else {
        write_mode
    };

    Ok(WriteContract {
        target_schema: target_schema.to_string(),
        stream_name: stream_name.to_string(),
        effective_stream: stream_name.to_string(),
        qualified_table: decode::qualified_name(target_schema, stream_name),
        effective_write_mode,
        schema_policy,
        checkpoint,
        copy_flush_bytes,
        load_method,
        profile_copy_flush_bytes: bench_profile_copy_flush_bytes(),
        is_replace,
        watermark_records: 0,
        ignored_columns: HashSet::new(),
        type_null_columns: HashSet::new(),
    })
}

async fn async_prepare_stream_once(
    ctx: &Context,
    client: &Client,
    schema_hint: &SchemaHint,
    mut contract: WriteContract,
) -> Result<WriteContract, String> {
    if let Err(e) = crate::watermark::ensure_table(client, &contract.target_schema).await {
        ctx.log(
            LogLevel::Warn,
            &format!("dest-postgres: watermarks table creation failed (non-fatal): {e}"),
        );
    }

    if contract.is_replace {
        let staging_name =
            prepare_staging(ctx, client, &contract.target_schema, &contract.stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
        ctx.log(
            LogLevel::Info,
            &format!("dest-postgres: Replace mode — writing to staging table '{staging_name}'"),
        );
        contract.effective_stream = staging_name;
        contract.effective_write_mode = Some(WriteMode::Append);
    }

    let mut schema_state = crate::ddl::SchemaState::new();
    if let Some(schema) = preflight_schema_from_hint(schema_hint) {
        schema_state
            .ensure_table(
                ctx,
                client,
                &contract.target_schema,
                &contract.effective_stream,
                contract.effective_write_mode.as_ref(),
                Some(&contract.schema_policy),
                &schema,
            )
            .await?;
    }

    contract.qualified_table =
        decode::qualified_name(&contract.target_schema, &contract.effective_stream);
    contract.ignored_columns = schema_state.ignored_columns;
    contract.type_null_columns = schema_state.type_null_columns;

    contract.watermark_records = if contract.is_replace {
        0
    } else {
        match crate::watermark::get(client, &contract.target_schema, &contract.stream_name).await {
            Ok(w) => {
                if w > 0 {
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: resuming from watermark — {w} records already committed for stream '{}'",
                            contract.stream_name
                        ),
                    );
                }
                w
            }
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("dest-postgres: watermark query failed (starting fresh): {e}"),
                );
                0
            }
        }
    };

    Ok(contract)
}

/// Checkpoint threshold configuration extracted from `StreamLimits`.
#[derive(Debug, Clone)]
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
    qualified_table: String,
    effective_write_mode: Option<WriteMode>,

    // Config
    load_method: LoadMethod,
    checkpoint_config: CheckpointConfig,
    copy_flush_bytes: Option<usize>,
    profile_copy_flush_bytes: Option<usize>,

    // Replace mode
    is_replace: bool,

    // Watermark resume
    watermark_records: u64,
    cumulative_records: u64,

    // Timing + stats
    flush_start: Instant,
    last_checkpoint_time: Instant,
    stats: WriteStats,

    // Schema tracking resolved at setup time
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
            qualified_table: config.qualified_table,
            effective_write_mode: config.effective_write_mode,
            load_method: config.load_method,
            checkpoint_config: config.checkpoint,
            copy_flush_bytes: config.copy_flush_bytes,
            profile_copy_flush_bytes: config.profile_copy_flush_bytes,
            is_replace: config.is_replace,
            watermark_records: config.watermark_records,
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
            ignored_columns: config.ignored_columns,
            type_null_columns: config.type_null_columns,
        })
    }

    /// Process a decoded Arrow batch.
    pub async fn process_batch(
        &mut self,
        schema: &Arc<Schema>,
        batches: &[RecordBatch],
    ) -> Result<(), String> {
        let n: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

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

        // Pre-compute column info
        let active_cols = decode::active_column_indices(schema, &self.ignored_columns);
        if active_cols.is_empty() {
            self.ctx.log(
                LogLevel::Warn,
                "dest-postgres: all columns ignored, skipping batch",
            );
            return Ok(());
        }
        let type_null_flags =
            decode::type_null_flags(&active_cols, schema, &self.type_null_columns);

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
            if self.copy_flush_bytes.is_none() {
                let avg_row_bytes = (batch_rows > 0).then(|| n / batch_rows as usize);
                let chosen =
                    adaptive_copy_flush_bytes(None, self.profile_copy_flush_bytes, avg_row_bytes);
                self.copy_flush_bytes = Some(chosen);
                self.ctx.log(
                    LogLevel::Debug,
                    &format!(
                        "dest-postgres: adaptive copy_flush_bytes={} (profile_default={} avg_row_bytes={})",
                        chosen,
                        self.profile_copy_flush_bytes
                            .map_or_else(|| "none".to_string(), |v| v.to_string()),
                        avg_row_bytes.unwrap_or_default()
                    ),
                );
            }

            crate::copy::write(
                self.ctx,
                self.client,
                &target,
                batches,
                self.copy_flush_bytes,
            )
            .await?
        } else {
            let upsert_clause = decode::build_upsert_clause(
                self.effective_write_mode.as_ref(),
                schema,
                &active_cols,
            );
            crate::insert::write(
                self.ctx,
                self.client,
                &target,
                batches,
                upsert_clause.as_deref(),
            )
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
        let should_checkpoint = (cfg.bytes > 0 && self.stats.bytes_since_commit >= cfg.bytes)
            || (cfg.rows > 0 && self.stats.rows_since_commit >= cfg.rows)
            || (cfg.seconds > 0 && self.last_checkpoint_time.elapsed().as_secs() >= cfg.seconds);

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

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::catalog::{ColumnSchema, SchemaHint};
    use rapidbyte_sdk::prelude::ArrowDataType;

    #[test]
    fn write_contract_clone_preserves_fields() {
        let contract = WriteContract {
            target_schema: "raw".to_string(),
            stream_name: "users".to_string(),
            effective_stream: "users".to_string(),
            qualified_table: "raw.users".to_string(),
            effective_write_mode: Some(WriteMode::Append),
            schema_policy: SchemaEvolutionPolicy::default(),
            checkpoint: CheckpointConfig {
                bytes: 1024,
                rows: 100,
                seconds: 30,
            },
            copy_flush_bytes: Some(4 * 1024 * 1024),
            load_method: LoadMethod::Copy,
            profile_copy_flush_bytes: Some(COPY_FLUSH_4MB),
            is_replace: false,
            watermark_records: 0,
            ignored_columns: std::collections::HashSet::new(),
            type_null_columns: std::collections::HashSet::new(),
        };

        let cloned = contract.clone();
        assert_eq!(cloned.stream_name, "users");
        assert_eq!(cloned.qualified_table, "raw.users");
        assert_eq!(cloned.copy_flush_bytes, Some(4 * 1024 * 1024));
    }

    #[test]
    fn prepare_stream_once_requires_non_empty_stream_name() {
        let result = prepare_stream_once(
            "raw",
            "",
            Some(WriteMode::Append),
            &SchemaHint::Columns(Vec::new()),
            SchemaEvolutionPolicy::default(),
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            LoadMethod::Copy,
        );

        assert!(result.is_err());
        assert!(result
            .expect_err("empty stream must fail")
            .contains("stream name"));
    }

    #[test]
    fn adaptive_flush_uses_user_override_when_set() {
        let chosen =
            adaptive_copy_flush_bytes(Some(2 * 1024 * 1024), Some(COPY_FLUSH_4MB), Some(80_000));
        assert_eq!(chosen, 2 * 1024 * 1024);
    }

    #[test]
    fn adaptive_flush_prefers_profile_default_when_set() {
        let chosen = adaptive_copy_flush_bytes(None, Some(COPY_FLUSH_4MB), Some(400));
        assert_eq!(chosen, COPY_FLUSH_4MB);
    }

    #[test]
    fn adaptive_flush_chooses_small_bucket() {
        let chosen = adaptive_copy_flush_bytes(None, None, Some(400));
        assert_eq!(chosen, 1024 * 1024);
    }

    #[test]
    fn adaptive_flush_chooses_large_row_bucket() {
        let chosen = adaptive_copy_flush_bytes(None, None, Some(70 * 1024));
        assert_eq!(chosen, 16 * 1024 * 1024);
    }

    #[test]
    fn preflight_schema_from_columns_builds_arrow_schema() {
        let hint = SchemaHint::Columns(vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: ArrowDataType::Utf8,
                nullable: true,
            },
        ]);

        let schema = preflight_schema_from_hint(&hint).expect("schema should be built");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
