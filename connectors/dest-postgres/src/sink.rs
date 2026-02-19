use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    DataErrorPolicy, Metric, MetricValue, SchemaEvolutionPolicy, WriteMode,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

use bytes::Bytes;
use futures_util::SinkExt;

use crate::ddl::{ensure_table_and_schema, prepare_staging, swap_staging_table};
use crate::format::{downcast_columns, format_copy_value, write_sql_value};

/// Result of a write operation with per-row error tracking.
struct WriteResult {
    rows_written: u64,
    rows_failed: u64,
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

/// Bundled parameters for batch write operations.
///
/// Groups the common arguments needed by `write_batch`
/// to keep function signatures under clippy's argument limit.
struct WriteContext<'a> {
    client: &'a Client,
    target_schema: &'a str,
    stream_name: &'a str,
    created_tables: &'a mut HashSet<String>,
    write_mode: Option<&'a WriteMode>,
    schema_policy: Option<&'a SchemaEvolutionPolicy>,
    on_data_error: DataErrorPolicy,
    load_method: &'a str,
    ignored_columns: &'a mut HashSet<String>,
    type_null_columns: &'a mut HashSet<String>,
}

/// Maximum rows per multi-value INSERT statement.
/// Keeps SQL string size manageable (~350KB for 7 columns).
const INSERT_CHUNK_SIZE: usize = 1000;

/// Buffer size for COPY data before flushing to the sink.
const COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB

/// Decode Arrow IPC bytes into schema and record batches.
fn decode_ipc(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;
    let schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;
    Ok((schema, batches))
}

/// Write an Arrow IPC batch to PostgreSQL.
///
/// Dispatches to COPY or INSERT based on `ctx.load_method`. If the load method
/// is "copy" but the write mode is Upsert, automatically falls back to INSERT
/// (COPY cannot handle ON CONFLICT). On batch failure with Skip/Dlq error
/// policy, falls back to single-row INSERTs.
async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<(WriteResult, u64), String> {
    // Decode Arrow IPC with timing
    let decode_start = Instant::now();
    let (arrow_schema, batches) = decode_ipc(ipc_bytes)?;
    let decode_nanos = decode_start.elapsed().as_nanos() as u64;

    if batches.is_empty() {
        return Ok((WriteResult { rows_written: 0, rows_failed: 0 }, decode_nanos));
    }

    let use_copy = ctx.load_method == "copy"
        && !matches!(ctx.write_mode, Some(WriteMode::Upsert { .. }));

    let (result, method_name) = if use_copy {
        (copy_batch(ctx, &arrow_schema, &batches).await, "COPY")
    } else {
        (insert_batch(ctx, &arrow_schema, &batches).await, "INSERT")
    };

    match result {
        Ok(count) => Ok((WriteResult { rows_written: count, rows_failed: 0 }, decode_nanos)),
        Err(e) => {
            if matches!(ctx.on_data_error, DataErrorPolicy::Fail) {
                return Err(e);
            }
            host_ffi::log(
                1,
                &format!(
                    "dest-postgres: {} failed ({}), falling back to per-row INSERT with skip",
                    method_name, e
                ),
            );
            let wr = write_rows_individually(
                ctx.client, ctx.target_schema, ctx.stream_name, &arrow_schema, &batches, ctx.write_mode,
                ctx.ignored_columns, ctx.type_null_columns,
            ).await?;
            Ok((wr, decode_nanos))
        }
    }
}

/// Internal: write via multi-value INSERT, returning row count.
///
/// Builds batched statements:
///   INSERT INTO t (c1, c2) VALUES (v1, v2), (v3, v4), ...
///
/// Chunks at INSERT_CHUNK_SIZE rows to keep SQL size bounded.
async fn insert_batch(ctx: &mut WriteContext<'_>, arrow_schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<u64, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(
        ctx.client, ctx.target_schema, ctx.stream_name,
        ctx.created_tables, ctx.write_mode, ctx.schema_policy,
        arrow_schema, ctx.ignored_columns, ctx.type_null_columns,
    ).await?;

    // Validate upsert primary_key columns
    if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
        for pk_col in primary_key {
            validate_pg_identifier(pk_col)
                .map_err(|e| format!("Invalid primary key column name '{}': {}", pk_col, e))?;
        }
    }

    // 3. Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ctx.ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping batch");
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    // 4. Insert rows using multi-value INSERT
    let mut total_rows: u64 = 0;

    for batch in batches {
        let num_rows = batch.num_rows();

        // Pre-downcast columns once per batch (not per cell)
        let typed_cols = downcast_columns(batch, &active_cols);
        // Track which active columns are type-nulled
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| ctx.type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        // Pre-compute upsert clause (same for every chunk)
        let upsert_clause: Option<String> =
            if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = active_cols
                    .iter()
                    .map(|&i| arrow_schema.field(i).name())
                    .filter(|name| !primary_key.contains(name))
                    .map(|name| format!("\"{}\" = EXCLUDED.\"{}\"", name, name))
                    .collect();
                if update_cols.is_empty() {
                    Some(format!(" ON CONFLICT ({}) DO NOTHING", pk_cols))
                } else {
                    Some(format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        pk_cols,
                        update_cols.join(", ")
                    ))
                }
            } else {
                None
            };

        // Process in chunks
        for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
            let chunk_end = (chunk_start + INSERT_CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            // Pre-allocate: header + estimated row width (15 bytes/col avg) + separators
            let header = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);
            let estimated_row_width = active_cols.len() * 15; // avg value width per col
            let mut sql = String::with_capacity(
                header.len() + chunk_size * (estimated_row_width + 3), // 3 = "()" + ","
            );
            sql.push_str(&header);

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (pos, typed_col) in typed_cols.iter().enumerate() {
                    if pos > 0 {
                        sql.push_str(", ");
                    }
                    if type_null_flags[pos] {
                        sql.push_str("NULL");
                    } else {
                        write_sql_value(&mut sql, typed_col, row_idx);
                    }
                }
                sql.push(')');
            }

            // Append pre-computed ON CONFLICT clause for upsert mode
            if let Some(ref clause) = upsert_clause {
                sql.push_str(clause);
            }

            ctx.client
                .execute(&sql, &[])
                .await
                .map_err(|e| {
                    format!(
                        "Multi-value INSERT failed for {}, rows {}-{}: {}",
                        ctx.stream_name, chunk_start, chunk_end, e
                    )
                })?;

            total_rows += chunk_size as u64;
        }
    }

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}

/// Internal: write via COPY FROM STDIN, returning row count.
///
/// Streams rows as tab-separated text directly to PostgreSQL's COPY protocol,
/// bypassing SQL parsing for significantly higher throughput.
async fn copy_batch(ctx: &mut WriteContext<'_>, arrow_schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<u64, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(
        ctx.client, ctx.target_schema, ctx.stream_name,
        ctx.created_tables, ctx.write_mode, ctx.schema_policy,
        arrow_schema, ctx.ignored_columns, ctx.type_null_columns,
    ).await?;

    // Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ctx.ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping COPY batch");
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        qualified_table, col_list
    );

    // 4. Start COPY and stream data
    let sink = ctx
        .client
        .copy_in(&copy_stmt)
        .await
        .map_err(|e| format!("COPY start failed: {}", e))?;
    let mut sink = Box::pin(sink);

    let mut total_rows: u64 = 0;
    let mut buf = Vec::with_capacity(COPY_FLUSH_BYTES);

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            for (pos, &col_idx) in active_cols.iter().enumerate() {
                if pos > 0 {
                    buf.push(b'\t');
                }
                if ctx.type_null_columns.contains(arrow_schema.field(col_idx).name()) {
                    buf.extend_from_slice(b"\\N");
                } else {
                    format_copy_value(&mut buf, batch.column(col_idx).as_ref(), row_idx);
                }
            }
            buf.push(b'\n');
            total_rows += 1;

            // Flush periodically to avoid unbounded memory growth
            if buf.len() >= COPY_FLUSH_BYTES {
                sink.send(Bytes::from(std::mem::take(&mut buf)))
                    .await
                    .map_err(|e| format!("COPY send failed: {}", e))?;
                buf = Vec::with_capacity(COPY_FLUSH_BYTES);
            }
        }
    }

    // Flush remaining data
    if !buf.is_empty() {
        sink.send(Bytes::from(buf))
            .await
            .map_err(|e| format!("COPY send failed: {}", e))?;
    }

    let _rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| format!("COPY finish failed: {}", e))?;

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: COPY wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}

/// Insert rows one at a time, skipping any that fail.
async fn write_rows_individually(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    write_mode: Option<&WriteMode>,
    ignored_columns: &HashSet<String>,
    type_null_columns: &HashSet<String>,
) -> Result<WriteResult, String> {
    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);

    // Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping per-row writes");
        return Ok(WriteResult { rows_written: 0, rows_failed: 0 });
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut rows_written = 0u64;
    let mut rows_failed = 0u64;

    for batch in batches {
        let typed_cols = downcast_columns(batch, &active_cols);
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut sql = format!("INSERT INTO {} ({}) VALUES (", qualified_table, col_list);
            for (pos, typed_col) in typed_cols.iter().enumerate() {
                if pos > 0 {
                    sql.push_str(", ");
                }
                if type_null_flags[pos] {
                    sql.push_str("NULL");
                } else {
                    write_sql_value(&mut sql, typed_col, row_idx);
                }
            }
            sql.push(')');

            // Append ON CONFLICT clause for upsert mode
            if let Some(WriteMode::Upsert { primary_key }) = write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = active_cols
                    .iter()
                    .map(|&i| arrow_schema.field(i).name())
                    .filter(|name| !primary_key.contains(name))
                    .map(|name| format!("\"{}\" = EXCLUDED.\"{}\"", name, name))
                    .collect();
                if update_cols.is_empty() {
                    sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", pk_cols));
                } else {
                    sql.push_str(&format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        pk_cols,
                        update_cols.join(", ")
                    ));
                }
            }

            match client.execute(&sql, &[]).await {
                Ok(_) => rows_written += 1,
                Err(e) => {
                    rows_failed += 1;
                    host_ffi::log(
                        1,
                        &format!(
                            "dest-postgres: skipping row {} — INSERT failed: {}",
                            row_idx, e
                        ),
                    );
                }
            }
        }
    }

    if rows_failed > 0 {
        host_ffi::log(
            1,
            &format!(
                "dest-postgres: per-row fallback complete — {} written, {} failed/skipped",
                rows_written, rows_failed
            ),
        );
    }

    Ok(WriteResult {
        rows_written,
        rows_failed,
    })
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
