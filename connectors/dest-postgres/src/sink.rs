use std::collections::HashSet;
use std::io::Cursor;
use std::time::Instant;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnPolicy, DataErrorPolicy, NullabilityPolicy, SchemaEvolutionPolicy, TypeChangePolicy, WriteMode,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

use std::io::Write;
use bytes::Bytes;
use futures_util::SinkExt;

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

    // Table creation tracking
    created_tables: HashSet<String>,
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
            created_tables: HashSet::new(),
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
        };

        let result = write_batch(&mut write_ctx, ipc_bytes).await?;

        // Accumulate stats
        self.total_rows += result.rows_written;
        self.total_failed += result.rows_failed;
        self.total_bytes += n as u64;
        self.bytes_since_commit += n as u64;
        self.rows_since_commit += result.rows_written;
        self.batches_written += 1;

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

        // Final checkpoint for remaining uncommitted data
        if self.bytes_since_commit > 0 {
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
            let _ = clear_watermark(self.client, self.target_schema, &self.stream_name).await;
        }

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
}

/// Create the target table if it doesn't exist, based on the Arrow schema.
///
/// When `primary_key` is provided with non-empty column names, a PRIMARY KEY
/// constraint is appended to the CREATE TABLE DDL. This is required for
/// upsert mode (`INSERT ... ON CONFLICT (pk)`).
async fn ensure_table(
    client: &Client,
    qualified_table: &str,
    arrow_schema: &arrow::datatypes::Schema,
    primary_key: Option<&[String]>,
) -> Result<(), String> {
    // Validate all column names before interpolating into DDL
    for field in arrow_schema.fields() {
        validate_pg_identifier(field.name())
            .map_err(|e| format!("Invalid column name: {}", e))?;
    }

    let columns_ddl: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_to_pg_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("\"{}\" {}{}", field.name(), pg_type, nullable)
        })
        .collect();

    let mut ddl_parts: Vec<String> = columns_ddl;

    if let Some(pk_cols) = primary_key {
        if !pk_cols.is_empty() {
            for pk in pk_cols {
                validate_pg_identifier(pk)
                    .map_err(|e| format!("Invalid primary key column: {}", e))?;
            }
            let pk = pk_cols
                .iter()
                .map(|k| format!("\"{}\"", k))
                .collect::<Vec<_>>()
                .join(", ");
            ddl_parts.push(format!("PRIMARY KEY ({})", pk));
        }
    }

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified_table,
        ddl_parts.join(", ")
    );

    host_ffi::log(3, &format!("dest-postgres: ensuring table: {}", ddl));

    client
        .execute(&ddl, &[])
        .await
        .map_err(|e| format!("Failed to create table {}: {}", qualified_table, e))?;

    Ok(())
}

/// Ensure the target schema, table, and schema drift handling are complete.
///
/// This is a shared helper for both `insert_batch` and `copy_batch` to avoid
/// duplicating the ~35-line DDL + drift detection block. It is a no-op when
/// the table has already been created in this session (tracked via
/// `ctx.created_tables`).
async fn ensure_table_and_schema(
    ctx: &mut WriteContext<'_>,
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<(), String> {
    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    if ctx.created_tables.contains(&qualified_table) {
        return Ok(());
    }

    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", ctx.target_schema);
    ctx.client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {}", ctx.target_schema, e))?;

    let pk = match ctx.write_mode {
        Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
        _ => None,
    };
    ensure_table(ctx.client, &qualified_table, arrow_schema, pk).await?;
    ctx.created_tables.insert(qualified_table.clone());

    // Detect schema drift and apply policy
    if let Some(policy) = ctx.schema_policy {
        if let Some(drift) =
            detect_schema_drift(ctx.client, ctx.target_schema, ctx.stream_name, arrow_schema)
                .await?
        {
            host_ffi::log(
                2,
                &format!(
                    "dest-postgres: schema drift detected for {}: {} new, {} removed, {} type changes, {} nullability changes",
                    qualified_table,
                    drift.new_columns.len(),
                    drift.removed_columns.len(),
                    drift.type_changes.len(),
                    drift.nullability_changes.len()
                ),
            );
            apply_schema_policy(ctx.client, &qualified_table, &drift, policy).await?;
        }
    }

    Ok(())
}

/// Drop an existing staging table if it exists.
async fn drop_staging_table(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let staging_table = format!("\"{}\".\"{}__rb_staging\"", target_schema, stream_name);
    let sql = format!("DROP TABLE IF EXISTS {} CASCADE", staging_table);
    client
        .execute(&sql, &[])
        .await
        .map_err(|e| format!("DROP staging table failed for {}: {}", staging_table, e))?;
    host_ffi::log(
        3,
        &format!("dest-postgres: dropped staging table {}", staging_table),
    );
    Ok(())
}

/// Atomically swap a staging table into the target position.
///
/// Uses PostgreSQL's transactional DDL: DROP target + RENAME staging -> target
/// inside a single transaction. Readers see either old data or new data, never partial.
async fn swap_staging_table(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let target_table = format!("\"{}\".\"{}\"", target_schema, stream_name);
    let staging_table = format!("\"{}\".\"{}__rb_staging\"", target_schema, stream_name);
    let staging_name_only = format!("\"{}\"", stream_name);

    // Atomic swap: DDL is transactional in PostgreSQL
    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| format!("Swap BEGIN failed: {}", e))?;

    let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", target_table);
    if let Err(e) = client.execute(&drop_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap DROP failed for {}: {}", target_table, e));
    }

    let rename_sql = format!(
        "ALTER TABLE {} RENAME TO {}",
        staging_table, staging_name_only
    );
    if let Err(e) = client.execute(&rename_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap RENAME failed: {}", e));
    }

    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| format!("Swap COMMIT failed: {}", e))?;

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: atomic swap {} -> {}",
            staging_table, target_table
        ),
    );
    Ok(())
}

/// Prepare a fresh staging table for Replace mode.
///
/// Drops any leftover staging table (from a previous failed run) and returns
/// the staging stream name to use for writes.
async fn prepare_staging(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<String, String> {
    drop_staging_table(client, target_schema, stream_name).await?;
    let staging_name = format!("{}__rb_staging", stream_name);
    Ok(staging_name)
}

/// Map Arrow data types back to PostgreSQL column types.
fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        _ => "TEXT",
    }
}

/// Detected differences between an incoming Arrow schema and an existing PG table.
#[derive(Debug, Default)]
struct SchemaDrift {
    /// Columns present in the Arrow schema but not in the existing table (name, pg_type).
    new_columns: Vec<(String, String)>,
    /// Columns present in the existing table but not in the Arrow schema.
    removed_columns: Vec<String>,
    /// Columns whose PG type differs (name, old_pg_type, new_pg_type).
    type_changes: Vec<(String, String, String)>,
    /// Columns whose nullability differs (name, was_nullable, now_nullable).
    nullability_changes: Vec<(String, bool, bool)>,
}

impl SchemaDrift {
    fn is_empty(&self) -> bool {
        self.new_columns.is_empty()
            && self.removed_columns.is_empty()
            && self.type_changes.is_empty()
            && self.nullability_changes.is_empty()
    }
}

/// Fetch existing column names, types, and nullability from information_schema.
async fn get_existing_columns(
    client: &Client,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<(String, String, bool)>, String> {
    let rows = client
        .query(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema_name, &table_name],
        )
        .await
        .map_err(|e| format!("Failed to query columns: {}", e))?;

    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let dtype: String = r.get(1);
            let nullable: String = r.get(2);
            (name, dtype, nullable == "YES")
        })
        .collect())
}

/// Check if a PG information_schema type and a DDL type refer to the same type.
///
/// PostgreSQL's `information_schema.data_type` uses SQL-standard names (e.g.
/// "integer", "character varying") whereas our DDL uses short forms (e.g.
/// "INTEGER", "TEXT"). This function normalises both sides before comparing.
fn pg_types_compatible(info_schema_type: &str, ddl_type: &str) -> bool {
    let a = info_schema_type.to_lowercase();
    let b = ddl_type.to_lowercase();

    let norm_a = match a.as_str() {
        "int" | "int4" | "integer" => "integer",
        "int2" | "smallint" => "smallint",
        "int8" | "bigint" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        other => other,
    };
    let norm_b = match b.as_str() {
        "int" | "int4" | "integer" => "integer",
        "int2" | "smallint" => "smallint",
        "int8" | "bigint" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        other => other,
    };
    norm_a == norm_b
}

/// Detect schema differences between an Arrow schema and an existing PG table.
///
/// Returns `Ok(None)` if the table does not exist yet (no columns found in
/// information_schema) or if the schemas are fully compatible.
/// Returns `Ok(Some(drift))` when differences are detected.
async fn detect_schema_drift(
    client: &Client,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<Option<SchemaDrift>, String> {
    let existing = get_existing_columns(client, schema_name, table_name).await?;
    if existing.is_empty() {
        return Ok(None); // Table doesn't exist yet
    }

    let existing_names: HashSet<&str> = existing.iter().map(|(n, _, _)| n.as_str()).collect();
    let arrow_names: HashSet<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // New columns: present in Arrow schema but absent from the existing table
    let new_columns: Vec<(String, String)> = arrow_schema
        .fields()
        .iter()
        .filter(|f| !existing_names.contains(f.name().as_str()))
        .map(|f| {
            (
                f.name().clone(),
                arrow_to_pg_type(f.data_type()).to_string(),
            )
        })
        .collect();

    // Removed columns: present in existing table but absent from Arrow schema
    let removed_columns: Vec<String> = existing
        .iter()
        .filter(|(n, _, _)| !arrow_names.contains(n.as_str()))
        .map(|(n, _, _)| n.clone())
        .collect();

    // Type and nullability changes for columns present in both schemas
    let mut type_changes = Vec::new();
    let mut nullability_changes = Vec::new();

    for field in arrow_schema.fields() {
        if let Some((_, old_type, old_nullable)) =
            existing.iter().find(|(n, _, _)| n == field.name())
        {
            let new_pg_type = arrow_to_pg_type(field.data_type());
            if !pg_types_compatible(old_type, new_pg_type) {
                type_changes.push((
                    field.name().clone(),
                    old_type.clone(),
                    new_pg_type.to_string(),
                ));
            }
            if *old_nullable != field.is_nullable() {
                nullability_changes.push((
                    field.name().clone(),
                    *old_nullable,
                    field.is_nullable(),
                ));
            }
        }
    }

    let drift = SchemaDrift {
        new_columns,
        removed_columns,
        type_changes,
        nullability_changes,
    };
    if drift.is_empty() {
        Ok(None)
    } else {
        Ok(Some(drift))
    }
}

/// Apply schema evolution policy to detected drift, executing DDL as needed.
async fn apply_schema_policy(
    client: &Client,
    qualified_table: &str,
    drift: &SchemaDrift,
    policy: &SchemaEvolutionPolicy,
) -> Result<(), String> {
    // Handle new columns
    for (col_name, pg_type) in &drift.new_columns {
        match policy.new_column {
            ColumnPolicy::Fail => {
                return Err(format!(
                    "Schema evolution: new column '{}' detected but policy is 'fail'",
                    col_name
                ));
            }
            ColumnPolicy::Add => {
                validate_pg_identifier(col_name)
                    .map_err(|e| format!("Invalid new column name '{}': {}", col_name, e))?;
                let sql = format!(
                    "ALTER TABLE {} ADD COLUMN \"{}\" {}",
                    qualified_table, col_name, pg_type
                );
                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| format!("ALTER TABLE ADD COLUMN '{}' failed: {}", col_name, e))?;
                host_ffi::log(
                    2,
                    &format!("dest-postgres: added column '{}' {}", col_name, pg_type),
                );
            }
            ColumnPolicy::Ignore => {
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: ignoring new column '{}' per schema policy",
                        col_name
                    ),
                );
            }
        }
    }

    // Handle removed columns
    for col_name in &drift.removed_columns {
        match policy.removed_column {
            ColumnPolicy::Fail => {
                return Err(format!(
                    "Schema evolution: column '{}' removed but policy is 'fail'",
                    col_name
                ));
            }
            _ => {
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: ignoring removed column '{}' per schema policy",
                        col_name
                    ),
                );
            }
        }
    }

    // Handle type changes
    for (col_name, old_type, new_type) in &drift.type_changes {
        match policy.type_change {
            TypeChangePolicy::Fail => {
                return Err(format!(
                    "Schema evolution: type change for '{}' ({} -> {}) but policy is 'fail'",
                    col_name, old_type, new_type
                ));
            }
            TypeChangePolicy::Coerce | TypeChangePolicy::Null => {
                host_ffi::log(
                    1,
                    &format!(
                        "dest-postgres: type change for '{}' ({} -> {}), policy={:?}",
                        col_name, old_type, new_type, policy.type_change
                    ),
                );
            }
        }
    }

    // Handle nullability changes
    for (col_name, was_nullable, now_nullable) in &drift.nullability_changes {
        match policy.nullability_change {
            NullabilityPolicy::Fail => {
                return Err(format!(
                    "Schema evolution: nullability change for '{}' ({} -> {}) but policy is 'fail'",
                    col_name, was_nullable, now_nullable
                ));
            }
            NullabilityPolicy::Allow => {
                host_ffi::log(
                    3,
                    &format!(
                        "dest-postgres: allowing nullability change for '{}'",
                        col_name
                    ),
                );
            }
        }
    }

    Ok(())
}

/// Maximum rows per multi-value INSERT statement.
/// Keeps SQL string size manageable (~350KB for 7 columns).
const INSERT_CHUNK_SIZE: usize = 1000;

/// Buffer size for COPY data before flushing to the sink.
const COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB

/// Write an Arrow IPC batch to PostgreSQL.
///
/// Dispatches to COPY or INSERT based on `ctx.load_method`. If the load method
/// is "copy" but the write mode is Upsert, automatically falls back to INSERT
/// (COPY cannot handle ON CONFLICT). On batch failure with Skip/Dlq error
/// policy, falls back to single-row INSERTs.
async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<WriteResult, String> {
    let use_copy = ctx.load_method == "copy"
        && !matches!(ctx.write_mode, Some(WriteMode::Upsert { .. }));

    let (result, method_name) = if use_copy {
        (copy_batch(ctx, ipc_bytes).await, "COPY")
    } else {
        (insert_batch(ctx, ipc_bytes).await, "INSERT")
    };

    match result {
        Ok(count) => Ok(WriteResult { rows_written: count, rows_failed: 0 }),
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
            write_rows_individually(
                ctx.client, ctx.target_schema, ctx.stream_name, ipc_bytes, ctx.write_mode,
            ).await
        }
    }
}

/// Internal: write via multi-value INSERT, returning row count.
///
/// Builds batched statements:
///   INSERT INTO t (c1, c2) VALUES (v1, v2), (v3, v4), ...
///
/// Chunks at INSERT_CHUNK_SIZE rows to keep SQL size bounded.
async fn insert_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<u64, String> {
    // 1. Decode Arrow IPC
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;

    let arrow_schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;

    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // 2. Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(ctx, &arrow_schema).await?;

    // Validate upsert primary_key columns
    if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
        for pk_col in primary_key {
            validate_pg_identifier(pk_col)
                .map_err(|e| format!("Invalid primary key column name '{}': {}", pk_col, e))?;
        }
    }

    // 3. Build column list
    let col_list = arrow_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    // 4. Insert rows using multi-value INSERT
    let mut total_rows: u64 = 0;

    for batch in &batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        // Process in chunks
        for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
            let chunk_end = (chunk_start + INSERT_CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            let mut sql = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for col_idx in 0..num_cols {
                    if col_idx > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format_sql_value(batch.column(col_idx).as_ref(), row_idx));
                }
                sql.push(')');
            }

            // Append ON CONFLICT clause for upsert mode
            if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = arrow_schema
                    .fields()
                    .iter()
                    .filter(|f| !primary_key.contains(f.name()))
                    .map(|f| {
                        format!("\"{}\" = EXCLUDED.\"{}\"", f.name(), f.name())
                    })
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
async fn copy_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<u64, String> {
    // 1. Decode Arrow IPC
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;
    let arrow_schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;

    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // 2. Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(ctx, &arrow_schema).await?;

    // 3. Build COPY statement
    let col_list = arrow_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
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

    for batch in &batches {
        let num_cols = batch.num_columns();
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..num_cols {
                if col_idx > 0 {
                    buf.push(b'\t');
                }
                format_copy_value(&mut buf, batch.column(col_idx).as_ref(), row_idx);
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
    ipc_bytes: &[u8],
    write_mode: Option<&WriteMode>,
) -> Result<WriteResult, String> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("IPC decode failed: {}", e))?;
    let arrow_schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;

    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);
    let col_list = arrow_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut rows_written = 0u64;
    let mut rows_failed = 0u64;

    for batch in &batches {
        let num_cols = batch.num_columns();
        for row_idx in 0..batch.num_rows() {
            let mut sql = format!("INSERT INTO {} ({}) VALUES (", qualified_table, col_list);
            for col_idx in 0..num_cols {
                if col_idx > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&format_sql_value(batch.column(col_idx).as_ref(), row_idx));
            }
            sql.push(')');

            // Append ON CONFLICT clause for upsert mode
            if let Some(WriteMode::Upsert { primary_key }) = write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = arrow_schema
                    .fields()
                    .iter()
                    .filter(|f| !primary_key.contains(f.name()))
                    .map(|f| format!("\"{}\" = EXCLUDED.\"{}\"", f.name(), f.name()))
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

/// Format an Arrow array value at a given row index as a SQL literal.
///
/// Returns "NULL" for null values, properly escapes strings,
/// and formats numbers/booleans as literals.
///
/// Supported Arrow types: Int16, Int32, Int64, Float32, Float64, Boolean, Utf8.
/// Other types fall back to "NULL" rather than panicking.
fn format_sql_value(col: &dyn Array, row_idx: usize) -> String {
    if col.is_null(row_idx) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                "'NaN'::real".to_string()
            } else if v.is_infinite() {
                if v > 0.0 { "'Infinity'::real".to_string() } else { "'-Infinity'::real".to_string() }
            } else {
                v.to_string()
            }
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                "'NaN'::double precision".to_string()
            } else if v.is_infinite() {
                if v > 0.0 { "'Infinity'::double precision".to_string() } else { "'-Infinity'::double precision".to_string() }
            } else {
                v.to_string()
            }
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row_idx) { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // Strip null bytes (libpq treats them as string terminators) and
            // escape single quotes by doubling them (PG standard_conforming_strings=on)
            let cleaned = val.replace('\0', "");
            format!("'{}'", cleaned.replace('\'', "''"))
        }
        _ => {
            // Unsupported type — insert NULL rather than panic
            "NULL".to_string()
        }
    }
}

/// Format an Arrow array value at a given row index for COPY text format.
///
/// COPY text format rules:
/// - NULL: `\N`
/// - Strings: backslash-escape `\`, tab, newline, carriage return; strip null bytes
/// - Booleans: `t` / `f`
/// - Numbers: decimal representation (NaN, Infinity as literals)
fn format_copy_value(buf: &mut Vec<u8>, col: &dyn Array, row_idx: usize) {
    if col.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }

    match col.data_type() {
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            buf.push(if arr.value(row_idx) { b't' } else { b'f' });
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // COPY text format: escape backslash, tab, newline, CR; strip null bytes
            for byte in val.bytes() {
                match byte {
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    0 => {} // skip null bytes
                    _ => buf.push(byte),
                }
            }
        }
        _ => {
            buf.extend_from_slice(b"\\N");
        }
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
