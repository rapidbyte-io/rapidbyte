use std::collections::HashSet;
use std::io::Cursor;

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
pub struct WriteResult {
    pub rows_written: u64,
    pub rows_failed: u64,
}

/// Bundled parameters for batch write operations.
///
/// Groups the common arguments needed by `write_batch` and `write_batch_copy`
/// to keep function signatures under clippy's argument limit.
pub struct WriteContext<'a> {
    pub client: &'a Client,
    pub target_schema: &'a str,
    pub stream_name: &'a str,
    pub created_tables: &'a mut HashSet<String>,
    pub write_mode: Option<&'a WriteMode>,
    pub schema_policy: Option<&'a SchemaEvolutionPolicy>,
    pub on_data_error: DataErrorPolicy,
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
pub async fn swap_staging_table(
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
pub async fn prepare_staging(
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
pub struct SchemaDrift {
    /// Columns present in the Arrow schema but not in the existing table (name, pg_type).
    pub new_columns: Vec<(String, String)>,
    /// Columns present in the existing table but not in the Arrow schema.
    pub removed_columns: Vec<String>,
    /// Columns whose PG type differs (name, old_pg_type, new_pg_type).
    pub type_changes: Vec<(String, String, String)>,
    /// Columns whose nullability differs (name, was_nullable, now_nullable).
    pub nullability_changes: Vec<(String, bool, bool)>,
}

impl SchemaDrift {
    pub fn is_empty(&self) -> bool {
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
pub async fn detect_schema_drift(
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
pub async fn apply_schema_policy(
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

/// Write an Arrow IPC batch to PostgreSQL using multi-value INSERT.
///
/// On batch failure with Skip/Dlq policy, falls back to single-row
/// INSERTs, skipping individual bad rows.
pub async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<WriteResult, String> {
    match insert_batch(ctx, ipc_bytes).await {
        Ok(count) => Ok(WriteResult { rows_written: count, rows_failed: 0 }),
        Err(e) => {
            if matches!(ctx.on_data_error, DataErrorPolicy::Fail) {
                return Err(e);
            }
            host_ffi::log(
                1,
                &format!(
                    "dest-postgres: batch INSERT failed ({}), retrying per-row with skip policy",
                    e
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
    if !ctx.created_tables.contains(&qualified_table) {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", ctx.target_schema);
        ctx.client
            .execute(&create_schema, &[])
            .await
            .map_err(|e| format!("Failed to create schema '{}': {}", ctx.target_schema, e))?;

        let pk = match ctx.write_mode {
            Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
            _ => None,
        };
        ensure_table(ctx.client, &qualified_table, &arrow_schema, pk).await?;
        ctx.created_tables.insert(qualified_table.clone());

        // Detect schema drift and apply policy
        if let Some(policy) = ctx.schema_policy {
            if let Some(drift) =
                detect_schema_drift(ctx.client, ctx.target_schema, ctx.stream_name, &arrow_schema)
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
    }

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

/// Write an Arrow IPC batch to PostgreSQL using COPY FROM STDIN (text format).
///
/// On COPY failure with Skip/Dlq policy, falls back to single-row INSERTs.
pub async fn write_batch_copy(
    ctx: &mut WriteContext<'_>,
    ipc_bytes: &[u8],
) -> Result<WriteResult, String> {
    match copy_batch(ctx, ipc_bytes).await {
        Ok(count) => Ok(WriteResult {
            rows_written: count,
            rows_failed: 0,
        }),
        Err(e) => {
            if matches!(ctx.on_data_error, DataErrorPolicy::Fail) {
                return Err(e);
            }
            host_ffi::log(
                1,
                &format!(
                    "dest-postgres: COPY failed ({}), falling back to per-row INSERT with skip",
                    e
                ),
            );
            write_rows_individually(
                ctx.client, ctx.target_schema, ctx.stream_name, ipc_bytes, ctx.write_mode,
            )
            .await
        }
    }
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
    if !ctx.created_tables.contains(&qualified_table) {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", ctx.target_schema);
        ctx.client
            .execute(&create_schema, &[])
            .await
            .map_err(|e| format!("Failed to create schema '{}': {}", ctx.target_schema, e))?;

        let pk = match ctx.write_mode {
            Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
            _ => None,
        };
        ensure_table(ctx.client, &qualified_table, &arrow_schema, pk).await?;
        ctx.created_tables.insert(qualified_table.clone());

        // Detect schema drift and apply policy
        if let Some(policy) = ctx.schema_policy {
            if let Some(drift) =
                detect_schema_drift(ctx.client, ctx.target_schema, ctx.stream_name, &arrow_schema)
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
    }

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
pub async fn ensure_watermarks_table(
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
pub async fn get_watermark(
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
pub async fn set_watermark(
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
pub fn count_ipc_rows(ipc_bytes: &[u8]) -> Result<u64, String> {
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
pub async fn clear_watermark(
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
