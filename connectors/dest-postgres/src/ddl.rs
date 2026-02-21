use std::collections::HashSet;

use arrow::datatypes::DataType;
use tokio_postgres::Client;

use crate::identifier::validate_pg_identifier;
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnPolicy, NullabilityPolicy, SchemaEvolutionPolicy, TypeChangePolicy, WriteMode,
};

/// Map Arrow data types back to PostgreSQL column types.
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
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

/// Create the target table if it doesn't exist, based on the Arrow schema.
///
/// When `primary_key` is provided with non-empty column names, a PRIMARY KEY
/// constraint is appended to the CREATE TABLE DDL. This is required for
/// upsert mode (`INSERT ... ON CONFLICT (pk)`).
pub(crate) async fn ensure_table(
    client: &Client,
    qualified_table: &str,
    arrow_schema: &arrow::datatypes::Schema,
    primary_key: Option<&[String]>,
) -> Result<(), String> {
    // Validate all column names before interpolating into DDL
    for field in arrow_schema.fields() {
        validate_pg_identifier(field.name()).map_err(|e| format!("Invalid column name: {}", e))?;
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
/// `created_tables`).
pub(crate) async fn ensure_table_and_schema(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    created_tables: &mut HashSet<String>,
    write_mode: Option<&WriteMode>,
    schema_policy: Option<&SchemaEvolutionPolicy>,
    arrow_schema: &arrow::datatypes::Schema,
    ignored_columns: &mut HashSet<String>,
    type_null_columns: &mut HashSet<String>,
) -> Result<(), String> {
    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);

    if created_tables.contains(&qualified_table) {
        return Ok(());
    }

    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;

    let pk = match write_mode {
        Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
        _ => None,
    };
    ensure_table(client, &qualified_table, arrow_schema, pk).await?;
    created_tables.insert(qualified_table.clone());

    // Detect schema drift and apply policy
    if let Some(policy) = schema_policy {
        if let Some(drift) =
            detect_schema_drift(client, target_schema, stream_name, arrow_schema).await?
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
            apply_schema_policy(
                client,
                &qualified_table,
                &drift,
                policy,
                ignored_columns,
                type_null_columns,
            )
            .await?;
        }
    }

    Ok(())
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
    ignored_columns: &mut HashSet<String>,
    type_null_columns: &mut HashSet<String>,
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
                ignored_columns.insert(col_name.clone());
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: ignoring new column '{}' per schema policy (excluded from writes)",
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
            ColumnPolicy::Ignore | ColumnPolicy::Add => {
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: column '{}' removed from source, keeping in table per policy",
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
            TypeChangePolicy::Coerce => {
                validate_pg_identifier(col_name)
                    .map_err(|e| format!("Invalid column name '{}': {}", col_name, e))?;
                let sql = format!(
                    "ALTER TABLE {} ALTER COLUMN \"{}\" TYPE {} USING \"{}\"::{}",
                    qualified_table, col_name, new_type, col_name, new_type
                );
                client.execute(&sql, &[]).await.map_err(|e| {
                    format!(
                        "Schema evolution: ALTER COLUMN '{}' TYPE {} failed: {}. \
                             Existing data may not be castable to the new type.",
                        col_name, new_type, e
                    )
                })?;
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: coerced '{}' from {} to {}",
                        col_name, old_type, new_type
                    ),
                );
            }
            TypeChangePolicy::Null => {
                type_null_columns.insert(col_name.clone());
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: type change for '{}' ({} -> {}), policy=Null — values will be NULL",
                        col_name, old_type, new_type
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
                let col = col_name.as_str();
                validate_pg_identifier(col)
                    .map_err(|e| format!("Invalid column name '{}': {}", col, e))?;

                if *was_nullable && !now_nullable {
                    // nullable -> NOT NULL: tighten constraint
                    let sql = format!(
                        "ALTER TABLE {} ALTER COLUMN \"{}\" SET NOT NULL",
                        qualified_table, col
                    );
                    match client.execute(&sql, &[]).await {
                        Ok(_) => {
                            host_ffi::log(2, &format!("dest-postgres: SET NOT NULL on '{}'", col));
                        }
                        Err(e) => {
                            // Existing NULLs prevent SET NOT NULL — log and continue
                            host_ffi::log(
                                1,
                                &format!(
                                    "dest-postgres: SET NOT NULL on '{}' failed (existing NULLs?): {}",
                                    col, e
                                ),
                            );
                        }
                    }
                } else if !was_nullable && *now_nullable {
                    // NOT NULL -> nullable: relax constraint
                    let sql = format!(
                        "ALTER TABLE {} ALTER COLUMN \"{}\" DROP NOT NULL",
                        qualified_table, col
                    );
                    client.execute(&sql, &[]).await.map_err(|e| {
                        format!("ALTER TABLE DROP NOT NULL on '{}' failed: {}", col, e)
                    })?;
                    host_ffi::log(2, &format!("dest-postgres: DROP NOT NULL on '{}'", col));
                }
            }
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
pub(crate) async fn swap_staging_table(
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
pub(crate) async fn prepare_staging(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<String, String> {
    drop_staging_table(client, target_schema, stream_name).await?;
    let staging_name = format!("{}__rb_staging", stream_name);
    Ok(staging_name)
}
