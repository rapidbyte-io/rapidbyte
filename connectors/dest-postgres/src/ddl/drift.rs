//! Schema drift detection and policy application.

use std::collections::HashSet;

use anyhow::Context;
use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnPolicy, NullabilityPolicy, SchemaEvolutionPolicy, TypeChangePolicy,
};

use crate::ddl::type_map::{arrow_to_pg_type, pg_types_compatible};

/// Detected differences between an incoming Arrow schema and an existing PG table.
#[derive(Debug, Default)]
pub(crate) struct SchemaDrift {
    /// Columns present in the Arrow schema but not in the existing table (name, pg_type).
    pub(crate) new_columns: Vec<(String, String)>,
    /// Columns present in the existing table but not in the Arrow schema.
    pub(crate) removed_columns: Vec<String>,
    /// Columns whose PG type differs (name, old_pg_type, new_pg_type).
    pub(crate) type_changes: Vec<(String, String, String)>,
    /// Columns whose nullability differs (name, was_nullable, now_nullable).
    pub(crate) nullability_changes: Vec<(String, bool, bool)>,
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
) -> anyhow::Result<Vec<(String, String, bool)>> {
    let rows = client
        .query(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema_name, &table_name],
        )
        .await
        .context("Failed to query existing columns")?;

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
pub(crate) async fn detect_schema_drift(
    client: &Client,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
) -> anyhow::Result<Option<SchemaDrift>> {
    let existing = get_existing_columns(client, schema_name, table_name).await?;
    if existing.is_empty() {
        return Ok(None);
    }

    let existing_names: HashSet<&str> = existing.iter().map(|(n, _, _)| n.as_str()).collect();
    let arrow_names: HashSet<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // New columns: present in Arrow schema but absent from the existing table.
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

    // Removed columns: present in existing table but absent from Arrow schema.
    let removed_columns: Vec<String> = existing
        .iter()
        .filter(|(n, _, _)| !arrow_names.contains(n.as_str()))
        .map(|(n, _, _)| n.clone())
        .collect();

    // Type and nullability changes for columns present in both schemas.
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
pub(crate) async fn apply_schema_policy(
    client: &Client,
    qualified_table: &str,
    drift: &SchemaDrift,
    policy: &SchemaEvolutionPolicy,
    ignored_columns: &mut HashSet<String>,
    type_null_columns: &mut HashSet<String>,
) -> anyhow::Result<()> {
    // Handle new columns.
    for (col_name, pg_type) in &drift.new_columns {
        match policy.new_column {
            ColumnPolicy::Fail => {
                anyhow::bail!(
                    "Schema evolution: new column '{}' detected but policy is 'fail'",
                    col_name
                );
            }
            ColumnPolicy::Add => {
                let sql = format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    qualified_table,
                    quote_identifier(col_name),
                    pg_type
                );
                client
                    .execute(&sql, &[])
                    .await
                    .with_context(|| format!("ALTER TABLE ADD COLUMN '{}' failed", col_name))?;
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

    // Handle removed columns.
    for col_name in &drift.removed_columns {
        match policy.removed_column {
            ColumnPolicy::Fail => {
                anyhow::bail!(
                    "Schema evolution: column '{}' removed but policy is 'fail'",
                    col_name
                );
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

    // Handle type changes.
    for (col_name, old_type, new_type) in &drift.type_changes {
        match policy.type_change {
            TypeChangePolicy::Fail => {
                anyhow::bail!(
                    "Schema evolution: type change for '{}' ({} -> {}) but policy is 'fail'",
                    col_name,
                    old_type,
                    new_type
                );
            }
            TypeChangePolicy::Coerce => {
                let col_ident = quote_identifier(col_name);
                let sql = format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                    qualified_table,
                    col_ident,
                    new_type,
                    col_ident,
                    new_type
                );
                client.execute(&sql, &[]).await.with_context(|| {
                    format!(
                        "Schema evolution: ALTER COLUMN '{}' TYPE {} failed",
                        col_name, new_type
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

    // Handle nullability changes.
    for (col_name, was_nullable, now_nullable) in &drift.nullability_changes {
        match policy.nullability_change {
            NullabilityPolicy::Fail => {
                anyhow::bail!(
                    "Schema evolution: nullability change for '{}' ({} -> {}) but policy is 'fail'",
                    col_name,
                    was_nullable,
                    now_nullable
                );
            }
            NullabilityPolicy::Allow => {
                let col_ident = quote_identifier(col_name);
                if *was_nullable && !now_nullable {
                    let sql = format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                        qualified_table, col_ident
                    );
                    match client.execute(&sql, &[]).await {
                        Ok(_) => {
                            host_ffi::log(
                                2,
                                &format!("dest-postgres: SET NOT NULL on '{}'", col_name),
                            );
                        }
                        Err(e) => {
                            host_ffi::log(
                                1,
                                &format!(
                                    "dest-postgres: SET NOT NULL on '{}' failed (existing NULLs?): {}",
                                    col_name, e
                                ),
                            );
                        }
                    }
                } else if !was_nullable && *now_nullable {
                    let sql = format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                        qualified_table, col_ident
                    );
                    client
                        .execute(&sql, &[])
                        .await
                        .with_context(|| {
                            format!("ALTER TABLE DROP NOT NULL on '{}' failed", col_name)
                        })?;
                    host_ffi::log(
                        2,
                        &format!("dest-postgres: DROP NOT NULL on '{}'", col_name),
                    );
                }
            }
        }
    }

    Ok(())
}
