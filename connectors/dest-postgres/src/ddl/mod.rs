//! DDL orchestration: table creation, schema drift handling, and staging swaps.

mod drift;
mod staging;

use std::collections::HashSet;

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::SchemaEvolutionPolicy;

use self::drift::{apply_schema_policy, detect_schema_drift};
use crate::type_map::arrow_to_pg_type;

pub(crate) use self::staging::{prepare_staging, swap_staging_table};

/// Create the target table if it doesn't exist, based on the Arrow schema.
async fn ensure_table(
    ctx: &Context,
    client: &Client,
    qualified_table: &str,
    arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
    primary_key: Option<&[String]>,
) -> Result<(), String> {
    let columns_ddl: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_to_pg_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("{} {}{}", quote_identifier(field.name()), pg_type, nullable)
        })
        .collect();

    let mut ddl_parts: Vec<String> = columns_ddl;

    if let Some(pk_cols) = primary_key {
        if !pk_cols.is_empty() {
            let pk = pk_cols
                .iter()
                .map(|k| quote_identifier(k))
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

    ctx.log(
        LogLevel::Debug,
        &format!("dest-postgres: ensuring table: {}", ddl),
    );

    client
        .execute(&ddl, &[])
        .await
        .map_err(|e| format!("Failed to create table {}: {e}", qualified_table))?;

    Ok(())
}

/// Ensure target schema, table, and schema drift handling are complete.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_table_and_schema(
    ctx: &Context,
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    created_tables: &mut HashSet<String>,
    write_mode: Option<&WriteMode>,
    schema_policy: Option<&SchemaEvolutionPolicy>,
    arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
    ignored_columns: &mut HashSet<String>,
    type_null_columns: &mut HashSet<String>,
) -> Result<(), String> {
    let qualified_table = crate::decode::qualified_name(target_schema, stream_name);

    if created_tables.contains(&qualified_table) {
        return Ok(());
    }

    let create_schema = format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_identifier(target_schema)
    );
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {e}", target_schema))?;

    let pk = match write_mode {
        Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
        _ => None,
    };
    ensure_table(ctx, client, &qualified_table, arrow_schema, pk).await?;
    created_tables.insert(qualified_table.clone());

    if let Some(policy) = schema_policy {
        if let Some(drift) =
            detect_schema_drift(client, target_schema, stream_name, arrow_schema).await?
        {
            ctx.log(
                LogLevel::Info,
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
                ctx,
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
