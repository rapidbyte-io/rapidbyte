//! Schema introspection for `PostgreSQL`.
//!
//! Queries `information_schema` to discover available tables and columns,
//! returning `Stream` definitions for the catalog.

use rapidbyte_sdk::prelude::*;
use tokio_postgres::Client;

use crate::types::Column;

/// Discover all base tables and columns in the `public` schema.
pub async fn discover_catalog(client: &Client) -> Result<Vec<Stream>, String> {
    let query = r"
        SELECT
            t.table_name,
            c.column_name,
            c.data_type,
            CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END as nullable
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE t.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name, c.ordinal_position
    ";

    let rows = client
        .query(query, &[])
        .await
        .map_err(|e| format!("discovery query failed: {e}"))?;

    let mut streams: Vec<Stream> = Vec::new();
    let mut current_table: Option<String> = None;
    let mut current_columns: Vec<Column> = Vec::new();

    for row in &rows {
        let table: String = row.get(0);
        let col_name: String = row.get(1);
        let data_type: String = row.get(2);
        let nullable: bool = row.get(3);

        // Flush previous table when table name changes
        if current_table.as_ref() != Some(&table) {
            if let Some(prev_table) = current_table.take() {
                streams.push(build_stream(&prev_table, &current_columns));
                current_columns.clear();
            }
            current_table = Some(table);
        }

        current_columns.push(Column::new(&col_name, &data_type, nullable));
    }

    // Flush final table
    if let Some(table) = current_table {
        streams.push(build_stream(&table, &current_columns));
    }

    Ok(streams)
}

/// Query column metadata for a single table in the `public` schema.
pub(crate) async fn query_table_columns(
    client: &Client,
    table_name: &str,
) -> Result<Vec<Column>, String> {
    let query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let rows = client
        .query(query, &[&table_name])
        .await
        .map_err(|e| format!("Schema query failed for {table_name}: {e}"))?;

    let columns: Vec<Column> = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let nullable: bool = row.get::<_, String>(2) == "YES";
            Column::new(&name, &data_type, nullable)
        })
        .collect();

    if columns.is_empty() {
        return Err(format!("Table '{table_name}' not found or has no columns"));
    }

    Ok(columns)
}

fn build_stream(table: &str, columns: &[Column]) -> Stream {
    Stream {
        name: table.to_string(),
        schema: columns.iter().map(Column::to_schema).collect(),
        supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc],
        source_defined_cursor: None,
        source_defined_primary_key: None,
    }
}
