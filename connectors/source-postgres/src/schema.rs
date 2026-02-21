use rapidbyte_sdk::protocol::{ArrowDataType, ColumnSchema, Stream, SyncMode};
use tokio_postgres::Client;

/// Map PostgreSQL data types to Arrow-compatible data types.
pub(crate) fn pg_type_to_arrow(pg_type: &str) -> ArrowDataType {
    match pg_type {
        "integer" | "int4" | "serial" => ArrowDataType::Int32,
        "bigint" | "int8" | "bigserial" => ArrowDataType::Int64,
        "smallint" | "int2" => ArrowDataType::Int16,
        "real" | "float4" => ArrowDataType::Float32,
        "double precision" | "float8" => ArrowDataType::Float64,
        "boolean" | "bool" => ArrowDataType::Boolean,
        "text" | "varchar" | "character varying" | "char" | "character" | "name" => {
            ArrowDataType::Utf8
        }
        // For v0.1, represent complex types as strings
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => ArrowDataType::Utf8,
        "date" => ArrowDataType::Utf8,
        "time" | "time without time zone" | "time with time zone" => ArrowDataType::Utf8,
        "numeric" | "decimal" => ArrowDataType::Utf8,
        "json" | "jsonb" => ArrowDataType::Utf8,
        "uuid" => ArrowDataType::Utf8,
        "bytea" => ArrowDataType::Utf8,
        "inet" | "cidr" | "macaddr" => ArrowDataType::Utf8,
        "interval" => ArrowDataType::Utf8,
        _ => ArrowDataType::Utf8, // Safe fallback
    }
}

/// Discover all user tables and their schemas from the PostgreSQL database.
pub async fn discover_catalog(client: &Client) -> Result<Vec<Stream>, String> {
    let query = r#"
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
    "#;

    let rows = client
        .query(query, &[])
        .await
        .map_err(|e| format!("Schema discovery query failed: {}", e))?;

    let mut streams: Vec<Stream> = Vec::new();
    let mut current_table: Option<String> = None;
    let mut current_columns: Vec<ColumnSchema> = Vec::new();

    for row in &rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let data_type: String = row.get(2);
        let nullable: bool = row.get(3);

        if current_table.as_ref() != Some(&table_name) {
            if let Some(prev_table) = current_table.take() {
                streams.push(Stream {
                    name: prev_table,
                    schema: std::mem::take(&mut current_columns),
                    supported_sync_modes: vec![
                        SyncMode::FullRefresh,
                        SyncMode::Incremental,
                        SyncMode::Cdc,
                    ],
                    source_defined_cursor: None,
                    source_defined_primary_key: None,
                });
            }
            current_table = Some(table_name);
        }

        current_columns.push(ColumnSchema {
            name: column_name,
            data_type: pg_type_to_arrow(&data_type),
            nullable,
        });
    }

    // Don't forget the last table
    if let Some(table_name) = current_table {
        streams.push(Stream {
            name: table_name,
            schema: current_columns,
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc],
            source_defined_cursor: None,
            source_defined_primary_key: None,
        });
    }

    Ok(streams)
}
