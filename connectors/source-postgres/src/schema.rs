//! PostgreSQL catalog discovery and type mapping for source connector.

use rapidbyte_sdk::protocol::{ArrowDataType, ColumnSchema, Stream, SyncMode};
use tokio_postgres::Client;

/// Map PostgreSQL data types to Arrow-compatible data types.
pub(crate) fn pg_type_to_arrow(pg_type: &str) -> ArrowDataType {
    match pg_type {
        "integer" | "int4" | "serial" => ArrowDataType::Int32,
        "bigint" | "int8" | "bigserial" => ArrowDataType::Int64,
        "smallint" | "int2" | "smallserial" => ArrowDataType::Int16,
        "real" | "float4" => ArrowDataType::Float32,
        "double precision" | "float8" => ArrowDataType::Float64,
        "boolean" | "bool" => ArrowDataType::Boolean,
        "text" | "varchar" | "character varying" | "char" | "character" | "name" | "bpchar" => {
            ArrowDataType::Utf8
        }
        // Native timestamp/date types mapped to Arrow temporal types
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => ArrowDataType::TimestampMicros,
        "date" => ArrowDataType::Date32,
        "bytea" => ArrowDataType::Binary,
        // json/jsonb extracted natively via serde_json::Value, stored as string
        "json" | "jsonb" => ArrowDataType::Utf8,
        // Types that need ::text cast — still stored as Utf8
        "time" | "time without time zone" | "time with time zone" => ArrowDataType::Utf8,
        "numeric" | "decimal" => ArrowDataType::Utf8,
        "uuid" => ArrowDataType::Utf8,
        "inet" | "cidr" | "macaddr" => ArrowDataType::Utf8,
        "interval" => ArrowDataType::Utf8,
        _ => ArrowDataType::Utf8, // Safe fallback
    }
}

/// Returns `true` if the given PostgreSQL type needs a `::text` cast in SQL
/// to be extractable as a `String` by tokio-postgres.
///
/// Types with native `FromSql` support in tokio-postgres return `false`:
/// integers, floats, bool, text-like, timestamps, date, bytea, json/jsonb.
/// Everything else returns `true`.
pub(crate) fn needs_text_cast(pg_type: &str) -> bool {
    match pg_type {
        // Integer types — native FromSql
        "integer" | "int4" | "serial" | "bigint" | "int8" | "bigserial"
        | "smallint" | "int2" | "smallserial" => false,
        // Float types — native FromSql
        "real" | "float4" | "double precision" | "float8" => false,
        // Boolean — native FromSql
        "boolean" | "bool" => false,
        // Text-like types — native FromSql
        "text" | "varchar" | "character varying" | "char" | "character" | "name" | "bpchar" => {
            false
        }
        // Timestamp variants — native FromSql (via chrono)
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => false,
        // Date — native FromSql (via chrono)
        "date" => false,
        // bytea — native FromSql as Vec<u8>
        "bytea" => false,
        // json/jsonb — native FromSql via serde_json::Value
        "json" | "jsonb" => false,
        // Everything else needs ::text cast
        _ => true,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_to_arrow_maps_native_types() {
        // Integer types
        assert_eq!(pg_type_to_arrow("integer"), ArrowDataType::Int32);
        assert_eq!(pg_type_to_arrow("int4"), ArrowDataType::Int32);
        assert_eq!(pg_type_to_arrow("serial"), ArrowDataType::Int32);
        assert_eq!(pg_type_to_arrow("smallserial"), ArrowDataType::Int16);
        assert_eq!(pg_type_to_arrow("bigint"), ArrowDataType::Int64);
        assert_eq!(pg_type_to_arrow("int8"), ArrowDataType::Int64);
        assert_eq!(pg_type_to_arrow("bigserial"), ArrowDataType::Int64);
        assert_eq!(pg_type_to_arrow("smallint"), ArrowDataType::Int16);
        assert_eq!(pg_type_to_arrow("int2"), ArrowDataType::Int16);

        // Float types
        assert_eq!(pg_type_to_arrow("real"), ArrowDataType::Float32);
        assert_eq!(pg_type_to_arrow("float4"), ArrowDataType::Float32);
        assert_eq!(pg_type_to_arrow("double precision"), ArrowDataType::Float64);
        assert_eq!(pg_type_to_arrow("float8"), ArrowDataType::Float64);

        // Boolean
        assert_eq!(pg_type_to_arrow("boolean"), ArrowDataType::Boolean);
        assert_eq!(pg_type_to_arrow("bool"), ArrowDataType::Boolean);

        // Text-like types (including bpchar alias)
        assert_eq!(pg_type_to_arrow("text"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("varchar"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("character varying"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("char"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("character"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("name"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("bpchar"), ArrowDataType::Utf8);

        // Timestamp types — now TimestampMicros
        assert_eq!(
            pg_type_to_arrow("timestamp without time zone"),
            ArrowDataType::TimestampMicros
        );
        assert_eq!(
            pg_type_to_arrow("timestamp with time zone"),
            ArrowDataType::TimestampMicros
        );
        assert_eq!(pg_type_to_arrow("timestamp"), ArrowDataType::TimestampMicros);
        assert_eq!(pg_type_to_arrow("timestamptz"), ArrowDataType::TimestampMicros);

        // Date — now Date32
        assert_eq!(pg_type_to_arrow("date"), ArrowDataType::Date32);

        // bytea — now Binary
        assert_eq!(pg_type_to_arrow("bytea"), ArrowDataType::Binary);

        // json/jsonb — remain Utf8
        assert_eq!(pg_type_to_arrow("json"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("jsonb"), ArrowDataType::Utf8);

        // Types stored as Utf8 via ::text cast
        assert_eq!(pg_type_to_arrow("time"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("numeric"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("decimal"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("uuid"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("inet"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("cidr"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("macaddr"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("interval"), ArrowDataType::Utf8);

        // Unknown types fall back to Utf8
        assert_eq!(pg_type_to_arrow("hstore"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("unknown_custom_type"), ArrowDataType::Utf8);
    }

    #[test]
    fn needs_text_cast_identifies_non_native_types() {
        // Native types — no cast needed
        assert!(!needs_text_cast("integer"));
        assert!(!needs_text_cast("int4"));
        assert!(!needs_text_cast("serial"));
        assert!(!needs_text_cast("smallserial"));
        assert!(!needs_text_cast("bigint"));
        assert!(!needs_text_cast("int8"));
        assert!(!needs_text_cast("bigserial"));
        assert!(!needs_text_cast("smallint"));
        assert!(!needs_text_cast("int2"));
        assert!(!needs_text_cast("real"));
        assert!(!needs_text_cast("float4"));
        assert!(!needs_text_cast("double precision"));
        assert!(!needs_text_cast("float8"));
        assert!(!needs_text_cast("boolean"));
        assert!(!needs_text_cast("bool"));
        assert!(!needs_text_cast("text"));
        assert!(!needs_text_cast("varchar"));
        assert!(!needs_text_cast("character varying"));
        assert!(!needs_text_cast("char"));
        assert!(!needs_text_cast("character"));
        assert!(!needs_text_cast("name"));
        assert!(!needs_text_cast("bpchar"));
        assert!(!needs_text_cast("timestamp without time zone"));
        assert!(!needs_text_cast("timestamp with time zone"));
        assert!(!needs_text_cast("timestamp"));
        assert!(!needs_text_cast("timestamptz"));
        assert!(!needs_text_cast("date"));
        assert!(!needs_text_cast("bytea"));
        assert!(!needs_text_cast("json"));
        assert!(!needs_text_cast("jsonb"));

        // Non-native types — need ::text cast
        assert!(needs_text_cast("numeric"));
        assert!(needs_text_cast("decimal"));
        assert!(needs_text_cast("uuid"));
        assert!(needs_text_cast("time"));
        assert!(needs_text_cast("time without time zone"));
        assert!(needs_text_cast("time with time zone"));
        assert!(needs_text_cast("timetz"));
        assert!(needs_text_cast("interval"));
        assert!(needs_text_cast("inet"));
        assert!(needs_text_cast("cidr"));
        assert!(needs_text_cast("macaddr"));
        assert!(needs_text_cast("money"));
        assert!(needs_text_cast("bit"));
        assert!(needs_text_cast("xml"));
        assert!(needs_text_cast("hstore"));
        assert!(needs_text_cast("unknown_custom_type"));
    }
}
