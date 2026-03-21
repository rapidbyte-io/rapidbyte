//! Schema introspection for `PostgreSQL`.
//!
//! Queries `information_schema` and publication metadata to discover available
//! tables and columns, returning typed `DiscoveredStream` definitions.

use std::collections::{BTreeMap, HashMap, HashSet};

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::schema::StreamSchema;
use tokio_postgres::Client;

use crate::config::DiscoverySettings;
use crate::types::Column;

/// Discovery metadata for a single column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveryColumn {
    pub name: String,
    pub pg_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_generated: bool,
}

impl DiscoveryColumn {
    #[must_use]
    fn to_schema_field(&self) -> rapidbyte_sdk::schema::SchemaField {
        Column::new(&self.name, &self.pg_type, self.nullable)
            .to_schema_field_with_flags(self.is_primary_key, self.is_generated)
    }

    #[must_use]
    fn as_column(&self) -> Column {
        Column::new(&self.name, &self.pg_type, self.nullable)
    }
}

/// Discover tables and columns using default discovery settings.
///
/// # Errors
///
/// Returns `Err` if any catalog query fails or result parsing encounters an
/// unexpected column type.
#[allow(dead_code)]
pub async fn discover_catalog(client: &Client) -> Result<Vec<DiscoveredStream>, String> {
    discover_catalog_with_settings(client, &DiscoverySettings::default()).await
}

pub(crate) async fn discover_catalog_with_settings(
    client: &Client,
    settings: &DiscoverySettings,
) -> Result<Vec<DiscoveredStream>, String> {
    let schema_name = settings.schema.as_deref().unwrap_or("public");

    let table_rows = query_catalog_rows(client, schema_name).await?;
    let primary_keys = query_primary_keys(client, schema_name).await?;

    let published_tables = match settings.publication.as_deref() {
        Some(publication_name) => Some(
            query_publication_tables(client, schema_name, publication_name).await?,
        ),
        None => None,
    };

    Ok(build_discovered_streams(
        table_rows,
        primary_keys,
        settings.publication.as_deref(),
        published_tables.as_ref(),
    ))
}

/// Query column metadata for a single table in the `public` schema.
pub(crate) async fn query_table_columns(
    client: &Client,
    table_name: &str,
) -> Result<Vec<Column>, String> {
    let (schema_name, bare_table_name) = split_schema_and_table_name(table_name);
    query_table_columns_in_schema(client, schema_name, bare_table_name).await
}

fn split_schema_and_table_name(source_table_name: &str) -> (&str, &str) {
    let mut parts = source_table_name.rsplitn(2, '.');
    let table_name = parts.next().unwrap_or(source_table_name);
    let schema_name = parts.next().unwrap_or("public");
    (schema_name, table_name)
}

fn build_discovered_streams(
    mut table_rows: Vec<CatalogRow>,
    primary_keys: HashMap<(String, String), Vec<String>>,
    publication: Option<&str>,
    published_tables: Option<&HashSet<String>>,
) -> Vec<DiscoveredStream> {
    table_rows.sort_by(|left, right| {
        (left.schema.as_str(), left.table.as_str(), left.ordinal_position)
            .cmp(&(right.schema.as_str(), right.table.as_str(), right.ordinal_position))
    });

    let mut grouped: BTreeMap<(String, String), Vec<DiscoveryColumn>> = BTreeMap::new();
    for row in table_rows {
        grouped
            .entry((row.schema, row.table))
            .or_default()
            .push(DiscoveryColumn {
                name: row.column,
                pg_type: row.data_type,
                nullable: row.nullable,
                is_primary_key: false,
                is_generated: row.is_generated,
            });
    }

    let mut streams = Vec::with_capacity(grouped.len());
    for ((schema, table), mut columns) in grouped {
        let primary_key = primary_keys
            .get(&(schema.clone(), table.clone()))
            .cloned()
            .unwrap_or_default();
        let primary_key_set: HashSet<&str> = primary_key.iter().map(String::as_str).collect();
        for column in &mut columns {
            column.is_primary_key = primary_key_set.contains(column.name.as_str());
        }

        let stream = build_discovered_stream(&schema, &table, &columns, &primary_key, publication);
        streams.push(stream);
    }

    if let Some(published_tables) = published_tables {
        streams = filter_published_streams(streams, published_tables);
    }

    streams
}

fn build_discovered_stream(
    schema_name: &str,
    table_name: &str,
    columns: &[DiscoveryColumn],
    primary_key: &[String],
    publication: Option<&str>,
) -> DiscoveredStream {
    let fields = columns
        .iter()
        .map(DiscoveryColumn::to_schema_field)
        .collect::<Vec<_>>();
    let default_cursor_field = suggest_cursor_field(columns);
    let stream_name = if schema_name == "public" {
        table_name.to_string()
    } else {
        format!("{schema_name}.{table_name}")
    };

    DiscoveredStream {
        name: stream_name,
        schema: StreamSchema {
            fields,
            primary_key: primary_key.to_vec(),
            partition_keys: vec![],
            source_defined_cursor: default_cursor_field.clone(),
            schema_id: None,
        },
        supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc],
        default_cursor_field,
        estimated_row_count: None,
        metadata_json: build_metadata_json(schema_name, publication),
    }
}

fn build_metadata_json(schema_name: &str, publication: Option<&str>) -> Option<String> {
    let include_schema = schema_name != "public" || publication.is_some();
    if !include_schema {
        return None;
    }

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "schema".to_string(),
        serde_json::Value::String(schema_name.to_string()),
    );
    if let Some(publication) = publication {
        metadata.insert(
            "publication".to_string(),
            serde_json::Value::String(publication.to_string()),
        );
    }

    Some(serde_json::Value::Object(metadata).to_string())
}

fn filter_published_streams(
    streams: Vec<DiscoveredStream>,
    published_tables: &HashSet<String>,
) -> Vec<DiscoveredStream> {
    streams
        .into_iter()
        .filter(|stream| published_tables.contains(&stream.name))
        .collect()
}

fn suggest_cursor_field(columns: &[DiscoveryColumn]) -> Option<String> {
    let mut candidates = columns
        .iter()
        .filter(|column| !column.is_generated)
        .filter(|column| {
            let info = column.as_column();
            info.is_cursor_candidate()
        })
        .collect::<Vec<_>>();

    candidates.sort_by_key(|column| {
        let info = column.as_column();
        (info.cursor_suggestion_rank(), info.name.clone())
    });

    candidates.first().map(|column| column.name.clone())
}

async fn query_catalog_rows(
    client: &Client,
    schema_name: &str,
) -> Result<Vec<CatalogRow>, String> {
    let query = r"
        SELECT
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END AS nullable,
            CASE WHEN c.is_generated = 'ALWAYS' THEN true ELSE false END AS is_generated,
            c.ordinal_position
        FROM information_schema.tables t
        JOIN information_schema.columns c
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
        WHERE t.table_schema = $1
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
    ";

    let rows = client
        .query(query, &[&schema_name])
        .await
        .map_err(|e| format!("discovery query failed for schema '{schema_name}': {e}"))?;

    let mut catalog_rows = Vec::with_capacity(rows.len());
    for row in rows {
        catalog_rows.push(CatalogRow {
            schema: row.get(0),
            table: row.get(1),
            column: row.get(2),
            data_type: row.get(3),
            nullable: row.get(4),
            is_generated: row.get(5),
            ordinal_position: row.get(6),
        });
    }

    Ok(catalog_rows)
}

async fn query_primary_keys(
    client: &Client,
    schema_name: &str,
) -> Result<HashMap<(String, String), Vec<String>>, String> {
    let query = r"
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = $1
        ORDER BY kcu.ordinal_position
    ";

    let rows = client
        .query(query, &[&schema_name])
        .await
        .map_err(|e| format!("primary key discovery failed for schema '{schema_name}': {e}"))?;

    let mut primary_keys: HashMap<(String, String), Vec<String>> = HashMap::new();
    for row in rows {
        let key = (row.get::<_, String>(0), row.get::<_, String>(1));
        let column: String = row.get(2);
        primary_keys.entry(key).or_default().push(column);
    }

    Ok(primary_keys)
}

async fn query_publication_tables(
    client: &Client,
    schema_name: &str,
    publication_name: &str,
) -> Result<HashSet<String>, String> {
    let query = r"
        SELECT schemaname, tablename
        FROM pg_catalog.pg_publication_tables
        WHERE pubname = $1 AND schemaname = $2
    ";

    let rows = client
        .query(query, &[&publication_name, &schema_name])
        .await
        .map_err(|e| format!("publication discovery failed for '{publication_name}': {e}"))?;

    let mut tables = HashSet::with_capacity(rows.len());
    for row in rows {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        if schema == "public" {
            tables.insert(table);
        } else {
            tables.insert(format!("{schema}.{table}"));
        }
    }

    Ok(tables)
}

async fn query_table_columns_in_schema(
    client: &Client,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<Column>, String> {
    let query = r"
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    ";

    let rows = client
        .query(query, &[&schema_name, &table_name])
        .await
        .map_err(|e| format!("Schema query failed for {schema_name}.{table_name}: {e}"))?;

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
        return Err(format!(
            "Table '{schema_name}.{table_name}' not found or has no columns"
        ));
    }

    Ok(columns)
}

#[derive(Debug)]
struct CatalogRow {
    schema: String,
    table: String,
    column: String,
    data_type: String,
    nullable: bool,
    is_generated: bool,
    ordinal_position: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(
        name: &str,
        pg_type: &str,
        nullable: bool,
        is_primary_key: bool,
        is_generated: bool,
    ) -> DiscoveryColumn {
        DiscoveryColumn {
            name: name.to_string(),
            pg_type: pg_type.to_string(),
            nullable,
            is_primary_key,
            is_generated,
        }
    }

    #[test]
    fn build_discovered_stream_marks_primary_key_columns() {
        let columns = vec![
            column("id", "int4", false, true, false),
            column("name", "text", false, false, false),
        ];

        let stream = build_discovered_stream(
            "analytics",
            "users",
            &columns,
            &["id".to_string()],
            None,
        );

        assert_eq!(stream.name, "analytics.users");
        assert_eq!(stream.schema.primary_key, vec!["id"]);
        assert!(stream.schema.fields[0].is_primary_key);
        assert!(!stream.schema.fields[1].is_primary_key);
    }

    #[test]
    fn build_discovered_stream_marks_generated_columns() {
        let columns = vec![
            column("id", "int4", false, true, false),
            column("row_hash", "text", false, false, true),
        ];

        let stream = build_discovered_stream(
            "analytics",
            "events",
            &columns,
            &["id".to_string()],
            None,
        );

        assert!(stream.schema.fields[1].is_generated);
    }

    #[test]
    fn build_discovered_stream_records_non_public_schema_metadata() {
        let columns = vec![column("id", "int4", false, true, false)];

        let stream = build_discovered_stream(
            "reporting",
            "invoices",
            &columns,
            &["id".to_string()],
            None,
        );

        assert_eq!(stream.name, "reporting.invoices");
        assert_eq!(
            stream.metadata_json.as_deref(),
            Some(r#"{"schema":"reporting"}"#)
        );
    }

    #[test]
    fn discover_streams_preserves_primary_key_order() {
        let rows = vec![
            CatalogRow {
                schema: "analytics".to_string(),
                table: "orders".to_string(),
                column: "tenant_id".to_string(),
                data_type: "int4".to_string(),
                nullable: false,
                is_generated: false,
                ordinal_position: 1,
            },
            CatalogRow {
                schema: "analytics".to_string(),
                table: "orders".to_string(),
                column: "id".to_string(),
                data_type: "int4".to_string(),
                nullable: false,
                is_generated: false,
                ordinal_position: 2,
            },
            CatalogRow {
                schema: "analytics".to_string(),
                table: "orders".to_string(),
                column: "created_at".to_string(),
                data_type: "timestamp".to_string(),
                nullable: false,
                is_generated: false,
                ordinal_position: 3,
            },
        ];
        let primary_keys = HashMap::from([(
            ("analytics".to_string(), "orders".to_string()),
            vec!["tenant_id".to_string(), "id".to_string()],
        )]);

        let streams = build_discovered_streams(rows, primary_keys, None, None);
        let stream = &streams[0];

        assert_eq!(stream.name, "analytics.orders");
        assert_eq!(stream.schema.primary_key, vec!["tenant_id", "id"]);
        assert!(stream.schema.fields[0].is_primary_key);
        assert!(stream.schema.fields[1].is_primary_key);
    }

    #[test]
    fn discover_streams_filters_unpublished_tables() {
        let rows = vec![
            CatalogRow {
                schema: "public".to_string(),
                table: "users".to_string(),
                column: "id".to_string(),
                data_type: "int4".to_string(),
                nullable: false,
                is_generated: false,
                ordinal_position: 1,
            },
            CatalogRow {
                schema: "public".to_string(),
                table: "orders".to_string(),
                column: "id".to_string(),
                data_type: "int4".to_string(),
                nullable: false,
                is_generated: false,
                ordinal_position: 1,
            },
        ];
        let primary_keys = HashMap::from([
            (("public".to_string(), "users".to_string()), vec!["id".to_string()]),
            (("public".to_string(), "orders".to_string()), vec!["id".to_string()]),
        ]);
        let published = HashSet::from(["users".to_string()]);

        let streams = build_discovered_streams(rows, primary_keys, Some("rapidbyte_users"), Some(&published));

        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "users");
    }

    #[test]
    fn discover_streams_uses_non_public_schema_name() {
        let rows = vec![CatalogRow {
            schema: "analytics".to_string(),
            table: "events".to_string(),
            column: "id".to_string(),
            data_type: "int4".to_string(),
            nullable: false,
            is_generated: false,
            ordinal_position: 1,
        }];
        let primary_keys = HashMap::from([(
            ("analytics".to_string(), "events".to_string()),
            vec!["id".to_string()],
        )]);

        let streams = build_discovered_streams(rows, primary_keys, None, None);

        assert_eq!(streams[0].name, "analytics.events");
        assert_eq!(
            streams[0].metadata_json.as_deref(),
            Some(r#"{"schema":"analytics"}"#)
        );
    }

    #[test]
    fn cursor_field_suggestion_prefers_updated_at() {
        let columns = vec![
            column("id", "int4", false, true, false),
            column("created_at", "timestamp", false, false, false),
            column("updated_at", "timestamp", false, false, false),
            column("generated_at", "timestamp", false, false, true),
        ];

        assert_eq!(suggest_cursor_field(&columns), Some("updated_at".to_string()));
    }

    #[test]
    fn build_discovered_stream_preserves_primary_key_order() {
        let columns = vec![
            column("tenant_id", "int4", false, true, false),
            column("id", "int4", false, true, false),
            column("created_at", "timestamp", false, false, false),
        ];
        let stream = build_discovered_stream(
            "analytics",
            "orders",
            &columns,
            &["tenant_id".to_string(), "id".to_string()],
            None,
        );

        assert_eq!(stream.name, "analytics.orders");
        assert_eq!(stream.schema.primary_key, vec!["tenant_id", "id"]);
        assert!(stream.schema.fields[0].is_primary_key);
        assert!(stream.schema.fields[1].is_primary_key);
    }
}
