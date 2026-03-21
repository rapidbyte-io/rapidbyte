//! Source schema discovery subcommand (discover).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_types::discovery::DiscoveredStream;
use rapidbyte_types::wire::SyncMode;

use crate::Verbosity;

/// Execute the `discover` command: discover available streams from a source plugin.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or schema discovery fails.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    pipeline_path: &Path,
    verbosity: Verbosity,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path, secrets).await?;

    // Discover typed streams from the source plugin
    let ctx = rapidbyte_engine::build_discover_context(registry_config).await?;
    let discovered_streams = rapidbyte_engine::discover_plugin(
        &ctx,
        &config.source.use_ref,
        Some(&config.source.config),
    )
    .await
    .map_err(anyhow::Error::from)?;

    // Human-readable output to stderr (skip in quiet mode)
    if verbosity != Verbosity::Quiet {
        let count = discovered_streams.len();
        eprintln!(
            "{} Discovered {} stream{}",
            style("\u{2713}").green().bold(),
            count,
            if count == 1 { "" } else { "s" },
        );
        eprintln!();

        // Compute column widths for the table
        let name_width = discovered_streams
            .iter()
            .map(|s| s.name.len())
            .max()
            .unwrap_or(6)
            .max(6);
        let sync_width = discovered_streams
            .iter()
            .map(|s| sync_label(s.supported_sync_modes.as_slice()).len())
            .max()
            .unwrap_or(4)
            .max(4);
        let cursor_width = discovered_streams
            .iter()
            .map(|s| cursor_label(s).len())
            .max()
            .unwrap_or(6)
            .max(6);

        // Header
        eprintln!(
            "  {:<name_w$}   {:<sync_w$}   {:<cursor_w$}   Columns",
            style("Stream").bold(),
            style("Sync").bold(),
            style("Cursor").bold(),
            name_w = name_width,
            sync_w = sync_width,
            cursor_w = cursor_width,
        );
        let rule_len = name_width + sync_width + cursor_width + 18;
        eprintln!("  {}", "\u{2500}".repeat(rule_len));

        for stream in &discovered_streams {
            let sync = sync_label(&stream.supported_sync_modes);
            let cursor = cursor_label(stream);
            let cols = stream.schema.fields.len();

            eprintln!(
                "  {:<name_w$}   {:<sync_w$}   {:<cursor_w$}   {}",
                stream.name,
                sync,
                cursor,
                cols,
                name_w = name_width,
                sync_w = sync_width,
                cursor_w = cursor_width,
            );

            // Verbose mode: show full schema per stream
            if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
                for col in &stream.schema.fields {
                    let nullable = if col.nullable {
                        style("NULL").dim()
                    } else {
                        style("NOT NULL").dim()
                    };
                    eprintln!(
                        "  {pad:>name_w$}     {:<24} {:<12} {}",
                        style(&col.name).cyan(),
                        col.arrow_type,
                        nullable,
                        pad = "",
                        name_w = name_width,
                    );
                }
            }
        }

        eprintln!();
    }

    // Machine-readable JSON on stdout (always emitted)
    let json = machine_readable_catalog_json(&discovered_streams)?;
    println!("@@CATALOG_JSON@@{json}");

    Ok(())
}

/// Return a human-friendly label for the "best" sync mode supported.
fn sync_label(modes: &[SyncMode]) -> &'static str {
    if modes.contains(&SyncMode::Cdc) {
        "cdc"
    } else if modes.contains(&SyncMode::Incremental) {
        "incremental"
    } else {
        "full"
    }
}

fn cursor_label(stream: &DiscoveredStream) -> &str {
    stream
        .default_cursor_field
        .as_deref()
        .or(stream.schema.source_defined_cursor.as_deref())
        .unwrap_or("\u{2014}")
}

struct CatalogOutput {
    streams: Vec<CatalogStreamOutput>,
}

struct CatalogStreamOutput {
    name: String,
    schema: Vec<CatalogColumnOutput>,
    supported_sync_modes: Vec<SyncMode>,
    source_defined_cursor: Option<String>,
    source_defined_primary_key: Option<Vec<String>>,
}

struct CatalogColumnOutput {
    name: String,
    data_type: serde_json::Value,
    nullable: bool,
}

fn machine_readable_catalog_json(streams: &[DiscoveredStream]) -> Result<String> {
    let output = CatalogOutput {
        streams: streams
            .iter()
            .map(|stream| CatalogStreamOutput {
                name: stream.name.clone(),
                schema: stream
                    .schema
                    .fields
                    .iter()
                    .map(|field| CatalogColumnOutput {
                        name: field.name.clone(),
                        data_type: catalog_data_type_value(&field.arrow_type),
                        nullable: field.nullable,
                    })
                    .collect(),
                supported_sync_modes: stream.supported_sync_modes.clone(),
                source_defined_cursor: stream
                    .default_cursor_field
                    .clone()
                    .or_else(|| stream.schema.source_defined_cursor.clone()),
                source_defined_primary_key: (!stream.schema.primary_key.is_empty())
                    .then(|| stream.schema.primary_key.clone()),
            })
            .collect(),
    };
    let json = serde_json::json!({
        "streams": output.streams.into_iter().map(|stream| {
            let mut value = serde_json::json!({
                "name": stream.name,
                "schema": stream.schema.into_iter().map(|field| serde_json::json!({
                    "name": field.name,
                    "data_type": field.data_type,
                    "nullable": field.nullable,
                })).collect::<Vec<_>>(),
                "supported_sync_modes": stream.supported_sync_modes,
            });
            if let Some(cursor) = stream.source_defined_cursor {
                value["source_defined_cursor"] = serde_json::Value::String(cursor);
            }
            if let Some(pk) = stream.source_defined_primary_key {
                value["source_defined_primary_key"] = serde_json::to_value(pk)?;
            }
            Ok(value)
        }).collect::<Result<Vec<_>>>()?,
    });
    Ok(serde_json::to_string(&json)?)
}

fn catalog_data_type_value(name: &str) -> serde_json::Value {
    let canonical = match name {
        "boolean" => Some("Boolean"),
        "int8" => Some("Int8"),
        "int16" => Some("Int16"),
        "int32" => Some("Int32"),
        "int64" => Some("Int64"),
        "uint8" => Some("UInt8"),
        "uint16" => Some("UInt16"),
        "uint32" => Some("UInt32"),
        "uint64" => Some("UInt64"),
        "float16" => Some("Float16"),
        "float32" => Some("Float32"),
        "float64" => Some("Float64"),
        "utf8" => Some("Utf8"),
        "large_utf8" => Some("LargeUtf8"),
        "binary" => Some("Binary"),
        "large_binary" => Some("LargeBinary"),
        "date32" => Some("Date32"),
        "date64" => Some("Date64"),
        "timestamp_millis" => Some("TimestampMillis"),
        "timestamp_micros" => Some("TimestampMicros"),
        "timestamp_nanos" => Some("TimestampNanos"),
        "decimal128" => Some("Decimal128"),
        "json" => Some("Json"),
        _ => None,
    };

    serde_json::Value::String(canonical.unwrap_or(name).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::schema::{SchemaField, StreamSchema};

    #[test]
    fn machine_readable_output_preserves_catalog_marker_shape() {
        let streams = vec![DiscoveredStream {
            name: "users".into(),
            schema: StreamSchema {
                fields: vec![SchemaField::new("id", "int64", false)],
                primary_key: vec!["id".into()],
                partition_keys: vec![],
                source_defined_cursor: Some("updated_at".into()),
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
            default_cursor_field: Some("updated_at".into()),
            estimated_row_count: Some(10),
            metadata_json: Some(r#"{"schema":"public"}"#.into()),
        }];

        let json = machine_readable_catalog_json(&streams).expect("catalog json should serialize");
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("catalog json should parse");

        assert!(value["streams"].is_array());
        assert_eq!(value["streams"][0]["name"], "users");
        assert_eq!(value["streams"][0]["schema"][0]["name"], "id");
        assert_eq!(value["streams"][0]["schema"][0]["data_type"], "Int64");
        assert_eq!(value["streams"][0]["source_defined_cursor"], "updated_at");
    }

    #[test]
    fn machine_readable_output_preserves_unknown_schema_type_strings() {
        let streams = vec![DiscoveredStream {
            name: "events".into(),
            schema: StreamSchema {
                fields: vec![SchemaField::new("occurred_at", "timestamp_tz", false)],
                primary_key: vec![],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }];

        let json = machine_readable_catalog_json(&streams).expect("catalog json should serialize");
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("catalog json should parse");

        assert_eq!(
            value["streams"][0]["schema"][0]["data_type"],
            "timestamp_tz"
        );
    }
}
