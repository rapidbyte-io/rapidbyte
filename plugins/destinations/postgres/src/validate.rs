//! Destination `PostgreSQL` compatibility validation.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::arrow::datatypes::{DataType, TimeUnit};
use rapidbyte_sdk::validation::ValidationReport;

use crate::config::Config;
use crate::contract::preflight_schema_from_stream_schema;

fn write_path_supports(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Null
    )
}

fn validate_schema_compatibility(schema: &StreamSchema) -> Result<(), String> {
    let Some(arrow_schema) = preflight_schema_from_stream_schema(schema)? else {
        return Ok(());
    };

    let incompatible: Vec<String> = schema
        .fields
        .iter()
        .zip(arrow_schema.fields())
        .filter(|(_, field)| !write_path_supports(field.data_type()))
        .map(|(source_field, parsed_field)| {
            format!(
                "{}: {} ({:?})",
                source_field.name,
                source_field.arrow_type,
                parsed_field.data_type()
            )
        })
        .collect();

    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unsupported field type(s) for write path: {}",
            incompatible.join(", ")
        ))
    }
}

pub(crate) fn validate(
    config: &Config,
    upstream: Option<&StreamSchema>,
) -> Result<ValidationReport, PluginError> {
    if let Some(schema) = upstream {
        if let Err(message) = validate_schema_compatibility(schema) {
            return Ok(ValidationReport::failed(&format!(
                "destination schema is incompatible with upstream schema: {message}"
            )));
        }
    }

    let mut report = ValidationReport::success(&format!(
        "PostgreSQL destination is compatible with schema '{}'",
        config.target_schema()
    ));
    if let Some(schema) = upstream {
        report = report.with_output_schema(schema.clone());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::schema::{SchemaField, StreamSchema};
    use rapidbyte_sdk::validation::ValidationStatus;

    use super::*;

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            load_method: crate::config::LoadMethod::Copy,
            copy_flush_bytes: None,
        }
    }

    fn upstream_schema() -> StreamSchema {
        StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        }
    }

    #[test]
    fn validate_returns_output_schema_for_upstream_compatibility() {
        let schema = upstream_schema();

        let report = validate(&base_config(), Some(&schema)).expect("validation");

        assert_eq!(report.status, ValidationStatus::Success);
        assert_eq!(report.output_schema, Some(schema));
    }

    #[test]
    fn validate_omits_output_schema_when_upstream_is_missing() {
        let report = validate(&base_config(), None).expect("validation");

        assert_eq!(report.status, ValidationStatus::Success);
        assert!(report.output_schema.is_none());
    }

    #[test]
    fn validate_rejects_unsupported_arrow_types() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("bad_utf8", "large_utf8", true),
                SchemaField::new("bad_ts", "timestamp_millis", true),
                SchemaField::new("bad_uint", "uint8", true),
            ],
            primary_key: vec![],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };

        let report = validate(&base_config(), Some(&schema)).expect("validation");

        assert_eq!(report.status, ValidationStatus::Failed);
        assert!(report.message.contains("large_utf8"));
        assert!(report.message.contains("timestamp_millis"));
        assert!(report.message.contains("uint8"));
    }

    #[test]
    fn validate_rejects_timestamp_precision_not_supported_by_write_path() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("bad_ts", "timestamp_nanos", true)],
            primary_key: vec![],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };

        let report = validate(&base_config(), Some(&schema)).expect("validation");

        assert_eq!(report.status, ValidationStatus::Failed);
        assert!(report.message.contains("timestamp_nanos"));
    }
}
