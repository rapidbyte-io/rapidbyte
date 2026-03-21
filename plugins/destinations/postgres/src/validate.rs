//! Destination `PostgreSQL` compatibility validation.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::validation::ValidationReport;

use crate::config::Config;

pub(crate) fn validate(
    config: &Config,
    upstream: Option<&StreamSchema>,
) -> Result<ValidationReport, PluginError> {
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
}
