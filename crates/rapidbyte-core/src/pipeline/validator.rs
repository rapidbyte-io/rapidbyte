use anyhow::{bail, Result};

use crate::pipeline::types::PipelineConfig;

/// Validate a parsed pipeline configuration.
/// Returns Ok(()) if valid, Err with all validation errors if not.
pub fn validate_pipeline(config: &PipelineConfig) -> Result<()> {
    let mut errors = Vec::new();

    if config.version != "1.0" {
        errors.push(format!(
            "Unsupported pipeline version '{}', expected '1.0'",
            config.version
        ));
    }

    if config.pipeline.trim().is_empty() {
        errors.push("Pipeline name must not be empty".to_string());
    }

    if config.source.use_ref.trim().is_empty() {
        errors.push("Source connector reference (use) must not be empty".to_string());
    }

    if config.source.streams.is_empty() {
        errors.push("Source must define at least one stream".to_string());
    }

    for (i, stream) in config.source.streams.iter().enumerate() {
        if stream.name.trim().is_empty() {
            errors.push(format!("Stream {} has an empty name", i));
        }
        match stream.sync_mode.as_str() {
            "full_refresh" | "incremental" => {}
            other => {
                errors.push(format!(
                    "Stream '{}' has invalid sync_mode '{}', expected 'full_refresh' or 'incremental'",
                    stream.name, other
                ));
            }
        }
        if stream.sync_mode == "incremental" && stream.cursor_field.is_none() {
            errors.push(format!(
                "Stream '{}' uses incremental sync but has no cursor_field",
                stream.name
            ));
        }
    }

    if config.destination.use_ref.trim().is_empty() {
        errors.push("Destination connector reference (use) must not be empty".to_string());
    }

    match config.destination.write_mode.as_str() {
        "append" | "replace" | "upsert" => {}
        other => {
            errors.push(format!(
                "Invalid destination write_mode '{}', expected 'append', 'replace', or 'upsert'",
                other
            ));
        }
    }

    if config.destination.write_mode == "upsert" && config.destination.primary_key.is_empty() {
        errors.push(
            "Destination write_mode 'upsert' requires at least one primary_key field".to_string(),
        );
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!(
            "Pipeline validation failed:\n  - {}",
            errors.join("\n  - ")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::parser::parse_pipeline_str;

    fn valid_yaml() -> &'static str {
        r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#
    }

    #[test]
    fn test_valid_pipeline_passes() {
        let config = parse_pipeline_str(valid_yaml()).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_wrong_version_fails() {
        let yaml = valid_yaml().replace("\"1.0\"", "\"2.0\"");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Unsupported pipeline version"));
    }

    #[test]
    fn test_empty_pipeline_name_fails() {
        let yaml = valid_yaml().replace("test_pipeline", "");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Pipeline name must not be empty"));
    }

    #[test]
    fn test_empty_source_ref_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: ""
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Source connector reference"));
    }

    #[test]
    fn test_no_streams_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config:
    host: localhost
  streams: []
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("at least one stream"));
    }

    #[test]
    fn test_invalid_sync_mode_fails() {
        let yaml = valid_yaml().replace("full_refresh", "invalid_mode");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("invalid sync_mode"));
    }

    #[test]
    fn test_incremental_without_cursor_fails() {
        let yaml = valid_yaml().replace("full_refresh", "incremental");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("no cursor_field"));
    }

    #[test]
    fn test_incremental_with_cursor_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_invalid_write_mode_fails() {
        let yaml = valid_yaml().replace("append", "delete_all");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Invalid destination write_mode"));
    }

    #[test]
    fn test_upsert_without_primary_key_fails() {
        let yaml = valid_yaml().replace("append", "upsert");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("requires at least one primary_key"));
    }

    #[test]
    fn test_multiple_errors_all_reported() {
        let yaml = r#"
version: "2.0"
pipeline: ""
source:
  use: ""
  config: {}
  streams: []
destination:
  use: ""
  config: {}
  write_mode: bad
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Unsupported pipeline version"));
        assert!(err.contains("Pipeline name must not be empty"));
        assert!(err.contains("Source connector reference"));
        assert!(err.contains("at least one stream"));
        assert!(err.contains("Destination connector reference"));
        assert!(err.contains("Invalid destination write_mode"));
    }
}
