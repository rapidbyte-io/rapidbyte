//! Semantic validation for parsed pipeline configuration values.

use anyhow::{bail, Result};
use rapidbyte_types::protocol::SyncMode;

use crate::pipeline::types::{PipelineConfig, PipelineWriteMode};

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
        if stream.sync_mode == SyncMode::Incremental && stream.cursor_field.is_none() {
            errors.push(format!(
                "Stream '{}' uses incremental sync but has no cursor_field",
                stream.name
            ));
        }
    }

    if config.destination.use_ref.trim().is_empty() {
        errors.push("Destination connector reference (use) must not be empty".to_string());
    }

    if config.destination.write_mode == PipelineWriteMode::Upsert
        && config.destination.primary_key.is_empty()
    {
        errors.push(
            "Destination write_mode 'upsert' requires at least one primary_key field".to_string(),
        );
    }

    if config.resources.max_inflight_batches == 0 {
        errors.push("max_inflight_batches must be at least 1".to_string());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("Pipeline validation failed:\n  - {}", errors.join("\n  - "));
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
    fn test_incremental_without_cursor_fails() {
        let yaml = valid_yaml().replace("full_refresh", "incremental");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("no cursor_field"));
    }

    #[test]
    fn test_upsert_without_primary_key_fails() {
        let yaml = valid_yaml().replace("append", "upsert");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("requires at least one primary_key"));
    }

    #[test]
    fn test_max_inflight_batches_zero_fails() {
        let yaml = r#"
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
resources:
  max_inflight_batches: 0
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("max_inflight_batches"));
    }

    #[test]
    fn test_upsert_with_primary_key_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_upsert
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
  write_mode: upsert
  primary_key: [id]
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_incremental_with_cursor_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_incr
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: id
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
    fn test_replace_write_mode_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_replace
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
  write_mode: replace
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }
}
