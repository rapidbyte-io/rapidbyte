//! Pipeline YAML parsing with variable substitution.
//!
//! Supports two kinds of references in pipeline YAML:
//! - `${ENV_VAR}` — resolved from the process environment
//! - `${prefix:path#key}` — resolved via a registered [`SecretProvider`]

use anyhow::{Context, Result};
use rapidbyte_secrets::SecretProviders;

use crate::config::types::PipelineConfig;

// Re-export variable substitution functions from the shared crate.
pub use rapidbyte_pipeline_config::{
    contains_secret_refs, extract_pipeline_name, reject_malformed_refs, substitute_secrets,
    substitute_variables,
};

/// Parse pre-resolved pipeline YAML without any variable substitution.
///
/// Used by the agent for YAML received from the controller, which has
/// already resolved all `${...}` references. No env vars or secrets
/// are expanded — the YAML is parsed as-is.
///
/// # Errors
///
/// Returns an error if the YAML is invalid.
pub fn parse_resolved(yaml_str: &str) -> Result<PipelineConfig> {
    serde_yaml::from_str(yaml_str).context("Failed to parse pipeline YAML")
}

/// Parse a pipeline YAML string, resolving all variable references.
///
/// Resolves `${ENV_VAR}` from the environment and `${prefix:path#key}`
/// from registered secret providers. After secret resolution, YAML parse
/// errors are redacted to prevent secret leakage.
///
/// # Errors
///
/// Returns an error if variable substitution, YAML parsing, or
/// validation fails.
pub async fn parse_pipeline(yaml_str: &str, secrets: &SecretProviders) -> Result<PipelineConfig> {
    // Reject malformed secret references (e.g. ${vault:path} without #key).
    reject_malformed_refs(yaml_str)?;

    let has_secrets = contains_secret_refs(yaml_str);

    // Reject secret refs when no providers are configured.
    if secrets.is_empty() && has_secrets {
        anyhow::bail!(
            "pipeline contains secret references (${{vault:...}}) \
             but no secret provider is configured"
        );
    }
    let substituted = substitute_variables(yaml_str, secrets).await?;

    serde_yaml::from_str(&substituted).map_err(|e| {
        if has_secrets {
            // Redact source to prevent secret leakage in error messages.
            anyhow::anyhow!(
                "pipeline YAML parsing failed after secret resolution \
                 (source redacted): line {}, column {}",
                e.location().map_or(0, |l| l.line()),
                e.location().map_or(0, |l| l.column()),
            )
        } else {
            anyhow::anyhow!(e).context("Failed to parse pipeline YAML")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_secrets() -> SecretProviders {
        SecretProviders::new()
    }

    #[tokio::test]
    async fn parse_pipeline_from_string() {
        std::env::set_var("RB_TEST_PG_HOST", "localhost");
        std::env::set_var("RB_TEST_PG_PASS", "secret");
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config:
    host: ${RB_TEST_PG_HOST}
    password: ${RB_TEST_PG_PASS}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline(yaml, &empty_secrets()).await.unwrap();
        assert_eq!(config.source.config["host"], "localhost");
        assert_eq!(config.source.config["password"], "secret");
        assert_eq!(config.pipeline, "test");
        std::env::remove_var("RB_TEST_PG_HOST");
        std::env::remove_var("RB_TEST_PG_PASS");
    }

    #[tokio::test]
    async fn parse_invalid_yaml_errors() {
        let yaml = "this is not: [valid: yaml: {{{}}}";
        let result = parse_pipeline(yaml, &empty_secrets()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn secret_refs_rejected_without_provider() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config:
    password: ${vault:secret/postgres#password}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let result = parse_pipeline(yaml, &empty_secrets()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("secret provider"));
    }
}
