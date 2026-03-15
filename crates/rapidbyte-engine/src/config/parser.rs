//! Pipeline YAML parsing with environment variable substitution.

use std::path::Path;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;

use crate::config::types::PipelineConfig;

static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid env var regex"));

static VAULT_REF_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{vault:([^#\}]+)#([^}]+)\}").expect("valid vault ref regex"));

/// Returns `true` if `input` contains any `${vault:...}` references.
fn contains_vault_refs(input: &str) -> bool {
    VAULT_REF_RE.is_match(input)
}

/// Substitute `${VAR_NAME}` patterns with environment variable values.
///
/// # Errors
///
/// Returns an error if any referenced environment variable is not set.
pub fn substitute_env_vars(input: &str) -> Result<String> {
    let mut result = input.to_string();
    let mut errors = Vec::new();

    for cap in ENV_VAR_RE.captures_iter(input) {
        let var_name = &cap[1];
        match std::env::var(var_name) {
            Ok(val) => {
                result = result.replace(&cap[0], &val);
            }
            Err(_) => {
                errors.push(var_name.to_string());
            }
        }
    }

    if !errors.is_empty() {
        anyhow::bail!("Missing environment variable(s): {}", errors.join(", "));
    }

    Ok(result)
}

/// Parse a pipeline YAML string (after env var substitution).
///
/// # Errors
///
/// Returns an error if env var substitution fails or the YAML is invalid.
pub fn parse_pipeline_str(yaml_str: &str) -> Result<PipelineConfig> {
    if contains_vault_refs(yaml_str) {
        anyhow::bail!(
            "pipeline contains ${{vault:...}} references but no Vault client is configured"
        );
    }
    let substituted = substitute_env_vars(yaml_str)?;
    let config: PipelineConfig =
        serde_yaml::from_str(&substituted).context("Failed to parse pipeline YAML")?;
    Ok(config)
}

/// Parse a pipeline YAML file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or the YAML is invalid.
pub fn parse_pipeline(path: &Path) -> Result<PipelineConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {}", path.display()))?;
    parse_pipeline_str(&content)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("RB_TEST_HOST", "myhost.example.com");
        let input = "host: ${RB_TEST_HOST}\nport: 5432";
        let result = substitute_env_vars(input).unwrap();
        assert!(result.contains("myhost.example.com"));
        assert!(!result.contains("${RB_TEST_HOST}"));
        std::env::remove_var("RB_TEST_HOST");
    }

    #[test]
    fn test_multiple_env_vars() {
        std::env::set_var("RB_TEST_A", "alpha");
        std::env::set_var("RB_TEST_B", "beta");
        let input = "${RB_TEST_A} and ${RB_TEST_B}";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "alpha and beta");
        std::env::remove_var("RB_TEST_A");
        std::env::remove_var("RB_TEST_B");
    }

    #[test]
    fn test_no_env_vars_passthrough() {
        let input = "host: localhost\nport: 5432";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_missing_env_var_errors() {
        let input = "host: ${RB_DEFINITELY_NOT_SET_12345}";
        let result = substitute_env_vars(input);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("RB_DEFINITELY_NOT_SET_12345"));
    }

    #[test]
    fn test_multiple_missing_env_vars_all_reported() {
        let input = "${RB_MISSING_X} and ${RB_MISSING_Y}";
        let result = substitute_env_vars(input);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("RB_MISSING_X"));
        assert!(err_msg.contains("RB_MISSING_Y"));
    }

    #[test]
    fn test_parse_pipeline_from_string() {
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
        let config = parse_pipeline_str(yaml).unwrap();
        assert_eq!(config.source.config["host"], "localhost");
        assert_eq!(config.source.config["password"], "secret");
        assert_eq!(config.pipeline, "test");
        std::env::remove_var("RB_TEST_PG_HOST");
        std::env::remove_var("RB_TEST_PG_PASS");
    }

    #[test]
    fn test_parse_invalid_yaml_errors() {
        let yaml = "this is not: [valid: yaml: {{{}}}";
        let result = parse_pipeline_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_parser_rejects_vault_refs() {
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
        let result = parse_pipeline_str(yaml);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Vault client"),
            "should mention Vault client"
        );
    }

    #[test]
    fn test_vault_ref_regex_matches() {
        assert!(contains_vault_refs("${vault:secret/pg#password}"));
        assert!(contains_vault_refs("host: ${vault:secret/data/pg#host}"));
        assert!(!contains_vault_refs("${NORMAL_ENV_VAR}"));
        assert!(!contains_vault_refs("no refs here"));
    }

    #[test]
    fn test_parse_pipeline_file_not_found() {
        let result = parse_pipeline(Path::new("/nonexistent/pipeline.yaml"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to read pipeline file"));
    }
}
