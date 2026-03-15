//! Pipeline YAML parsing with variable substitution.
//!
//! Supports two kinds of references in pipeline YAML:
//! - `${ENV_VAR}` — resolved from the process environment
//! - `${prefix:path#key}` — resolved via a registered [`SecretProvider`]

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use anyhow::Result;
use rapidbyte_secrets::SecretProviders;
use regex::Regex;

use crate::config::types::PipelineConfig;

/// Matches `${WORD}` (env var) but NOT `${prefix:...}` (secret ref).
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid env var regex"));

/// Matches `${prefix:path#key}` (secret provider reference).
static SECRET_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([a-z]+):([^#\}]+)#([^}]+)\}").expect("valid secret ref regex")
});

/// Returns `true` if `input` contains any `${prefix:...}` secret references.
pub fn contains_secret_refs(input: &str) -> bool {
    SECRET_REF_RE.is_match(input)
}

/// Substitute all `${...}` references: env vars and secret provider refs.
///
/// Each unique secret path is fetched once even if referenced multiple times.
/// Resolution is atomic — all references must resolve or the entire
/// substitution fails.
///
/// # Errors
///
/// Returns an error if any env var is missing, a secret provider prefix
/// has no registered provider, or a secret read fails.
pub async fn substitute_variables(input: &str, secrets: &SecretProviders) -> Result<String> {
    let mut secret_values: HashMap<String, String> = HashMap::new();
    let mut secret_errors = Vec::new();

    // Resolve secret refs
    let mut seen = HashSet::new();
    for cap in SECRET_REF_RE.captures_iter(input) {
        let full_match = cap[0].to_string();
        if seen.insert(full_match.clone()) {
            let prefix = &cap[1];
            let path = &cap[2];
            let key = &cap[3];
            match secrets.resolve(prefix, path, key).await {
                Ok(val) => {
                    secret_values.insert(full_match, val);
                }
                Err(e) => {
                    secret_errors.push(format!("{prefix}:{path}#{key}: {e}"));
                }
            }
        }
    }

    if !secret_errors.is_empty() {
        anyhow::bail!(
            "Failed to resolve secret(s):\n  {}",
            secret_errors.join("\n  ")
        );
    }

    // Replace secret refs first, then env vars.
    let mut result = input.to_string();
    for (pattern, value) in &secret_values {
        result = result.replace(pattern, value);
    }

    // Resolve env vars
    let mut env_errors = Vec::new();
    let snapshot = result.clone();
    for cap in ENV_VAR_RE.captures_iter(&snapshot) {
        let var_name = &cap[1];
        match std::env::var(var_name) {
            Ok(val) => {
                result = result.replace(&cap[0], &val);
            }
            Err(_) => {
                env_errors.push(var_name.to_string());
            }
        }
    }

    if !env_errors.is_empty() {
        anyhow::bail!("Missing environment variable(s): {}", env_errors.join(", "));
    }

    Ok(result)
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
    // Reject secret refs when no providers are configured.
    if secrets.is_empty() && contains_secret_refs(yaml_str) {
        anyhow::bail!(
            "pipeline contains secret references (${{vault:...}}) \
             but no secret provider is configured"
        );
    }

    let has_secrets = contains_secret_refs(yaml_str);
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
    async fn env_var_substitution() {
        std::env::set_var("RB_TEST_HOST", "myhost.example.com");
        let input = "host: ${RB_TEST_HOST}\nport: 5432";
        let result = substitute_variables(input, &empty_secrets()).await.unwrap();
        assert!(result.contains("myhost.example.com"));
        assert!(!result.contains("${RB_TEST_HOST}"));
        std::env::remove_var("RB_TEST_HOST");
    }

    #[tokio::test]
    async fn multiple_env_vars() {
        std::env::set_var("RB_TEST_A", "alpha");
        std::env::set_var("RB_TEST_B", "beta");
        let input = "${RB_TEST_A} and ${RB_TEST_B}";
        let result = substitute_variables(input, &empty_secrets()).await.unwrap();
        assert_eq!(result, "alpha and beta");
        std::env::remove_var("RB_TEST_A");
        std::env::remove_var("RB_TEST_B");
    }

    #[tokio::test]
    async fn no_refs_passthrough() {
        let input = "host: localhost\nport: 5432";
        let result = substitute_variables(input, &empty_secrets()).await.unwrap();
        assert_eq!(result, input);
    }

    #[tokio::test]
    async fn missing_env_var_errors() {
        let input = "host: ${RB_DEFINITELY_NOT_SET_12345}";
        let result = substitute_variables(input, &empty_secrets()).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("RB_DEFINITELY_NOT_SET_12345"));
    }

    #[tokio::test]
    async fn multiple_missing_env_vars_all_reported() {
        let input = "${RB_MISSING_X} and ${RB_MISSING_Y}";
        let result = substitute_variables(input, &empty_secrets()).await;
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("RB_MISSING_X"));
        assert!(msg.contains("RB_MISSING_Y"));
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

    #[test]
    fn secret_ref_regex_matches() {
        assert!(contains_secret_refs("${vault:secret/pg#password}"));
        assert!(contains_secret_refs("${aws:arn:something#key}"));
        assert!(!contains_secret_refs("${NORMAL_ENV_VAR}"));
        assert!(!contains_secret_refs("no refs here"));
    }
}
