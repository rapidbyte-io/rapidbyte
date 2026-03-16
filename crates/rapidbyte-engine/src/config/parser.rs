//! Pipeline YAML parsing with variable substitution.
//!
//! Supports two kinds of references in pipeline YAML:
//! - `${ENV_VAR}` — resolved from the process environment
//! - `${prefix:path#key}` — resolved via a registered [`SecretProvider`]

use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use rapidbyte_secrets::{SecretError, SecretProviders};
use regex::Regex;

use crate::config::types::PipelineConfig;

/// Matches `${WORD}` (env var) but NOT `${prefix:...}` (secret ref).
static ENV_VAR_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid env var regex"));

/// Matches `${prefix:path#key}` (secret provider reference).
/// Case-insensitive prefix; allows letters, digits, hyphens, underscores
/// (e.g. `vault`, `aws-sm`, `gcp_secrets`, `vault1`).
static SECRET_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\$\{([a-z][a-z0-9_-]*):([^#\}]+)#([^}]+)\}").expect("valid secret ref regex")
});

/// Matches `${prefix:...}` patterns that are NOT well-formed secret refs
/// (e.g. missing `#key`). Used to reject malformed references.
/// Same prefix rules as `SECRET_REF_RE`.
static MALFORMED_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\$\{([a-z][a-z0-9_-]*):[^}]*\}").expect("valid malformed ref regex")
});

/// Returns `true` if `input` contains any `${prefix:...}` secret references.
pub fn contains_secret_refs(input: &str) -> bool {
    SECRET_REF_RE.is_match(input)
}

/// Check for malformed `${prefix:...}` patterns (e.g. missing `#key`).
///
/// # Errors
///
/// Returns an error listing all malformed references found.
pub fn reject_malformed_refs(input: &str) -> Result<()> {
    let mut malformed = Vec::new();
    for cap in MALFORMED_REF_RE.captures_iter(input) {
        let full = &cap[0];
        // If the well-formed regex doesn't match this, it's malformed.
        if !SECRET_REF_RE.is_match(full) {
            malformed.push(full.to_string());
        }
    }
    if !malformed.is_empty() {
        anyhow::bail!(
            "malformed secret reference(s) (expected ${{prefix:path#key}}): {}",
            malformed.join(", ")
        );
    }
    Ok(())
}

/// Collected match with byte range for positional replacement.
struct MatchReplacement {
    start: usize,
    end: usize,
    value: String,
}

/// Apply positional replacements right-to-left so indices stay valid.
///
/// This avoids global `String::replace` which would expand patterns
/// that appear inside replacement values (e.g. a secret value
/// containing `${HOST}` should NOT be expanded as an env var).
fn apply_replacements(input: &str, mut replacements: Vec<MatchReplacement>) -> String {
    replacements.sort_by(|a, b| b.start.cmp(&a.start));
    let mut result = input.to_string();
    for r in replacements {
        result.replace_range(r.start..r.end, &r.value);
    }
    result
}

/// Substitute only `${prefix:path#key}` secret references, leaving
/// `${ENV_VAR}` patterns untouched.
///
/// Used by the controller at dispatch time: secrets are resolved
/// centrally, but env vars are left for the agent to expand from its
/// own environment.
///
/// # Errors
///
/// Returns an error if a secret provider prefix has no registered
/// provider or a secret read fails.
#[allow(clippy::missing_panics_doc)] // regex group 0 always exists
pub async fn substitute_secrets(input: &str, secrets: &SecretProviders) -> Result<String> {
    let mut replacements = Vec::new();
    let mut secret_errors: Vec<(String, SecretError)> = Vec::new();

    collect_secret_replacements(input, secrets, &mut replacements, &mut secret_errors).await;

    if !secret_errors.is_empty() {
        let any_transient = secret_errors.iter().any(|(_, e)| e.is_transient());
        let msg = format!(
            "Failed to resolve secret(s):\n  {}",
            secret_errors
                .iter()
                .map(|(ref_name, e)| format!("{ref_name}: {e}"))
                .collect::<Vec<_>>()
                .join("\n  ")
        );
        if any_transient {
            return Err(SecretError::Unavailable(msg).into());
        }
        anyhow::bail!("{msg}");
    }

    Ok(apply_replacements(input, replacements))
}

/// Shared logic for collecting secret-ref replacements. Normalizes
/// captured prefixes to lowercase so `${Vault:...}` matches the
/// `"vault"` provider registration.
async fn collect_secret_replacements(
    input: &str,
    secrets: &SecretProviders,
    replacements: &mut Vec<MatchReplacement>,
    secret_errors: &mut Vec<(String, SecretError)>,
) {
    let mut secret_cache: HashMap<String, String> = HashMap::new();

    for cap in SECRET_REF_RE.captures_iter(input) {
        let m = cap.get(0).unwrap();
        let full_match = m.as_str().to_string();
        let prefix = cap[1].to_ascii_lowercase();
        let path = &cap[2];
        let key = &cap[3];

        let value = if let Some(cached) = secret_cache.get(&full_match) {
            cached.clone()
        } else {
            match secrets.resolve(&prefix, path, key).await {
                Ok(val) => {
                    secret_cache.insert(full_match, val.clone());
                    val
                }
                Err(e) => {
                    secret_errors.push((format!("{prefix}:{path}#{key}"), e));
                    continue;
                }
            }
        };
        replacements.push(MatchReplacement {
            start: m.start(),
            end: m.end(),
            value,
        });
    }
}

/// Substitute all `${...}` references: env vars and secret provider refs.
///
/// Each unique secret path is fetched once even if referenced multiple times.
/// Resolution is atomic — all references must resolve or the entire
/// substitution fails.
///
/// Replacement is positional — secret values containing `${...}` are
/// never expanded as env vars.
///
/// # Errors
///
/// Returns an error if any env var is missing, a secret provider prefix
/// has no registered provider, or a secret read fails.
#[allow(clippy::missing_panics_doc)] // regex group 0 always exists
pub async fn substitute_variables(input: &str, secrets: &SecretProviders) -> Result<String> {
    let mut replacements = Vec::new();
    let mut secret_errors: Vec<(String, SecretError)> = Vec::new();
    let mut env_errors = Vec::new();

    // Collect secret ref replacements.
    collect_secret_replacements(input, secrets, &mut replacements, &mut secret_errors).await;

    if !secret_errors.is_empty() {
        let any_transient = secret_errors.iter().any(|(_, e)| e.is_transient());
        let msg = format!(
            "Failed to resolve secret(s):\n  {}",
            secret_errors
                .iter()
                .map(|(ref_name, e)| format!("{ref_name}: {e}"))
                .collect::<Vec<_>>()
                .join("\n  ")
        );
        if any_transient {
            return Err(SecretError::Unavailable(msg).into());
        }
        anyhow::bail!("{msg}");
    }

    // Collect env var replacements (from original input positions).
    for cap in ENV_VAR_RE.captures_iter(input) {
        let m = cap.get(0).unwrap();
        let var_name = &cap[1];
        match std::env::var(var_name) {
            Ok(val) => {
                replacements.push(MatchReplacement {
                    start: m.start(),
                    end: m.end(),
                    value: val,
                });
            }
            Err(_) => {
                env_errors.push(var_name.to_string());
            }
        }
    }

    if !env_errors.is_empty() {
        anyhow::bail!("Missing environment variable(s): {}", env_errors.join(", "));
    }

    Ok(apply_replacements(input, replacements))
}

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
        // Mixed case is detected (case-insensitive prefix).
        assert!(contains_secret_refs("${Vault:secret/pg#password}"));
        assert!(contains_secret_refs("${VAULT:secret/pg#password}"));
        // Prefixes with digits, hyphens, underscores.
        assert!(contains_secret_refs("${vault1:path#key}"));
        assert!(contains_secret_refs("${aws-sm:path#key}"));
        assert!(contains_secret_refs("${gcp_secrets:path#key}"));
        assert!(!contains_secret_refs("${NORMAL_ENV_VAR}"));
        assert!(!contains_secret_refs("no refs here"));
    }

    #[test]
    fn malformed_secret_refs_are_rejected() {
        // Missing #key
        assert!(reject_malformed_refs("${vault:secret/pg}").is_err());
        // Missing path
        assert!(reject_malformed_refs("${vault:}").is_err());
        // Mixed case — still rejected as malformed
        assert!(reject_malformed_refs("${Vault:secret/pg}").is_err());
        // Prefixes with digits/hyphens/underscores — still malformed without #key
        assert!(reject_malformed_refs("${vault1:secret/pg}").is_err());
        assert!(reject_malformed_refs("${aws-sm:path}").is_err());
        assert!(reject_malformed_refs("${gcp_secrets:path}").is_err());
        // Well-formed ref should pass
        assert!(reject_malformed_refs("${vault:secret/pg#password}").is_ok());
        // Extended prefixes with #key should pass
        assert!(reject_malformed_refs("${aws-sm:path#key}").is_ok());
        // Normal env var should pass
        assert!(reject_malformed_refs("${ENV_VAR}").is_ok());
        // No refs should pass
        assert!(reject_malformed_refs("plain text").is_ok());
    }

    #[tokio::test]
    async fn substitute_secrets_leaves_env_vars_intact() {
        use std::sync::Arc;

        struct FakeProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for FakeProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Ok("resolved-secret".to_owned())
            }
        }

        let mut secrets = SecretProviders::new();
        secrets.register("vault", Arc::new(FakeProvider));

        let input = "host: ${DB_HOST}\npassword: ${vault:secret/db#password}";
        let result = substitute_secrets(input, &secrets).await.unwrap();

        // Secret ref resolved, env var left untouched.
        assert_eq!(result, "host: ${DB_HOST}\npassword: resolved-secret");
    }

    #[tokio::test]
    async fn mixed_case_prefix_resolves_via_lowercase_provider() {
        use std::sync::Arc;

        struct FakeProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for FakeProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Ok("secret-value".to_owned())
            }
        }

        let mut secrets = SecretProviders::new();
        secrets.register("vault", Arc::new(FakeProvider));

        // ${Vault:...} with uppercase V should resolve via "vault" provider.
        let input = "password: ${Vault:secret/db#password}";
        let result = substitute_variables(input, &secrets).await.unwrap();
        assert_eq!(result, "password: secret-value");

        // ${VAULT:...} all-caps should also resolve.
        let input = "password: ${VAULT:secret/db#password}";
        let result = substitute_variables(input, &secrets).await.unwrap();
        assert_eq!(result, "password: secret-value");
    }

    #[tokio::test]
    async fn secret_values_with_dollar_braces_are_opaque() {
        // A secret value that looks like an env var should NOT be expanded,
        // even when the same env var pattern appears elsewhere in the YAML.
        use std::sync::Arc;

        struct FakeProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for FakeProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                // Return a value that contains ${RB_OPAQUE_HOST} — same pattern
                // as an env var that exists in the original YAML.
                Ok("${RB_OPAQUE_HOST}".to_owned())
            }
        }

        std::env::set_var("RB_OPAQUE_HOST", "real-host");

        let mut secrets = SecretProviders::new();
        secrets.register("vault", Arc::new(FakeProvider));

        // Both ${RB_OPAQUE_HOST} (env) and ${vault:...} (secret) are present.
        // The env var should expand to "real-host" but the secret value
        // "${RB_OPAQUE_HOST}" should be preserved literally.
        let input = "host: ${RB_OPAQUE_HOST}\npassword: ${vault:secret/test#key}";
        let result = substitute_variables(input, &secrets).await.unwrap();
        assert_eq!(result, "host: real-host\npassword: ${RB_OPAQUE_HOST}");

        std::env::remove_var("RB_OPAQUE_HOST");
    }
}
