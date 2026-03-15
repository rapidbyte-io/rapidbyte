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

// ── Vault-aware async parsing ────────────────────────────────────────────────

/// Substitute both `${ENV_VAR}` and `${vault:mount/path#key}` references.
///
/// Env vars are resolved from the process environment. Vault references
/// are resolved via the provided [`rapidbyte_vault::VaultClient`]. Each
/// unique Vault path is fetched once even if referenced multiple times.
///
/// # Errors
///
/// Returns an error if any env var is missing or any Vault read fails.
/// Resolution is atomic — all references must resolve or the entire
/// substitution fails.
#[cfg(feature = "vault")]
pub async fn substitute_variables(
    input: &str,
    vault: Option<&rapidbyte_vault::VaultClient>,
) -> Result<String> {
    let mut vault_values: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut vault_errors = Vec::new();

    if let Some(client) = vault {
        let mut seen = std::collections::HashSet::new();
        for cap in VAULT_REF_RE.captures_iter(input) {
            let full_match = cap[0].to_string();
            if seen.insert(full_match.clone()) {
                let mount_path = &cap[1];
                let key = &cap[2];
                let (mount, path) = mount_path.split_once('/').unwrap_or((mount_path, ""));
                match client.read_secret(mount, path, key).await {
                    Ok(val) => {
                        vault_values.insert(full_match, val);
                    }
                    Err(e) => {
                        vault_errors.push(format!("vault:{mount_path}#{key}: {e}"));
                    }
                }
            }
        }
    } else if contains_vault_refs(input) {
        anyhow::bail!(
            "pipeline contains ${{vault:...}} references but no Vault client is configured"
        );
    }

    if !vault_errors.is_empty() {
        anyhow::bail!(
            "Failed to resolve Vault secret(s):\n  {}",
            vault_errors.join("\n  ")
        );
    }

    // Replace vault refs first, then env vars.
    let mut result = input.to_string();
    for (pattern, value) in &vault_values {
        result = result.replace(pattern, value);
    }

    let mut env_errors = Vec::new();
    let after_vault = result.clone();
    for cap in ENV_VAR_RE.captures_iter(&after_vault) {
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

/// Parse a pipeline YAML string, resolving both env vars and Vault secrets.
///
/// After secret resolution, YAML parse errors are redacted to prevent
/// secret leakage in error messages.
///
/// # Errors
///
/// Returns an error if variable substitution fails or YAML is invalid.
#[cfg(feature = "vault")]
pub async fn parse_pipeline_with_vault(
    yaml_str: &str,
    vault: Option<&rapidbyte_vault::VaultClient>,
) -> Result<PipelineConfig> {
    let has_secrets = vault.is_some() && contains_vault_refs(yaml_str);
    let substituted = substitute_variables(yaml_str, vault).await?;
    serde_yaml::from_str(&substituted).map_err(|e| {
        if has_secrets {
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
