//! Controller configuration types and validation.

use std::net::SocketAddr;
use std::time::Duration;

use crate::store;

/// Configuration for the controller server.
#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Authentication configuration.
#[derive(Clone)]
pub struct AuthConfig {
    /// Bearer tokens for authentication.
    pub tokens: Vec<String>,
    pub signing_key: Vec<u8>,
}

/// Timer intervals for background tasks.
#[derive(Clone)]
pub struct TimerConfig {
    pub agent_reap_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub lease_check_interval: Duration,
    pub reconciliation_timeout: Duration,
    pub preview_cleanup_interval: Duration,
}

/// OCI registry configuration broadcast to agents.
#[derive(Clone)]
pub struct RegistryConfig {
    /// OCI registry URL broadcast to agents on registration (e.g. `registry.example.com`).
    /// Empty or `None` means no registry is configured.
    pub url: Option<String>,
    /// Use HTTP instead of HTTPS when agents pull from the registry.
    pub insecure: bool,
}

/// Plugin signature trust policy.
#[derive(Clone)]
pub struct TrustConfig {
    /// Plugin signature trust policy broadcast to agents: "skip", "warn", or "verify".
    pub policy: String,
    /// Paths to trusted Ed25519 public key PEM files. Contents are read and sent to agents.
    pub trusted_key_paths: Vec<std::path::PathBuf>,
}

pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub metadata_database_url: Option<String>,
    pub auth: AuthConfig,
    pub timers: TimerConfig,
    pub tls: Option<ServerTlsConfig>,
    /// Optional Prometheus metrics listen address (e.g. `127.0.0.1:9190`).
    /// Prometheus endpoint is only started when this is set.
    pub metrics_listen: Option<String>,
    pub registry: RegistryConfig,
    pub trust: TrustConfig,
}

/// Default signing key used when no explicit key is configured.
/// Shared between controller and agent so preview tickets work out of the box.
/// **Not suitable for production** — always set `RAPIDBYTE_SIGNING_KEY` in deployed environments.
pub(crate) const DEFAULT_SIGNING_KEY: &[u8] = b"rapidbyte-dev-signing-key-not-for-production";

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".parse().unwrap(),
            metadata_database_url: None,
            auth: AuthConfig {
                tokens: Vec::new(),
                signing_key: DEFAULT_SIGNING_KEY.to_vec(),
            },
            timers: TimerConfig {
                agent_reap_interval: Duration::from_secs(15),
                agent_reap_timeout: Duration::from_secs(60),
                lease_check_interval: Duration::from_secs(10),
                reconciliation_timeout: Duration::from_secs(300),
                preview_cleanup_interval: Duration::from_secs(30),
            },
            tls: None,
            metrics_listen: None,
            registry: RegistryConfig {
                url: None,
                insecure: false,
            },
            trust: TrustConfig {
                policy: "skip".to_owned(),
                trusted_key_paths: Vec::new(),
            },
        }
    }
}

/// Validate the controller configuration.
///
/// # Errors
///
/// Returns an error if the auth or signing key configuration is invalid.
pub fn validate(config: &ControllerConfig) -> anyhow::Result<()> {
    validate_auth_config(config)?;
    validate_signing_key_config(config)?;
    validate_trust_config(config)?;
    Ok(())
}

fn validate_auth_config(config: &ControllerConfig) -> anyhow::Result<()> {
    let has_valid_token = config.auth.tokens.iter().any(|t| !t.trim().is_empty());
    if !has_valid_token {
        anyhow::bail!("Controller auth is required. Set --auth-token / RAPIDBYTE_AUTH_TOKEN.");
    }
    Ok(())
}

fn validate_signing_key_config(config: &ControllerConfig) -> anyhow::Result<()> {
    if config.auth.signing_key == DEFAULT_SIGNING_KEY {
        anyhow::bail!(
            "Controller preview signing key must be set explicitly. Pass --signing-key / RAPIDBYTE_SIGNING_KEY."
        );
    }
    Ok(())
}

fn validate_trust_config(config: &ControllerConfig) -> anyhow::Result<()> {
    match config.trust.policy.as_str() {
        "skip" | "warn" | "verify" => {}
        other => {
            anyhow::bail!("Invalid trust policy \"{other}\". Must be one of: skip, warn, verify.")
        }
    }
    if config.trust.policy == "verify" && config.trust.trusted_key_paths.is_empty() {
        anyhow::bail!(
            "Trust policy \"verify\" requires at least one trusted key path. Set --trusted-key-path."
        );
    }
    Ok(())
}

pub(crate) fn metadata_database_url(config: &ControllerConfig) -> anyhow::Result<&str> {
    match config.metadata_database_url.as_deref().map(str::trim) {
        Some("") | None => {
            anyhow::bail!(
                "Controller metadata database URL is required. Set --metadata-database-url / RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL."
            );
        }
        Some(url) => Ok(url),
    }
}

pub(crate) async fn initialize_metadata_store(
    config: &ControllerConfig,
) -> anyhow::Result<store::MetadataStore> {
    let url = metadata_database_url(config)?;
    store::initialize_metadata_store(url).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_is_required_by_default() {
        let config = ControllerConfig::default();
        let err = validate_auth_config(&config).unwrap_err();
        assert!(err.to_string().contains("Controller auth is required"));
    }

    #[test]
    fn default_signing_key_requires_explicit_override() {
        let config = ControllerConfig {
            auth: AuthConfig {
                tokens: vec!["secret".into()],
                signing_key: DEFAULT_SIGNING_KEY.to_vec(),
            },
            ..Default::default()
        };
        let err = validate_signing_key_config(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("preview signing key must be set explicitly"));
    }

    #[test]
    fn metadata_database_url_is_required() {
        let config = ControllerConfig {
            auth: AuthConfig {
                tokens: vec!["secret".into()],
                signing_key: b"signing".to_vec(),
            },
            ..Default::default()
        };
        let err = metadata_database_url(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("Controller metadata database URL is required"));
    }

    #[test]
    fn metadata_database_url_rejects_whitespace() {
        let config = ControllerConfig {
            auth: AuthConfig {
                tokens: vec!["secret".into()],
                signing_key: b"signing".to_vec(),
            },
            metadata_database_url: Some("   ".into()),
            ..Default::default()
        };
        let err = metadata_database_url(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("Controller metadata database URL is required"));
    }

    #[test]
    fn metadata_database_url_accepts_non_empty_value() {
        let config = ControllerConfig {
            auth: AuthConfig {
                tokens: vec!["secret".into()],
                signing_key: b"signing".to_vec(),
            },
            metadata_database_url: Some("postgresql://localhost/controller".into()),
            ..Default::default()
        };
        assert_eq!(
            metadata_database_url(&config).unwrap(),
            "postgresql://localhost/controller"
        );
    }
}
