//! `HashiCorp` Vault KV v2 secret provider.

use std::collections::HashMap;

use anyhow::{Context, Result};
use tokio::sync::OnceCell;

use crate::{SecretError, SecretProvider};

/// Vault connection and authentication configuration.
///
/// `Debug` is deliberately not derived — contains secrets.
pub struct VaultConfig {
    /// Vault server address (e.g. `http://127.0.0.1:8200`).
    pub address: String,
    /// Authentication method.
    pub auth: VaultAuth,
}

/// Vault authentication method.
pub enum VaultAuth {
    /// Pre-existing token (e.g. from `VAULT_TOKEN` env var).
    Token(String),
    /// `AppRole` machine-to-machine authentication.
    AppRole { role_id: String, secret_id: String },
}

/// Vault KV v2 secret provider.
///
/// Reads secrets from a `HashiCorp` Vault server. Path format in pipeline
/// YAML: `${vault:mount/path#key}` (e.g. `${vault:secret/postgres#password}`).
///
/// `AppRole` authentication is deferred until the first `read_secret` call,
/// so constructing the provider never performs network I/O unless a token
/// is already provided.
pub struct VaultProvider {
    client: vaultrs::client::VaultClient,
    /// Deferred `AppRole` credentials — `None` for token auth.
    approle_creds: Option<(String, String)>,
    /// Ensures `AppRole` login happens exactly once.
    auth_once: OnceCell<()>,
}

impl VaultProvider {
    /// Create a new Vault provider.
    ///
    /// For [`VaultAuth::Token`], the token is set directly on the client.
    /// For [`VaultAuth::AppRole`], credentials are stored and authentication
    /// is deferred until the first [`SecretProvider::read_secret`] call.
    ///
    /// # Errors
    ///
    /// Returns an error if client settings are invalid.
    pub fn new(config: &VaultConfig) -> Result<Self> {
        let mut settings = vaultrs::client::VaultClientSettingsBuilder::default();
        settings.address(&config.address);

        let approle_creds = match &config.auth {
            VaultAuth::Token(token) => {
                settings.token(token);
                None
            }
            VaultAuth::AppRole { role_id, secret_id } => Some((role_id.clone(), secret_id.clone())),
        };

        let client = vaultrs::client::VaultClient::new(
            settings.build().context("invalid Vault client settings")?,
        )
        .context("failed to create Vault client")?;

        Ok(Self {
            client,
            approle_creds,
            auth_once: OnceCell::new(),
        })
    }

    /// Ensure `AppRole` auth has completed. No-op for token auth.
    async fn ensure_authenticated(&self) -> Result<(), SecretError> {
        if let Some((role_id, secret_id)) = &self.approle_creds {
            let role_id = role_id.clone();
            let secret_id = secret_id.clone();
            self.auth_once
                .get_or_try_init(|| async {
                    vaultrs::auth::approle::login(&self.client, "approle", &role_id, &secret_id)
                        .await
                        .context("Vault AppRole authentication failed")?;
                    Ok(())
                })
                .await
                .map_err(|e: anyhow::Error| SecretError::Unavailable(format!("{e:#}")))?;
        }
        Ok(())
    }
}

/// Classify a Vault API error into a `SecretError` variant.
///
/// Uses structured `ClientError` variants and HTTP status codes where
/// available, falling back to case-insensitive text matching for
/// transport-level errors that lack structured fields.
fn classify_vault_error(error: vaultrs::error::ClientError, path: &str) -> SecretError {
    match &error {
        // Structured API errors — classify by HTTP status code.
        vaultrs::error::ClientError::APIError { code, .. } => match *code {
            403 => {
                SecretError::AuthFailed(format!("failed to read Vault secret at {path}: {error}"))
            }
            404 => SecretError::NotFound(format!("Vault secret not found at {path}")),
            500..=599 => {
                SecretError::Unavailable(format!("failed to read Vault secret at {path}: {error}"))
            }
            _ => SecretError::Other(
                anyhow::Error::new(error).context(format!("failed to read Vault secret at {path}")),
            ),
        },
        // Transport/network errors (reqwest under the hood) and empty
        // responses (Vault may be starting up) — transient.
        vaultrs::error::ClientError::RestClientError { .. }
        | vaultrs::error::ClientError::RestClientBuildError { .. }
        | vaultrs::error::ClientError::ResponseEmptyError => {
            SecretError::Unavailable(format!("failed to read Vault secret at {path}: {error}"))
        }
        // Everything else — permanent.
        _ => SecretError::Other(
            anyhow::Error::new(error).context(format!("failed to read Vault secret at {path}")),
        ),
    }
}

#[async_trait::async_trait]
impl SecretProvider for VaultProvider {
    async fn read_secret(&self, path: &str, key: &str) -> Result<String, SecretError> {
        self.ensure_authenticated().await?;

        // Split mount from path: "secret/postgres" → mount="secret", path="postgres"
        let (mount, secret_path) = path.split_once('/').ok_or_else(|| {
            SecretError::InvalidPath(format!(
                "invalid Vault path '{path}': expected mount/path format"
            ))
        })?;

        // Deserialize as Value to handle mixed types (string, number, bool).
        let data: HashMap<String, serde_json::Value> =
            vaultrs::kv2::read(&self.client, mount, secret_path)
                .await
                .map_err(|e| classify_vault_error(e, path))?;

        let value = data.get(key).ok_or_else(|| {
            SecretError::NotFound(format!("key '{key}' not found in Vault secret {path}"))
        })?;

        // Convert to string: strings stay as-is, numbers/bools use their
        // JSON representation, null becomes empty string.
        match value {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Null => Ok(String::new()),
            other => Ok(other.to_string()),
        }
    }
}
