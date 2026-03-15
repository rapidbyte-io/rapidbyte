//! HashiCorp Vault KV v2 secret provider.

use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::SecretProvider;

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
    /// AppRole machine-to-machine authentication.
    AppRole { role_id: String, secret_id: String },
}

/// Vault KV v2 secret provider.
///
/// Reads secrets from a HashiCorp Vault server. Path format in pipeline
/// YAML: `${vault:mount/path#key}` (e.g. `${vault:secret/postgres#password}`).
pub struct VaultProvider {
    client: vaultrs::client::VaultClient,
}

impl VaultProvider {
    /// Create a new Vault provider and authenticate.
    ///
    /// For [`VaultAuth::Token`], the token is set directly.
    /// For [`VaultAuth::AppRole`], the client exchanges role_id/secret_id
    /// for a token immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if Vault is unreachable or authentication fails.
    pub async fn new(config: VaultConfig) -> Result<Self> {
        let mut settings = vaultrs::client::VaultClientSettingsBuilder::default();
        settings.address(&config.address);

        match &config.auth {
            VaultAuth::Token(token) => {
                settings.token(token);
            }
            VaultAuth::AppRole { .. } => {}
        }

        let client = vaultrs::client::VaultClient::new(
            settings.build().context("invalid Vault client settings")?,
        )
        .context("failed to create Vault client")?;

        let provider = Self { client };

        if let VaultAuth::AppRole { role_id, secret_id } = &config.auth {
            provider.approle_login(role_id, secret_id).await?;
        }

        Ok(provider)
    }

    async fn approle_login(&self, role_id: &str, secret_id: &str) -> Result<()> {
        vaultrs::auth::approle::login(&self.client, "approle", role_id, secret_id)
            .await
            .context("Vault AppRole authentication failed")?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SecretProvider for VaultProvider {
    async fn read_secret(&self, path: &str, key: &str) -> Result<String> {
        // Split mount from path: "secret/postgres" → mount="secret", path="postgres"
        let (mount, secret_path) = path
            .split_once('/')
            .with_context(|| format!("invalid Vault path '{path}': expected mount/path format"))?;

        // Deserialize as Value to handle mixed types (string, number, bool).
        let data: HashMap<String, serde_json::Value> =
            vaultrs::kv2::read(&self.client, mount, secret_path)
                .await
                .with_context(|| format!("failed to read Vault secret at {path}"))?;

        let value = data
            .get(key)
            .with_context(|| format!("key '{key}' not found in Vault secret {path}"))?;

        // Convert to string: strings stay as-is, numbers/bools use their
        // JSON representation, null becomes empty string.
        match value {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Null => Ok(String::new()),
            other => Ok(other.to_string()),
        }
    }
}
