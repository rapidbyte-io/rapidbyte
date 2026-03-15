//! Vault secret resolution for pipeline configs.
//!
//! Wraps the `vaultrs` crate to provide a simplified API for reading
//! KV v2 secrets referenced in pipeline YAML via `${vault:mount/path#key}`.

use std::collections::HashMap;

use anyhow::{Context, Result};

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

/// A configured Vault client for reading KV v2 secrets.
pub struct VaultClient {
    client: vaultrs::client::VaultClient,
}

impl VaultClient {
    /// Create a new Vault client and authenticate.
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
            VaultAuth::AppRole { .. } => {
                // Token will be set after AppRole login below.
            }
        }

        let client = vaultrs::client::VaultClient::new(
            settings.build().context("invalid Vault client settings")?,
        )
        .context("failed to create Vault client")?;

        let client = Self { client };

        if let VaultAuth::AppRole { role_id, secret_id } = &config.auth {
            client.approle_login(role_id, secret_id).await?;
        }

        Ok(client)
    }

    async fn approle_login(&self, role_id: &str, secret_id: &str) -> Result<()> {
        vaultrs::auth::approle::login(&self.client, "approle", role_id, secret_id)
            .await
            .context("Vault AppRole authentication failed")?;
        Ok(())
    }

    /// Read a single field from a KV v2 secret.
    ///
    /// # Arguments
    ///
    /// * `mount` — KV v2 mount point (e.g. `"secret"`)
    /// * `path` — Secret path within the mount (e.g. `"postgres"`)
    /// * `key` — Field name within the secret data (e.g. `"password"`)
    ///
    /// # Errors
    ///
    /// Returns an error if the path does not exist, the key is not found
    /// in the secret data, or Vault is unreachable.
    pub async fn read_secret(&self, mount: &str, path: &str, key: &str) -> Result<String> {
        let data: HashMap<String, String> = vaultrs::kv2::read(&self.client, mount, path)
            .await
            .with_context(|| format!("failed to read Vault secret at {mount}/{path}"))?;

        data.get(key)
            .cloned()
            .with_context(|| format!("key '{key}' not found in Vault secret {mount}/{path}"))
    }
}
