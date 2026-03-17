use async_trait::async_trait;
use rapidbyte_secrets::SecretProviders;

use crate::domain::ports::secrets::{SecretError, SecretResolver};

pub struct VaultSecretResolver {
    secrets: SecretProviders,
}

impl VaultSecretResolver {
    #[must_use]
    pub fn new(secrets: SecretProviders) -> Self {
        Self { secrets }
    }
}

#[async_trait]
impl SecretResolver for VaultSecretResolver {
    async fn resolve(&self, yaml: &str) -> Result<String, SecretError> {
        rapidbyte_pipeline_config::substitute_secrets(yaml, &self.secrets)
            .await
            .map_err(|e| SecretError(e.to_string()))
    }
}
