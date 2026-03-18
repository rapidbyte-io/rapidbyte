use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SecretError(pub String);

#[async_trait]
pub trait SecretResolver: Send + Sync {
    async fn resolve(&self, yaml: &str) -> Result<String, SecretError>;
}
