use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{HealthStatus, ServerConfig, VersionInfo};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for server health, version, and configuration queries.
#[async_trait]
pub trait ServerService: Send + Sync {
    async fn health(&self) -> Result<HealthStatus>;
    async fn version(&self) -> Result<VersionInfo>;
    async fn config(&self) -> Result<ServerConfig>;
}
