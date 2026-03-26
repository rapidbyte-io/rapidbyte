use async_trait::async_trait;
use serde::Serialize;

use super::ServiceError;

/// Driving-port trait for server-level metadata endpoints (health, version, config).
#[async_trait]
pub trait ServerService: Send + Sync {
    /// Return the current health status of the server.
    ///
    /// # Errors
    ///
    /// Returns `ServiceError::Internal` if an underlying check fails.
    async fn health(&self) -> Result<HealthStatus, ServiceError>;

    /// Return version information for the running binary.
    ///
    /// # Errors
    ///
    /// Returns `ServiceError::Internal` on unexpected failure.
    async fn version(&self) -> Result<VersionInfo, ServiceError>;

    /// Return a summary of the server's runtime configuration.
    ///
    /// # Errors
    ///
    /// Returns `ServiceError::Internal` on unexpected failure.
    async fn config(&self) -> Result<ServerConfigInfo, ServiceError>;
}

/// Current health of the server and its dependencies.
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: String,
    pub mode: String,
    pub uptime_secs: u64,
    pub state_backend: String,
    pub state_backend_healthy: bool,
    pub agents_connected: u32,
}

/// Version information for the running binary.
#[derive(Debug, Clone, Serialize)]
pub struct VersionInfo {
    pub version: String,
    pub mode: String,
}

/// Summary of the server's runtime configuration.
#[derive(Debug, Clone, Serialize)]
pub struct ServerConfigInfo {
    pub mode: String,
    pub port: u16,
    pub state_backend: String,
    pub auth_required: bool,
}
