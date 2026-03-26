use async_trait::async_trait;
use serde::Serialize;

use super::ServiceError;

/// Driving-port trait for the Connections domain.
///
/// REST and gRPC adapters call these methods; implementations live in
/// `application/services/connection.rs`.
#[async_trait]
pub trait ConnectionService: Send + Sync {
    /// List all configured connections.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] on internal failures.
    async fn list(&self) -> Result<Vec<ConnectionSummary>, ServiceError>;

    /// Get details for a single connection by name.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] when the connection does not exist,
    /// or another [`ServiceError`] variant on internal failures.
    async fn get(&self, name: &str) -> Result<ConnectionDetail, ServiceError>;

    /// Test connectivity for a connection by name.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] when the connection does not exist,
    /// or another [`ServiceError`] variant on failure.
    async fn test(&self, name: &str) -> Result<ConnectionTestResponse, ServiceError>;

    /// Discover streams (tables/topics) exposed by a connection.
    /// When `table` is `Some`, only that table's metadata is returned.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] when the connection does not exist,
    /// or another [`ServiceError`] variant on failure.
    async fn discover(
        &self,
        name: &str,
        table: Option<&str>,
    ) -> Result<ConnectionDiscoverResponse, ServiceError>;
}

/// Brief summary of a connection, returned by the list endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionSummary {
    pub name: String,
    pub connector: String,
    pub used_by: Vec<String>,
}

/// Full detail for a single connection, returned by the get endpoint.
/// Sensitive fields (passwords, tokens) must be redacted by the implementation.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionDetail {
    pub name: String,
    pub connector: String,
    /// Connection config with sensitive fields redacted.
    pub config: serde_json::Value,
    pub used_by: Vec<String>,
}

/// Response from `POST /api/v1/connections/{name}/test`.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionTestResponse {
    pub name: String,
    pub status: String,
    pub latency_ms: Option<u64>,
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<serde_json::Value>,
}

/// Response from `GET /api/v1/connections/{name}/discover`.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionDiscoverResponse {
    pub connection: String,
    pub streams: Vec<serde_json::Value>,
}
