use async_trait::async_trait;
use serde::Serialize;

/// Result of a connection test.
#[derive(Debug, Clone, Serialize)]
pub struct TestResult {
    pub status: String,
    pub latency_ms: Option<u64>,
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ConnectionErrorInfo>,
}

/// Structured error information returned in a `TestResult`.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionErrorInfo {
    pub code: String,
    pub message: String,
}

/// Result of schema discovery on a connection.
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveryResult {
    pub connection: String,
    pub streams: Vec<DiscoveredStream>,
}

/// A single discovered table / stream.
#[derive(Debug, Clone, Serialize)]
pub struct DiscoveredStream {
    pub schema: Option<String>,
    pub table: String,
    pub estimated_rows: Option<u64>,
    pub columns: Option<u32>,
}

/// Errors that can occur during connection testing or discovery.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionTestError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("plugin error: {0}")]
    Plugin(String),
}

/// Driven port for testing and discovering connections.
///
/// Implementations live outside this crate (e.g. in the CLI / engine layer
/// where Wasmtime plugins are available).
#[async_trait]
pub trait ConnectionTester: Send + Sync {
    /// Test reachability and authentication for a connection defined by
    /// `connection_config` (the raw YAML/JSON config value).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionTestError::Connection`] if the remote host is
    /// unreachable or authentication fails, or
    /// [`ConnectionTestError::Plugin`] if the plugin itself fails.
    async fn test(
        &self,
        connection_config: &serde_json::Value,
    ) -> Result<TestResult, ConnectionTestError>;

    /// Discover available streams (tables/topics) for the given connection.
    /// When `table` is `Some`, only that table's metadata is returned.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectionTestError`] variants as in [`ConnectionTester::test`].
    async fn discover(
        &self,
        connection_config: &serde_json::Value,
        table: Option<&str>,
    ) -> Result<DiscoveryResult, ConnectionTestError>;
}
