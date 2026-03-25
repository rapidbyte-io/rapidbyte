use async_trait::async_trait;
use serde::Serialize;

use super::ServiceError;

/// Driving-port trait for the Plugins domain.
///
/// REST adapters call these methods; implementations live in
/// `application/services/plugin.rs`.
#[async_trait]
pub trait PluginService: Send + Sync {
    /// List all locally installed plugins.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] on internal failures.
    async fn list(&self) -> Result<Vec<PluginSummary>, ServiceError>;

    /// Search the remote registry by keyword and optional type filter.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] on internal failures.
    async fn search(
        &self,
        request: PluginSearchRequest,
    ) -> Result<Vec<PluginSearchResult>, ServiceError>;

    /// Fetch full metadata for a plugin by reference.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] when the plugin does not exist,
    /// or another [`ServiceError`] variant on internal failures.
    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail, ServiceError>;

    /// Download and install a plugin by reference.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] on network, I/O, or registry failures.
    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult, ServiceError>;

    /// Remove an installed plugin by reference.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::NotFound`] when the plugin is not installed,
    /// or another [`ServiceError`] variant on internal failures.
    async fn remove(&self, plugin_ref: &str) -> Result<(), ServiceError>;
}

/// Brief summary of an installed plugin, returned by the list endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct PluginSummary {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub size_bytes: u64,
    pub installed_at: chrono::DateTime<chrono::Utc>,
}

/// Parameters for a plugin registry search.
#[derive(Debug, Clone)]
pub struct PluginSearchRequest {
    pub query: String,
    pub plugin_type: Option<String>,
}

/// A single result from the plugin registry search.
#[derive(Debug, Clone, Serialize)]
pub struct PluginSearchResult {
    pub name: String,
    pub description: String,
    pub latest_version: String,
    pub plugin_type: String,
}

/// Full metadata for a single plugin, returned by the info endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct PluginDetail {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub description: String,
    pub size_bytes: u64,
    pub manifest: Option<serde_json::Value>,
    pub available_versions: Vec<String>,
    pub installed: bool,
}

/// Response returned after a successful plugin install.
#[derive(Debug, Clone, Serialize)]
pub struct PluginInstallResult {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub size_bytes: u64,
    pub installed_at: chrono::DateTime<chrono::Utc>,
}
