use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// A plugin that is currently installed on the host.
#[derive(Debug, Clone, Serialize)]
pub struct InstalledPlugin {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub size_bytes: u64,
    pub installed_at: DateTime<Utc>,
}

/// An entry in the remote plugin registry.
#[derive(Debug, Clone, Serialize)]
pub struct RegistryEntry {
    pub name: String,
    pub description: String,
    pub latest_version: String,
    pub plugin_type: String,
}

/// Full metadata for a single plugin, including installation status.
#[derive(Debug, Clone, Serialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub description: String,
    pub size_bytes: u64,
    pub manifest: Option<serde_json::Value>,
    pub available_versions: Vec<String>,
    pub installed: bool,
}

/// Errors that can occur when interacting with the plugin registry.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("plugin not found: {0}")]
    NotFound(String),
    #[error("registry error: {0}")]
    Registry(String),
    #[error("io error: {0}")]
    Io(String),
}

/// Driven port for plugin lifecycle management.
///
/// Implementations live outside this crate (e.g. in the CLI / engine layer
/// where the local plugin store and remote registry are accessible).
#[async_trait]
pub trait PluginRegistry: Send + Sync {
    /// List all locally installed plugins.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] on I/O or registry failures.
    async fn list_installed(&self) -> Result<Vec<InstalledPlugin>, RegistryError>;

    /// Search the remote registry by keyword and optional type filter.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] on I/O or registry failures.
    async fn search(
        &self,
        query: &str,
        plugin_type: Option<&str>,
    ) -> Result<Vec<RegistryEntry>, RegistryError>;

    /// Fetch full metadata for a plugin by its reference (e.g. `org/name:version`).
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::NotFound`] if the plugin does not exist, or
    /// another [`RegistryError`] variant on failure.
    async fn info(&self, plugin_ref: &str) -> Result<PluginMetadata, RegistryError>;

    /// Download and install a plugin by reference.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] on network, I/O, or registry failures.
    async fn install(&self, plugin_ref: &str) -> Result<InstalledPlugin, RegistryError>;

    /// Remove an installed plugin by reference.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::NotFound`] if the plugin is not installed, or
    /// another [`RegistryError`] variant on failure.
    async fn remove(&self, plugin_ref: &str) -> Result<(), RegistryError>;
}
