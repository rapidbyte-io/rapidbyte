use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{
    PluginDetail, PluginInstallResult, PluginSearchRequest, PluginSearchResult, PluginSummary,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for plugin management and registry search.
#[async_trait]
pub trait PluginService: Send + Sync {
    async fn list(&self) -> Result<Vec<PluginSummary>>;
    async fn search(&self, request: PluginSearchRequest) -> Result<Vec<PluginSearchResult>>;
    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail>;
    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult>;
    async fn remove(&self, plugin_ref: &str) -> Result<()>;
}
