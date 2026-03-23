//! Local-mode implementation of [`PluginService`].
//!
//! All methods are stubs for Phase 1 — plugin management requires
//! registry integration that will be implemented in a later phase.

use async_trait::async_trait;

use crate::error::ApiError;
use crate::traits::PluginService;
use crate::types::{
    PluginDetail, PluginInstallResult, PluginSearchRequest, PluginSearchResult, PluginSummary,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Local-mode plugin service.
///
/// All methods return empty results or `NotImplemented` errors.
/// Plugin registry integration will be added in a future phase.
pub struct LocalPluginService;

impl LocalPluginService {
    /// Create a new `LocalPluginService`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalPluginService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PluginService for LocalPluginService {
    async fn list(&self) -> Result<Vec<PluginSummary>> {
        Ok(vec![])
    }

    async fn search(&self, _request: PluginSearchRequest) -> Result<Vec<PluginSearchResult>> {
        Err(ApiError::not_implemented("plugin search"))
    }

    async fn info(&self, _plugin_ref: &str) -> Result<PluginDetail> {
        Err(ApiError::not_implemented("plugin info"))
    }

    async fn install(&self, _plugin_ref: &str) -> Result<PluginInstallResult> {
        Err(ApiError::not_implemented("plugin install"))
    }

    async fn remove(&self, _plugin_ref: &str) -> Result<()> {
        Err(ApiError::not_implemented("plugin remove"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service() -> LocalPluginService {
        LocalPluginService::new()
    }

    #[tokio::test]
    async fn list_returns_empty() {
        let svc = make_service();
        let result = svc.list().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn search_returns_not_implemented() {
        let svc = make_service();
        let err = svc
            .search(PluginSearchRequest {
                query: "postgres".into(),
                plugin_type: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn info_returns_not_implemented() {
        let svc = make_service();
        let err = svc.info("postgres@1.0.0").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn install_returns_not_implemented() {
        let svc = make_service();
        let err = svc.install("postgres@1.0.0").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn remove_returns_not_implemented() {
        let svc = make_service();
        let err = svc.remove("postgres@1.0.0").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[test]
    fn default_creates_service() {
        let svc = LocalPluginService;
        assert!(std::mem::size_of_val(&svc) == 0);
    }
}
