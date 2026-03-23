//! Shared test utilities for the API crate.
//!
//! Provides [`test_api_context`] to build an [`ApiContext`] backed by
//! in-memory local-mode services with an empty (or provided) pipeline
//! catalog. No filesystem or network access required.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_registry::RegistryConfig;
use rapidbyte_secrets::SecretProviders;

use crate::context::ApiContext;
use crate::run_manager::RunManager;
use crate::services::{
    LocalConnectionService, LocalOperationsService, LocalPipelineService, LocalPluginService,
    LocalRunService, LocalServerService,
};

/// Build an [`ApiContext`] for tests with an empty pipeline catalog.
#[must_use]
pub fn test_api_context() -> ApiContext {
    test_api_context_with_catalog(HashMap::new())
}

/// Build an [`ApiContext`] with a pre-populated pipeline catalog.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn test_api_context_with_catalog(catalog: HashMap<String, PipelineConfig>) -> ApiContext {
    let catalog = Arc::new(catalog);
    let run_manager = Arc::new(RunManager::new());
    let registry_config = Arc::new(RegistryConfig::default());
    let secrets = Arc::new(SecretProviders::default());

    ApiContext {
        pipelines: Arc::new(LocalPipelineService::new(
            Arc::clone(&catalog),
            Arc::clone(&run_manager),
            Arc::clone(&registry_config),
            Arc::clone(&secrets),
        )),
        runs: Arc::new(LocalRunService::new(Arc::clone(&run_manager))),
        connections: Arc::new(LocalConnectionService::new(Arc::clone(&catalog))),
        plugins: Arc::new(LocalPluginService),
        operations: Arc::new(LocalOperationsService::new(
            Arc::clone(&catalog),
            Arc::clone(&run_manager),
        )),
        server: Arc::new(LocalServerService::new(Instant::now(), 8080, false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PipelineFilter;

    #[tokio::test]
    async fn test_context_has_empty_catalog() {
        let ctx = test_api_context();
        let pipelines = ctx.pipelines.list(PipelineFilter::default()).await.unwrap();
        assert!(pipelines.items.is_empty());
    }

    #[tokio::test]
    async fn test_context_is_healthy() {
        let ctx = test_api_context();
        let health = ctx.server.health().await.unwrap();
        assert_eq!(health.status, "healthy");
    }
}
