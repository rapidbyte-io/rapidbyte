use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tracing::warn;

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_registry::RegistryConfig;
use rapidbyte_secrets::SecretProviders;

use crate::run_manager::RunManager;
use crate::services::{
    LocalConnectionService, LocalOperationsService, LocalPipelineService, LocalPluginService,
    LocalRunService, LocalServerService,
};
use crate::traits::{
    ConnectionService, OperationsService, PipelineService, PluginService, RunService, ServerService,
};

/// Runtime server configuration passed into [`ApiContext::from_project`].
///
/// These values are determined at startup (CLI flags, environment) and
/// surfaced by the `/server/config` endpoint.
#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    /// TCP port the HTTP server is listening on.
    pub port: u16,
    /// Whether bearer-token authentication is required.
    pub auth_required: bool,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            auth_required: false,
        }
    }
}

/// Deployment mode for the API server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Local single-process mode (CLI-embedded server).
    Local,
    /// Distributed mode — delegates to a remote controller.
    Distributed {
        /// URL of the remote controller.
        controller_url: String,
    },
}

/// Dependency-injection container for API services.
///
/// Each field holds an `Arc<dyn Trait>` so the context is cheaply
/// cloneable and can be shared across request handlers.
pub struct ApiContext {
    pub pipelines: Arc<dyn PipelineService>,
    pub runs: Arc<dyn RunService>,
    pub connections: Arc<dyn ConnectionService>,
    pub plugins: Arc<dyn PluginService>,
    pub operations: Arc<dyn OperationsService>,
    pub server: Arc<dyn ServerService>,
}

impl ApiContext {
    /// Build an `ApiContext` from a project directory.
    ///
    /// In `Local` mode, scans `project_dir` for `*.yaml` / `*.yml`
    /// pipeline files, parses each into a [`PipelineConfig`], and wires
    /// all six service implementations.
    ///
    /// `secrets` is consumed and shared across services that need it.
    /// `server_config` carries runtime values (port, auth) that the
    /// server-info endpoint should report.
    ///
    /// # Errors
    ///
    /// Returns an error for distributed mode (not yet implemented) or
    /// if the project directory cannot be read.
    pub async fn from_project(
        project_dir: &Path,
        mode: DeploymentMode,
        secrets: SecretProviders,
        registry_config: RegistryConfig,
        server_config: ApiServerConfig,
    ) -> Result<Self> {
        match mode {
            DeploymentMode::Local => {
                Self::build_local(project_dir, secrets, registry_config, server_config).await
            }
            DeploymentMode::Distributed { .. } => {
                anyhow::bail!("distributed mode not yet implemented")
            }
        }
    }

    /// Build a local-mode context by scanning pipeline YAML files.
    async fn build_local(
        project_dir: &Path,
        secrets: SecretProviders,
        registry_config: RegistryConfig,
        server_config: ApiServerConfig,
    ) -> Result<Self> {
        let catalog = scan_pipelines(project_dir, &secrets).await;

        let catalog = Arc::new(catalog);
        let run_manager = Arc::new(RunManager::new());
        let registry_config = Arc::new(registry_config);
        let secrets = Arc::new(secrets);

        Ok(Self {
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
            server: Arc::new(LocalServerService::new(
                Instant::now(),
                server_config.port,
                server_config.auth_required,
            )),
        })
    }
}

/// Scan a directory for `.yaml` / `.yml` files, parse each as a pipeline
/// config, and collect them into a map keyed by the pipeline name.
///
/// Files that fail to read or parse are logged as warnings and skipped.
async fn scan_pipelines(dir: &Path, secrets: &SecretProviders) -> HashMap<String, PipelineConfig> {
    let mut catalog = HashMap::new();

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(?dir, %err, "failed to read project directory");
            return catalog;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                warn!(%err, "failed to read directory entry");
                continue;
            }
        };

        let path = entry.path();

        // Only process regular files with .yaml or .yml extension.
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("");
        if ext != "yaml" && ext != "yml" {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(err) => {
                warn!(file = %path.display(), %err, "failed to read pipeline file");
                continue;
            }
        };

        let config = match rapidbyte_pipeline_config::parse_pipeline(&content, secrets).await {
            Ok(c) => c,
            Err(err) => {
                warn!(file = %path.display(), %err, "failed to parse pipeline file");
                continue;
            }
        };

        if let Err(err) = rapidbyte_pipeline_config::validate_pipeline(&config) {
            warn!(file = %path.display(), %err, "pipeline validation failed");
            continue;
        }

        let name = config.pipeline.clone();
        if catalog.contains_key(&name) {
            warn!(
                pipeline = %name,
                file = %path.display(),
                "duplicate pipeline name, skipping"
            );
            continue;
        }

        catalog.insert(name, config);
    }

    catalog
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::types::PipelineFilter;

    #[tokio::test]
    async fn from_project_local_mode_with_no_pipelines() {
        let dir = TempDir::new().unwrap();
        let ctx = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Local,
            SecretProviders::default(),
            RegistryConfig::default(),
            ApiServerConfig::default(),
        )
        .await
        .unwrap();

        let health = ctx.server.health().await.unwrap();
        assert_eq!(health.status, "healthy");

        let pipelines = ctx.pipelines.list(PipelineFilter::default()).await.unwrap();
        assert!(pipelines.items.is_empty());
    }

    #[tokio::test]
    async fn distributed_mode_returns_error() {
        let dir = TempDir::new().unwrap();
        let result = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Distributed {
                controller_url: "http://localhost:9090".into(),
            },
            SecretProviders::default(),
            RegistryConfig::default(),
            ApiServerConfig::default(),
        )
        .await;
        match result {
            Ok(_) => panic!("expected distributed mode to fail"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("distributed mode not yet implemented"),
                    "unexpected error: {err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn from_project_skips_non_yaml_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("readme.txt"), "not a pipeline").unwrap();
        std::fs::write(dir.path().join("config.json"), "{}").unwrap();

        let ctx = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Local,
            SecretProviders::default(),
            RegistryConfig::default(),
            ApiServerConfig::default(),
        )
        .await
        .unwrap();

        let pipelines = ctx.pipelines.list(PipelineFilter::default()).await.unwrap();
        assert!(pipelines.items.is_empty());
    }

    #[tokio::test]
    async fn from_project_skips_invalid_yaml() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("bad.yaml"), "this is not: [valid: {{{}}}").unwrap();

        let ctx = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Local,
            SecretProviders::default(),
            RegistryConfig::default(),
            ApiServerConfig::default(),
        )
        .await
        .unwrap();

        let pipelines = ctx.pipelines.list(PipelineFilter::default()).await.unwrap();
        assert!(pipelines.items.is_empty());
    }
}
