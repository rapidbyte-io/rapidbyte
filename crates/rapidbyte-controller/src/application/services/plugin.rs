use async_trait::async_trait;

use crate::domain::ports::plugin_registry::RegistryError;
use crate::traits::plugin::{
    PluginDetail, PluginInstallResult, PluginSearchRequest, PluginSearchResult, PluginService,
    PluginSummary,
};
use crate::traits::ServiceError;

use super::AppServices;

/// Map a [`RegistryError`] to a [`ServiceError`].
fn registry_error_to_service(err: RegistryError, plugin_ref: &str) -> ServiceError {
    match err {
        RegistryError::NotFound(_) => ServiceError::NotFound {
            resource: "plugin".into(),
            id: plugin_ref.to_string(),
        },
        RegistryError::Registry(msg) => {
            // Registry errors from NoOpPluginRegistry indicate the operation
            // requires engine context that isn't available in this mode.
            if msg.contains("requires engine context") {
                ServiceError::NotImplemented { feature: msg }
            } else {
                ServiceError::Internal { message: msg }
            }
        }
        RegistryError::Io(msg) => ServiceError::Internal { message: msg },
    }
}

#[async_trait]
impl PluginService for AppServices {
    async fn list(&self) -> Result<Vec<PluginSummary>, ServiceError> {
        let plugins = self
            .ctx
            .plugin_registry
            .list_installed()
            .await
            .map_err(|e| registry_error_to_service(e, ""))?;

        Ok(plugins
            .into_iter()
            .map(|p| PluginSummary {
                name: p.name,
                version: p.version,
                plugin_type: p.plugin_type,
                size_bytes: p.size_bytes,
                installed_at: p.installed_at,
            })
            .collect())
    }

    async fn search(
        &self,
        request: PluginSearchRequest,
    ) -> Result<Vec<PluginSearchResult>, ServiceError> {
        let entries = self
            .ctx
            .plugin_registry
            .search(&request.query, request.plugin_type.as_deref())
            .await
            .map_err(|e| registry_error_to_service(e, &request.query))?;

        Ok(entries
            .into_iter()
            .map(|e| PluginSearchResult {
                name: e.name,
                description: e.description,
                latest_version: e.latest_version,
                plugin_type: e.plugin_type,
            })
            .collect())
    }

    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail, ServiceError> {
        let meta = self
            .ctx
            .plugin_registry
            .info(plugin_ref)
            .await
            .map_err(|e| registry_error_to_service(e, plugin_ref))?;

        Ok(PluginDetail {
            name: meta.name,
            version: meta.version,
            plugin_type: meta.plugin_type,
            description: meta.description,
            size_bytes: meta.size_bytes,
            manifest: meta.manifest,
            available_versions: meta.available_versions,
            installed: meta.installed,
        })
    }

    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult, ServiceError> {
        let installed = self
            .ctx
            .plugin_registry
            .install(plugin_ref)
            .await
            .map_err(|e| registry_error_to_service(e, plugin_ref))?;

        Ok(PluginInstallResult {
            name: installed.name,
            version: installed.version,
            plugin_type: installed.plugin_type,
            size_bytes: installed.size_bytes,
            installed_at: installed.installed_at,
        })
    }

    async fn remove(&self, plugin_ref: &str) -> Result<(), ServiceError> {
        self.ctx
            .plugin_registry
            .remove(plugin_ref)
            .await
            .map_err(|e| registry_error_to_service(e, plugin_ref))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_app_services;

    #[tokio::test]
    async fn list_returns_empty() {
        let services = fake_app_services();
        let result = services.list().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn search_returns_empty() {
        let services = fake_app_services();
        let result = services
            .search(PluginSearchRequest {
                query: "postgres".into(),
                plugin_type: None,
            })
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn install_returns_not_implemented() {
        let services = fake_app_services();
        let result = services.install("rapidbyte/source-postgres:1.0.0").await;
        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }
}
