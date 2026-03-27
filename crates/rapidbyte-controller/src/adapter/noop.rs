use async_trait::async_trait;

use crate::domain::ports::connection_tester::{
    ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
};
use crate::domain::ports::pipeline_inspector::{
    CheckOutput, DiffOutput, InspectorError, PipelineInspector,
};
use crate::domain::ports::plugin_registry::{
    InstalledPlugin, PluginMetadata, PluginRegistry, RegistryEntry, RegistryError,
};

// ---------------------------------------------------------------------------
// NoOpConnectionTester
// ---------------------------------------------------------------------------

/// Placeholder [`ConnectionTester`] used when engine context is unavailable.
///
/// Both methods return a [`ConnectionTestError::Plugin`] error so callers know
/// that connection testing requires a real engine-backed implementation.
pub struct NoOpConnectionTester;

#[async_trait]
impl ConnectionTester for NoOpConnectionTester {
    async fn test(&self, _config: &serde_json::Value) -> Result<TestResult, ConnectionTestError> {
        Err(ConnectionTestError::Plugin(
            "connection testing requires engine context".into(),
        ))
    }

    async fn discover(
        &self,
        _config: &serde_json::Value,
        _table: Option<&str>,
    ) -> Result<DiscoveryResult, ConnectionTestError> {
        Err(ConnectionTestError::Plugin(
            "connection discovery requires engine context".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// NoOpPipelineInspector
// ---------------------------------------------------------------------------

/// Placeholder [`PipelineInspector`] used when engine context is unavailable.
///
/// Both methods return a [`InspectorError::Plugin`] error so callers know
/// that pipeline inspection requires a real engine-backed implementation.
pub struct NoOpPipelineInspector;

#[async_trait]
impl PipelineInspector for NoOpPipelineInspector {
    async fn check(&self, _pipeline_yaml: &str) -> Result<CheckOutput, InspectorError> {
        Err(InspectorError::Plugin("requires engine context".into()))
    }

    async fn diff(&self, _pipeline_yaml: &str) -> Result<DiffOutput, InspectorError> {
        Err(InspectorError::Plugin("requires engine context".into()))
    }
}

// ---------------------------------------------------------------------------
// NoOpPluginRegistry
// ---------------------------------------------------------------------------

/// Placeholder [`PluginRegistry`] used when engine context is unavailable.
///
/// Read operations return empty results; mutating operations return a
/// [`RegistryError::Unavailable`] error so callers know that plugin
/// operations require a real engine-backed implementation.
pub struct NoOpPluginRegistry;

#[async_trait]
impl PluginRegistry for NoOpPluginRegistry {
    async fn list_installed(&self) -> Result<Vec<InstalledPlugin>, RegistryError> {
        Ok(vec![])
    }

    async fn search(
        &self,
        _query: &str,
        _plugin_type: Option<&str>,
    ) -> Result<Vec<RegistryEntry>, RegistryError> {
        Ok(vec![])
    }

    async fn info(&self, _plugin_ref: &str) -> Result<PluginMetadata, RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin info requires engine context".into(),
        ))
    }

    async fn install(&self, _plugin_ref: &str) -> Result<InstalledPlugin, RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin installation requires engine context".into(),
        ))
    }

    async fn remove(&self, _plugin_ref: &str) -> Result<(), RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin removal requires engine context".into(),
        ))
    }
}
