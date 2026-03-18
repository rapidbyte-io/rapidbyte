//! [`PluginResolver`] adapter backed by the OCI registry and local filesystem.
//!
//! Wraps [`rapidbyte_runtime::resolve_plugin`] and manifest validation logic
//! from [`crate::plugin::resolver`] into a trait-object-safe adapter.

use async_trait::async_trait;
use rapidbyte_registry::RegistryConfig;
use rapidbyte_types::wire::PluginKind;

use crate::domain::error::PipelineError;
use crate::domain::ports::resolver::{PluginResolver, ResolvedPlugin};
use crate::plugin::resolver::{load_and_validate_manifest, validate_config_against_schema};

/// Plugin resolver that uses the OCI registry (with local filesystem fallback).
///
/// Resolution strategy:
/// 1. Resolve the WASM path via [`rapidbyte_runtime::resolve_plugin`]
///    (OCI pull or local search).
/// 2. Load and validate the plugin manifest from the WASM binary.
/// 3. If a config is provided and a manifest schema exists, validate
///    the config against the JSON Schema.
/// 4. Return the resolved WASM path and manifest.
pub struct RegistryPluginResolver {
    registry_config: RegistryConfig,
}

impl RegistryPluginResolver {
    /// Create a new resolver with the given registry configuration.
    #[must_use]
    pub fn new(registry_config: RegistryConfig) -> Self {
        Self { registry_config }
    }
}

#[async_trait]
impl PluginResolver for RegistryPluginResolver {
    async fn resolve(
        &self,
        plugin_ref: &str,
        expected_kind: PluginKind,
        config_json: Option<&serde_json::Value>,
    ) -> Result<ResolvedPlugin, PipelineError> {
        // 1. Resolve WASM path (OCI registry or local filesystem)
        let wasm_path =
            rapidbyte_runtime::resolve_plugin(plugin_ref, expected_kind, &self.registry_config)
                .await
                .map_err(PipelineError::Infrastructure)?;

        // 2. Load manifest from the WASM binary and validate kind/protocol
        let manifest = load_and_validate_manifest(&wasm_path, plugin_ref, expected_kind)
            .map_err(PipelineError::Infrastructure)?;

        // 3. Validate config against manifest schema (if both are present)
        if let (Some(config), Some(ref m)) = (config_json, &manifest) {
            validate_config_against_schema(plugin_ref, config, m)
                .map_err(PipelineError::Infrastructure)?;
        }

        Ok(ResolvedPlugin {
            wasm_path,
            manifest,
        })
    }
}
