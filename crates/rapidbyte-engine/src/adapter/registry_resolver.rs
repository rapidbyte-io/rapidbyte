//! [`PluginResolver`] adapter backed by the OCI registry and local filesystem.
//!
//! Self-contained implementation — manifest loading and config schema
//! validation live directly in this module.

use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rapidbyte_registry::RegistryConfig;
use rapidbyte_types::manifest::PluginManifest;
use rapidbyte_types::wire::{PluginKind, ProtocolVersion};

use crate::domain::error::PipelineError;
use crate::domain::ports::resolver::{PluginResolver, ResolvedPlugin};

// ── Public adapter ──────────────────────────────────────────────────

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

// ── Manifest loading and validation ─────────────────────────────────

/// Load a plugin manifest from the WASM binary and validate kind/protocol version.
///
/// # Errors
///
/// Returns an error if the manifest cannot be loaded, the plugin kind does not match
/// `expected_kind`, or the protocol version is incompatible.
pub fn load_and_validate_manifest(
    wasm_path: &Path,
    plugin_ref: &str,
    expected_kind: PluginKind,
) -> Result<Option<PluginManifest>> {
    let manifest = rapidbyte_runtime::load_plugin_manifest(wasm_path)?;

    if let Some(ref m) = manifest {
        validate_manifest_compatibility(m, plugin_ref, expected_kind)?;

        tracing::info!(plugin = m.id, version = m.version, "Loaded plugin manifest");
    } else {
        tracing::debug!(
            plugin = plugin_ref,
            "No manifest found, skipping pre-flight validation"
        );
    }

    Ok(manifest)
}

fn validate_manifest_compatibility(
    manifest: &PluginManifest,
    plugin_ref: &str,
    expected_kind: PluginKind,
) -> Result<()> {
    if !manifest.supports_kind(expected_kind) {
        anyhow::bail!("Plugin '{plugin_ref}' does not support {expected_kind:?} kind");
    }

    let host_protocol = ProtocolVersion::current();
    if manifest.protocol_version != host_protocol {
        anyhow::bail!(
            "Plugin '{plugin_ref}' protocol version mismatch: manifest={:?}, host={:?}. \
             Plugins built for a different protocol version are rejected; \
             rebuild the plugin against rapidbyte:plugin@9.0.0.",
            manifest.protocol_version,
            host_protocol
        );
    }

    Ok(())
}

/// Validate plugin configuration against the JSON Schema declared in its manifest.
///
/// # Errors
///
/// Returns an error if the manifest's JSON Schema is invalid or the configuration
/// does not conform to the schema.
pub fn validate_config_against_schema(
    plugin_ref: &str,
    config: &serde_json::Value,
    manifest: &PluginManifest,
) -> Result<()> {
    let Some(schema_value) = &manifest.config_schema else {
        return Ok(());
    };

    let validator = jsonschema::validator_for(schema_value)
        .with_context(|| format!("Invalid JSON Schema in manifest for plugin '{plugin_ref}'"))?;

    let errors: Vec<String> = validator
        .iter_errors(config)
        .map(|e| format!("  - {e}"))
        .collect();

    if !errors.is_empty() {
        anyhow::bail!(
            "Configuration validation failed for plugin '{}':\n{}",
            plugin_ref,
            errors.join("\n"),
        );
    }

    tracing::debug!(plugin = plugin_ref, "Config schema validation passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::manifest::{
        Permissions, PluginManifest, ResourceLimits, Roles, SourceCapabilities,
    };
    use rapidbyte_types::wire::SyncMode;

    fn test_source_manifest(protocol_version: ProtocolVersion) -> PluginManifest {
        PluginManifest {
            id: "rapidbyte/source-test".into(),
            name: "Test Source".into(),
            version: "0.1.0".into(),
            description: String::new(),
            author: None,
            license: None,
            protocol_version,
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh],
                    features: vec![],
                }),
                ..Roles::default()
            },
            config_schema: None,
        }
    }

    #[test]
    fn validate_manifest_compatibility_accepts_current_protocol_version() {
        validate_manifest_compatibility(
            &test_source_manifest(ProtocolVersion::V9),
            "test-plugin",
            PluginKind::Source,
        )
        .unwrap();
    }

    #[test]
    fn validate_manifest_compatibility_rejects_v5_with_migration_guidance() {
        let err = validate_manifest_compatibility(
            &test_source_manifest(ProtocolVersion::V5),
            "test-plugin",
            PluginKind::Source,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("protocol version mismatch"));
        assert!(message.contains("manifest=V5"));
        assert!(message.contains("host=V9"));
        assert!(message.contains("rebuild the plugin against rapidbyte:plugin@9.0.0"));
    }

    fn manifest_with_schema(schema: serde_json::Value) -> PluginManifest {
        PluginManifest {
            config_schema: Some(schema),
            ..test_source_manifest(ProtocolVersion::V9)
        }
    }

    #[test]
    fn config_matches_schema_passes() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "host": { "type": "string" } },
            "required": ["host"]
        });
        let manifest = manifest_with_schema(schema);
        let config = serde_json::json!({ "host": "localhost" });
        let result = validate_config_against_schema("test-plugin", &config, &manifest);
        assert!(result.is_ok());
    }

    #[test]
    fn config_violates_schema_returns_error_with_details() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "host": { "type": "string" } },
            "required": ["host"]
        });
        let manifest = manifest_with_schema(schema);
        let config = serde_json::json!({ "port": 5432 }); // missing "host"
        let result = validate_config_against_schema("test-plugin", &config, &manifest);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("test-plugin"));
    }
}
