use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rapidbyte_registry::RegistryConfig;
use rapidbyte_types::manifest::{Permissions, PluginManifest};
use rapidbyte_types::wire::{PluginKind, ProtocolVersion};

use crate::error::PipelineError;

pub struct ResolvedPlugins {
    pub source_wasm: PathBuf,
    pub dest_wasm: PathBuf,
    pub source_manifest: Option<PluginManifest>,
    pub dest_manifest: Option<PluginManifest>,
    pub source_permissions: Option<Permissions>,
    pub dest_permissions: Option<Permissions>,
}

/// Resolve source and destination plugin paths, load manifests, and extract permissions.
///
/// Uses the registry-aware resolver: OCI references (e.g.
/// `registry.example.com/source/postgres:1.2.0`) are resolved via cache/pull,
/// while bare names fall back to local filesystem search.
///
/// # Errors
///
/// Returns `PipelineError::Infrastructure` if plugin paths cannot be resolved
/// or manifests fail validation.
pub async fn resolve_plugins(
    config: &rapidbyte_pipeline_config::types::PipelineConfig,
    registry_config: &RegistryConfig,
) -> Result<ResolvedPlugins, PipelineError> {
    let source_wasm = rapidbyte_runtime::resolve_plugin(
        &config.source.use_ref,
        PluginKind::Source,
        registry_config,
    )
    .await
    .map_err(PipelineError::Infrastructure)?;

    let dest_wasm = rapidbyte_runtime::resolve_plugin(
        &config.destination.use_ref,
        PluginKind::Destination,
        registry_config,
    )
    .await
    .map_err(PipelineError::Infrastructure)?;

    let source_manifest =
        load_and_validate_manifest(&source_wasm, &config.source.use_ref, PluginKind::Source)
            .map_err(PipelineError::Infrastructure)?;
    let dest_manifest = load_and_validate_manifest(
        &dest_wasm,
        &config.destination.use_ref,
        PluginKind::Destination,
    )
    .map_err(PipelineError::Infrastructure)?;

    Ok(ResolvedPlugins {
        source_permissions: source_manifest.as_ref().map(|m| m.permissions.clone()),
        dest_permissions: dest_manifest.as_ref().map(|m| m.permissions.clone()),
        source_wasm,
        dest_wasm,
        source_manifest,
        dest_manifest,
    })
}

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
             Protocol V5 plugins are intentionally rejected because the host import ABI changed; \
             rebuild the plugin against rapidbyte:plugin@6.0.0.",
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
            &test_source_manifest(ProtocolVersion::V6),
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
        assert!(message.contains("host=V6"));
        assert!(message.contains("rebuild the plugin against rapidbyte:plugin@6.0.0"));
    }
}
