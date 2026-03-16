//! Plugin validation and discovery runners.

use std::sync::Arc;

use anyhow::{Context, Result};
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, dest_validation_to_sdk,
    source_bindings, source_error_to_sdk, source_validation_to_sdk, transform_bindings,
    transform_error_to_sdk, transform_validation_to_sdk, ComponentHostState, WasmRuntime,
};
use rapidbyte_state::SqliteStateBackend;
use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::wire::PluginKind;

/// Validate a plugin configuration by invoking its lifecycle validation
/// entrypoint in-process.
///
/// # Errors
///
/// Returns an error if the component cannot be loaded, opened, validated,
/// or if the plugin reports an invalid configuration.
#[allow(clippy::too_many_lines)]
pub(crate) fn validate_plugin(
    wasm_path: &std::path::Path,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    stream_name: &str,
    permissions: Option<&Permissions>,
) -> Result<ValidationResult> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, "Validating plugin");

    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;
    let state = Arc::new(SqliteStateBackend::in_memory()?);

    let mut builder = ComponentHostState::builder()
        .pipeline("check")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("validate:{kind:?}:{plugin_id}"))
        .stream(stream_name)
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(source_validation_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(dest_validation_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            let linker = create_component_linker(&module.engine, "transform", |linker| {
                transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = transform_bindings::RapidbyteTransform::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_transform();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Transform open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
                .map(transform_validation_to_sdk)
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot validate plugin with unknown kind")
        }
    }
}

/// Discover available streams from a source plugin.
pub(crate) fn run_discover(
    wasm_path: &std::path::Path,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<Catalog> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let mut builder = ComponentHostState::builder()
        .pipeline("discover")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("discover:source:{plugin_id}"))
        .stream("discover")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)?;
        Ok(())
    })?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)?;
    let iface = bindings.rapidbyte_plugin_source();
    let config_json = serde_json::to_string(config)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Opening source plugin for discover"
    );
    let session = iface
        .call_open(&mut store, &config_json)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!("Source open failed for discover: {e}"))?;

    let discover_json = iface
        .call_discover(&mut store, session)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let catalog = serde_json::from_str::<Catalog>(&discover_json)
        .context("Failed to parse discover catalog JSON")?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Closing source plugin after discover"
    );
    if let Err(err) = iface.call_close(&mut store, session)? {
        tracing::warn!(
            "Source close failed after discover: {}",
            source_error_to_sdk(err)
        );
    }

    Ok(catalog)
}
