//! WASM module loading with parallel compilation.

use std::sync::Arc;
use std::time::Instant;

use rapidbyte_runtime::{parse_plugin_ref, LoadedComponent, WasmRuntime};
use rapidbyte_types::manifest::{Permissions, ResourceLimits};
use rapidbyte_types::wire::PluginKind;

use crate::config::types::PipelineConfig;
use crate::error::PipelineError;
use crate::plugin::resolver::{load_and_validate_manifest, validate_config_against_schema};

/// A WASM component loaded with its compilation timing.
pub(crate) struct LoadedModule {
    pub(crate) component: LoadedComponent,
    pub(crate) load_ms: u64,
}

/// A transform WASM module with associated plugin metadata and config.
#[derive(Clone)]
pub(crate) struct TransformModule {
    pub(crate) module: LoadedComponent,
    pub(crate) plugin_id: String,
    pub(crate) plugin_version: String,
    pub(crate) config: serde_json::Value,
    pub(crate) load_ms: u64,
    pub(crate) permissions: Option<Permissions>,
    pub(crate) manifest_limits: ResourceLimits,
}

/// All loaded plugin modules for a pipeline run.
pub(crate) struct PluginModules {
    pub(crate) source_module: LoadedComponent,
    pub(crate) dest_module: LoadedComponent,
    pub(crate) source_module_load_ms: u64,
    pub(crate) dest_module_load_ms: u64,
    pub(crate) transform_modules: Vec<TransformModule>,
}

/// Spawn a blocking task to load a single WASM module, returning load timing.
fn spawn_module_load(
    runtime: Arc<WasmRuntime>,
    wasm_path: std::path::PathBuf,
) -> tokio::task::JoinHandle<Result<LoadedModule, PipelineError>> {
    tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let component = runtime
            .load_module(&wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        #[allow(clippy::cast_possible_truncation)]
        let load_ms = start.elapsed().as_millis() as u64;
        Ok(LoadedModule { component, load_ms })
    })
}

/// Load all plugin modules (source, destination, transforms) in parallel where possible.
///
/// Source and destination are loaded concurrently via `spawn_blocking`. Transform modules
/// are loaded sequentially after manifest validation.
///
/// # Errors
///
/// Returns `PipelineError::Infrastructure` if any module fails to load or a blocking
/// task panics.
pub(crate) async fn load_all_modules(
    config: &PipelineConfig,
    plugins: &crate::plugin::resolver::ResolvedPlugins,
    registry_config: &rapidbyte_registry::RegistryConfig,
) -> Result<PluginModules, PipelineError> {
    let runtime = Arc::new(WasmRuntime::new().map_err(PipelineError::Infrastructure)?);
    tracing::info!(
        source = %plugins.source_wasm.display(),
        destination = %plugins.dest_wasm.display(),
        "Loading plugin modules"
    );

    let source_load_task = spawn_module_load(runtime.clone(), plugins.source_wasm.clone());
    let dest_load_task = spawn_module_load(runtime.clone(), plugins.dest_wasm.clone());

    let source_loaded = source_load_task
        .await
        .map_err(|e| PipelineError::task_panicked("Source module load", e))??;
    let dest_loaded = dest_load_task
        .await
        .map_err(|e| PipelineError::task_panicked("Destination module load", e))??;

    tracing::info!(
        source_ms = source_loaded.load_ms,
        dest_ms = dest_loaded.load_ms,
        "Plugin modules loaded"
    );

    let mut transform_modules = Vec::with_capacity(config.transforms.len());
    for tc in &config.transforms {
        let wasm_path =
            rapidbyte_runtime::resolve_plugin(&tc.use_ref, PluginKind::Transform, registry_config)
                .await
                .map_err(PipelineError::Infrastructure)?;
        let manifest = load_and_validate_manifest(&wasm_path, &tc.use_ref, PluginKind::Transform)
            .map_err(PipelineError::Infrastructure)?;
        if let Some(ref m) = manifest {
            validate_config_against_schema(&tc.use_ref, &tc.config, m)
                .map_err(PipelineError::Infrastructure)?;
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let transform_manifest_limits = manifest
            .as_ref()
            .map(|m| m.limits.clone())
            .unwrap_or_default();
        let load_start = Instant::now();
        let module = runtime
            .load_module(&wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        #[allow(clippy::cast_possible_truncation)]
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        let (id, ver) = parse_plugin_ref(&tc.use_ref);
        transform_modules.push(TransformModule {
            module,
            plugin_id: id,
            plugin_version: ver,
            config: tc.config.clone(),
            load_ms,
            permissions: transform_perms,
            manifest_limits: transform_manifest_limits,
        });
    }

    Ok(PluginModules {
        source_module: source_loaded.component,
        dest_module: dest_loaded.component,
        source_module_load_ms: source_loaded.load_ms,
        dest_module_load_ms: dest_loaded.load_ms,
        transform_modules,
    })
}
