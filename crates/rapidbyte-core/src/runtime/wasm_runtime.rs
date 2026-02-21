//! Wasmtime component runtime and optional AOT caching helpers.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};

use super::component_runtime::ComponentHostState;
use super::connector_resolve::sha256_hex;

const RAPIDBYTE_WASMTIME_AOT_ENV: &str = "RAPIDBYTE_WASMTIME_AOT";
const RAPIDBYTE_WASMTIME_AOT_DIR_ENV: &str = "RAPIDBYTE_WASMTIME_AOT_DIR";

#[derive(Clone)]
pub struct LoadedComponent {
    pub engine: Arc<Engine>,
    pub component: Arc<Component>,
}

/// Manages loading connector components.
pub struct WasmRuntime {
    engine: Arc<Engine>,
    aot_cache_dir: Option<PathBuf>,
    aot_compat_hash: u64,
}

#[derive(Clone, Copy, Debug)]
enum AotLoadKind {
    CacheHit,
    Compiled,
}

pub fn create_component_linker<F>(
    engine: &Engine,
    role: &str,
    add_bindings: F,
) -> Result<Linker<ComponentHostState>>
where
    F: FnOnce(&mut Linker<ComponentHostState>) -> Result<()>,
{
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .with_context(|| format!("Failed to add WASI imports for {}", role))?;
    add_bindings(&mut linker)?;
    Ok(linker)
}

impl WasmRuntime {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);

        let engine = Engine::new(&config).context("Failed to initialize Wasmtime engine")?;
        let mut hasher = DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        let aot_compat_hash = hasher.finish();

        let aot_cache_dir = if env_flag_enabled(RAPIDBYTE_WASMTIME_AOT_ENV, true) {
            let dir = resolve_aot_cache_dir();
            match std::fs::create_dir_all(&dir) {
                Ok(()) => {
                    tracing::debug!(dir = %dir.display(), "Wasmtime AOT cache enabled");
                    Some(dir)
                }
                Err(err) => {
                    tracing::warn!(
                        dir = %dir.display(),
                        error = %err,
                        "Failed to create Wasmtime AOT cache directory; falling back to JIT"
                    );
                    None
                }
            }
        } else {
            tracing::debug!("Wasmtime AOT disabled via RAPIDBYTE_WASMTIME_AOT=0");
            None
        };

        Ok(Self {
            engine: Arc::new(engine),
            aot_cache_dir,
            aot_compat_hash,
        })
    }

    /// Load a component from a file on disk.
    pub fn load_module(&self, wasm_path: &Path) -> Result<LoadedComponent> {
        let load_start = Instant::now();
        let mut aot_load_kind: Option<AotLoadKind> = None;

        let component = if self.aot_cache_dir.is_some() {
            match self.load_module_aot(wasm_path) {
                Ok((component, kind)) => {
                    aot_load_kind = Some(kind);
                    component
                }
                Err(err) => {
                    tracing::warn!(
                        path = %wasm_path.display(),
                        error = %err,
                        "AOT load failed; falling back to direct Wasm load"
                    );
                    Component::from_file(&self.engine, wasm_path).with_context(|| {
                        format!("Failed to load Wasm component: {}", wasm_path.display())
                    })?
                }
            }
        } else {
            Component::from_file(&self.engine, wasm_path).with_context(|| {
                format!("Failed to load Wasm component: {}", wasm_path.display())
            })?
        };

        let load_ms = load_start.elapsed().as_millis() as u64;
        match aot_load_kind {
            Some(AotLoadKind::CacheHit) => {
                tracing::info!(
                    path = %wasm_path.display(),
                    load_ms,
                    "Loaded connector module from AOT cache"
                );
            }
            Some(AotLoadKind::Compiled) => {
                tracing::info!(
                    path = %wasm_path.display(),
                    load_ms,
                    "Precompiled connector module for AOT cache"
                );
            }
            None => {
                tracing::info!(
                    path = %wasm_path.display(),
                    load_ms,
                    "Loaded connector module without AOT cache"
                );
            }
        }

        Ok(LoadedComponent {
            engine: self.engine.clone(),
            component: Arc::new(component),
        })
    }

    pub fn new_store(&self, host_state: ComponentHostState) -> Store<ComponentHostState> {
        Store::new(&self.engine, host_state)
    }

    fn load_module_aot(&self, wasm_path: &Path) -> Result<(Component, AotLoadKind)> {
        let cache_dir = self
            .aot_cache_dir
            .as_ref()
            .context("AOT cache directory is not configured")?;

        let wasm_bytes = std::fs::read(wasm_path)
            .with_context(|| format!("Failed to read Wasm component: {}", wasm_path.display()))?;
        let wasm_hash = sha256_hex(&wasm_bytes);
        let artifact_path = self.aot_artifact_path(cache_dir, wasm_path, &wasm_hash);

        if artifact_path.exists() {
            // SAFETY: this artifact is generated by Wasmtime's `precompile_component` and
            // scoped to the current engine compatibility hash in its filename. If loading
            // fails (e.g. corruption), we remove it and rebuild.
            match unsafe { Component::deserialize_file(&self.engine, &artifact_path) } {
                Ok(component) => {
                    tracing::debug!(
                        path = %wasm_path.display(),
                        artifact = %artifact_path.display(),
                        "Loaded connector from Wasmtime AOT artifact"
                    );
                    return Ok((component, AotLoadKind::CacheHit));
                }
                Err(err) => {
                    tracing::warn!(
                        artifact = %artifact_path.display(),
                        error = %err,
                        "Failed to load cached Wasmtime AOT artifact; recompiling"
                    );
                    let _ = std::fs::remove_file(&artifact_path);
                }
            }
        }

        let precompiled = self
            .engine
            .precompile_component(&wasm_bytes)
            .with_context(|| format!("Failed to AOT-precompile {}", wasm_path.display()))?;

        if let Err(err) = write_file_atomic(&artifact_path, &precompiled) {
            tracing::warn!(
                artifact = %artifact_path.display(),
                error = %err,
                "Failed to persist Wasmtime AOT artifact; continuing with in-memory artifact"
            );
        }

        // SAFETY: these bytes were produced by `Engine::precompile_component` on this exact
        // engine, so deserializing them is trusted and sound.
        let component = unsafe { Component::deserialize(&self.engine, &precompiled) }
            .with_context(|| {
                format!(
                    "Failed to deserialize in-memory Wasmtime AOT artifact for {}",
                    wasm_path.display()
                )
            })?;

        tracing::debug!(
            path = %wasm_path.display(),
            artifact = %artifact_path.display(),
            "Compiled connector into Wasmtime AOT artifact"
        );
        Ok((component, AotLoadKind::Compiled))
    }

    fn aot_artifact_path(&self, cache_dir: &Path, wasm_path: &Path, wasm_hash: &str) -> PathBuf {
        let stem = wasm_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("component");
        let stem = sanitize_cache_component_name(stem);
        let short_hash = &wasm_hash[..wasm_hash.len().min(16)];
        cache_dir.join(format!(
            "{}-{}-{:016x}.cwasm",
            stem, short_hash, self.aot_compat_hash
        ))
    }
}

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            other => {
                tracing::warn!(
                    env = name,
                    value = other,
                    "Invalid boolean env value, using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn resolve_aot_cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(RAPIDBYTE_WASMTIME_AOT_DIR_ENV) {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".rapidbyte")
            .join("cache")
            .join("wasmtime-aot");
    }

    std::env::temp_dir().join("rapidbyte").join("wasmtime-aot")
}

fn sanitize_cache_component_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "component".to_string()
    } else {
        sanitized
    }
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("Invalid artifact path (no parent): {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create directory: {}", parent.display()))?;

    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("artifact");
    let tmp = parent.join(format!(
        ".{}.{}.{}.tmp",
        filename,
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));

    std::fs::write(&tmp, bytes)
        .with_context(|| format!("Failed to write temp artifact: {}", tmp.display()))?;

    match std::fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = std::fs::remove_file(&tmp);
            if path.exists() {
                Ok(())
            } else {
                Err(err).with_context(|| {
                    format!("Failed to move artifact into place: {}", path.display())
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_cache_component_name() {
        assert_eq!(sanitize_cache_component_name("abc-123"), "abc-123");
        assert_eq!(sanitize_cache_component_name("abc/123"), "abc_123");
        assert_eq!(sanitize_cache_component_name(""), "component");
    }

    #[test]
    fn test_env_flag_enabled_defaults() {
        assert!(env_flag_enabled("RAPIDBYTE_TEST_FLAG_NOT_SET", true));
        assert!(!env_flag_enabled("RAPIDBYTE_TEST_FLAG_NOT_SET", false));
    }
}
