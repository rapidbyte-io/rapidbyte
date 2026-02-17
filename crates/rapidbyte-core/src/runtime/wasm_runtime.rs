use std::path::Path;

use anyhow::{Context, Result};
use wasmedge_sdk::{ImportObjectBuilder, Module};

use super::host_functions::{
    host_emit_record_batch, host_get_state, host_log, host_report_progress, host_set_state,
    HostState,
};

/// Manages loading Wasm modules.
pub struct WasmRuntime;

impl WasmRuntime {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    /// Load a Wasm module from a file on disk.
    pub fn load_module(&self, wasm_path: &Path) -> Result<Module> {
        let module = Module::from_file(None, wasm_path)
            .with_context(|| format!("Failed to load Wasm module: {}", wasm_path.display()))?;
        Ok(module)
    }

    /// Load a Wasm module from in-memory bytes (WAT or Wasm binary).
    pub fn load_module_from_bytes(&self, bytes: &[u8]) -> Result<Module> {
        let wasm_bytes = if bytes.starts_with(b"(") {
            wasmedge_sdk::wat2wasm(bytes).context("Failed to convert WAT to Wasm")?
        } else {
            bytes.into()
        };
        let module =
            Module::from_bytes(None, wasm_bytes).context("Failed to load Wasm module from bytes")?;
        Ok(module)
    }
}

/// Create an ImportObject with all "rapidbyte" host functions registered.
///
/// The returned ImportObject must be kept alive for the lifetime of the Vm
/// that uses it. The caller creates a `HashMap<String, &mut dyn SyncInst>`,
/// inserts the import object, then passes it to `Store::new` and `Vm::new`.
pub fn create_host_imports(
    host_state: HostState,
) -> Result<wasmedge_sdk::ImportObject<HostState>> {
    let mut import_builder = ImportObjectBuilder::new("rapidbyte", host_state)
        .context("Failed to create import object builder")?;

    // rb_host_emit_record_batch(stream_ptr, stream_len, batch_ptr, batch_len) -> i32
    import_builder
        .with_func::<(i32, i32, i32, i32), i32>(
            "rb_host_emit_record_batch",
            host_emit_record_batch,
        )
        .context("Failed to register rb_host_emit_record_batch")?;

    // rb_host_get_state(key_ptr, key_len, out_ptr, out_len) -> i32
    import_builder
        .with_func::<(i32, i32, i32, i32), i32>("rb_host_get_state", host_get_state)
        .context("Failed to register rb_host_get_state")?;

    // rb_host_set_state(key_ptr, key_len, val_ptr, val_len) -> i32
    import_builder
        .with_func::<(i32, i32, i32, i32), i32>("rb_host_set_state", host_set_state)
        .context("Failed to register rb_host_set_state")?;

    // rb_host_log(level, msg_ptr, msg_len) -> i32
    import_builder
        .with_func::<(i32, i32, i32), i32>("rb_host_log", host_log)
        .context("Failed to register rb_host_log")?;

    // rb_host_report_progress(records: i64, bytes: i64) -> i32
    import_builder
        .with_func::<(i64, i64), i32>("rb_host_report_progress", host_report_progress)
        .context("Failed to register rb_host_report_progress")?;

    Ok(import_builder.build())
}

/// Resolve a connector reference (e.g. "rapidbyte/source-postgres@v0.1.0")
/// to a .wasm file path on disk.
///
/// Resolution order:
/// 1. RAPIDBYTE_CONNECTOR_DIR env var
/// 2. ~/.rapidbyte/plugins/
///
/// The connector ref is mapped to a filename by extracting the connector name
/// and replacing hyphens with underscores: "source-postgres" -> "source_postgres.wasm"
pub fn resolve_connector_path(connector_ref: &str) -> Result<std::path::PathBuf> {
    let name = connector_ref
        .split('/')
        .last()
        .unwrap_or(connector_ref)
        .split('@')
        .next()
        .unwrap_or(connector_ref);

    let filename = format!("{}.wasm", name.replace('-', "_"));

    // Check RAPIDBYTE_CONNECTOR_DIR first
    if let Ok(dir) = std::env::var("RAPIDBYTE_CONNECTOR_DIR") {
        let path = Path::new(&dir).join(&filename);
        if path.exists() {
            return Ok(path);
        }
    }

    // Fall back to ~/.rapidbyte/plugins/
    if let Ok(home) = std::env::var("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".rapidbyte")
            .join("plugins")
            .join(&filename);
        if path.exists() {
            return Ok(path);
        }
    }

    anyhow::bail!(
        "Connector '{}' not found. Searched for '{}' in RAPIDBYTE_CONNECTOR_DIR and ~/.rapidbyte/plugins/",
        connector_ref,
        filename
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_resolve_connector_name_parsing() {
        let refs_and_expected = vec![
            ("rapidbyte/source-postgres@v0.1.0", "source_postgres.wasm"),
            ("source-postgres", "source_postgres.wasm"),
            ("dest-postgres@v0.1.0", "dest_postgres.wasm"),
            ("my-connector", "my_connector.wasm"),
        ];

        for (connector_ref, expected_filename) in refs_and_expected {
            let name = connector_ref
                .split('/')
                .last()
                .unwrap_or(connector_ref)
                .split('@')
                .next()
                .unwrap_or(connector_ref);
            let filename = format!("{}.wasm", name.replace('-', "_"));
            assert_eq!(
                filename, expected_filename,
                "Failed for ref: {}",
                connector_ref
            );
        }
    }
}
