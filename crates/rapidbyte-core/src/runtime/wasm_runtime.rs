use std::path::Path;

use anyhow::{Context, Result};
use rapidbyte_sdk::manifest::ConnectorManifest;
use wasmedge_sdk::{ImportObjectBuilder, Module};

use super::host_functions::{
    host_checkpoint, host_emit_batch, host_last_error, host_log, host_metric_fn, host_next_batch,
    host_state_get, host_state_put, HostState,
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
        let module = Module::from_bytes(None, wasm_bytes)
            .context("Failed to load Wasm module from bytes")?;
        Ok(module)
    }
}

/// Create an ImportObject with all "rapidbyte" host functions registered.
///
/// The returned ImportObject must be kept alive for the lifetime of the Vm
/// that uses it. The caller creates a `HashMap<String, &mut dyn SyncInst>`,
/// inserts the import object, then passes it to `Store::new` and `Vm::new`.
pub fn create_host_imports(host_state: HostState) -> Result<wasmedge_sdk::ImportObject<HostState>> {
    let mut import_builder = ImportObjectBuilder::new("rapidbyte", host_state)
        .context("Failed to create import object builder")?;

    // rb_host_log(level, msg_ptr, msg_len) -> i32
    import_builder
        .with_func::<(i32, i32, i32), i32>("rb_host_log", host_log)
        .context("Failed to register rb_host_log")?;

    // rb_host_emit_batch(ptr: u32, len: u32) -> i32
    import_builder
        .with_func::<(i32, i32), i32>("rb_host_emit_batch", host_emit_batch)
        .context("Failed to register rb_host_emit_batch")?;

    // rb_host_next_batch(out_ptr: u32, out_cap: u32) -> i32
    import_builder
        .with_func::<(i32, i32), i32>("rb_host_next_batch", host_next_batch)
        .context("Failed to register rb_host_next_batch")?;

    // rb_host_last_error(out_ptr: u32, out_cap: u32) -> i32
    import_builder
        .with_func::<(i32, i32), i32>("rb_host_last_error", host_last_error)
        .context("Failed to register rb_host_last_error")?;

    // rb_host_state_get(scope: i32, key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
    import_builder
        .with_func::<(i32, i32, i32, i32, i32), i32>("rb_host_state_get", host_state_get)
        .context("Failed to register rb_host_state_get")?;

    // rb_host_state_put(scope: i32, key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
    import_builder
        .with_func::<(i32, i32, i32, i32, i32), i32>("rb_host_state_put", host_state_put)
        .context("Failed to register rb_host_state_put")?;

    // rb_host_checkpoint(kind: i32, payload_ptr: u32, payload_len: u32) -> i32
    import_builder
        .with_func::<(i32, i32, i32), i32>("rb_host_checkpoint", host_checkpoint)
        .context("Failed to register rb_host_checkpoint")?;

    // rb_host_metric(payload_ptr: u32, payload_len: u32) -> i32
    import_builder
        .with_func::<(i32, i32), i32>("rb_host_metric", host_metric_fn)
        .context("Failed to register rb_host_metric")?;

    Ok(import_builder.build())
}

/// Parse a connector reference into (connector_id, connector_version).
///
/// Examples:
/// - "rapidbyte/source-postgres@v0.1.0" -> ("source-postgres", "0.1.0")
/// - "source-postgres@v0.1.0"           -> ("source-postgres", "0.1.0")
/// - "source-postgres"                  -> ("source-postgres", "unknown")
pub fn parse_connector_ref(connector_ref: &str) -> (String, String) {
    let after_slash = connector_ref
        .split('/')
        .next_back()
        .unwrap_or(connector_ref);

    let (name, version) = match after_slash.split_once('@') {
        Some((n, v)) => (n.to_string(), v.strip_prefix('v').unwrap_or(v).to_string()),
        None => (after_slash.to_string(), "unknown".to_string()),
    };

    (name, version)
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
        .next_back()
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

/// Derive the manifest file path from a WASM binary path.
/// `source_postgres.wasm` -> `source_postgres.manifest.json`
pub fn manifest_path_from_wasm(wasm_path: &Path) -> std::path::PathBuf {
    let stem = wasm_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    wasm_path.with_file_name(format!("{}.manifest.json", stem))
}

/// Verify WASM binary checksum against the manifest-declared value.
pub fn verify_wasm_checksum(wasm_path: &Path, expected_hex: &str) -> Result<bool> {
    use sha2::{Digest, Sha256};

    let bytes = std::fs::read(wasm_path)
        .with_context(|| format!("Failed to read WASM binary: {}", wasm_path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let actual = format!("{:x}", hasher.finalize());

    Ok(actual == expected_hex)
}

/// Load a connector manifest from disk. Returns None if the manifest file
/// doesn't exist (backwards-compatible with connectors that don't have one yet).
pub fn load_connector_manifest(wasm_path: &Path) -> Result<Option<ConnectorManifest>> {
    let manifest_path = manifest_path_from_wasm(wasm_path);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;
    let manifest: ConnectorManifest = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse manifest: {}", manifest_path.display()))?;

    if let Some(ref expected) = manifest.checksum {
        let valid = verify_wasm_checksum(wasm_path, expected)?;
        if !valid {
            anyhow::bail!(
                "WASM checksum mismatch for {}. Expected: {}, file may be corrupted or tampered.",
                wasm_path.display(),
                expected,
            );
        }
        tracing::debug!(
            path = %wasm_path.display(),
            "WASM checksum verified"
        );
    }

    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_connector_ref() {
        assert_eq!(
            parse_connector_ref("rapidbyte/source-postgres@v0.1.0"),
            ("source-postgres".to_string(), "0.1.0".to_string())
        );
        assert_eq!(
            parse_connector_ref("dest-postgres@v0.1.0"),
            ("dest-postgres".to_string(), "0.1.0".to_string())
        );
        assert_eq!(
            parse_connector_ref("source-postgres"),
            ("source-postgres".to_string(), "unknown".to_string())
        );
        assert_eq!(
            parse_connector_ref("rapidbyte/my-connector"),
            ("my-connector".to_string(), "unknown".to_string())
        );
        // Version without v prefix
        assert_eq!(
            parse_connector_ref("conn@1.2.3"),
            ("conn".to_string(), "1.2.3".to_string())
        );
    }

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
                .next_back()
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

    #[test]
    fn test_manifest_path_from_wasm_path() {
        let wasm = std::path::PathBuf::from("/plugins/source_postgres.wasm");
        let manifest = manifest_path_from_wasm(&wasm);
        assert_eq!(
            manifest,
            std::path::PathBuf::from("/plugins/source_postgres.manifest.json")
        );
    }

    #[test]
    fn test_manifest_path_from_wasm_path_aot() {
        let wasm = std::path::PathBuf::from("/plugins/source_postgres.so");
        let manifest = manifest_path_from_wasm(&wasm);
        assert_eq!(
            manifest,
            std::path::PathBuf::from("/plugins/source_postgres.manifest.json")
        );
    }

    #[test]
    fn test_verify_checksum_matches() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let wasm_path = dir.path().join("test.wasm");
        let mut f = std::fs::File::create(&wasm_path).unwrap();
        f.write_all(b"fake wasm content").unwrap();

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"fake wasm content");
        let expected = format!("{:x}", hasher.finalize());

        assert!(verify_wasm_checksum(&wasm_path, &expected).unwrap());
        assert!(!verify_wasm_checksum(&wasm_path, "wrong_checksum").unwrap());
    }
}
