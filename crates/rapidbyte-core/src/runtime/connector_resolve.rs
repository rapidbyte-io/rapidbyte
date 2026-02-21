use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rapidbyte_types::manifest::ConnectorManifest;
use sha2::{Digest, Sha256};

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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
pub fn resolve_connector_path(connector_ref: &str) -> Result<PathBuf> {
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
        let path = PathBuf::from(home)
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
pub fn manifest_path_from_wasm(wasm_path: &Path) -> PathBuf {
    let stem = wasm_path.file_stem().unwrap_or_default().to_string_lossy();
    wasm_path.with_file_name(format!("{}.manifest.json", stem))
}

/// Verify WASM binary checksum against the manifest-declared value.
pub fn verify_wasm_checksum(wasm_path: &Path, expected: &str) -> Result<bool> {
    // Strip algorithm prefix (e.g., "sha256:abcd..." -> "abcd...")
    let expected_hex = expected.strip_prefix("sha256:").unwrap_or(expected);

    let bytes = std::fs::read(wasm_path)
        .with_context(|| format!("Failed to read WASM binary: {}", wasm_path.display()))?;
    let actual = sha256_hex(&bytes);

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

    if let Some(ref expected) = manifest.artifact.checksum {
        let valid = verify_wasm_checksum(wasm_path, expected)?;
        if !valid {
            anyhow::bail!(
                "WASM checksum mismatch for {}. Expected: {}, file may be corrupted or tampered.",
                wasm_path.display(),
                expected,
            );
        }
        tracing::debug!(path = %wasm_path.display(), "WASM checksum verified");
    }

    Ok(Some(manifest))
}
