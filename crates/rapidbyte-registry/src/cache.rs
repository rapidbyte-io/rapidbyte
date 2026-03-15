//! Local disk cache for OCI plugin artifacts.
//!
//! Cache layout on disk:
//!
//! ```text
//! <root>/<registry>/<repository>/<tag>/
//!   plugin.wasm       — the WASM binary
//!   manifest.json     — PluginManifest JSON
//!   sha256            — hex digest of plugin.wasm
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::reference::PluginRef;
use crate::verify::sha256_hex;

/// A cached plugin entry on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    /// The plugin reference this entry corresponds to.
    pub plugin_ref: PluginRef,
    /// Path to `plugin.wasm` on disk.
    pub wasm_path: PathBuf,
    /// Path to `manifest.json` on disk.
    pub manifest_path: PathBuf,
    /// SHA-256 hex digest of the WASM bytes.
    pub digest: String,
    /// Size of the WASM binary in bytes.
    pub size_bytes: u64,
}

/// A local disk cache for plugin WASM artifacts and manifests.
#[derive(Debug, Clone)]
pub struct PluginCache {
    root: PathBuf,
}

impl PluginCache {
    /// Create a `PluginCache` at the platform default location (`~/.rapidbyte/plugins/`).
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory cannot be determined.
    pub fn default_location() -> Result<Self> {
        let home = dirs::home_dir().context("unable to determine home directory")?;
        Ok(Self {
            root: home.join(".rapidbyte").join("plugins"),
        })
    }

    /// Create a `PluginCache` with a custom root directory.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Return the directory path for a given plugin reference.
    fn entry_dir(&self, plugin_ref: &PluginRef) -> PathBuf {
        // Sanitize ':' in registry (e.g. localhost:5050) for Windows path compat.
        let safe_registry = plugin_ref.registry.replace(':', "_");
        self.root
            .join(safe_registry)
            .join(&plugin_ref.repository)
            .join(&plugin_ref.tag)
    }

    /// Store a plugin artifact in the cache.
    ///
    /// The `artifact_config` contains the wasm digest and optional signature.
    /// It is always persisted as `artifact_config.json` so trust verification
    /// works on cache hits.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache directory cannot be created or files cannot
    /// be written.
    pub fn store(
        &self,
        plugin_ref: &PluginRef,
        manifest_json: &[u8],
        wasm_bytes: &[u8],
        artifact_config: &crate::artifact::PluginArtifactConfig,
    ) -> Result<CacheEntry> {
        let dir = self.entry_dir(plugin_ref);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create cache directory: {}", dir.display()))?;

        let wasm_path = dir.join("plugin.wasm");
        let manifest_path = dir.join("manifest.json");
        let digest_path = dir.join("sha256");

        let digest = sha256_hex(wasm_bytes);

        fs::write(&wasm_path, wasm_bytes)
            .with_context(|| format!("failed to write {}", wasm_path.display()))?;
        fs::write(&manifest_path, manifest_json)
            .with_context(|| format!("failed to write {}", manifest_path.display()))?;
        fs::write(&digest_path, &digest)
            .with_context(|| format!("failed to write {}", digest_path.display()))?;

        let config_path = dir.join("artifact_config.json");
        let config_json =
            serde_json::to_vec(artifact_config).context("failed to serialize artifact config")?;
        fs::write(&config_path, &config_json)
            .with_context(|| format!("failed to write {}", config_path.display()))?;

        // Store the original ref so list() can reconstruct it accurately
        // (the filesystem path has sanitized ':' → '_').
        let ref_path = dir.join("ref.json");
        let ref_json = serde_json::to_vec(plugin_ref).context("failed to serialize plugin ref")?;
        fs::write(&ref_path, &ref_json)
            .with_context(|| format!("failed to write {}", ref_path.display()))?;

        #[allow(clippy::cast_possible_truncation)]
        let size_bytes = wasm_bytes.len() as u64;

        Ok(CacheEntry {
            plugin_ref: plugin_ref.clone(),
            wasm_path,
            manifest_path,
            digest,
            size_bytes,
        })
    }

    /// Load the artifact config (including signature) for a cached plugin.
    ///
    /// Returns `None` if the cache entry is corrupt or missing the config file.
    #[must_use]
    pub fn load_artifact_config(
        &self,
        plugin_ref: &PluginRef,
    ) -> Option<crate::artifact::PluginArtifactConfig> {
        let config_path = self.entry_dir(plugin_ref).join("artifact_config.json");
        let data = fs::read(&config_path).ok()?;
        serde_json::from_slice(&data).ok()
    }

    /// Look up a cached plugin by reference.
    ///
    /// Returns `None` if the entry does not exist or if the WASM file's
    /// SHA-256 does not match the stored sidecar digest (indicating
    /// corruption or tampering).
    #[must_use]
    pub fn lookup(&self, plugin_ref: &PluginRef) -> Option<CacheEntry> {
        let dir = self.entry_dir(plugin_ref);
        let wasm_path = dir.join("plugin.wasm");
        let manifest_path = dir.join("manifest.json");
        let digest_path = dir.join("sha256");

        // All three files must exist.
        if !wasm_path.is_file() || !manifest_path.is_file() || !digest_path.is_file() {
            return None;
        }

        let stored_digest = fs::read_to_string(&digest_path).ok()?;
        let stored_digest = stored_digest.trim().to_owned();

        // Verify the WASM binary matches the stored digest to detect
        // on-disk tampering before the binary is loaded for execution.
        let wasm_bytes = fs::read(&wasm_path).ok()?;
        let actual_digest = sha256_hex(&wasm_bytes);

        if actual_digest != stored_digest {
            tracing::warn!(
                plugin_ref = %plugin_ref,
                expected = %stored_digest,
                actual = %actual_digest,
                "cache digest mismatch — treating as miss",
            );
            return None;
        }

        #[allow(clippy::cast_possible_truncation)]
        let size_bytes = wasm_bytes.len() as u64;

        Some(CacheEntry {
            plugin_ref: plugin_ref.clone(),
            wasm_path,
            manifest_path,
            digest: actual_digest,
            size_bytes,
        })
    }

    /// List all cached plugin entries by walking the cache directory tree.
    ///
    /// # Errors
    ///
    /// Returns an error if the root directory cannot be read. Missing or corrupt
    /// entries are silently skipped.
    pub fn list(&self) -> Result<Vec<CacheEntry>> {
        let mut entries = Vec::new();

        if !self.root.is_dir() {
            return Ok(entries);
        }

        // Walk: root / registry / repository-parts / tag
        // We look for directories containing `plugin.wasm` + `manifest.json` + `sha256`.
        Self::walk_for_entries(&self.root, &self.root, &mut entries);

        Ok(entries)
    }

    /// Recursively walk looking for cache leaf directories (those containing
    /// `plugin.wasm`).
    fn walk_for_entries(root: &Path, dir: &Path, entries: &mut Vec<CacheEntry>) {
        let wasm_path = dir.join("plugin.wasm");
        let manifest_path = dir.join("manifest.json");
        let digest_path = dir.join("sha256");

        if wasm_path.is_file() && manifest_path.is_file() && digest_path.is_file() {
            // Try to reconstruct the PluginRef from the relative path.
            if let Some(entry) = Self::entry_from_dir(root, dir) {
                entries.push(entry);
            }
            return;
        }

        // Recurse into subdirectories.
        let Ok(read_dir) = fs::read_dir(dir) else {
            return;
        };
        for child in read_dir.flatten() {
            if child.file_type().is_ok_and(|ft| ft.is_dir()) {
                Self::walk_for_entries(root, &child.path(), entries);
            }
        }
    }

    /// Reconstruct a `CacheEntry` from a leaf directory path relative to the
    /// cache root.
    fn entry_from_dir(_root: &Path, dir: &Path) -> Option<CacheEntry> {
        // Read the stored ref (preferred — preserves original registry with ':').
        let ref_path = dir.join("ref.json");
        let plugin_ref: PluginRef = fs::read(&ref_path)
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())?;

        let wasm_path = dir.join("plugin.wasm");
        let digest_path = dir.join("sha256");

        let digest = fs::read_to_string(&digest_path).ok()?;
        let digest = digest.trim().to_owned();

        let size_bytes = fs::metadata(&wasm_path).ok()?.len();

        Some(CacheEntry {
            plugin_ref,
            wasm_path,
            manifest_path: dir.join("manifest.json"),
            digest,
            size_bytes,
        })
    }

    /// Remove a cached plugin entry.
    ///
    /// Returns `true` if the entry existed and was removed, `false` if it did
    /// not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be removed.
    pub fn remove(&self, plugin_ref: &PluginRef) -> Result<bool> {
        let dir = self.entry_dir(plugin_ref);

        if !dir.is_dir() {
            return Ok(false);
        }

        fs::remove_dir_all(&dir)
            .with_context(|| format!("failed to remove cache directory: {}", dir.display()))?;

        // Clean up empty parent directories up to the root.
        self.prune_empty_parents(&dir);

        Ok(true)
    }

    /// Remove empty parent directories between `child` and `self.root`.
    fn prune_empty_parents(&self, child: &Path) {
        let mut current = child.to_path_buf();
        while let Some(parent) = current.parent() {
            if parent == self.root || !parent.starts_with(&self.root) {
                break;
            }
            // Only remove if empty.
            if fs::read_dir(parent).is_ok_and(|mut d| d.next().is_none()) {
                let _ = fs::remove_dir(parent);
            } else {
                break;
            }
            current = parent.to_path_buf();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verify::sha256_hex;
    use tempfile::tempdir;

    fn test_ref(tag: &str) -> PluginRef {
        PluginRef {
            registry: "registry.example.com".into(),
            repository: "source/postgres".into(),
            tag: tag.into(),
        }
    }

    fn dummy_manifest() -> Vec<u8> {
        br#"{"name":"source-postgres","version":"1.0.0"}"#.to_vec()
    }

    fn dummy_wasm() -> Vec<u8> {
        b"\x00asm\x01\x00\x00\x00fake-wasm-content".to_vec()
    }

    fn unsigned_config(wasm: &[u8]) -> crate::artifact::PluginArtifactConfig {
        crate::artifact::PluginArtifactConfig {
            wasm_sha256: crate::verify::sha256_hex(wasm),
            signature: None,
        }
    }

    #[test]
    fn store_and_lookup_roundtrip() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");
        let wasm = dummy_wasm();
        let manifest = dummy_manifest();

        let stored = cache
            .store(&pref, &manifest, &wasm, &unsigned_config(&wasm))
            .unwrap();
        assert_eq!(stored.plugin_ref, pref);
        assert_eq!(stored.size_bytes, wasm.len() as u64);
        assert_eq!(stored.digest, sha256_hex(&wasm));

        let looked_up = cache.lookup(&pref).expect("entry should exist");
        assert_eq!(looked_up.plugin_ref, pref);
        assert_eq!(looked_up.digest, stored.digest);
        assert_eq!(looked_up.size_bytes, stored.size_bytes);
        assert_eq!(looked_up.wasm_path, stored.wasm_path);
        assert_eq!(looked_up.manifest_path, stored.manifest_path);

        // Verify actual file contents.
        assert_eq!(fs::read(&looked_up.wasm_path).unwrap(), wasm);
        assert_eq!(fs::read(&looked_up.manifest_path).unwrap(), manifest);
    }

    #[test]
    fn lookup_returns_none_for_missing() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        assert!(cache.lookup(&pref).is_none());
    }

    #[test]
    fn store_overwrites_existing() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let wasm_v1 = b"wasm-v1".to_vec();
        let wasm_v2 = b"wasm-v2".to_vec();
        let manifest = dummy_manifest();

        let _first = cache
            .store(&pref, &manifest, &wasm_v1, &unsigned_config(&wasm_v1))
            .unwrap();
        let second = cache
            .store(&pref, &manifest, &wasm_v2, &unsigned_config(&wasm_v2))
            .unwrap();

        let entry = cache.lookup(&pref).expect("entry should exist");
        assert_eq!(entry.digest, sha256_hex(&wasm_v2));
        assert_eq!(entry.digest, second.digest);
        assert_eq!(fs::read(&entry.wasm_path).unwrap(), wasm_v2);
    }

    #[test]
    fn list_returns_all_entries() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let manifest = dummy_manifest();

        let refs: Vec<PluginRef> = vec![
            test_ref("1.0.0"),
            test_ref("2.0.0"),
            PluginRef {
                registry: "ghcr.io".into(),
                repository: "org/dest/clickhouse".into(),
                tag: "0.1.0".into(),
            },
        ];

        for r in &refs {
            let wasm = dummy_wasm();
            cache
                .store(r, &manifest, &wasm, &unsigned_config(&wasm))
                .unwrap();
        }

        let mut listed = cache.list().unwrap();
        // Sort for deterministic comparison.
        listed.sort_by(|a, b| a.plugin_ref.to_string().cmp(&b.plugin_ref.to_string()));

        assert_eq!(listed.len(), 3);

        let listed_refs: Vec<String> = listed.iter().map(|e| e.plugin_ref.to_string()).collect();
        let mut expected_refs: Vec<String> = refs.iter().map(ToString::to_string).collect();
        expected_refs.sort();

        assert_eq!(listed_refs, expected_refs);
    }

    #[test]
    fn remove_deletes_entry() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let wasm = dummy_wasm();
        cache
            .store(&pref, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();
        assert!(cache.lookup(&pref).is_some());

        let removed = cache.remove(&pref).unwrap();
        assert!(removed);
        assert!(cache.lookup(&pref).is_none());
    }

    #[test]
    fn remove_returns_false_for_missing() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let removed = cache.remove(&pref).unwrap();
        assert!(!removed);
    }

    #[test]
    fn corrupt_digest_treated_as_miss() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let wasm = dummy_wasm();
        let entry = cache
            .store(&pref, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();

        // Corrupt the digest file so it no longer matches the WASM.
        let digest_path = entry.wasm_path.parent().unwrap().join("sha256");
        fs::write(
            &digest_path,
            "badc0ffee00000000000000000000000000000000000000000000000000bad",
        )
        .unwrap();

        // lookup() verifies WASM against the sidecar — mismatch → miss.
        assert!(cache.lookup(&pref).is_none());
    }

    #[test]
    fn tampered_wasm_treated_as_miss() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let wasm = dummy_wasm();
        let entry = cache
            .store(&pref, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();

        // Tamper the WASM binary while leaving the digest untouched.
        fs::write(&entry.wasm_path, b"tampered-wasm-content").unwrap();

        // lookup() re-hashes the WASM — mismatch → miss.
        assert!(cache.lookup(&pref).is_none());
    }

    #[test]
    fn list_on_empty_cache() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let entries = cache.list().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn list_returns_entries_with_corrupt_digest() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());

        let ref_a = test_ref("1.0.0");
        let ref_b = test_ref("2.0.0");

        let wasm = dummy_wasm();
        cache
            .store(&ref_a, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();
        let wasm = dummy_wasm();
        let entry_b = cache
            .store(&ref_b, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();

        // Corrupt the digest of the second entry.
        let bad_digest = "0000000000000000000000000000000000000000000000000000000000000000";
        let digest_path = entry_b.wasm_path.parent().unwrap().join("sha256");
        fs::write(digest_path, bad_digest).unwrap();

        // Both entries returned; the caller is responsible for verifying
        // against artifact_config.
        let entries = cache.list().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn tampered_artifact_config_detected_by_digest_mismatch() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        let wasm = dummy_wasm();
        let entry = cache
            .store(&pref, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();

        // Tamper the artifact_config.json to have a different wasm_sha256
        let config_path = entry
            .wasm_path
            .parent()
            .unwrap()
            .join("artifact_config.json");
        let tampered_config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: "aaaa".repeat(16),
            signature: Some("fake-signature".to_owned()),
        };
        fs::write(&config_path, serde_json::to_vec(&tampered_config).unwrap()).unwrap();

        // Cache lookup still succeeds (digest file matches wasm)
        let looked_up = cache.lookup(&pref).expect("entry should exist");

        // But loading the artifact config shows the tampered digest
        let loaded_config = cache.load_artifact_config(&pref).unwrap();
        assert_ne!(
            loaded_config.wasm_sha256, looked_up.digest,
            "tampered config digest should not match cached wasm digest"
        );
    }

    #[test]
    fn port_registry_roundtrips_through_store_list_remove() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = PluginRef::parse("localhost:5050/source/postgres:1.0.0").unwrap();
        let wasm = dummy_wasm();

        // Store
        cache
            .store(&pref, &dummy_manifest(), &wasm, &unsigned_config(&wasm))
            .unwrap();

        // Lookup preserves original ref
        let entry = cache.lookup(&pref).expect("should find cached entry");
        assert_eq!(entry.plugin_ref, pref);
        assert_eq!(entry.plugin_ref.registry, "localhost:5050");

        // List preserves original ref (not sanitized localhost_5050)
        let listed = cache.list().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].plugin_ref.registry, "localhost:5050");

        // Remove works with the original ref
        assert!(cache.remove(&pref).unwrap());
        assert!(cache.lookup(&pref).is_none());
    }
}
