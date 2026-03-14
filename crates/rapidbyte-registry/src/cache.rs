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
use tracing::warn;

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
        self.root
            .join(&plugin_ref.registry)
            .join(&plugin_ref.repository)
            .join(&plugin_ref.tag)
    }

    /// Store a plugin in the cache, computing its SHA-256 digest.
    ///
    /// Overwrites any existing cache entry for the same reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache directory cannot be created or files cannot be written.
    pub fn store(
        &self,
        plugin_ref: &PluginRef,
        manifest_json: &[u8],
        wasm_bytes: &[u8],
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

    /// Look up a cached plugin by reference.
    ///
    /// Returns `None` if the entry does not exist or if the on-disk digest does
    /// not match the WASM file (indicating corruption). A warning is logged on
    /// digest mismatch.
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

        let wasm_bytes = fs::read(&wasm_path).ok()?;
        let actual_digest = sha256_hex(&wasm_bytes);

        if actual_digest != stored_digest {
            warn!(
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
    fn entry_from_dir(root: &Path, dir: &Path) -> Option<CacheEntry> {
        let rel = dir.strip_prefix(root).ok()?;
        let components: Vec<&str> = rel
            .components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();

        // At minimum: registry / repo-part / tag  (3 components).
        if components.len() < 3 {
            return None;
        }

        let registry = components[0].to_owned();
        let tag = components[components.len() - 1].to_owned();
        let repository = components[1..components.len() - 1].join("/");

        let plugin_ref = PluginRef {
            registry,
            repository,
            tag,
        };

        let wasm_path = dir.join("plugin.wasm");
        let digest_path = dir.join("sha256");

        let stored_digest = fs::read_to_string(&digest_path).ok()?;
        let stored_digest = stored_digest.trim().to_owned();

        let wasm_bytes = fs::read(&wasm_path).ok()?;
        let actual_digest = sha256_hex(&wasm_bytes);

        if actual_digest != stored_digest {
            warn!(
                plugin_ref = %plugin_ref,
                "skipping corrupt cache entry during list",
            );
            return None;
        }

        #[allow(clippy::cast_possible_truncation)]
        let size_bytes = wasm_bytes.len() as u64;

        Some(CacheEntry {
            plugin_ref,
            wasm_path,
            manifest_path: dir.join("manifest.json"),
            digest: actual_digest,
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

    #[test]
    fn store_and_lookup_roundtrip() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");
        let wasm = dummy_wasm();
        let manifest = dummy_manifest();

        let stored = cache.store(&pref, &manifest, &wasm).unwrap();
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

        let _first = cache.store(&pref, &manifest, &wasm_v1).unwrap();
        let second = cache.store(&pref, &manifest, &wasm_v2).unwrap();

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
            cache.store(r, &manifest, &dummy_wasm()).unwrap();
        }

        let mut listed = cache.list().unwrap();
        // Sort for deterministic comparison.
        listed.sort_by(|a, b| a.plugin_ref.to_string().cmp(&b.plugin_ref.to_string()));

        assert_eq!(listed.len(), 3);

        let listed_refs: Vec<String> = listed.iter().map(|e| e.plugin_ref.to_string()).collect();
        let mut expected_refs: Vec<String> = refs.iter().map(|r| r.to_string()).collect();
        expected_refs.sort();

        assert_eq!(listed_refs, expected_refs);
    }

    #[test]
    fn remove_deletes_entry() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());
        let pref = test_ref("1.0.0");

        cache
            .store(&pref, &dummy_manifest(), &dummy_wasm())
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

        let entry = cache
            .store(&pref, &dummy_manifest(), &dummy_wasm())
            .unwrap();

        // Corrupt the digest file.
        let digest_path = entry.wasm_path.parent().unwrap().join("sha256");
        fs::write(
            &digest_path,
            "badc0ffee00000000000000000000000000000000000000000000000000bad",
        )
        .unwrap();

        // Lookup should return None due to digest mismatch.
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
    fn list_skips_corrupt_entries() {
        let tmp = tempdir().unwrap();
        let cache = PluginCache::new(tmp.path().to_path_buf());

        let good_ref = test_ref("1.0.0");
        let bad_ref = test_ref("2.0.0");

        cache
            .store(&good_ref, &dummy_manifest(), &dummy_wasm())
            .unwrap();
        let bad_entry = cache
            .store(&bad_ref, &dummy_manifest(), &dummy_wasm())
            .unwrap();

        // Corrupt the digest of the second entry.
        let digest_path = bad_entry.wasm_path.parent().unwrap().join("sha256");
        fs::write(
            digest_path,
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let entries = cache.list().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].plugin_ref, good_ref);
    }
}
