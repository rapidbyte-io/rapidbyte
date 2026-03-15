# OCI Plugin Registry — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace filesystem-based plugin resolution with an OCI registry-backed pull/cache/verify system, giving every plugin a versioned, content-addressed identity with CLI management commands.

**Architecture:** A new `rapidbyte-registry` crate wraps `oci-client` to pull OCI artifacts containing plugin manifests + WASM binaries. A local disk cache (`~/.rapidbyte/plugins/`) stores pulled artifacts indexed by digest. The engine's `resolve_plugin_path` is replaced with a registry-aware resolver that checks cache → pulls from registry on miss → verifies SHA-256 → returns the cached path. The CLI gains `plugin pull/push/inspect/tags/list/remove` subcommands. The controller broadcasts its registry URL to agents via a new field in the registration response.

**Tech Stack:** Rust, `oci-client` (0.16), `sha2` (digest verification), `zot` (test registry in Docker Compose), `tokio`, `serde_json`

---

## File Structure

### New crate: `crates/rapidbyte-registry/`

| File | Responsibility |
|------|---------------|
| `Cargo.toml` | Dependencies: oci-client, sha2, serde, serde_json, anyhow, tokio, dirs |
| `src/lib.rs` | Public API: `RegistryClient`, `PluginCache`, `PluginRef` |
| `src/reference.rs` | Parse `registry.example.com/source/postgres:1.2.0` into components |
| `src/client.rs` | OCI pull/push/tags/manifest operations via `oci-client` |
| `src/cache.rs` | Local disk cache: store, lookup, list, remove by digest |
| `src/artifact.rs` | OCI artifact layout: pack/unpack manifest + wasm layers |
| `src/verify.rs` | SHA-256 digest verification of cached wasm binaries |

### Modified files

| File | Change |
|------|--------|
| `Cargo.toml` (workspace) | Add `rapidbyte-registry` to members + workspace deps |
| `crates/rapidbyte-runtime/src/plugin.rs` | Replace filesystem resolver with registry-aware resolver |
| `crates/rapidbyte-cli/src/main.rs` | Convert `Plugins` command to `Plugin` subcommand group |
| `crates/rapidbyte-cli/src/commands/plugins.rs` → `plugin.rs` | Rewrite as subcommand dispatcher with pull/push/inspect/tags/list/remove |
| `crates/rapidbyte-cli/Cargo.toml` | Add rapidbyte-registry dependency |
| `crates/rapidbyte-engine/Cargo.toml` | Add rapidbyte-registry dependency |
| `crates/rapidbyte-agent/Cargo.toml` | Add rapidbyte-registry dependency |
| `docker-compose.yml` | Add `zot` OCI registry service for testing |
| `proto/rapidbyte/v1/controller.proto` | Add `registry_url` field to `RegisterAgentResponse` |

---

## Dependency Graph

```
Task 1: Create rapidbyte-registry crate skeleton + reference parsing
Task 2: OCI client wrapper (pull manifest, pull blob, list tags)
Task 3: Local disk cache (store, lookup, list, remove, digest verify)
Task 4: OCI artifact layout (pack manifest+wasm, unpack)
Task 5: Docker Compose zot registry + integration tests
Task 6: CLI plugin subcommands (pull, push, inspect, tags, list, remove)
Task 7: Engine integration (replace filesystem resolver)
Task 8: Controller registry config broadcast to agents
```

```
Task 1 ──→ Task 2 ──→ Task 4 ──→ Task 5
                  └──→ Task 3 ──→ Task 5
Task 5 ──→ Task 6 ──→ Task 7 ──→ Task 8
```

Tasks 2 and 3 can be done in parallel after Task 1. Task 4 depends on Task 2. Task 5 depends on 2, 3, 4. Tasks 6-8 are sequential.

---

### Task 1: Create `rapidbyte-registry` crate skeleton + reference parsing

**Files:**
- Create: `crates/rapidbyte-registry/Cargo.toml`
- Create: `crates/rapidbyte-registry/src/lib.rs`
- Create: `crates/rapidbyte-registry/src/reference.rs`
- Modify: `Cargo.toml` (workspace root)

This task sets up the crate and implements plugin reference parsing. A `PluginRef` parses strings like `registry.example.com/source/postgres:1.2.0` into registry host, repository path, and tag components.

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "rapidbyte-registry"
version.workspace = true
edition.workspace = true

[dependencies]
anyhow = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = "2"
```

- [ ] **Step 2: Add to workspace**

In root `Cargo.toml`, add `"crates/rapidbyte-registry"` to `[workspace] members`. Add `rapidbyte-registry = { path = "crates/rapidbyte-registry" }` to `[workspace.dependencies]`.

- [ ] **Step 3: Write reference parsing tests**

In `src/reference.rs`, write tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_reference() {
        let r = PluginRef::parse("registry.example.com/source/postgres:1.2.0").unwrap();
        assert_eq!(r.registry, "registry.example.com");
        assert_eq!(r.repository, "source/postgres");
        assert_eq!(r.tag, "1.2.0");
    }

    #[test]
    fn parse_reference_with_port() {
        let r = PluginRef::parse("localhost:5000/source/postgres:latest").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.repository, "source/postgres");
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn parse_reference_without_tag_defaults_to_latest() {
        let r = PluginRef::parse("registry.example.com/source/postgres").unwrap();
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn parse_reference_with_nested_repo() {
        let r = PluginRef::parse("ghcr.io/rapidbyte-io/plugins/source/postgres:1.0.0").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "rapidbyte-io/plugins/source/postgres");
        assert_eq!(r.tag, "1.0.0");
    }

    #[test]
    fn reject_empty_string() {
        assert!(PluginRef::parse("").is_err());
    }

    #[test]
    fn reject_no_registry() {
        assert!(PluginRef::parse("postgres:1.0.0").is_err());
    }

    #[test]
    fn display_roundtrips() {
        let input = "registry.example.com/source/postgres:1.2.0";
        let r = PluginRef::parse(input).unwrap();
        assert_eq!(r.to_string(), input);
    }
}
```

- [ ] **Step 4: Implement PluginRef**

```rust
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PluginRef {
    pub registry: String,
    pub repository: String,
    pub tag: String,
}

impl PluginRef {
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!input.is_empty(), "plugin reference cannot be empty");

        // Split tag: everything after the last ':'
        let (base, tag) = match input.rfind(':') {
            Some(pos) if !input[pos + 1..].contains('/') => {
                (&input[..pos], &input[pos + 1..])
            }
            _ => (input, "latest"),
        };

        // Split registry from repository at the first '/'
        let slash = base.find('/').ok_or_else(|| {
            anyhow::anyhow!("plugin reference must include registry and repository: {input}")
        })?;

        let registry = &base[..slash];
        let repository = &base[slash + 1..];

        // Validate: registry must look like a host (contains '.' or ':')
        anyhow::ensure!(
            registry.contains('.') || registry.contains(':'),
            "plugin reference must start with a registry host (e.g., registry.example.com/...): {input}"
        );
        anyhow::ensure!(!repository.is_empty(), "repository path cannot be empty: {input}");

        Ok(Self {
            registry: registry.to_owned(),
            repository: repository.to_owned(),
            tag: tag.to_owned(),
        })
    }
}

impl fmt::Display for PluginRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}:{}", self.registry, self.repository, self.tag)
    }
}
```

- [ ] **Step 5: Wire up lib.rs**

```rust
//! OCI-based plugin registry client, cache, and verification.
//!
//! | Module      | Responsibility |
//! |-------------|----------------|
//! | `reference` | Plugin reference parsing (`registry/repo:tag`) |

pub mod reference;

pub use reference::PluginRef;
```

- [ ] **Step 6: Build and test**

```bash
cargo test -p rapidbyte-registry
```

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(registry): create rapidbyte-registry crate with PluginRef parsing"
```

---

### Task 2: OCI client wrapper

**Files:**
- Modify: `crates/rapidbyte-registry/Cargo.toml`
- Create: `crates/rapidbyte-registry/src/client.rs`
- Modify: `crates/rapidbyte-registry/src/lib.rs`

Wraps `oci-client` to provide typed operations: pull manifest, pull blob by digest, list tags, push artifact.

- [ ] **Step 1: Add oci-client dependency**

Add to `crates/rapidbyte-registry/Cargo.toml`:
```toml
oci-client = "0.16"
tokio = { workspace = true }
tracing = { workspace = true }
```

Also add `oci-client = "0.16"` to the workspace `[workspace.dependencies]` in root `Cargo.toml`.

- [ ] **Step 2: Implement RegistryClient**

In `src/client.rs`:

```rust
//! OCI registry client wrapper.

use anyhow::{Context, Result};
use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};

use crate::PluginRef;

/// Configuration for connecting to an OCI registry.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Use HTTP instead of HTTPS (for local dev registries).
    pub insecure: bool,
    /// Optional username/password authentication.
    pub auth: Option<(String, String)>,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            insecure: false,
            auth: None,
        }
    }
}

/// Client for OCI registry operations.
pub struct RegistryClient {
    client: Client,
    auth: RegistryAuth,
}

impl RegistryClient {
    /// Create a new registry client.
    pub fn new(config: &RegistryConfig) -> Result<Self> {
        let protocol = if config.insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };
        let client_config = ClientConfig {
            protocol,
            ..Default::default()
        };
        let client = Client::new(client_config);
        let auth = match &config.auth {
            Some((user, pass)) => RegistryAuth::Basic(user.clone(), pass.clone()),
            None => RegistryAuth::Anonymous,
        };
        Ok(Self { client, auth })
    }

    /// List all tags for a plugin reference.
    pub async fn list_tags(&self, plugin_ref: &PluginRef) -> Result<Vec<String>> {
        let reference = to_oci_reference(plugin_ref)?;
        let response = self
            .client
            .list_tags(&reference, &self.auth, None, None)
            .await
            .context("failed to list tags")?;
        Ok(response.tags)
    }

    /// Pull the full OCI image (manifest + layers) for a plugin reference.
    pub async fn pull(
        &self,
        plugin_ref: &PluginRef,
    ) -> Result<oci_client::client::ImageData> {
        let reference = to_oci_reference(plugin_ref)?;
        let image = self
            .client
            .pull(
                &reference,
                &self.auth,
                vec!["application/vnd.rapidbyte.plugin.wasm"],
            )
            .await
            .context("failed to pull plugin artifact")?;
        Ok(image)
    }

    /// Pull only the manifest (no layers) for inspect operations.
    pub async fn pull_manifest(
        &self,
        plugin_ref: &PluginRef,
    ) -> Result<(oci_client::manifest::OciImageManifest, String)> {
        let reference = to_oci_reference(plugin_ref)?;
        let (manifest, digest) = self
            .client
            .pull_manifest(&reference, &self.auth)
            .await
            .context("failed to pull manifest")?;
        Ok((manifest, digest))
    }

    /// Push an OCI image (manifest + layers) to the registry.
    pub async fn push(
        &self,
        plugin_ref: &PluginRef,
        config_data: Vec<u8>,
        layers: Vec<Vec<u8>>,
        config_media_type: &str,
        layer_media_types: Vec<String>,
    ) -> Result<String> {
        let reference = to_oci_reference(plugin_ref)?;

        let image_manifest = oci_client::manifest::OciImageManifest::build(
            &layers
                .iter()
                .zip(layer_media_types.iter())
                .map(|(data, media_type)| {
                    oci_client::client::ImageLayer::new(
                        data.clone(),
                        media_type.clone(),
                        None,
                    )
                })
                .collect::<Vec<_>>(),
            &oci_client::client::Config {
                data: config_data,
                media_type: config_media_type.to_string(),
                annotations: None,
            },
            None,
        );

        let response = self
            .client
            .push(&reference, &image_manifest.0, image_manifest.1, &self.auth, None)
            .await
            .context("failed to push plugin artifact")?;
        Ok(response.config_url)
    }
}

fn to_oci_reference(plugin_ref: &PluginRef) -> Result<Reference> {
    let s = format!("{}/{}:{}", plugin_ref.registry, plugin_ref.repository, plugin_ref.tag);
    s.parse::<Reference>()
        .with_context(|| format!("invalid OCI reference: {s}"))
}
```

Note: The exact `oci-client` API may differ slightly from what's shown above. Read the `oci-client` docs and adjust struct names and method signatures as needed during implementation. The key operations are: `pull`, `pull_manifest`, `list_tags`, and `push`.

- [ ] **Step 3: Export from lib.rs**

Add `pub mod client;` and `pub use client::{RegistryClient, RegistryConfig};`

- [ ] **Step 4: Build**

```bash
cargo check -p rapidbyte-registry
```

This won't have tests yet — integration tests come in Task 5 (requires the Docker Compose registry).

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(registry): add OCI registry client wrapper"
```

---

### Task 3: Local disk cache

**Files:**
- Modify: `crates/rapidbyte-registry/Cargo.toml`
- Create: `crates/rapidbyte-registry/src/cache.rs`
- Create: `crates/rapidbyte-registry/src/verify.rs`
- Modify: `crates/rapidbyte-registry/src/lib.rs`

The cache stores pulled plugin artifacts on disk indexed by digest. Each entry is a directory containing the manifest JSON and wasm binary.

- [ ] **Step 1: Add sha2 + dirs dependencies**

```toml
sha2 = "0.10"
dirs = "6"
hex = "0.4"
```

- [ ] **Step 2: Implement digest verification in verify.rs**

```rust
//! SHA-256 digest verification.

use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};

/// Compute the SHA-256 hex digest of the given data.
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

/// Verify that the data matches the expected SHA-256 hex digest.
pub fn verify_sha256(data: &[u8], expected: &str) -> Result<()> {
    let actual = sha256_hex(data);
    ensure!(
        actual == expected,
        "digest mismatch: expected {expected}, got {actual}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_of_empty() {
        let hash = sha256_hex(b"");
        assert_eq!(hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    #[test]
    fn verify_correct_digest() {
        let data = b"hello world";
        let digest = sha256_hex(data);
        verify_sha256(data, &digest).unwrap();
    }

    #[test]
    fn verify_wrong_digest() {
        assert!(verify_sha256(b"hello", "0000000000000000").is_err());
    }
}
```

- [ ] **Step 3: Implement PluginCache in cache.rs**

```rust
//! Local disk cache for pulled plugin artifacts.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::reference::PluginRef;
use crate::verify;

/// Entry in the local plugin cache.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub plugin_ref: PluginRef,
    pub wasm_path: PathBuf,
    pub manifest_path: PathBuf,
    pub digest: String,
    pub size_bytes: u64,
}

/// Local disk cache for plugin artifacts.
pub struct PluginCache {
    root: PathBuf,
}

impl PluginCache {
    /// Create a cache at the default location (~/.rapidbyte/plugins/).
    pub fn default_location() -> Result<Self> {
        let home = dirs::home_dir().context("cannot determine home directory")?;
        let root = home.join(".rapidbyte").join("plugins");
        Ok(Self { root })
    }

    /// Create a cache at a custom root directory.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Store a pulled plugin artifact in the cache.
    pub fn store(
        &self,
        plugin_ref: &PluginRef,
        manifest_json: &[u8],
        wasm_bytes: &[u8],
    ) -> Result<CacheEntry> {
        let digest = verify::sha256_hex(wasm_bytes);
        let entry_dir = self.entry_dir(plugin_ref);
        fs::create_dir_all(&entry_dir)
            .with_context(|| format!("failed to create cache dir: {}", entry_dir.display()))?;

        let wasm_path = entry_dir.join("plugin.wasm");
        let manifest_path = entry_dir.join("manifest.json");
        let digest_path = entry_dir.join("sha256");

        fs::write(&wasm_path, wasm_bytes).context("failed to write wasm")?;
        fs::write(&manifest_path, manifest_json).context("failed to write manifest")?;
        fs::write(&digest_path, &digest).context("failed to write digest")?;

        Ok(CacheEntry {
            plugin_ref: plugin_ref.clone(),
            wasm_path,
            manifest_path,
            digest,
            size_bytes: wasm_bytes.len() as u64,
        })
    }

    /// Look up a cached plugin. Returns None if not cached.
    pub fn lookup(&self, plugin_ref: &PluginRef) -> Option<CacheEntry> {
        let entry_dir = self.entry_dir(plugin_ref);
        let wasm_path = entry_dir.join("plugin.wasm");
        let manifest_path = entry_dir.join("manifest.json");
        let digest_path = entry_dir.join("sha256");

        if !wasm_path.exists() || !manifest_path.exists() {
            return None;
        }

        let digest = fs::read_to_string(&digest_path).ok()?;
        let size_bytes = fs::metadata(&wasm_path).ok()?.len();

        // Verify integrity
        let wasm_bytes = fs::read(&wasm_path).ok()?;
        if verify::verify_sha256(&wasm_bytes, digest.trim()).is_err() {
            tracing::warn!(
                plugin = %plugin_ref,
                "cache entry has invalid digest, treating as cache miss"
            );
            return None;
        }

        Some(CacheEntry {
            plugin_ref: plugin_ref.clone(),
            wasm_path,
            manifest_path,
            digest: digest.trim().to_owned(),
            size_bytes,
        })
    }

    /// List all cached plugins.
    pub fn list(&self) -> Result<Vec<CacheEntry>> {
        let mut entries = Vec::new();
        self.walk_entries(&self.root, &mut entries)?;
        Ok(entries)
    }

    /// Remove a cached plugin.
    pub fn remove(&self, plugin_ref: &PluginRef) -> Result<bool> {
        let entry_dir = self.entry_dir(plugin_ref);
        if entry_dir.exists() {
            fs::remove_dir_all(&entry_dir)
                .with_context(|| format!("failed to remove cache entry: {}", entry_dir.display()))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Cache directory for a specific plugin reference.
    /// Layout: <root>/<registry>/<repository>/<tag>/
    fn entry_dir(&self, plugin_ref: &PluginRef) -> PathBuf {
        self.root
            .join(&plugin_ref.registry)
            .join(&plugin_ref.repository)
            .join(&plugin_ref.tag)
    }

    fn walk_entries(&self, dir: &Path, entries: &mut Vec<CacheEntry>) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        // Look for directories containing plugin.wasm
        let wasm_path = dir.join("plugin.wasm");
        if wasm_path.exists() {
            // Reconstruct PluginRef from path components
            if let Some(entry) = self.entry_from_dir(dir) {
                entries.push(entry);
            }
            return Ok(());
        }
        // Recurse into subdirectories
        if let Ok(read_dir) = fs::read_dir(dir) {
            for entry in read_dir.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    self.walk_entries(&entry.path(), entries)?;
                }
            }
        }
        Ok(())
    }

    fn entry_from_dir(&self, dir: &Path) -> Option<CacheEntry> {
        let wasm_path = dir.join("plugin.wasm");
        let manifest_path = dir.join("manifest.json");
        let digest_path = dir.join("sha256");

        let relative = dir.strip_prefix(&self.root).ok()?;
        let components: Vec<&str> = relative.components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();

        // Minimum: registry/repo/tag (3 components), but repo can be nested
        if components.len() < 3 {
            return None;
        }

        let registry = components[0].to_owned();
        let tag = components.last()?.to_string();
        let repository = components[1..components.len() - 1].join("/");

        let digest = fs::read_to_string(&digest_path).ok()?.trim().to_owned();
        let size_bytes = fs::metadata(&wasm_path).ok()?.len();

        Some(CacheEntry {
            plugin_ref: PluginRef {
                registry,
                repository,
                tag,
            },
            wasm_path,
            manifest_path,
            digest,
            size_bytes,
        })
    }
}
```

- [ ] **Step 4: Write cache tests**

Add tests for `store`, `lookup` (hit + miss + corrupt digest), `list`, `remove` using `tempdir`.

- [ ] **Step 5: Export from lib.rs**

Add `pub mod cache; pub mod verify;` and re-exports.

- [ ] **Step 6: Build and test**

```bash
cargo test -p rapidbyte-registry
```

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(registry): add local disk cache with SHA-256 digest verification"
```

---

### Task 4: OCI artifact layout

**Files:**
- Create: `crates/rapidbyte-registry/src/artifact.rs`
- Modify: `crates/rapidbyte-registry/src/lib.rs`

Defines how a rapidbyte plugin is stored as an OCI artifact (two layers: manifest JSON + wasm binary).

- [ ] **Step 1: Define media types and artifact structure**

```rust
//! OCI artifact layout for rapidbyte plugins.
//!
//! A rapidbyte plugin artifact has two layers:
//! - Layer 0: `application/vnd.rapidbyte.plugin.manifest+json` — PluginManifest JSON
//! - Layer 1: `application/vnd.rapidbyte.plugin.wasm` — the .wasm binary
//!
//! The config blob is `application/vnd.rapidbyte.plugin.config.v1+json`
//! containing only the sha256 digest of the wasm binary.

pub const CONFIG_MEDIA_TYPE: &str = "application/vnd.rapidbyte.plugin.config.v1+json";
pub const MANIFEST_MEDIA_TYPE: &str = "application/vnd.rapidbyte.plugin.manifest+json";
pub const WASM_MEDIA_TYPE: &str = "application/vnd.rapidbyte.plugin.wasm";

/// Config blob embedded in the OCI artifact.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PluginArtifactConfig {
    pub wasm_sha256: String,
}

/// Unpack an OCI image into manifest JSON and wasm bytes.
pub fn unpack_artifact(
    image: &oci_client::client::ImageData,
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let manifest_layer = image
        .layers
        .iter()
        .find(|l| l.media_type == MANIFEST_MEDIA_TYPE)
        .ok_or_else(|| anyhow::anyhow!("artifact missing manifest layer"))?;

    let wasm_layer = image
        .layers
        .iter()
        .find(|l| l.media_type == WASM_MEDIA_TYPE)
        .ok_or_else(|| anyhow::anyhow!("artifact missing wasm layer"))?;

    // Verify wasm digest from config
    let config: PluginArtifactConfig = serde_json::from_slice(&image.config.data)?;
    crate::verify::verify_sha256(&wasm_layer.data, &config.wasm_sha256)?;

    Ok((manifest_layer.data.clone(), wasm_layer.data.clone()))
}

/// Pack manifest JSON and wasm bytes into OCI artifact components.
pub fn pack_artifact(
    manifest_json: &[u8],
    wasm_bytes: &[u8],
) -> (Vec<u8>, Vec<Vec<u8>>, Vec<String>) {
    let config = PluginArtifactConfig {
        wasm_sha256: crate::verify::sha256_hex(wasm_bytes),
    };
    let config_data = serde_json::to_vec(&config).expect("config serialization should not fail");

    let layers = vec![manifest_json.to_vec(), wasm_bytes.to_vec()];
    let media_types = vec![
        MANIFEST_MEDIA_TYPE.to_string(),
        WASM_MEDIA_TYPE.to_string(),
    ];

    (config_data, layers, media_types)
}
```

Note: The exact `oci_client::client::ImageData` struct fields may differ. Check the `oci-client` API docs and adjust field access (e.g., `image.layers`, `image.config.data`) during implementation.

- [ ] **Step 2: Export from lib.rs, build and test**

- [ ] **Step 3: Commit**

```bash
git commit -am "feat(registry): define OCI artifact layout for plugin packaging"
```

---

### Task 5: Docker Compose zot registry + integration tests

**Files:**
- Modify: `docker-compose.yml`
- Create: `crates/rapidbyte-registry/tests/integration.rs`

- [ ] **Step 1: Add zot to docker-compose.yml**

Add a zot registry service alongside the existing postgres:

```yaml
  registry:
    image: ghcr.io/project-zot/zot-linux-amd64:v2.1.1
    ports:
      - "5050:5000"
    environment:
      # Minimal config: HTTP (no TLS), no auth, in-memory storage
      ZOT_LOG_LEVEL: warn
```

- [ ] **Step 2: Write integration test**

In `crates/rapidbyte-registry/tests/integration.rs`:

```rust
//! Integration tests for OCI registry operations.
//!
//! Requires a running OCI registry at localhost:5050 (see docker-compose.yml).
//! Skip with: cargo test -p rapidbyte-registry --test integration -- --ignored

use rapidbyte_registry::{
    artifact, cache::PluginCache, client::{RegistryClient, RegistryConfig}, PluginRef,
};

fn test_registry_config() -> RegistryConfig {
    RegistryConfig {
        insecure: true, // HTTP for local zot
        auth: None,
    }
}

fn test_manifest() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": "test/plugin",
        "name": "Test Plugin",
        "version": "0.1.0",
        "protocol_version": "6"
    }))
    .unwrap()
}

#[tokio::test]
#[ignore] // requires docker-compose registry
async fn push_and_pull_roundtrip() {
    let client = RegistryClient::new(&test_registry_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/plugin:0.1.0").unwrap();

    let manifest_json = test_manifest();
    let wasm_bytes = b"fake-wasm-binary-for-testing".to_vec();

    let (config_data, layers, media_types) = artifact::pack_artifact(&manifest_json, &wasm_bytes);

    // Push
    client
        .push(&plugin_ref, config_data, layers, artifact::CONFIG_MEDIA_TYPE, media_types)
        .await
        .expect("push should succeed");

    // Pull
    let image = client.pull(&plugin_ref).await.expect("pull should succeed");
    let (pulled_manifest, pulled_wasm) = artifact::unpack_artifact(&image).expect("unpack should succeed");

    assert_eq!(pulled_manifest, manifest_json);
    assert_eq!(pulled_wasm, wasm_bytes);
}

#[tokio::test]
#[ignore]
async fn list_tags_after_push() {
    let client = RegistryClient::new(&test_registry_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/tags-test:1.0.0").unwrap();

    let (config_data, layers, media_types) = artifact::pack_artifact(&test_manifest(), b"wasm1");
    client
        .push(&plugin_ref, config_data, layers, artifact::CONFIG_MEDIA_TYPE, media_types)
        .await
        .unwrap();

    let tags = client
        .list_tags(&PluginRef::parse("localhost:5050/test/tags-test:latest").unwrap())
        .await
        .unwrap();
    assert!(tags.contains(&"1.0.0".to_string()));
}

#[tokio::test]
#[ignore]
async fn pull_cache_store_and_lookup() {
    let client = RegistryClient::new(&test_registry_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/cache-test:0.1.0").unwrap();

    let manifest_json = test_manifest();
    let wasm_bytes = b"cached-wasm-content".to_vec();

    let (config_data, layers, media_types) = artifact::pack_artifact(&manifest_json, &wasm_bytes);
    client
        .push(&plugin_ref, config_data, layers, artifact::CONFIG_MEDIA_TYPE, media_types)
        .await
        .unwrap();

    let image = client.pull(&plugin_ref).await.unwrap();
    let (pulled_manifest, pulled_wasm) = artifact::unpack_artifact(&image).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let cache = PluginCache::new(tmp.path().to_path_buf());
    let entry = cache.store(&plugin_ref, &pulled_manifest, &pulled_wasm).unwrap();

    assert!(entry.wasm_path.exists());
    assert_eq!(entry.size_bytes, wasm_bytes.len() as u64);

    // Lookup should find it
    let found = cache.lookup(&plugin_ref);
    assert!(found.is_some());
    assert_eq!(found.unwrap().digest, entry.digest);
}
```

- [ ] **Step 3: Run integration tests (requires `docker compose up -d`)**

```bash
docker compose up -d registry
cargo test -p rapidbyte-registry --test integration -- --ignored
docker compose down
```

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(registry): add zot to docker-compose and integration tests"
```

---

### Task 6: CLI plugin subcommands

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Rewrite: `crates/rapidbyte-cli/src/commands/plugins.rs` → `crates/rapidbyte-cli/src/commands/plugin.rs`
- Modify: `crates/rapidbyte-cli/Cargo.toml`

Convert the existing `Plugins` unit command to a `Plugin` subcommand group with pull, push, inspect, tags, list, remove.

- [ ] **Step 1: Add rapidbyte-registry to CLI deps**

In `crates/rapidbyte-cli/Cargo.toml`, add `rapidbyte-registry = { workspace = true }`.

- [ ] **Step 2: Define PluginCommands enum in main.rs**

Replace the `Plugins` variant with:
```rust
/// Manage plugins
Plugin {
    #[command(subcommand)]
    command: PluginCommands,
},
```

And define:
```rust
#[derive(Subcommand)]
enum PluginCommands {
    /// Pull a plugin from an OCI registry
    Pull {
        /// Plugin reference (e.g., registry.example.com/source/postgres:1.2.0)
        plugin_ref: String,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// Push a plugin to an OCI registry
    Push {
        /// Plugin reference (e.g., registry.example.com/source/postgres:1.2.0)
        plugin_ref: String,
        /// Path to the .wasm file
        wasm_path: PathBuf,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// Inspect plugin metadata without downloading the wasm binary
    Inspect {
        /// Plugin reference
        plugin_ref: String,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// List available tags for a plugin
    Tags {
        /// Plugin reference (tag is ignored)
        plugin_ref: String,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// List locally cached plugins
    List,
    /// Remove a plugin from the local cache
    Remove {
        /// Plugin reference
        plugin_ref: String,
    },
}
```

- [ ] **Step 3: Implement command handlers in commands/plugin.rs**

Create `crates/rapidbyte-cli/src/commands/plugin.rs` with an `execute` function for each subcommand. Each command constructs a `RegistryClient` and/or `PluginCache` and performs the operation.

The `pull` command: parse ref → check cache → pull from registry → unpack → store in cache → print result.

The `push` command: read .wasm from disk → extract manifest from wasm (using `rapidbyte_runtime::extract_manifest_from_wasm`) → pack artifact → push to registry.

The `inspect` command: pull manifest only → print plugin metadata.

The `tags` command: list tags → print.

The `list` command: scan cache → print table.

The `remove` command: remove from cache → print result.

- [ ] **Step 4: Wire dispatch in main.rs**

```rust
Commands::Plugin { command } => commands::plugin::execute(command).await,
```

- [ ] **Step 5: Build and test**

```bash
cargo check -p rapidbyte-cli
```

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(cli): add plugin pull/push/inspect/tags/list/remove subcommands"
```

---

### Task 7: Engine integration — replace filesystem resolver

**Files:**
- Modify: `crates/rapidbyte-runtime/src/plugin.rs`
- Modify: `crates/rapidbyte-engine/src/resolve.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml`
- Modify: `crates/rapidbyte-runtime/Cargo.toml`

Replace the filesystem-only `resolve_plugin_path` with a registry-aware resolver that checks cache → pulls from registry on miss.

- [ ] **Step 1: Add registry dep to runtime**

Add `rapidbyte-registry = { workspace = true }` to `crates/rapidbyte-runtime/Cargo.toml`.

- [ ] **Step 2: Add a registry-aware resolve function**

In `plugin.rs`, add a new `resolve_plugin_from_registry` function that:
1. Parses the `use` field as a `PluginRef`
2. Checks the local cache
3. On cache miss, pulls from the registry (requires `RegistryClient` + tokio runtime)
4. Returns the path to the cached `.wasm` file

The existing `resolve_plugin_path` should be updated to try `PluginRef::parse` first. If parsing succeeds (the `use` field looks like an OCI reference), use the registry path. If parsing fails (bare name like `postgres`), reject with a clear error message pointing to the new OCI reference format.

- [ ] **Step 3: Update resolve.rs in engine**

The engine's `resolve_plugins` should pass through to the updated runtime resolver. Since the registry pull is async but plugin resolution currently happens in sync context, this may require making `resolve_plugins` async or using `tokio::runtime::Handle::current().block_on()` for the pull.

- [ ] **Step 4: Build and test**

```bash
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude transform-validate
```

- [ ] **Step 5: Update pipeline YAML format in docs/test fixtures**

Update any YAML fixtures that use bare `use: postgres` to use full OCI references for the test registry. E2E tests may need adjustment.

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(engine): replace filesystem plugin resolver with OCI registry"
```

---

### Task 8: Controller registry config broadcast to agents

**Files:**
- Modify: `proto/rapidbyte/v1/controller.proto`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

The controller tells agents which registry to use via the registration response.

- [ ] **Step 1: Add registry_url to RegisterAgentResponse proto**

```protobuf
message RegisterAgentResponse {
  string agent_id = 1;
  string registry_url = 2;   // NEW: OCI registry base URL for plugin pulls
  bool registry_insecure = 3; // NEW: use HTTP instead of HTTPS
}
```

- [ ] **Step 2: Regenerate proto code**

```bash
cargo build -p rapidbyte-controller  # triggers prost-build
```

- [ ] **Step 3: Controller sets registry_url from config**

In `agent_service.rs` register handler, populate `registry_url` from the controller's config (add a `registry_url` field to `ControllerConfig`).

- [ ] **Step 4: Agent stores and uses registry config**

In `worker.rs`, after registration, store the `registry_url` and `registry_insecure` from the response. Use them when constructing the `RegistryConfig` for plugin pulls during task execution.

- [ ] **Step 5: Build and test**

```bash
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude transform-validate
```

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(controller): broadcast registry URL to agents on registration"
```

---

## Verification

After all tasks are complete:

1. `cargo test --workspace` — all tests pass
2. `cargo clippy --workspace --all-targets -- -D warnings` — no warnings
3. Integration test with Docker Compose:
```bash
docker compose up -d
# Push a test plugin
cargo run -- plugin push localhost:5050/source/postgres:test plugins/sources/postgres/target/wasm32-wasip2/release/source_postgres.wasm --insecure
# Pull it back
cargo run -- plugin pull localhost:5050/source/postgres:test --insecure
# Inspect it
cargo run -- plugin inspect localhost:5050/source/postgres:test --insecure
# List tags
cargo run -- plugin tags localhost:5050/source/postgres --insecure
# List cached
cargo run -- plugin list
# Run a pipeline using the registry reference
cargo run -- run test-pipeline.yaml  # with use: localhost:5050/source/postgres:test
docker compose down
```
4. Agent registration test: verify the agent receives and uses the controller's registry URL.
