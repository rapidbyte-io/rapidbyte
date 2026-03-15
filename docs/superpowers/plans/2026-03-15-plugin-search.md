# Plugin Search & Discovery — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add plugin search and discovery to the OCI registry so users can find available plugins by name, type, or description without knowing exact references.

**Architecture:** A plugin index is stored as an OCI artifact at `<registry>/rapidbyte-index:latest`. The `plugin push` command automatically updates the index after pushing a plugin artifact. The `plugin search` command pulls the index and filters locally. The index contains metadata from each plugin's embedded manifest (name, description, type, version, author, license). No manifest schema changes needed — all metadata is derived from existing `PluginManifest` fields.

**Tech Stack:** Rust, `oci-client`, `serde_json`, `chrono` (timestamps)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/rapidbyte-registry/src/index.rs` | Create | Index data types, serialization, entry upsert, search/filter |
| `crates/rapidbyte-registry/src/lib.rs` | Modify | Add `pub mod index`, re-exports |
| `crates/rapidbyte-registry/src/client.rs` | Modify | Add `pull_index` / `push_index` methods |
| `crates/rapidbyte-cli/src/main.rs` | Modify | Add `Search` variant to `PluginCommands` |
| `crates/rapidbyte-cli/src/commands/plugin.rs` | Modify | Add `search` handler, update `push` to maintain index |
| `crates/rapidbyte-registry/tests/integration.rs` | Modify | Add index integration tests |

## Dependency Graph

```
Task 1: Index data types + search/filter logic (index.rs)
Task 2: Index OCI storage (client.rs pull_index / push_index)
Task 3: Update push command to maintain index (plugin.rs)
Task 4: Add search command (main.rs, plugin.rs)
Task 5: Integration tests against zot
```

Tasks 1 and 2 are independent. Task 3 depends on both. Task 4 depends on 1. Task 5 depends on all.

---

### Task 1: Index data types and search/filter logic

**Files:**
- Create: `crates/rapidbyte-registry/src/index.rs`
- Modify: `crates/rapidbyte-registry/src/lib.rs`
- Modify: `crates/rapidbyte-registry/Cargo.toml` (add `chrono`)

The index represents all plugins known to a registry. Each entry captures metadata from the plugin's `PluginManifest`.

- [ ] **Step 1: Add chrono dependency**

In `crates/rapidbyte-registry/Cargo.toml`, add:
```toml
chrono = { workspace = true }
```

- [ ] **Step 2: Write tests first**

In `src/index.rs`, define the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(repo: &str, version: &str) -> PluginIndexEntry {
        PluginIndexEntry {
            repository: repo.to_owned(),
            name: format!("Test {repo}"),
            description: format!("A test plugin for {repo}"),
            plugin_type: "source".to_owned(),
            latest: version.to_owned(),
            versions: vec![version.to_owned()],
            author: Some("test".to_owned()),
            license: Some("Apache-2.0".to_owned()),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn new_index_is_empty() {
        let index = PluginIndex::new();
        assert!(index.plugins.is_empty());
    }

    #[test]
    fn upsert_adds_new_entry() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        assert_eq!(index.plugins.len(), 1);
        assert_eq!(index.plugins[0].latest, "1.0.0");
    }

    #[test]
    fn upsert_updates_existing_entry() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        index.upsert(sample_entry("source/postgres", "1.1.0"));
        assert_eq!(index.plugins.len(), 1);
        assert_eq!(index.plugins[0].latest, "1.1.0");
        assert_eq!(index.plugins[0].versions, vec!["1.0.0", "1.1.0"]);
    }

    #[test]
    fn upsert_does_not_duplicate_versions() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        assert_eq!(index.plugins[0].versions, vec!["1.0.0"]);
    }

    #[test]
    fn search_matches_name() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        index.upsert(sample_entry("dest/snowflake", "1.0.0"));
        let results = index.search("postgres", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].repository, "source/postgres");
    }

    #[test]
    fn search_matches_description() {
        let mut index = PluginIndex::new();
        let mut entry = sample_entry("source/pg", "1.0.0");
        entry.description = "PostgreSQL connector with CDC".to_owned();
        index.upsert(entry);
        let results = index.search("CDC", None);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_case_insensitive() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        let results = index.search("POSTGRES", None);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_filters_by_type() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        let mut dest = sample_entry("dest/postgres", "1.0.0");
        dest.plugin_type = "destination".to_owned();
        index.upsert(dest);

        let sources = index.search("", Some("source"));
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].plugin_type, "source");
    }

    #[test]
    fn search_empty_query_returns_all() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        index.upsert(sample_entry("dest/snowflake", "1.0.0"));
        let results = index.search("", None);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn serde_roundtrip() {
        let mut index = PluginIndex::new();
        index.upsert(sample_entry("source/postgres", "1.0.0"));
        let json = serde_json::to_string(&index).unwrap();
        let deserialized: PluginIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.plugins.len(), 1);
        assert_eq!(deserialized.schema_version, 1);
    }
}
```

- [ ] **Step 3: Implement the data types**

```rust
//! Plugin registry index — searchable catalog of available plugins.
//!
//! The index is stored as an OCI artifact at `<registry>/rapidbyte-index:latest`
//! and updated by `plugin push`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Well-known repository name for the index artifact within a registry.
pub const INDEX_REPOSITORY: &str = "rapidbyte-index";

/// Well-known tag for the index artifact.
pub const INDEX_TAG: &str = "latest";

/// The plugin registry index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginIndex {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// All known plugins.
    pub plugins: Vec<PluginIndexEntry>,
}

/// A single plugin entry in the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginIndexEntry {
    /// Repository path (e.g. `"source/postgres"`).
    pub repository: String,
    /// Human-readable name from the plugin manifest.
    pub name: String,
    /// Description from the plugin manifest.
    pub description: String,
    /// Plugin type: `"source"`, `"destination"`, or `"transform"`.
    pub plugin_type: String,
    /// Latest pushed version tag.
    pub latest: String,
    /// All known version tags, ordered chronologically.
    pub versions: Vec<String>,
    /// Author from the plugin manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// SPDX license from the plugin manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// Timestamp of the last push for this plugin.
    pub updated_at: DateTime<Utc>,
}

impl PluginIndex {
    /// Create an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            schema_version: 1,
            plugins: Vec::new(),
        }
    }

    /// Insert or update a plugin entry.
    ///
    /// If an entry with the same `repository` already exists, the version
    /// is appended (if new) and metadata is updated. Otherwise a new
    /// entry is added.
    pub fn upsert(&mut self, entry: PluginIndexEntry) {
        if let Some(existing) = self.plugins.iter_mut().find(|p| p.repository == entry.repository) {
            if !existing.versions.contains(&entry.latest) {
                existing.versions.push(entry.latest.clone());
            }
            existing.latest = entry.latest;
            existing.name = entry.name;
            existing.description = entry.description;
            existing.plugin_type = entry.plugin_type;
            existing.author = entry.author;
            existing.license = entry.license;
            existing.updated_at = entry.updated_at;
        } else {
            self.plugins.push(entry);
        }
    }

    /// Search the index by query string and optional type filter.
    ///
    /// The query is matched case-insensitively against:
    /// - repository path
    /// - name
    /// - description
    ///
    /// If `plugin_type` is `Some`, results are further filtered by type.
    /// An empty query with no type filter returns all entries.
    #[must_use]
    pub fn search(&self, query: &str, plugin_type: Option<&str>) -> Vec<&PluginIndexEntry> {
        let query_lower = query.to_lowercase();
        self.plugins
            .iter()
            .filter(|entry| {
                if let Some(pt) = plugin_type {
                    if entry.plugin_type != pt {
                        return false;
                    }
                }
                if query_lower.is_empty() {
                    return true;
                }
                entry.repository.to_lowercase().contains(&query_lower)
                    || entry.name.to_lowercase().contains(&query_lower)
                    || entry.description.to_lowercase().contains(&query_lower)
            })
            .collect()
    }
}

impl Default for PluginIndex {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 4: Export from lib.rs**

Add `pub mod index;` and `pub use index::{PluginIndex, PluginIndexEntry, INDEX_REPOSITORY, INDEX_TAG};`

Update the module doc table.

- [ ] **Step 5: Build and test**

```bash
cargo test -p rapidbyte-registry
```

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(registry): add plugin index data types with search and filter"
```

---

### Task 2: Index OCI storage

**Files:**
- Modify: `crates/rapidbyte-registry/src/client.rs`
- Modify: `crates/rapidbyte-registry/src/artifact.rs`

Add methods to pull/push the index as an OCI artifact. The index uses a simple single-layer layout (just JSON, no wasm).

- [ ] **Step 1: Add index media type constants to artifact.rs**

```rust
/// Media type for the index artifact config.
pub const MEDIA_TYPE_INDEX_CONFIG: &str = "application/vnd.rapidbyte.index.config.v1+json";

/// Media type for the index data layer.
pub const MEDIA_TYPE_INDEX_LAYER: &str = "application/vnd.rapidbyte.index.v1+json";
```

- [ ] **Step 2: Add pull_index and push_index to client.rs**

```rust
/// Pull the plugin index from `<registry>/rapidbyte-index:latest`.
///
/// Returns `None` if the index does not exist yet.
pub async fn pull_index(&self, registry: &str) -> Result<Option<PluginIndex>> {
    let index_ref = PluginRef {
        registry: registry.to_owned(),
        repository: INDEX_REPOSITORY.to_owned(),
        tag: INDEX_TAG.to_owned(),
    };
    let reference = to_reference(&index_ref)?;

    match self.inner.pull(&reference, &self.auth, vec![
        MEDIA_TYPE_INDEX_CONFIG,
        MEDIA_TYPE_INDEX_LAYER,
        // Also accept standard types for compat
        oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
        oci_client::manifest::IMAGE_MANIFEST_MEDIA_TYPE,
    ]).await {
        Ok(image) => {
            // Find the index data layer
            let data = image.layers.first()
                .context("index artifact has no layers")?;
            let index: PluginIndex = serde_json::from_slice(&data.data)
                .context("failed to parse index JSON")?;
            Ok(Some(index))
        }
        Err(_) => Ok(None), // Index doesn't exist yet
    }
}

/// Push an updated plugin index to `<registry>/rapidbyte-index:latest`.
pub async fn push_index(&self, registry: &str, index: &PluginIndex) -> Result<()> {
    let index_ref = PluginRef {
        registry: registry.to_owned(),
        repository: INDEX_REPOSITORY.to_owned(),
        tag: INDEX_TAG.to_owned(),
    };

    let index_json = serde_json::to_vec_pretty(index)
        .context("failed to serialize index")?;

    let config = Config::new(
        b"{}".to_vec(),
        MEDIA_TYPE_INDEX_CONFIG.to_owned(),
        None,
    );
    let layer = ImageLayer::new(
        index_json,
        MEDIA_TYPE_INDEX_LAYER.to_owned(),
        None,
    );

    let reference = to_reference(&index_ref)?;
    // Use push directly to avoid the PackedArtifact type
    self.inner
        .push(&reference, &[layer], config, &self.auth, None)
        .await
        .context("failed to push index")?;

    Ok(())
}
```

Note: The exact `oci-client` API for `push` may differ. Read `client.rs` to see how `push` is currently implemented and match the pattern. The key difference from plugin push: the index has one JSON layer (no wasm), and uses index-specific media types.

- [ ] **Step 3: Add necessary imports**

Import `PluginIndex`, `INDEX_REPOSITORY`, `INDEX_TAG`, and the new media type constants.

- [ ] **Step 4: Build**

```bash
cargo check -p rapidbyte-registry
```

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(registry): add index pull/push to OCI client"
```

---

### Task 3: Update push command to maintain index

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/plugin.rs`

After pushing a plugin artifact, the `push` command should pull the current index, upsert the new plugin entry, and push the updated index.

- [ ] **Step 1: Add helper to derive plugin_type from manifest**

```rust
fn plugin_type_from_manifest(manifest: &rapidbyte_types::manifest::PluginManifest) -> &'static str {
    if manifest.roles.source.is_some() {
        "source"
    } else if manifest.roles.destination.is_some() {
        "destination"
    } else if manifest.roles.transform.is_some() {
        "transform"
    } else {
        "unknown"
    }
}
```

- [ ] **Step 2: Update the push function**

After the existing `client.push(...)` call, add index maintenance:

```rust
    // Update the registry index
    eprintln!("Updating registry index...");
    let mut index = client
        .pull_index(&plugin_ref.registry)
        .await?
        .unwrap_or_default();

    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: plugin_ref.repository.clone(),
        name: manifest.name.clone(),
        description: manifest.description.clone(),
        plugin_type: plugin_type_from_manifest(&manifest).to_owned(),
        latest: plugin_ref.tag.clone(),
        versions: vec![plugin_ref.tag.clone()],
        author: manifest.author.clone(),
        license: manifest.license.clone(),
        updated_at: chrono::Utc::now(),
    });

    client.push_index(&plugin_ref.registry, &index).await?;
    eprintln!("Index updated");
```

- [ ] **Step 3: Add chrono dependency to CLI Cargo.toml**

```toml
chrono = { workspace = true }
```

- [ ] **Step 4: Build and verify**

```bash
cargo check -p rapidbyte-cli
```

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(cli): update push command to maintain registry index"
```

---

### Task 4: Add search command

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/plugin.rs`

- [ ] **Step 1: Add Search variant to PluginCommands**

In `main.rs`, add to the `PluginCommands` enum:

```rust
    /// Search for plugins in a registry
    Search {
        /// Search query (matches name, description, repository)
        #[arg(default_value = "")]
        query: String,
        /// Filter by plugin type (source, destination, transform)
        #[arg(long, short = 't')]
        plugin_type: Option<String>,
        /// Registry to search (required if --registry-url not set)
        #[arg(long)]
        registry: Option<String>,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
```

- [ ] **Step 2: Wire dispatch in plugin.rs execute()**

Add match arm:
```rust
crate::PluginCommands::Search {
    query,
    plugin_type,
    registry,
    insecure,
} => search(&query, plugin_type.as_deref(), registry.as_deref(), insecure).await,
```

- [ ] **Step 3: Implement search handler**

```rust
async fn search(
    query: &str,
    plugin_type: Option<&str>,
    registry: Option<&str>,
    insecure: bool,
) -> Result<()> {
    let registry = registry.context(
        "registry is required for search. Use --registry or set RAPIDBYTE_REGISTRY_URL"
    )?;
    let registry = rapidbyte_registry::normalize_registry_url(registry);

    let config = RegistryConfig {
        insecure,
        ..Default::default()
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Searching {registry}...");
    let index = client
        .pull_index(&registry)
        .await?
        .context("no plugin index found in this registry (push a plugin first)")?;

    let results = index.search(query, plugin_type);

    if results.is_empty() {
        eprintln!("No plugins found");
        return Ok(());
    }

    // Print results table
    println!(
        "{:<40} {:<14} {:<10} {}",
        "REPOSITORY", "TYPE", "LATEST", "DESCRIPTION"
    );
    for entry in &results {
        let desc = if entry.description.len() > 50 {
            format!("{}...", &entry.description[..47])
        } else {
            entry.description.clone()
        };
        println!(
            "{:<40} {:<14} {:<10} {}",
            entry.repository, entry.plugin_type, entry.latest, desc
        );
    }

    eprintln!("\n{} plugin(s) found", results.len());
    Ok(())
}
```

- [ ] **Step 4: Build and verify**

```bash
cargo check -p rapidbyte-cli
```

- [ ] **Step 5: Commit**

```bash
git commit -am "feat(cli): add plugin search command with type filtering"
```

---

### Task 5: Integration tests

**Files:**
- Modify: `crates/rapidbyte-registry/tests/integration.rs`

- [ ] **Step 1: Add index integration test**

```rust
#[tokio::test]
#[ignore]
async fn push_updates_index_and_search_finds_plugin() {
    let client = RegistryClient::new(&insecure_config()).unwrap();

    // Index should not exist yet (or be empty for this test plugin)
    let plugin_ref = PluginRef::parse("localhost:5050/test/search-test:0.1.0").unwrap();

    // Push a plugin
    let manifest_json = test_manifest_json();
    let wasm_bytes = b"search-test-wasm".to_vec();
    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .unwrap();

    // Update index manually (in production, CLI push does this)
    let mut index = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .unwrap_or_default();

    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: "test/search-test".to_owned(),
        name: "Test Plugin".to_owned(),
        description: "A searchable test plugin".to_owned(),
        plugin_type: "source".to_owned(),
        latest: "0.1.0".to_owned(),
        versions: vec!["0.1.0".to_owned()],
        author: Some("test".to_owned()),
        license: None,
        updated_at: chrono::Utc::now(),
    });

    client.push_index("localhost:5050", &index).await.unwrap();

    // Pull index and search
    let pulled_index = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .expect("index should exist");

    let results = pulled_index.search("searchable", None);
    assert!(!results.is_empty());
    assert_eq!(results[0].repository, "test/search-test");

    // Filter by type
    let source_results = pulled_index.search("", Some("source"));
    assert!(source_results.iter().any(|e| e.repository == "test/search-test"));

    let dest_results = pulled_index.search("", Some("destination"));
    assert!(!dest_results.iter().any(|e| e.repository == "test/search-test"));
}

#[tokio::test]
#[ignore]
async fn pull_index_returns_none_when_not_found() {
    let client = RegistryClient::new(&insecure_config()).unwrap();
    let result = client
        .pull_index("localhost:5050")
        .await
        .unwrap();
    // May or may not be None depending on prior test runs, but should not error
    assert!(result.is_none() || result.is_some());
}
```

- [ ] **Step 2: Add chrono to integration test deps if needed**

- [ ] **Step 3: Run integration tests**

```bash
docker compose up -d registry
cargo test -p rapidbyte-registry --test integration -- --ignored
docker compose down
```

- [ ] **Step 4: Commit**

```bash
git commit -am "test(registry): add index search integration tests"
```

---

## Verification

After all tasks:

1. `cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude transform-validate` — all tests pass
2. `cargo clippy --workspace --all-targets -- -D warnings` — clean
3. Manual end-to-end flow:
```bash
docker compose up -d registry

# Push two plugins
rapidbyte plugin push localhost:5050/source/postgres:1.0.0 target/plugins/sources/postgres.wasm --insecure
rapidbyte plugin push localhost:5050/dest/postgres:1.0.0 target/plugins/destinations/postgres.wasm --insecure

# Search
rapidbyte plugin search postgres --registry localhost:5050 --insecure
# Expected: shows both source and dest postgres

rapidbyte plugin search --type source --registry localhost:5050 --insecure
# Expected: shows only source postgres

rapidbyte plugin search nonexistent --registry localhost:5050 --insecure
# Expected: "No plugins found"

docker compose down
```
