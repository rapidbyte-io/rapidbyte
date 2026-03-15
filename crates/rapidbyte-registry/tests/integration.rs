//! Integration tests for OCI registry operations.
//!
//! These tests require a running OCI registry at `localhost:5050`.
//! Start it with: `docker compose up -d registry`
//!
//! Run with: `cargo test -p rapidbyte-registry --test integration -- --ignored`

use rapidbyte_registry::artifact::{self, unpack_artifact};
use rapidbyte_registry::cache::PluginCache;
use rapidbyte_registry::client::{RegistryClient, RegistryConfig};
use rapidbyte_registry::PluginRef;
use rapidbyte_types::wire::PluginKind;

fn insecure_config() -> RegistryConfig {
    RegistryConfig {
        insecure: true,
        ..Default::default()
    }
}

fn test_manifest_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": "test/plugin",
        "name": "Test Plugin",
        "version": "0.1.0",
        "protocol_version": "6",
        "description": "A test plugin for integration tests",
        "permissions": { "network": [] },
        "limits": {},
        "roles": { "source": true }
    }))
    .unwrap()
}

#[tokio::test]
#[ignore] // requires: docker compose up -d registry
async fn push_and_pull_roundtrip() {
    let client = RegistryClient::new(&insecure_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/roundtrip:0.1.0").unwrap();

    let manifest_json = test_manifest_json();
    let wasm_bytes = b"fake-wasm-binary-for-roundtrip-test".to_vec();

    // Pack and push
    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes, None);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .expect("push should succeed");

    // Pull and unpack
    let image = client.pull(&plugin_ref).await.expect("pull should succeed");
    let unpacked = unpack_artifact(&image).expect("unpack should succeed");

    assert_eq!(unpacked.manifest_json, manifest_json);
    assert_eq!(unpacked.wasm_bytes, wasm_bytes);
}

#[tokio::test]
#[ignore]
async fn list_tags_returns_pushed_versions() {
    let client = RegistryClient::new(&insecure_config()).unwrap();

    // Push two versions
    let manifest = test_manifest_json();
    for tag in ["1.0.0", "1.1.0"] {
        let plugin_ref = PluginRef::parse(&format!("localhost:5050/test/tags-list:{tag}")).unwrap();
        let packed = artifact::pack_artifact(&manifest, b"wasm", None);
        client
            .push(&plugin_ref, packed.layers, packed.config)
            .await
            .unwrap();
    }

    // List
    let any_ref = PluginRef::parse("localhost:5050/test/tags-list:latest").unwrap();
    let tags = client.list_tags(&any_ref).await.unwrap();

    assert!(tags.contains(&"1.0.0".to_string()));
    assert!(tags.contains(&"1.1.0".to_string()));
}

#[tokio::test]
#[ignore]
async fn pull_and_cache_end_to_end() {
    let client = RegistryClient::new(&insecure_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/cache-e2e:0.1.0").unwrap();

    let manifest_json = test_manifest_json();
    let wasm_bytes = b"cached-wasm-content-for-e2e".to_vec();

    // Push
    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes, None);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .unwrap();

    // Pull
    let image = client.pull(&plugin_ref).await.unwrap();
    let unpacked = unpack_artifact(&image).unwrap();

    // Store in cache
    let tmp = tempfile::tempdir().unwrap();
    let cache = PluginCache::new(tmp.path().to_path_buf());
    let entry = cache
        .store(
            &plugin_ref,
            &unpacked.manifest_json,
            &unpacked.wasm_bytes,
            &unpacked.config,
        )
        .unwrap();

    assert!(entry.wasm_path.exists());
    assert_eq!(entry.size_bytes, wasm_bytes.len() as u64);

    // Lookup should find it
    let found = cache.lookup(&plugin_ref).expect("should find cached entry");
    assert_eq!(found.digest, entry.digest);

    // Wasm content should match
    let cached_wasm = std::fs::read(&found.wasm_path).unwrap();
    assert_eq!(cached_wasm, wasm_bytes);
}

#[tokio::test]
#[ignore]
async fn inspect_pulls_manifest_only() {
    let client = RegistryClient::new(&insecure_config()).unwrap();
    let plugin_ref = PluginRef::parse("localhost:5050/test/inspect:0.1.0").unwrap();

    // Push first
    let manifest_json = test_manifest_json();
    let packed = artifact::pack_artifact(&manifest_json, b"inspect-wasm", None);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .unwrap();

    // Inspect (manifest only, no layers)
    let (manifest, digest) = client
        .pull_manifest_only(&plugin_ref)
        .await
        .expect("inspect should succeed");

    assert!(!digest.is_empty());
    assert!(!manifest.layers.is_empty());
}

#[tokio::test]
#[ignore]
async fn index_push_pull_and_search_roundtrip() {
    let client = RegistryClient::new(&insecure_config()).unwrap();

    // Push a plugin first
    let plugin_ref = PluginRef::parse("localhost:5050/test/index-search:0.2.0").unwrap();
    let packed = artifact::pack_artifact(&test_manifest_json(), b"index-search-wasm", None);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .unwrap();

    // Build and push an index
    let mut index = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .unwrap_or_default();

    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: "test/index-search".to_owned(),
        name: "Index Search Test".to_owned(),
        description: "A plugin for testing index search".to_owned(),
        plugin_type: PluginKind::Source,
        latest: "0.2.0".to_owned(),
        versions: vec!["0.2.0".to_owned()],
        author: Some("test-author".to_owned()),
        license: None,
        updated_at: chrono::Utc::now(),
    });

    client.push_index("localhost:5050", &index).await.unwrap();

    // Pull the index back and search
    let pulled = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .expect("index should exist after push");

    // Search by description
    let results = pulled.search("testing index", None);
    assert!(
        !results.is_empty(),
        "should find plugin by description search"
    );
    assert_eq!(results[0].repository, "test/index-search");

    // Search by type filter
    let source_results = pulled.search("", Some(PluginKind::Source));
    assert!(source_results
        .iter()
        .any(|e| e.repository == "test/index-search"));

    let dest_results = pulled.search("", Some(PluginKind::Destination));
    assert!(!dest_results
        .iter()
        .any(|e| e.repository == "test/index-search"));
}

#[tokio::test]
#[ignore]
async fn index_upsert_adds_version_without_duplicating() {
    let client = RegistryClient::new(&insecure_config()).unwrap();

    // Start fresh or with existing index
    let mut index = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .unwrap_or_default();

    // Push version 1.0.0
    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: "test/versioned".to_owned(),
        name: "Versioned Plugin".to_owned(),
        description: "Tests version accumulation".to_owned(),
        plugin_type: PluginKind::Transform,
        latest: "1.0.0".to_owned(),
        versions: vec!["1.0.0".to_owned()],
        author: None,
        license: None,
        updated_at: chrono::Utc::now(),
    });

    // Push version 1.1.0
    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: "test/versioned".to_owned(),
        name: "Versioned Plugin".to_owned(),
        description: "Tests version accumulation".to_owned(),
        plugin_type: PluginKind::Transform,
        latest: "1.1.0".to_owned(),
        versions: vec!["1.1.0".to_owned()],
        author: None,
        license: None,
        updated_at: chrono::Utc::now(),
    });

    client.push_index("localhost:5050", &index).await.unwrap();

    // Pull back and verify
    let pulled = client
        .pull_index("localhost:5050")
        .await
        .unwrap()
        .expect("index should exist");

    let entry = pulled
        .search("versioned", None)
        .into_iter()
        .find(|e| e.repository == "test/versioned")
        .expect("should find versioned plugin");

    assert_eq!(entry.latest, "1.1.0");
    assert!(entry.versions.contains(&"1.0.0".to_owned()));
    assert!(entry.versions.contains(&"1.1.0".to_owned()));
    // No duplicates
    assert_eq!(entry.versions.iter().filter(|v| *v == "1.0.0").count(), 1);
}
