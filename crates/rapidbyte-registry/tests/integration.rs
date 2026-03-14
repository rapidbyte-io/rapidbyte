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
    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .expect("push should succeed");

    // Pull and unpack
    let image = client.pull(&plugin_ref).await.expect("pull should succeed");
    let (pulled_manifest, pulled_wasm) = unpack_artifact(&image).expect("unpack should succeed");

    assert_eq!(pulled_manifest, manifest_json);
    assert_eq!(pulled_wasm, wasm_bytes);
}

#[tokio::test]
#[ignore]
async fn list_tags_returns_pushed_versions() {
    let client = RegistryClient::new(&insecure_config()).unwrap();

    // Push two versions
    let manifest = test_manifest_json();
    for tag in ["1.0.0", "1.1.0"] {
        let plugin_ref = PluginRef::parse(&format!("localhost:5050/test/tags-list:{tag}")).unwrap();
        let packed = artifact::pack_artifact(&manifest, b"wasm");
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
    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes);
    client
        .push(&plugin_ref, packed.layers, packed.config)
        .await
        .unwrap();

    // Pull
    let image = client.pull(&plugin_ref).await.unwrap();
    let (pulled_manifest, pulled_wasm) = unpack_artifact(&image).unwrap();

    // Store in cache
    let tmp = tempfile::tempdir().unwrap();
    let cache = PluginCache::new(tmp.path().to_path_buf());
    let entry = cache
        .store(&plugin_ref, &pulled_manifest, &pulled_wasm)
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
    let packed = artifact::pack_artifact(&manifest_json, b"inspect-wasm");
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
