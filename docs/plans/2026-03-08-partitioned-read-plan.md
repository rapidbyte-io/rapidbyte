# Feature::PartitionedRead Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace hardcoded plugin name comparisons in auto-parallelism, partitioning, and autotune with a manifest-declared `Feature::PartitionedRead` flag.

**Architecture:** Add a `PartitionedRead` variant to the existing `Feature` enum in `rapidbyte-types`. The postgres source plugin declares this feature. The engine derives a `bool` from the source manifest's features and threads it to the three decision sites, replacing `source_connector_id == "source-postgres"` string comparisons.

**Tech Stack:** Rust, serde, existing `Feature` enum pattern

**Design doc:** `docs/plans/2026-03-08-partitioned-read-feature-design.md`

---

### Task 1: Add `Feature::PartitionedRead` variant and test serde round-trip

**Files:**
- Modify: `crates/rapidbyte-types/src/wire.rs:65-76`

**Step 1: Write the failing test**

Add to the existing `mod tests` block at the bottom of `wire.rs`:

```rust
#[test]
fn feature_partitioned_read_serde_roundtrip() {
    let feature = Feature::PartitionedRead;
    let json = serde_json::to_string(&feature).unwrap();
    assert_eq!(json, "\"partitioned_read\"");
    let back: Feature = serde_json::from_str(&json).unwrap();
    assert_eq!(back, Feature::PartitionedRead);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-types feature_partitioned_read_serde_roundtrip`
Expected: FAIL — `PartitionedRead` variant does not exist yet.

**Step 3: Write minimal implementation**

Add the variant to the `Feature` enum in `wire.rs` (after `BulkLoadCopy`):

```rust
/// Source supports parallel partitioned reads (mod/range sharding).
PartitionedRead,
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-types feature_partitioned_read_serde_roundtrip`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/wire.rs
git commit -m "feat(types): add Feature::PartitionedRead variant"
```

---

### Task 2: Declare `PartitionedRead` in postgres source plugin

**Files:**
- Modify: `plugins/sources/postgres/src/main.rs` (the `open()` return where `PluginInfo` is constructed)

**Step 1: Find the `PluginInfo` construction**

Look for the `features: vec![Feature::Cdc]` line in the source plugin's `open()` method.

**Step 2: Add `PartitionedRead` to the features list**

Change:
```rust
features: vec![Feature::Cdc],
```
To:
```rust
features: vec![Feature::Cdc, Feature::PartitionedRead],
```

**Step 3: Build the plugin to verify it compiles**

Run: `cd plugins/sources/postgres && cargo build`
Expected: Compiles successfully.

**Step 4: Commit**

```bash
git add plugins/sources/postgres/src/main.rs
git commit -m "feat(source-postgres): declare PartitionedRead feature"
```

---

### Task 3: Add manifest helper to check for a feature

**Files:**
- Modify: `crates/rapidbyte-types/src/manifest.rs`

**Step 1: Write the failing test**

Add to the existing `mod tests` block in `manifest.rs`:

```rust
#[test]
fn manifest_has_source_feature() {
    let manifest = ConnectorManifest {
        id: "test/pg".to_string(),
        name: "Test".to_string(),
        version: "0.1.0".to_string(),
        description: String::new(),
        author: None,
        protocol_version: ProtocolVersion::V5,
        roles: Roles {
            source: Some(SourceCapabilities {
                supported_sync_modes: vec![SyncMode::FullRefresh],
                features: vec![Feature::PartitionedRead],
            }),
            destination: None,
            transform: None,
        },
        permissions: Permissions::default(),
        limits: ResourceLimits::default(),
        config_schema: None,
    };

    assert!(manifest.has_source_feature(Feature::PartitionedRead));
    assert!(!manifest.has_source_feature(Feature::Cdc));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-types manifest_has_source_feature`
Expected: FAIL — `has_source_feature` method does not exist.

**Step 3: Write minimal implementation**

Add to the `impl ConnectorManifest` block (or create one if it doesn't exist):

```rust
impl ConnectorManifest {
    /// Check whether the source role declares a given feature.
    pub fn has_source_feature(&self, feature: Feature) -> bool {
        self.roles
            .source
            .as_ref()
            .is_some_and(|s| s.features.contains(&feature))
    }
}
```

Note: Also check if there's an existing `impl ConnectorManifest` block with `supports_role` — if so, add the method there.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-types manifest_has_source_feature`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/manifest.rs
git commit -m "feat(types): add ConnectorManifest::has_source_feature helper"
```

---

### Task 4: Replace hardcoded checks in autotune

**Files:**
- Modify: `crates/rapidbyte-engine/src/autotune.rs:36-63` (function signature and body)

**Step 1: Update the function signature**

Change `resolve_stream_autotune` parameter from:
```rust
pub fn resolve_stream_autotune(
    config: &PipelineConfig,
    baseline_parallelism: u32,
    source_connector_id: &str,
) -> StreamAutotuneDecision {
```
To:
```rust
pub fn resolve_stream_autotune(
    config: &PipelineConfig,
    baseline_parallelism: u32,
    supports_partitioned_read: bool,
) -> StreamAutotuneDecision {
```

**Step 2: Replace the name check in the body**

Change line 46 from:
```rust
let partition_strategy = if source_connector_id == "source-postgres" {
```
To:
```rust
let partition_strategy = if supports_partitioned_read {
```

**Step 3: Update tests**

In the test module, update all calls to `resolve_stream_autotune` to pass `true` instead of `"source-postgres"`:

```rust
// Was: resolve_stream_autotune(&config, 3, "source-postgres");
// Now:
resolve_stream_autotune(&config, 3, true);
```

There are 3 test call sites to update.

**Step 4: Run tests to verify**

Run: `cargo test -p rapidbyte-engine autotune`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/autotune.rs
git commit -m "refactor(autotune): replace plugin name check with supports_partitioned_read bool"
```

---

### Task 5: Replace hardcoded checks in orchestrator

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`

This is the largest task. Three changes in production code plus threading the manifest through.

**Step 1: Add `source_manifest` parameter to `build_stream_contexts`**

Change the signature from:
```rust
fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
    max_records: Option<u64>,
) -> Result<StreamBuild, PipelineError> {
```
To:
```rust
fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
    max_records: Option<u64>,
    source_manifest: Option<&ConnectorManifest>,
) -> Result<StreamBuild, PipelineError> {
```

**Step 2: Derive `supports_partitioned_read` bool from manifest**

At the top of `build_stream_contexts`, replace:
```rust
let source_connector_id = parse_connector_ref(&config.source.use_ref).0;
```
With:
```rust
let supports_partitioned_read = source_manifest
    .is_some_and(|m| m.has_source_feature(Feature::PartitionedRead));
```

**Step 3: Update the autotune call**

Change:
```rust
let autotune_decision = crate::autotune::resolve_stream_autotune(
    config,
    baseline_parallelism,
    &source_connector_id,
);
```
To:
```rust
let autotune_decision = crate::autotune::resolve_stream_autotune(
    config,
    baseline_parallelism,
    supports_partitioned_read,
);
```

**Step 4: Update the partitioning check**

Change:
```rust
let should_partition = source_connector_id == "source-postgres" && configured_parallelism > 1;
```
To:
```rust
let should_partition = supports_partitioned_read && configured_parallelism > 1;
```

**Step 5: Update `resolve_auto_parallelism_for_cores`**

Add `supports_partitioned_read: bool` parameter. Replace:
```rust
let source_connector_id = parse_connector_ref(&config.source.use_ref).0;
let eligible_streams = if source_connector_id == "source-postgres" {
```
With:
```rust
let eligible_streams = if supports_partitioned_read {
```

Thread this through `resolve_auto_parallelism` and `resolve_effective_parallelism` similarly — they call `resolve_auto_parallelism_for_cores`, so they also need the bool parameter (or derive it themselves).

Alternatively, since `resolve_effective_parallelism` is only called from `build_stream_contexts` which now has the manifest, you can inline the bool there.

**Step 6: Update the callsite in `run_pipeline_once`**

Where `build_stream_contexts` is called (~line 478), pass the manifest:
```rust
let source_manifest_for_build = connectors.source_manifest.clone();
// ...
build_stream_contexts(&config_for_build, state_for_build.as_ref(), max_records, source_manifest_for_build.as_ref())
```

**Step 7: Update tests**

The test helpers `config_with_parallelism` tests call `build_stream_contexts` without a manifest. Pass `None` for tests that don't test partitioning. For tests that test partitioning (`full_refresh_with_parallelism_fans_out_stream_contexts`), construct a minimal manifest with `PartitionedRead`:

```rust
let manifest = ConnectorManifest {
    id: "test/pg".to_string(),
    name: "Test".to_string(),
    version: "0.1.0".to_string(),
    description: String::new(),
    author: None,
    protocol_version: ProtocolVersion::V5,
    roles: Roles {
        source: Some(SourceCapabilities {
            supported_sync_modes: vec![SyncMode::FullRefresh],
            features: vec![Feature::PartitionedRead],
        }),
        ..Default::default()
    },
    permissions: Permissions::default(),
    limits: ResourceLimits::default(),
    config_schema: None,
};
let build = build_stream_contexts(&config, &state, None, Some(&manifest))
    .expect("stream contexts built");
```

For tests that verify non-partitioning behavior (`incremental_streams_remain_unpartitioned`, `replace_mode_streams_remain_unpartitioned`), pass `Some(&manifest)` too — they should still not partition because their streams are incremental/replace, not because the plugin lacks the feature.

Also update the auto-parallelism tests that use `config_with_parallelism_expr`. These pass `"source-postgres"` as source_use — update these to pass `"postgres"` and add a `supports_partitioned_read: bool` param where needed (or adjust the test to check the feature directly).

**Step 8: Run all tests**

Run: `cargo test -p rapidbyte-engine`
Expected: PASS

**Step 9: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "refactor(orchestrator): replace plugin name checks with Feature::PartitionedRead"
```

---

### Task 6: Full verification

**Step 1: Run workspace tests**

Run: `cargo test --workspace`
Expected: All tests pass.

**Step 2: Build plugins**

Run: `just build-plugins` (or build each plugin individually)
Expected: All plugins compile.

**Step 3: Run E2E tests**

Run: `just e2e`
Expected: All E2E tests pass.

**Step 4: Run benchmark**

Run: `just bench postgres 1000000 --profile small --iters 3`
Expected: Throughput matches baseline (~490K+ rows/s, ~180+ MB/s, ~2.7+ CPU cores).

**Step 5: Commit any fixups, then done**
