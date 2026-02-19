# Inflight Batches Enforcement & CDC Sync Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enforce `max_inflight_batches` from pipeline config instead of hardcoding channel capacity to 16, add CDC sync mode to the protocol, and document `max_parallel_requests` as future-facing.

**Architecture:** Three small, independent changes: (1) add `max_inflight_batches` to pipeline YAML `resources:` config, wire it through the orchestrator to set the `mpsc::sync_channel` capacity, and validate it; (2) add `SyncMode::Cdc` variant to the SDK protocol enum, accept it in the pipeline validator, and route it in the orchestrator; (3) `max_parallel_requests` requires no code changes — just stays at its default of 1 until SaaS/HTTP connectors are built.

**Tech Stack:** Rust, serde, mpsc channels

---

### Task 1: Add `max_inflight_batches` to ResourceConfig and enforce in orchestrator

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/types.rs` (ResourceConfig struct + Default impl)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs:141-151` (limits construction) and `:233` (channel creation)
- Modify: `crates/rapidbyte-core/src/pipeline/validator.rs` (validation)
- Test: `crates/rapidbyte-core/src/pipeline/types.rs` (existing tests module)
- Test: `crates/rapidbyte-core/src/pipeline/validator.rs` (existing tests module)

**Step 1: Write failing tests**

In `crates/rapidbyte-core/src/pipeline/types.rs`, add to the existing `tests` module:

```rust
#[test]
fn test_deserialize_max_inflight_batches() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
resources:
  max_inflight_batches: 32
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.resources.max_inflight_batches, 32);
}

#[test]
fn test_max_inflight_batches_defaults_to_16() {
    let config = ResourceConfig::default();
    assert_eq!(config.resources.max_inflight_batches, 16);
}
```

In `crates/rapidbyte-core/src/pipeline/validator.rs`, add to the existing `tests` module:

```rust
#[test]
fn test_max_inflight_batches_zero_fails() {
    let yaml = r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
resources:
  max_inflight_batches: 0
"#;
    let config = parse_pipeline_str(yaml).unwrap();
    let err = validate_pipeline(&config).unwrap_err().to_string();
    assert!(err.contains("max_inflight_batches"));
}

#[test]
fn test_max_inflight_batches_valid_passes() {
    let yaml = r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
resources:
  max_inflight_batches: 8
"#;
    let config = parse_pipeline_str(yaml).unwrap();
    assert!(validate_pipeline(&config).is_ok());
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --workspace -- test_deserialize_max_inflight_batches test_max_inflight_batches`
Expected: FAIL — field does not exist on `ResourceConfig`

**Step 3: Add `max_inflight_batches` field to ResourceConfig**

In `crates/rapidbyte-core/src/pipeline/types.rs`:

Add to `ResourceConfig` struct (after `compression` field):
```rust
/// Channel capacity between pipeline stages. Controls backpressure.
/// Must be >= 1. Default: 16.
#[serde(default = "default_max_inflight_batches")]
pub max_inflight_batches: u32,
```

Add default function:
```rust
fn default_max_inflight_batches() -> u32 {
    16
}
```

Update `Default for ResourceConfig` impl to include:
```rust
max_inflight_batches: default_max_inflight_batches(),
```

**Step 4: Add validation for `max_inflight_batches`**

In `crates/rapidbyte-core/src/pipeline/validator.rs`, add after the compression validation block (before the `if errors.is_empty()` check):

```rust
if config.resources.max_inflight_batches == 0 {
    errors.push("max_inflight_batches must be at least 1".to_string());
}
```

**Step 5: Wire `max_inflight_batches` into orchestrator**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`:

Update `limits` construction (~line 141-151) to set `max_inflight_batches` from config:
```rust
let limits = StreamLimits {
    max_batch_bytes: if max_batch > 0 {
        max_batch
    } else {
        StreamLimits::default().max_batch_bytes
    },
    max_inflight_batches: config.resources.max_inflight_batches,
    checkpoint_interval_bytes: checkpoint_interval,
    checkpoint_interval_rows: config.resources.checkpoint_interval_rows,
    checkpoint_interval_seconds: config.resources.checkpoint_interval_seconds,
    ..StreamLimits::default()
};
```

Update channel creation (~line 233) to use limits value instead of hardcoded 16:
```rust
let (tx, rx) = mpsc::sync_channel::<Frame>(limits.max_inflight_batches as usize);
```

**Step 6: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: All tests PASS including new ones

**Step 7: Commit**

```bash
git add crates/rapidbyte-core/src/pipeline/types.rs crates/rapidbyte-core/src/pipeline/validator.rs crates/rapidbyte-core/src/engine/orchestrator.rs
git commit -m "feat(core): enforce max_inflight_batches from pipeline config

The channel capacity between pipeline stages was hardcoded to 16.
Now reads from resources.max_inflight_batches in pipeline YAML
(default: 16, must be >= 1)."
```

---

### Task 2: Add `SyncMode::Cdc` variant to protocol

**Files:**
- Modify: `crates/rapidbyte-sdk/src/protocol.rs:4-8` (SyncMode enum)
- Modify: `crates/rapidbyte-core/src/pipeline/validator.rs:33-41` (sync_mode validation)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs:166-169` (sync_mode match)
- Test: `crates/rapidbyte-sdk/src/protocol.rs` (existing tests module)
- Test: `crates/rapidbyte-core/src/pipeline/validator.rs` (existing tests module)

**Step 1: Write failing tests**

In `crates/rapidbyte-sdk/src/protocol.rs`, add to the existing `tests` module:

```rust
#[test]
fn test_sync_mode_cdc_roundtrip() {
    let mode = SyncMode::Cdc;
    let json = serde_json::to_string(&mode).unwrap();
    assert_eq!(json, "\"cdc\"");
    let back: SyncMode = serde_json::from_str(&json).unwrap();
    assert_eq!(mode, back);
}
```

In `crates/rapidbyte-core/src/pipeline/validator.rs`, add to existing `tests` module:

```rust
#[test]
fn test_cdc_sync_mode_passes() {
    let yaml = r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: cdc
destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
    let config = parse_pipeline_str(yaml).unwrap();
    assert!(validate_pipeline(&config).is_ok());
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --workspace -- test_sync_mode_cdc test_cdc_sync_mode`
Expected: FAIL — `Cdc` variant doesn't exist / `"cdc"` not in valid sync_modes

**Step 3: Add `Cdc` variant to SyncMode enum**

In `crates/rapidbyte-sdk/src/protocol.rs`, update the `SyncMode` enum:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    FullRefresh,
    Incremental,
    Cdc,
}
```

**Step 4: Accept `"cdc"` in pipeline validator**

In `crates/rapidbyte-core/src/pipeline/validator.rs`, update the sync_mode match (line 34):

```rust
match stream.sync_mode.as_str() {
    "full_refresh" | "incremental" | "cdc" => {}
    other => {
        errors.push(format!(
            "Stream '{}' has invalid sync_mode '{}', expected 'full_refresh', 'incremental', or 'cdc'",
            stream.name, other
        ));
    }
}
```

**Step 5: Route `"cdc"` in orchestrator**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, update the sync_mode match (~line 166):

```rust
let sync_mode = match s.sync_mode.as_str() {
    "incremental" => SyncMode::Incremental,
    "cdc" => SyncMode::Cdc,
    _ => SyncMode::FullRefresh,
};
```

**Step 6: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: All tests PASS

**Step 7: Commit**

```bash
git add crates/rapidbyte-sdk/src/protocol.rs crates/rapidbyte-core/src/pipeline/validator.rs crates/rapidbyte-core/src/engine/orchestrator.rs
git commit -m "feat(sdk): add SyncMode::Cdc variant to protocol

Adds 'cdc' as a valid sync_mode for streams. The orchestrator routes
it correctly and the validator accepts it. No connector changes needed
yet — CDC logical replication is future work."
```

---

### Task 3: Run full test suite and verify

**Files:**
- None (verification only)

**Step 1: Run all workspace tests**

Run: `cargo test --workspace`
Expected: All tests PASS (including all new tests from Tasks 1-2)

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Verify connectors still build**

Run: `cd connectors/source-postgres && cargo build --release 2>&1 | tail -1`
Run: `cd connectors/dest-postgres && cargo build --release 2>&1 | tail -1`
Expected: Both compile successfully (the SDK protocol change adds a new enum variant which is backwards-compatible)
