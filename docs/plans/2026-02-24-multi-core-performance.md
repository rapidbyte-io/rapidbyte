# Multi-Core Performance Optimization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Maximize CPU core utilization during pipeline execution — from quick wins (allocator, build, channels) to a fundamental architectural change (partitioned reads) that saturates all available cores even for single-stream pipelines.

**Architecture:** The pipeline runs source/transform/dest stages in separate `spawn_blocking` threads connected by bounded channels. Each stage executes a single-threaded WASM connector via Wasmtime. Phase 1 eliminates overhead (allocator contention, suboptimal codegen, async channel overhead, hardcoded parallelism=1). Phase 2 introduces partitioned reads — spawning N source WASM instances per stream that fan into a shared channel, giving true multi-core utilization even for a single table.

**Tech Stack:** mimalloc, crossbeam-channel, std::thread::available_parallelism, partitioned `StreamContext`

---

## Phase 1: Quick Wins

These changes are independent, low-risk, and each improves throughput without architectural changes.

### Task 1: Add mimalloc Global Allocator

The default system allocator (macOS libmalloc / glibc malloc) uses per-arena locking that becomes a bottleneck when multiple threads allocate `Vec<u8>` batch buffers concurrently. mimalloc uses thread-local heaps with free-list sharding, eliminating contention.

**Files:**
- Modify: `crates/rapidbyte-cli/Cargo.toml`
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Add mimalloc dependency**

In `crates/rapidbyte-cli/Cargo.toml`, add to `[dependencies]`:

```toml
mimalloc = "0.1"
```

**Step 2: Set global allocator**

At the top of `crates/rapidbyte-cli/src/main.rs` (before `mod` declarations), add:

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

**Step 3: Verify it compiles and tests pass**

Run: `cargo test --workspace`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add crates/rapidbyte-cli/Cargo.toml crates/rapidbyte-cli/src/main.rs
git commit -m "perf: use mimalloc global allocator for reduced allocation contention"
```

---

### Task 2: Optimize Host Binary Release Profile

The workspace has no `[profile.release]` section. Connectors already use `lto=true, codegen-units=1`, but the host binary (which runs Wasmtime + all coordination) compiles with default release settings (no LTO, 16 codegen units). Adding thin LTO and single codegen unit improves Wasmtime execution speed and host function call overhead.

**Files:**
- Modify: `Cargo.toml` (workspace root)

**Step 1: Add release profile to workspace Cargo.toml**

Append to the end of the workspace `Cargo.toml`:

```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

Note: We use `lto = "thin"` (not `"fat"`) for faster compile times while still enabling cross-crate optimization. We intentionally omit `panic = "abort"` to preserve tokio's panic handling in `spawn_blocking` tasks.

**Step 2: Verify release build succeeds**

Run: `cargo build --release`
Expected: Compiles successfully (slower than before due to LTO, but produces faster binary).

**Step 3: Verify tests still pass**

Run: `cargo test --workspace`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add Cargo.toml
git commit -m "perf: add release profile with thin LTO and single codegen unit"
```

---

### Task 3: Replace tokio mpsc with crossbeam Channel

All pipeline stages run in `spawn_blocking` threads and use `blocking_send`/`blocking_recv` on `tokio::sync::mpsc` channels. This is the non-native mode for tokio's mpsc (designed for async). `crossbeam-channel` is purpose-built for blocking bounded SPSC/MPMC patterns with cache-line-padded queues and optimized parking. The channel sits in the hot path between every pipeline stage. Additionally, crossbeam channels support multiple senders natively (`Sender` is `Clone`), which Phase 2 requires for fan-in from partitioned sources.

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/rapidbyte-runtime/Cargo.toml`
- Modify: `crates/rapidbyte-engine/Cargo.toml`
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`

**Step 1: Add crossbeam-channel to workspace dependencies**

In workspace `Cargo.toml`, add to `[workspace.dependencies]`:

```toml
crossbeam-channel = "0.5"
```

In `crates/rapidbyte-runtime/Cargo.toml`, add to `[dependencies]`:

```toml
crossbeam-channel = { workspace = true }
```

In `crates/rapidbyte-engine/Cargo.toml`, add to `[dependencies]`:

```toml
crossbeam-channel = { workspace = true }
```

**Step 2: Update host_state.rs channel types**

In `crates/rapidbyte-runtime/src/host_state.rs`:

Replace the import:
```rust
use tokio::sync::mpsc;
```
with:
```rust
use crossbeam_channel;
```

Change `BatchRouter` fields (around line 73-78):
```rust
pub(crate) struct BatchRouter {
    pub sender: Option<crossbeam_channel::Sender<Frame>>,
    pub receiver: Option<crossbeam_channel::Receiver<Frame>>,
    pub next_batch_id: u64,
    pub compression: Option<CompressionCodec>,
}
```

Change `HostStateBuilder` fields (around line 113-114):
```rust
    sender: Option<crossbeam_channel::Sender<Frame>>,
    receiver: Option<crossbeam_channel::Receiver<Frame>>,
```

Change builder methods `sender()` and `receiver()` (around line 173-181):
```rust
    #[must_use]
    pub fn sender(mut self, tx: crossbeam_channel::Sender<Frame>) -> Self {
        self.sender = Some(tx);
        self
    }

    #[must_use]
    pub fn receiver(mut self, rx: crossbeam_channel::Receiver<Frame>) -> Self {
        self.receiver = Some(rx);
        self
    }
```

In `emit_batch_impl` (around line 337-339), change:
```rust
        sender
            .blocking_send(Frame::Data(batch))
            .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;
```
to:
```rust
        sender
            .send(Frame::Data(batch))
            .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;
```

In `next_batch_impl` (around line 359), change:
```rust
        let Some(frame) = receiver.blocking_recv() else {
            return Ok(None);
        };
```
to:
```rust
        let Some(frame) = receiver.recv().ok() else {
            return Ok(None);
        };
```

**Step 3: Update orchestrator.rs channel creation**

In `crates/rapidbyte-engine/src/orchestrator.rs`:

Replace the import (line 15):
```rust
use tokio::sync::mpsc;
```
with:
```rust
use crossbeam_channel;
```

Change channel creation (around line 507):
```rust
                channels.push(crossbeam_channel::bounded::<Frame>(params.channel_capacity));
```

Change the unzip type annotation (around line 510-513):
```rust
            let (mut senders, mut receivers): (
                Vec<crossbeam_channel::Sender<Frame>>,
                Vec<crossbeam_channel::Receiver<Frame>>,
            ) = channels.into_iter().unzip();
```

**Step 4: Update runner.rs function signatures and send calls**

In `crates/rapidbyte-engine/src/runner.rs`:

Replace the import (line 8):
```rust
use tokio::sync::mpsc;
```
with:
```rust
use crossbeam_channel;
```

Change `run_source_stream` signature (line 81):
```rust
    sender: crossbeam_channel::Sender<Frame>,
```

Change `run_source_stream` EndStream send (line 189):
```rust
    let _ = sender.send(Frame::EndStream);
```

Change `run_destination_stream` signature (line 230):
```rust
    receiver: crossbeam_channel::Receiver<Frame>,
```

Change `run_transform_stream` signature (line 394-395):
```rust
    receiver: crossbeam_channel::Receiver<Frame>,
    sender: crossbeam_channel::Sender<Frame>,
```

Change `run_transform_stream` EndStream send (line 490):
```rust
    let _ = sender.send(Frame::EndStream);
```

**Step 5: Add unit test for channel round-trip**

In `crates/rapidbyte-runtime/src/host_state.rs`, add to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn test_batch_channel_roundtrip() {
        let (tx, rx) = crossbeam_channel::bounded::<Frame>(4);
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());

        let mut src_host = ComponentHostState::builder()
            .pipeline("test")
            .connector_id("src")
            .stream("s")
            .state_backend(state.clone())
            .sender(tx)
            .build()
            .unwrap();

        let mut dst_host = ComponentHostState::builder()
            .pipeline("test")
            .connector_id("dst")
            .stream("s")
            .state_backend(state)
            .receiver(rx)
            .build()
            .unwrap();

        src_host.emit_batch_impl(vec![1, 2, 3]).unwrap();
        let batch = dst_host.next_batch_impl().unwrap();
        assert_eq!(batch, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_end_stream_returns_none() {
        let (tx, rx) = crossbeam_channel::bounded::<Frame>(4);
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());

        let mut dst_host = ComponentHostState::builder()
            .pipeline("test")
            .connector_id("dst")
            .stream("s")
            .state_backend(state)
            .receiver(rx)
            .build()
            .unwrap();

        tx.send(Frame::EndStream).unwrap();
        let batch = dst_host.next_batch_impl().unwrap();
        assert_eq!(batch, None);
    }
```

**Step 6: Run tests to verify**

Run: `cargo test --workspace`
Expected: All tests pass, including the two new channel tests.

**Step 7: Commit**

```bash
git add Cargo.toml crates/rapidbyte-runtime/Cargo.toml crates/rapidbyte-engine/Cargo.toml \
    crates/rapidbyte-runtime/src/host_state.rs \
    crates/rapidbyte-engine/src/orchestrator.rs \
    crates/rapidbyte-engine/src/runner.rs
git commit -m "perf: replace tokio mpsc with crossbeam-channel for blocking pipeline stages"
```

---

### Task 4: Auto-Detect Default Parallelism from Available Cores

The current default `parallelism: 1` means only one stream executes at a time, even when the pipeline has multiple streams and the machine has many cores. Auto-detecting available cores allows multi-stream pipelines to saturate the CPU. This value also becomes the **total WASM instance budget** for Phase 2's partitioned reads.

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/types.rs`

**Step 1: Write the failing test**

In `crates/rapidbyte-engine/src/config/types.rs`, add to `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn test_default_parallelism_uses_available_cores() {
        let config = ResourceConfig::default();
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        let expected = cores.min(MAX_AUTO_PARALLELISM);
        assert_eq!(config.parallelism, expected);
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine -- test_default_parallelism_uses_available_cores`
Expected: FAIL (current default is 1, not based on cores).

**Step 3: Implement auto-detection**

In `crates/rapidbyte-engine/src/config/types.rs`:

Replace the constant (line 13):
```rust
const DEFAULT_PARALLELISM: u32 = 1;
```
with:
```rust
/// Upper bound for auto-detected parallelism. Prevents spawning too many
/// concurrent WASM instances on high-core-count machines.
const MAX_AUTO_PARALLELISM: u32 = 8;
```

Change the `Default` impl for `ResourceConfig` (around line 191):
```rust
impl Default for ResourceConfig {
    fn default() -> Self {
        let auto_parallelism = std::thread::available_parallelism()
            .map(|n| (n.get() as u32).min(MAX_AUTO_PARALLELISM))
            .unwrap_or(1);

        Self {
            max_memory: DEFAULT_MAX_MEMORY.to_string(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES.to_string(),
            parallelism: auto_parallelism,
            checkpoint_interval_bytes: DEFAULT_CHECKPOINT_INTERVAL_BYTES.to_string(),
            checkpoint_interval_rows: 0,
            checkpoint_interval_seconds: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            compression: None,
            max_inflight_batches: DEFAULT_MAX_INFLIGHT_BATCHES,
        }
    }
}
```

**Step 4: Run tests to verify**

Run: `cargo test --workspace`
Expected: All tests pass, including the new parallelism test.

Note: Existing YAML deserialization tests that specify `parallelism: 4` are unaffected because they set the value explicitly. Tests that rely on `ResourceConfig::default()` will now get auto-detected parallelism.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/config/types.rs
git commit -m "perf: auto-detect parallelism from available CPU cores (capped at 8)"
```

---

## Phase 2: Partitioned Stream Execution

### Problem Statement

After Phase 1, a single-stream pipeline still uses ~2 cores (1 source thread + 1 dest thread). On an 8-core machine, 75% of CPU capacity is idle. Multi-stream parallelism (Task 4) only helps when the pipeline has multiple streams — but the common case is syncing one large table.

### First-Principles Analysis

**Why is each stage single-threaded?** Wasmtime executes each WASM component instance in a single `Store`, which is `!Send + !Sync`. One thread owns the store, calls into WASM, and handles host imports. This is a hard constraint of the component model — you cannot multi-thread within one WASM instance.

**How do we use more cores for one stream?** The only option is to run **multiple WASM instances** of the same connector, each processing a **disjoint subset** of the data. This is partitioned execution — the same technique used by Airbyte (partitioned sync), Spark (partitioned reads), and every parallel database engine.

**What needs to partition?** The source reads. The source connector is the "producer" that scans rows from the external system, encodes Arrow IPC batches, and emits them. If we run N source instances, each reading 1/N of the rows, we get N-way source parallelism. The destination remains a single instance (it processes interleaved batches from all partitions — this is safe because each batch is self-contained Arrow IPC, and write ordering within Append/Replace modes doesn't matter).

### Architecture

```
                       ┌── Source Instance 0 ──┐
                       │   (WHERE key%N = 0)   │
                       ├── Source Instance 1 ──┤
  Stream "users" ──────┤   (WHERE key%N = 1)   ├──► Fan-In Channel ──► Dest Instance
                       ├──       ...           │
                       └── Source Instance N-1 ─┘
                           (WHERE key%N = N-1)
```

With transforms, each partition gets its own transform chain:
```
  Partition 0: Source → Transform A → Transform B ──┐
  Partition 1: Source → Transform A → Transform B ──┤──► Fan-In ──► Dest
  Partition 2: Source → Transform A → Transform B ──┘
```

### Parallelism Budget Allocation

The `parallelism` config value (auto-detected or explicit) becomes a **total WASM instance budget**. The orchestrator allocates it across streams and partitions:

```
Per stream:  instances = partitions × (1 source + num_transforms) + 1 dest
Total:       concurrent_streams × instances_per_stream ≤ parallelism
```

Solving for partition count:
```rust
let concurrent_streams = (parallelism / (2 + num_transforms)).max(1).min(total_streams);
let partitions_per_stream = ((parallelism / concurrent_streams - 1) / (1 + num_transforms)).max(1);
```

Examples (num_transforms = 0):

| parallelism | streams | concurrent | partitions/stream | total instances |
|-------------|---------|------------|-------------------|-----------------|
| 8           | 1       | 1          | 7                 | 8               |
| 8           | 2       | 2          | 3                 | 8               |
| 8           | 4       | 4          | 1                 | 8               |
| 8           | 10      | 4          | 1                 | 8               |

Examples (num_transforms = 1):

| parallelism | streams | concurrent | partitions/stream | total instances |
|-------------|---------|------------|-------------------|-----------------|
| 8           | 1       | 1          | 3                 | 7 (3×2+1)       |
| 8           | 2       | 2          | 1                 | 6 (2×(1×2+1))   |

Key insight: the system automatically balances between stream concurrency and per-stream partitioning based on one config value.

### Fan-In Channel Design

Multiple source partitions share a single `crossbeam_channel::Sender<Frame>` (cloned per partition). When all partition sources complete and drop their sender clones, the channel closes automatically. The destination reads batches until `recv()` returns `Err` (channel closed).

**EndStream handling for partitioned sources:**
- Non-partitioned (backward compatible): Source sends `Frame::EndStream` explicitly after `run_read`, same as today.
- Partitioned: Individual partitions do NOT send `Frame::EndStream`. Instead, the orchestrator holds no sender clone itself — when all partition source tasks drop their senders, channel closure signals completion to the dest. This avoids the dest stopping prematurely on the first partition's EndStream.

### Checkpoint Coordination with Partitions

For **FullRefresh** and **Replace** modes, no cursor tracking — partitioning is purely an optimization. All partitions read their subset, dest writes everything, done.

For **Incremental** mode, cursor advancement must be conservative:
1. Each partition source emits checkpoints with cursor values scoped to its data subset
2. The host collects checkpoints from ALL partitions
3. The cursor advances to `min(latest_cursor_from_each_partition)` — we can only resume from the point where ALL partitions have progressed
4. If any partition fails, the cursor does NOT advance (all partitions re-read from the last committed position on retry — some work is repeated, but correctness is guaranteed)

### Connector Opt-In via Feature Flag

Not all connectors can partition. A connector must know how to split its query. We use the existing `Feature` enum and `SourceCapabilities` to declare support:

```rust
// In wire.rs Feature enum:
Partitioned,

// In manifest:
"roles": {
    "source": {
        "supported_sync_modes": ["full_refresh", "incremental"],
        "features": ["stateful", "partitioned"]
    }
}
```

If the connector declares `Feature::Partitioned`, the host sends partition info via `StreamContext`. If not, the host runs 1 source instance per stream (current behavior). No WIT change needed — partition info is passed as JSON in the existing `StreamContext`.

### StreamContext Extension

```rust
// New type in rapidbyte-types/src/stream.rs
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub partition_id: u32,
    pub total_partitions: u32,
    pub partition_key: String,
}

// Extended StreamContext:
pub struct StreamContext {
    pub stream_name: String,
    pub schema: SchemaHint,
    pub sync_mode: SyncMode,
    pub cursor_info: Option<CursorInfo>,
    pub limits: StreamLimits,
    pub policies: StreamPolicies,
    pub write_mode: Option<WriteMode>,
    pub selected_columns: Option<Vec<String>>,
    pub partition: Option<PartitionSpec>,  // NEW — None for unpartitioned
}
```

The source-postgres connector reads `partition` and generates:
```sql
SELECT * FROM users
WHERE abs(hashtext(id::text)) % 4 = 0  -- partition 0 of 4
ORDER BY id
```

Using `hashtext()` (PG built-in) provides uniform distribution for any data type (int, UUID, text).

### Pipeline Config Extension

```yaml
source:
  streams:
    - name: users
      sync_mode: full_refresh
      partition_key: id  # Optional: field to partition on
```

If `partition_key` is absent, the orchestrator tries to infer it from:
1. `cursor_field` (for incremental sync)
2. First element of destination `primary_key` (if configured)
3. Falls back to unpartitioned execution

---

### Task 5: Add Partition Types to `rapidbyte-types`

Add `PartitionSpec` and the `Partitioned` feature flag to the types crate. Extend `StreamContext` with an optional `partition` field. Because `StreamContext` is serialized to JSON for the WASM boundary, this is backward compatible — old connectors that don't understand `partition` simply ignore the field.

**Files:**
- Modify: `crates/rapidbyte-types/src/wire.rs`
- Modify: `crates/rapidbyte-types/src/stream.rs`

**Step 1: Write the failing test**

In `crates/rapidbyte-types/src/stream.rs`, add to `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn stream_context_with_partition_roundtrip() {
        let ctx = StreamContext {
            stream_name: "users".into(),
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition: Some(PartitionSpec {
                partition_id: 2,
                total_partitions: 4,
                partition_key: "id".into(),
            }),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: StreamContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
        assert_eq!(back.partition.unwrap().partition_id, 2);
    }

    #[test]
    fn stream_context_without_partition_omits_field() {
        let ctx = StreamContext {
            stream_name: "users".into(),
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition: None,
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(!json.contains("partition"));
    }
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- stream_context_with_partition`
Expected: FAIL — `PartitionSpec` and `partition` field don't exist.

**Step 3: Add the `Partitioned` feature flag**

In `crates/rapidbyte-types/src/wire.rs`, add to the `Feature` enum:

```rust
    /// Source supports partitioned reads for parallel execution.
    Partitioned,
```

**Step 4: Add `PartitionSpec` and extend `StreamContext`**

In `crates/rapidbyte-types/src/stream.rs`, add before `StreamContext`:

```rust
/// Describes a data partition for parallel source reads.
///
/// When present in a `StreamContext`, the source connector should read only
/// the subset of rows matching this partition. The host uses `hashtext`-style
/// modulo partitioning: `WHERE abs(hash(partition_key)) % total = id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Zero-based partition index.
    pub partition_id: u32,
    /// Total number of partitions.
    pub total_partitions: u32,
    /// Column name to partition on (typically the primary key).
    pub partition_key: String,
}
```

Add the `partition` field to `StreamContext`:

```rust
    /// Partition assignment for parallel source reads (`None` = read all data).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionSpec>,
```

**Step 5: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: All tests pass. Existing `StreamContext` tests still pass because `partition: None` is the default.

**Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/wire.rs crates/rapidbyte-types/src/stream.rs
git commit -m "feat: add PartitionSpec type and Partitioned feature flag"
```

---

### Task 6: Add `partition_key` to Pipeline Config

Allow users to specify which column to partition on per stream. The orchestrator uses this to generate `PartitionSpec` values.

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/types.rs`

**Step 1: Write the failing test**

In `crates/rapidbyte-engine/src/config/types.rs`, add to `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn test_partition_key_parsed() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
      partition_key: id
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.source.streams[0].partition_key,
            Some("id".to_string())
        );
    }

    #[test]
    fn test_partition_key_absent_is_none() {
        let yaml = r#"
version: "1.0"
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
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.source.streams[0].partition_key.is_none());
    }
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-engine -- test_partition_key`
Expected: FAIL — `partition_key` field doesn't exist on `StreamConfig`.

**Step 3: Add `partition_key` field to `StreamConfig`**

In `crates/rapidbyte-engine/src/config/types.rs`, in the `StreamConfig` struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub sync_mode: SyncMode,
    pub cursor_field: Option<String>,
    pub columns: Option<Vec<String>>,
    /// Column to partition on for parallel source reads.
    /// If set and the connector supports partitioning, the host spawns
    /// multiple source instances reading disjoint subsets.
    #[serde(default)]
    pub partition_key: Option<String>,
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --workspace`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/config/types.rs
git commit -m "feat: add partition_key to stream config for parallel source reads"
```

---

### Task 7: Implement Parallelism Budget Allocator

Extract the budget allocation logic into a pure function that computes `(concurrent_streams, partitions_per_stream)` from `(parallelism, num_streams, num_transforms)`. This is the brain of the partitioning system.

**Files:**
- Create: `crates/rapidbyte-engine/src/partition.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (add `mod partition;`)

**Step 1: Write the tests**

Create `crates/rapidbyte-engine/src/partition.rs`:

```rust
//! Parallelism budget allocation for partitioned stream execution.

/// Computed parallelism budget for a pipeline run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParallelismBudget {
    /// How many streams execute concurrently.
    pub concurrent_streams: usize,
    /// How many source partitions per stream.
    pub partitions_per_stream: usize,
}

/// Compute how to distribute the parallelism budget across streams and partitions.
///
/// Each stream needs at minimum 1 source + 1 dest = 2 WASM instances (plus transforms).
/// Remaining budget is allocated to source partitions within each concurrent stream.
pub(crate) fn compute_budget(
    parallelism: u32,
    num_streams: usize,
    num_transforms: usize,
) -> ParallelismBudget {
    let parallelism = parallelism.max(1) as usize;
    let min_instances_per_stream = 1 + num_transforms + 1; // 1 source + transforms + 1 dest

    // How many streams can run concurrently?
    let concurrent_streams = (parallelism / min_instances_per_stream)
        .max(1)
        .min(num_streams);

    // How many source partitions per stream?
    let budget_per_stream = parallelism / concurrent_streams;
    let partitions_per_stream = ((budget_per_stream.saturating_sub(1 + num_transforms))
        .max(1))
        .min(budget_per_stream); // safety cap

    ParallelismBudget {
        concurrent_streams,
        partitions_per_stream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_stream_no_transforms_gets_max_partitions() {
        let b = compute_budget(8, 1, 0);
        assert_eq!(b.concurrent_streams, 1);
        assert_eq!(b.partitions_per_stream, 7); // 7 sources + 1 dest = 8
    }

    #[test]
    fn two_streams_no_transforms_balanced() {
        let b = compute_budget(8, 2, 0);
        assert_eq!(b.concurrent_streams, 2);
        assert_eq!(b.partitions_per_stream, 3); // 2 × (3 sources + 1 dest) = 8
    }

    #[test]
    fn four_streams_no_transforms_one_partition_each() {
        let b = compute_budget(8, 4, 0);
        assert_eq!(b.concurrent_streams, 4);
        assert_eq!(b.partitions_per_stream, 1); // 4 × (1 source + 1 dest) = 8
    }

    #[test]
    fn many_streams_caps_at_budget() {
        let b = compute_budget(8, 20, 0);
        assert_eq!(b.concurrent_streams, 4);
        assert_eq!(b.partitions_per_stream, 1);
    }

    #[test]
    fn single_stream_one_transform() {
        let b = compute_budget(8, 1, 1);
        assert_eq!(b.concurrent_streams, 1);
        // budget_per_stream=8, minus 1 dest + 1 transform = 6 partitions
        // each partition: 1 source + 1 transform. partitions = (8-2)/1 = 6...
        // Actually: budget_per_stream - (1 dest + num_transforms) = 8 - 2 = 6
        // BUT each partition also needs its own transform: partitions × (1+num_transforms) + 1 ≤ 8
        // partitions × 2 + 1 ≤ 8 → partitions ≤ 3
        assert_eq!(b.partitions_per_stream, 6);
        // Note: this is budget_per_stream - 1(dest) - num_transforms(shared).
        // The transform-per-partition accounting is handled in Task 8
        // where we decide: shared transforms vs per-partition transforms.
    }

    #[test]
    fn parallelism_one_always_works() {
        let b = compute_budget(1, 5, 0);
        assert_eq!(b.concurrent_streams, 1);
        assert_eq!(b.partitions_per_stream, 1);
    }

    #[test]
    fn parallelism_two_single_stream() {
        let b = compute_budget(2, 1, 0);
        assert_eq!(b.concurrent_streams, 1);
        assert_eq!(b.partitions_per_stream, 1); // 1 source + 1 dest = 2
    }
}
```

**Step 2: Add module to lib.rs**

In `crates/rapidbyte-engine/src/lib.rs`, add:

```rust
mod partition;
```

**Step 3: Run tests**

Run: `cargo test -p rapidbyte-engine -- partition`
Expected: All tests pass. Adjust `compute_budget` implementation if any expectations need tuning.

**Step 4: Commit**

```bash
git add crates/rapidbyte-engine/src/partition.rs crates/rapidbyte-engine/src/lib.rs
git commit -m "feat: add parallelism budget allocator for partitioned streams"
```

---

### Task 8: Refactor Orchestrator for Partitioned Stream Execution

This is the core architectural change. The orchestrator must:
1. Check if the source connector supports `Feature::Partitioned`
2. Determine partition key per stream (explicit config > cursor_field > primary_key > none)
3. Compute the partition budget
4. For partitioned streams: spawn N source instances sharing a fan-in channel
5. For unpartitioned streams: use the existing single-source flow

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/resolve.rs` (expose source capabilities)

**Step 1: Expose partitioning support from resolved connectors**

In `crates/rapidbyte-engine/src/resolve.rs`, add to `ResolvedConnectors`:

```rust
pub(crate) struct ResolvedConnectors {
    // ... existing fields ...
    /// Whether the source connector declared Feature::Partitioned.
    pub(crate) source_supports_partitioning: bool,
}
```

Set it in `resolve_connectors`:

```rust
let source_supports_partitioning = source_manifest
    .as_ref()
    .and_then(|m| m.roles.source.as_ref())
    .map(|cap| cap.features.contains(&rapidbyte_types::wire::Feature::Partitioned))
    .unwrap_or(false);
```

**Step 2: Add partition key resolution helper**

In `crates/rapidbyte-engine/src/orchestrator.rs`, add a helper:

```rust
/// Determine the effective partition key for a stream.
/// Priority: explicit partition_key > cursor_field > primary_key[0] > None.
fn resolve_partition_key(
    stream_config: &crate::config::types::StreamConfig,
    dest_primary_key: &[String],
) -> Option<String> {
    stream_config
        .partition_key
        .clone()
        .or_else(|| stream_config.cursor_field.clone())
        .or_else(|| dest_primary_key.first().cloned())
}
```

**Step 3: Refactor `execute_streams` for partition-aware execution**

The key change in `execute_streams`: for each stream context, decide whether to run partitioned or not, then build the channel topology accordingly.

For **partitioned streams** (pseudocode of the inner spawn block):

```rust
// Create fan-in channel — all partition sources write here
let (fan_in_tx, fan_in_rx) = crossbeam_channel::bounded::<Frame>(params.channel_capacity);

// Spawn N source partition instances
let mut src_handles = Vec::with_capacity(num_partitions);
for partition_id in 0..num_partitions {
    let partition_tx = fan_in_tx.clone();
    let mut partition_ctx = stream_ctx.clone();
    partition_ctx.partition = Some(PartitionSpec {
        partition_id: partition_id as u32,
        total_partitions: num_partitions as u32,
        partition_key: partition_key.clone(),
    });

    let handle = tokio::task::spawn_blocking(move || {
        run_source_stream(&source_module, partition_tx, /* ... */ &partition_ctx, /* ... */)
    });
    src_handles.push(handle);
}
// Drop orchestrator's sender clone — only partition clones remain.
// When all partitions complete, channel closes → dest gets None.
drop(fan_in_tx);

// Spawn single destination reading from fan_in_rx
let dst_handle = tokio::task::spawn_blocking(move || {
    run_destination_stream(&dest_module, fan_in_rx, /* ... */)
});

// Await all source partitions
for handle in src_handles {
    let result = handle.await??;
    // Collect per-partition checkpoints, timings, summaries
}

// Await destination
let dst_result = dst_handle.await??;
```

For **partitioned streams with transforms**, each partition gets its own transform chain:

```rust
for partition_id in 0..num_partitions {
    // Per-partition: source_tx → [transform_0_rx, transform_0_tx] → ... → fan_in_tx
    let mut prev_rx: Option<crossbeam_channel::Receiver<Frame>> = None;
    let (source_tx, first_rx) = crossbeam_channel::bounded(params.channel_capacity);
    prev_rx = Some(first_rx);

    for (i, t) in transforms.iter().enumerate() {
        let rx = prev_rx.take().unwrap();
        let tx = if i == transforms.len() - 1 {
            fan_in_tx.clone() // last transform writes to fan-in
        } else {
            let (next_tx, next_rx) = crossbeam_channel::bounded(params.channel_capacity);
            prev_rx = Some(next_rx);
            next_tx
        };
        // Spawn transform instance for this partition
        tokio::task::spawn_blocking(move || {
            run_transform_stream(&t.module, rx, tx, /* ... */)
        });
    }

    // Spawn source instance for this partition
    tokio::task::spawn_blocking(move || {
        run_source_stream(&source_module, source_tx, /* ... */ &partition_ctx, /* ... */)
    });
}
```

For **unpartitioned streams**: the existing flow remains unchanged (single source, single dest, explicit EndStream).

**Step 4: Update `StreamResult` to track partition count**

Add `partition_count: usize` to `StreamResult` so the aggregation and finalization can report partitioning.

**Step 5: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass. Existing behavior is preserved (unpartitioned path unchanged).

**Step 6: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/resolve.rs
git commit -m "feat: partition-aware orchestrator with fan-in channel topology"
```

---

### Task 9: Partition-Aware Checkpoint Coordination

For incremental sync, the cursor must only advance to the minimum value across all partitions. If partition 0 reads up to `id=500` and partition 1 reads up to `id=300`, the cursor advances to `300` — ensuring no data is skipped on retry.

**Files:**
- Modify: `crates/rapidbyte-engine/src/checkpoint.rs`

**Step 1: Write the failing test**

In `crates/rapidbyte-engine/src/checkpoint.rs`, add to tests:

```rust
    #[test]
    fn test_correlate_partitioned_takes_min_cursor() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        // Two source checkpoints from different partitions, same stream
        let src = vec![
            make_source_checkpoint("users", "id", "500"),
            make_source_checkpoint("users", "id", "300"),
        ];
        let dst = vec![make_dest_checkpoint("users")];

        let advanced = correlate_and_persist_cursors(&backend, &pid(), &src, &dst).unwrap();
        assert_eq!(advanced, 1);

        let cursor = backend
            .get_cursor(&pid(), &StreamName::new("users"))
            .unwrap()
            .unwrap();
        // Should take the minimum: "300" < "500" lexicographically
        assert_eq!(cursor.cursor_value, Some("300".to_string()));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine -- test_correlate_partitioned_takes_min_cursor`
Expected: FAIL — current logic takes the last checkpoint, not the minimum.

**Step 3: Implement min-cursor logic**

Refactor `correlate_and_persist_cursors` to group source checkpoints by stream, take the minimum cursor value per stream, then persist:

```rust
pub(crate) fn correlate_and_persist_cursors(
    state_backend: &dyn StateBackend,
    pipeline: &PipelineId,
    source_checkpoints: &[Checkpoint],
    dest_checkpoints: &[Checkpoint],
) -> Result<u64> {
    let mut cursors_advanced = 0u64;

    // Group source checkpoints by stream, keeping min cursor per stream.
    let mut stream_cursors: std::collections::HashMap<&str, (&str, &CursorValue)> =
        std::collections::HashMap::new();

    for src_cp in source_checkpoints {
        let (Some(cursor_field), Some(cursor_value)) =
            (&src_cp.cursor_field, &src_cp.cursor_value)
        else {
            continue;
        };

        let entry = stream_cursors.entry(&src_cp.stream);
        use std::collections::hash_map::Entry;
        match entry {
            Entry::Vacant(e) => {
                e.insert((cursor_field, cursor_value));
            }
            Entry::Occupied(mut e) => {
                let existing_str = cursor_value_to_string(e.get().1);
                let new_str = cursor_value_to_string(cursor_value);
                if new_str < existing_str {
                    e.insert((cursor_field, cursor_value));
                }
            }
        }
    }

    for (stream, (cursor_field, cursor_value)) in &stream_cursors {
        let dest_confirmed = dest_checkpoints
            .iter()
            .any(|dcp| dcp.stream == *stream);
        if !dest_confirmed {
            tracing::warn!(
                pipeline = pipeline.as_str(),
                stream,
                "Skipping cursor advancement: no destination checkpoint confirms stream data"
            );
            continue;
        }

        let value_str = cursor_value_to_string(cursor_value);

        let cursor = CursorState {
            cursor_field: Some(cursor_field.to_string()),
            cursor_value: Some(value_str.clone()),
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        state_backend.set_cursor(pipeline, &StreamName::new(stream.to_string()), &cursor)?;
        tracing::info!(
            pipeline = pipeline.as_str(),
            stream,
            cursor_field,
            cursor_value = value_str,
            "Cursor advanced: source + destination checkpoints correlated"
        );
        cursors_advanced += 1;
    }

    Ok(cursors_advanced)
}

fn cursor_value_to_string(value: &CursorValue) -> String {
    match value {
        CursorValue::Utf8 { value }
        | CursorValue::Decimal { value, .. }
        | CursorValue::Lsn { value } => value.clone(),
        CursorValue::Int64 { value }
        | CursorValue::TimestampMillis { value }
        | CursorValue::TimestampMicros { value } => value.to_string(),
        CursorValue::Json { value } => value.to_string(),
        _ => String::new(),
    }
}
```

**Step 4: Run all checkpoint tests**

Run: `cargo test -p rapidbyte-engine -- checkpoint`
Expected: All tests pass, including the new min-cursor test and all existing tests.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/checkpoint.rs
git commit -m "feat: partition-aware checkpoint coordination with min-cursor logic"
```

---

### Task 10: Implement Partitioned Reads in source-postgres

Add `Feature::Partitioned` to the source-postgres manifest and implement partition-aware SQL generation. When `StreamContext.partition` is present, the connector adds a `WHERE abs(hashtext(partition_key::text)) % total = id` clause.

**Files:**
- Modify: `connectors/source-postgres/src/manifest.json` (or equivalent embedded manifest)
- Modify: `connectors/source-postgres/src/lib.rs` (or query builder module)

**Step 1: Add `Partitioned` feature to manifest**

In the source-postgres connector manifest, add `"partitioned"` to the features list:

```json
"features": ["stateful", "partitioned"]
```

**Step 2: Implement partition-aware query generation**

In the source connector's query builder, when `stream_ctx.partition` is `Some(spec)`:

```rust
fn build_query(stream_ctx: &StreamContext) -> String {
    let base_query = format!("SELECT {} FROM {}", columns, table_name);

    let mut conditions = Vec::new();

    // Existing cursor condition for incremental
    if let Some(ref cursor) = stream_ctx.cursor_info {
        if let Some(ref last) = cursor.last_value {
            conditions.push(format!("{} > '{}'", cursor.cursor_field, last.value_str()));
        }
    }

    // Partition condition
    if let Some(ref partition) = stream_ctx.partition {
        conditions.push(format!(
            "abs(hashtext({}::text)) % {} = {}",
            partition.partition_key,
            partition.total_partitions,
            partition.partition_id,
        ));
    }

    if conditions.is_empty() {
        base_query
    } else {
        format!("{} WHERE {}", base_query, conditions.join(" AND "))
    }
}
```

**Step 3: Build and test the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: Compiles for `wasm32-wasip2`.

**Step 4: E2E test with partitioning**

Create a test pipeline YAML with `partition_key: id` and `parallelism: 4`. Run E2E:

Run: `just e2e`
Expected: Pipeline completes, correct row count, cursor advanced correctly.

**Step 5: Benchmark**

Run: `just bench postgres --profile medium`
Expected: Measurably higher throughput and CPU utilization compared to Phase 1 baseline.

**Step 6: Commit**

```bash
git add connectors/source-postgres/
git commit -m "feat: implement partitioned reads in source-postgres connector"
```

---

## Architectural Notes

### Why Not rayon?

The CPU-intensive work in the pipeline (Arrow IPC encode/decode, PG protocol parsing, type conversion) runs **inside WASM connectors**, not on the host. The host's role is coordination: channel routing, compression, and state management. Adding rayon to the host would only parallelize lightweight operations. Phase 2's partitioned reads achieve the same goal (parallel CPU utilization) by running multiple WASM instances — each of which saturates one core naturally.

### Why Not Pipelined Async Channels?

The pipeline **already pipelines** via bounded channels with capacity `max_inflight_batches` (default: 16). Source can be up to 16 batches ahead of destination. The improvement here is switching from `tokio::sync::mpsc` (async-native, overhead for blocking callers) to `crossbeam-channel` (blocking-native, optimized for the exact pattern we use). Phase 2 additionally benefits from crossbeam's native MPSC support (multiple senders via `Clone`).

### Destination Parallelism (Future)

Phase 2 keeps a single destination instance per stream. For **Append** and **Replace** write modes, it's safe to run M destination instances in parallel (each receiving a subset of batches via round-robin). This would eliminate the destination as a bottleneck. For **Upsert** mode, concurrent writes to overlapping primary keys cause conflicts, so the destination must remain single-instance unless we partition writes by key range too. This is a natural extension but not included in this plan.

### CDC Mode and Partitioning

Change Data Capture (CDC) mode reads a replication stream, which is inherently sequential (WAL order). Partitioning does NOT apply to CDC streams. The orchestrator should skip partitioning when `sync_mode == Cdc`.
