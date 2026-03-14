# Eliminate Metrics Fallback Path Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the legacy `MetricsRuntime::Fallback` code path so all components (including benchmarks) use the OTel-based metrics pipeline exclusively.

**Architecture:** Today, `MetricsRuntime` has two variants: `Borrowed` (OTel-backed, used by CLI and agent) and `Fallback` (raw snapshot accumulator, used when no OTel provider is available). The fallback exists because benchmark isolation and the `run_pipeline()` convenience wrapper don't pass an OTel provider. The fix: make every caller provide an OTel provider. The fallback variant, `metric_or_perf_fallback()`, host-side `ReadPerf`/`WritePerf` usage, and `with_raw_snapshot()` can then be removed. Note: `ReadPerf`/`WritePerf` types stay in `rapidbyte-types` because plugins use them to organize timing data before emitting OTel histograms — only the host-side fallback consumption is removed.

**Tech Stack:** Rust, OpenTelemetry SDK (`opentelemetry_sdk`), `rapidbyte-metrics` crate

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `benchmarks/Cargo.toml` | Modify | Add `rapidbyte-metrics` dependency |
| `benchmarks/src/isolation.rs` | Modify | Initialize OTel provider, pass to stream runners |
| `crates/rapidbyte-agent/src/executor.rs` | Modify | Remove Option wrappers, apply same throwaway-provider pattern as `run_pipeline()` |
| `crates/rapidbyte-engine/src/orchestrator.rs` | Modify | Remove `Fallback` variant, `metric_or_perf_fallback()`, fallback-related code and tests |
| `crates/rapidbyte-engine/src/runner.rs` | Modify | Remove `fallback_metrics_snapshot` parameter from all three stream runners |
| `crates/rapidbyte-runtime/src/host_state.rs` | Modify | Remove `with_raw_snapshot()`, `update_raw_snapshot()`, `raw_snapshot` field |
| `crates/rapidbyte-types/src/metric.rs` | Modify | Remove `ReadSummary.perf` and `WriteSummary.perf` Option fields (types stay for plugin use) |

## Dependency Graph

```
Task 1: Wire OTel into benchmark isolation (additive, no removals)
Task 2: Remove fallback_metrics_snapshot from runner.rs (signature change)
Task 3: Remove MetricsRuntime::Fallback and metric_or_perf_fallback (orchestrator cleanup)
  └── includes: agent executor.rs, run_pipeline() convenience wrapper
Task 4: Remove with_raw_snapshot from HostTimings (runtime cleanup)
Task 5: Remove host-side ReadPerf/WritePerf usage (types + orchestrator cleanup)
```

Tasks 2-5 are sequential (each depends on the prior). Task 1 can be done first independently and makes Tasks 2-5 safe.

---

### Task 1: Wire OTel into benchmark isolation

**Files:**
- Modify: `benchmarks/Cargo.toml`
- Modify: `benchmarks/src/isolation.rs`

This task makes benchmarks use the OTel path, eliminating the last caller that passes `None` for the fallback snapshot.

- [ ] **Step 1: Add rapidbyte-metrics dependency to benchmarks**

In `benchmarks/Cargo.toml`, add to `[dependencies]`:
```toml
rapidbyte-metrics = { path = "../crates/rapidbyte-metrics" }
```

Also add `opentelemetry_sdk` if not already present (check workspace dependencies).

- [ ] **Step 2: Create a helper in isolation.rs for benchmark OTel setup**

At the top of `benchmarks/src/isolation.rs`, add a helper that creates a lightweight OTel provider for the benchmark run:

```rust
use rapidbyte_metrics::snapshot::SnapshotReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Create a lightweight OTel meter provider for isolated benchmark runs.
fn benchmark_metrics() -> (SdkMeterProvider, SnapshotReader) {
    let reader = SnapshotReader::new();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.build_reader())
        .build();
    (provider, reader)
}
```

- [ ] **Step 3: Update `execute_source_benchmark` to use OTel metrics**

Before the `run_source_stream` call (~line 72), create the metrics runtime:

```rust
let (bench_provider, bench_reader) = benchmark_metrics();
opentelemetry::global::set_meter_provider(bench_provider.clone());
```

After `run_source_stream` returns, extract the OTel snapshot for connector_metrics:

```rust
let snap = bench_reader.flush_and_snapshot_for_run(
    &bench_provider,
    "benchmark-source",
    Some("benchmark-source-run"),
);
```

Update the `connector_metrics` JSON to use `snap` values:
```rust
connector_metrics: serde_json::json!({
    "source": {
        "duration_secs": result.duration_secs,
        "connect_secs": snap.source_connect_secs,
        "query_secs": snap.source_query_secs,
        "fetch_secs": snap.source_fetch_secs,
        "arrow_encode_secs": snap.source_encode_secs,
    },
    "checkpoint_count": result.checkpoints.len(),
}),
```

- [ ] **Step 4: Update `execute_destination_benchmark` similarly**

Same pattern: create `benchmark_metrics()`, set global provider, snapshot after run, populate `connector_metrics` with dest timing values from the snapshot.

- [ ] **Step 5: Update `execute_transform_benchmark` similarly**

Same pattern for transforms.

- [ ] **Step 6: Build and test**

```bash
cargo check -p rapidbyte-benchmarks && cargo test -p rapidbyte-benchmarks
```
Expected: compiles and tests pass.

- [ ] **Step 7: Commit**

```bash
git add benchmarks/
git commit -m "feat(benchmarks): wire OTel metrics into isolation benchmarks"
```

---

### Task 2: Remove `fallback_metrics_snapshot` from stream runners

**Files:**
- Modify: `crates/rapidbyte-engine/src/runner.rs:107-125, 282-300, 468-487`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` (call sites + `StreamParams` struct)
- Modify: `benchmarks/src/isolation.rs` (update call sites)

- [ ] **Step 1: Remove `fallback_metrics_snapshot` parameter from `run_source_stream`**

In `runner.rs`, remove the `fallback_metrics_snapshot` parameter (lines 113-115) and the conditional `with_raw_snapshot` call (lines 132-134). The `HostTimings` construction becomes:
```rust
let mut host_timings = HostTimings::new(pipeline_name, &stream_ctx.stream_name, shard_index)
    .with_run_label(metric_run_label);
```

- [ ] **Step 2: Remove `fallback_metrics_snapshot` from `run_destination_stream` and `run_transform_stream`**

Same pattern for the other two stream runner functions.

- [ ] **Step 3: Update all call sites in orchestrator.rs**

In `execute_streams()` (~lines 1165, 1274, 1304, 1390), remove the `fallback_metrics_snapshot` argument from each `run_*_stream()` call. Also remove the `fallback_metrics_snapshot` field from `StreamParams` struct (line 106-107).

- [ ] **Step 4: Update call sites in isolation.rs**

Remove the `None` argument that was passed for `fallback_metrics_snapshot` in each `execute_*_benchmark` function.

- [ ] **Step 5: Build and test**

```bash
cargo test --workspace
```
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git commit -am "refactor(runner): remove fallback_metrics_snapshot parameter from stream runners"
```

---

### Task 3: Remove `MetricsRuntime::Fallback` and `metric_or_perf_fallback`

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-agent/src/executor.rs`

- [ ] **Step 1: Remove the `Fallback` variant from `MetricsRuntime` enum**

Change the enum to a simple struct since there's only one variant left:
```rust
struct MetricsRuntime<'a> {
    snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
}
```

Update `snapshot_for_run` to call directly without matching. Remove `fallback_metrics_snapshot()` method.

- [ ] **Step 2: Update `prepare_metrics_runtime` to require both params**

Change signature from `Option<&SnapshotReader>, Option<&SdkMeterProvider>` to requiring both. Remove the `Fallback` construction path.

- [ ] **Step 3: Update `run_pipeline` convenience wrapper**

Have `run_pipeline()` create a throwaway provider internally so callers (E2E tests) don't need to manage OTel setup:
```rust
pub async fn run_pipeline(...) -> Result<PipelineOutcome, PipelineError> {
    let reader = rapidbyte_metrics::snapshot::SnapshotReader::new();
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader.build_reader())
        .build();
    run_pipeline_with_metrics(config, options, progress_tx, cancel_token, &reader, &provider).await
}
```

- [ ] **Step 4: Update `run_pipeline_with_metrics` signature**

Change parameters from `Option<&SnapshotReader>` and `Option<&SdkMeterProvider>` to required references `&SnapshotReader` and `&SdkMeterProvider`.

- [ ] **Step 5: Update agent executor**

In `crates/rapidbyte-agent/src/executor.rs`:
- Remove the local `MetricsRuntime` struct (lines 18-22) — no longer needed since the orchestrator's version is now a simple struct
- Update `execute_task_with_metrics` to take required `&SnapshotReader` and `&SdkMeterProvider` (not Option)
- Update `execute_task()` convenience wrapper to create a throwaway provider (same pattern as `run_pipeline()`)
- Update the closure at lines 117-126 to pass the references directly

- [ ] **Step 6: Remove `metric_or_perf_fallback()` function**

Delete the function (line 963-969). Update `build_source_timing()` and `build_dest_timing()` to use OTel snapshot values directly:
```rust
// Before:
connect_secs: metric_or_perf_fallback(snap.source_connect_secs, fallback_perf.map(|p| p.connect_secs)),
// After:
connect_secs: snap.source_connect_secs,
```

- [ ] **Step 7: Remove `merge_read_perf_maxima` and `merge_write_perf_maxima`**

These functions merge `ReadPerf`/`WritePerf` across streams for the fallback path. They become dead code.

- [ ] **Step 8: Update `build_source_timing` and `build_dest_timing` signatures**

Remove the `fallback_perf: Option<&ReadPerf>` / perf access patterns. The functions should take only the `PipelineMetricsSnapshot`.

- [ ] **Step 9: Update `compute_wasm_overhead_secs` signature**

Remove fallback perf parameters, use snapshot values directly.

- [ ] **Step 10: Update tests in orchestrator.rs**

Remove or rewrite the following tests that specifically tested fallback behavior:
- `fallback_metrics_runtime_captures_host_timings_without_overwriting_global_provider` (line 2864)
- `source_timing_falls_back_to_summary_perf_when_snapshot_is_missing` (line ~3097)
- `dest_timing_falls_back_to_summary_perf_when_snapshot_is_missing` (line ~3115)
- `wasm_overhead_uses_destination_perf_fallback_when_snapshot_is_missing` (line ~3134)
- `dry_run_result_uses_summary_perf_when_snapshot_is_missing` (line ~3153)

Update remaining `finalize_run_state_tests` that import `ReadPerf`/`WritePerf` to remove those imports and use OTel snapshot values instead.

- [ ] **Step 11: Build and test**

```bash
cargo test --workspace
```

- [ ] **Step 12: Commit**

```bash
git commit -am "refactor(orchestrator,agent): remove MetricsRuntime::Fallback and metric_or_perf_fallback"
```

---

### Task 4: Remove `with_raw_snapshot` from `HostTimings`

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`

- [ ] **Step 1: Remove `raw_snapshot` field, `with_raw_snapshot()`, and `update_raw_snapshot()`**

In `HostTimings`:
- Remove the `raw_snapshot: Option<Arc<Mutex<PipelineMetricsSnapshot>>>` field
- Remove `pub fn with_raw_snapshot(self, ...)` method (lines 88-96)
- Remove `fn update_raw_snapshot(&self, ...)` method (lines 98-107)
- Remove all calls to `self.update_raw_snapshot(...)` in `record_emit_batch`, `record_next_batch`, `record_compress`, `record_decompress`

These methods duplicated every OTel recording into the raw snapshot for the fallback path. With the fallback removed, they're dead code.

- [ ] **Step 2: Build and test**

```bash
cargo test --workspace
```

- [ ] **Step 3: Commit**

```bash
git commit -am "refactor(runtime): remove HostTimings raw_snapshot fallback"
```

---

### Task 5: Remove host-side `ReadPerf`/`WritePerf` consumption

**Files:**
- Modify: `crates/rapidbyte-types/src/metric.rs` — remove `ReadSummary.perf` and `WriteSummary.perf` Option fields
- Modify: `crates/rapidbyte-engine/src/runner.rs` — remove `perf: None` construction
- Verify: `crates/rapidbyte-types/src/metric.rs` serde test `write_summary_roundtrip` — update or remove

**Important:** Do NOT remove the `ReadPerf` and `WritePerf` struct definitions. Plugins use these types to organize timing data before calling `ctx.histogram()`. Only remove the host-side `ReadSummary.perf` / `WriteSummary.perf` fields that carried the (always-None) perf data across the WIT boundary.

- [ ] **Step 1: Remove `perf` fields from `ReadSummary` and `WriteSummary`**

In `crates/rapidbyte-types/src/metric.rs`, remove:
```rust
pub perf: Option<ReadPerf>,  // from ReadSummary
pub perf: Option<WritePerf>, // from WriteSummary
```

- [ ] **Step 2: Update construction sites in runner.rs**

Remove `perf: None` from `ReadSummary` and `WriteSummary` construction (lines ~217, ~401).

- [ ] **Step 3: Update serde test**

In `metric.rs`, update the `write_summary_roundtrip` test (~line 103) to remove the `perf` field from the test fixture.

- [ ] **Step 4: Remove re-exports if applicable**

Check `crates/rapidbyte-sdk/src/prelude.rs` — if `ReadPerf`/`WritePerf` are re-exported there, keep them (plugins need them). Only remove re-exports of `ReadSummary.perf` / `WriteSummary.perf` if they exist separately.

- [ ] **Step 5: Build and test**

```bash
cargo test --workspace
```

- [ ] **Step 6: Commit**

```bash
git commit -am "refactor(types): remove host-side ReadSummary.perf and WriteSummary.perf fields"
```

---

## Verification

After all tasks are complete:

1. `cargo test --workspace` — all tests pass
2. `cargo clippy --workspace --all-targets -- -D warnings` — no warnings
3. Verify removal:
```bash
grep -r "metric_or_perf_fallback\|with_raw_snapshot\|Fallback.*raw_snapshot\|fallback_metrics_snapshot\|update_raw_snapshot" crates/ benchmarks/
```
Should return no results.
4. Verify `ReadPerf`/`WritePerf` remain only in plugins and types:
```bash
grep -r "ReadPerf\|WritePerf" crates/ plugins/ --include="*.rs" -l
```
Should only show: `crates/rapidbyte-types/src/metric.rs`, `crates/rapidbyte-sdk/src/prelude.rs`, and `plugins/*/src/metrics.rs`, `plugins/*/src/reader.rs`, `plugins/*/src/writer.rs`, `plugins/*/src/cdc/mod.rs`.
5. `just bench postgres --profile small` — benchmarks produce valid connector_metrics with OTel-sourced timing values
