# Metrics Optimization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize the OTel metrics hot path by caching instruments, reducing per-emission allocations, consolidating duplicated code, and cleaning up dead code.

**Architecture:** The `rapidbyte-metrics` crate's instrument accessors currently resolve from the global meter provider on every call (2 Mutex + 1 RwLock per access). Since the provider is set once at startup, instruments can be cached in `OnceLock` statics. Label parsing allocates on every plugin metric emission — built-in metrics can use pre-built label sets. The 8-arm histogram dispatch and 3-place name aliasing are consolidated into a single lookup table. Dead instruments and duplicated test helpers are cleaned up.

**Tech Stack:** Rust, OpenTelemetry SDK, `std::sync::OnceLock`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/rapidbyte-metrics/src/instruments.rs` | Modify | Cache instruments in OnceLock statics, remove dead accessors |
| `crates/rapidbyte-metrics/src/labels.rs` | Modify | Add `scope_labels()` builder for pre-allocated label sets |
| `crates/rapidbyte-metrics/src/plugin_names.rs` | Create | Single source of truth for builtin plugin duration name↔instrument mapping |
| `crates/rapidbyte-metrics/src/lib.rs` | Modify | Add `pub mod plugin_names`, update module doc table |
| `crates/rapidbyte-metrics/src/snapshot.rs` | Modify | Use `plugin_names` instead of local name functions |
| `crates/rapidbyte-metrics/src/test_support.rs` | Create | Shared `#[cfg(test)]` test provider helpers |
| `crates/rapidbyte-runtime/src/host_state.rs` | Modify | Use lookup table for histogram dispatch, pre-build scope labels |
| `crates/rapidbyte-sdk/src/context.rs` | Modify | Align reserved label set with host |
| `crates/rapidbyte-controller/src/server.rs` | Modify | Use shared test provider |

## Dependency Graph

```
Task 1: Cache instruments in OnceLock statics (instruments.rs)
Task 2: Shared test provider helpers (test_support.rs)
Task 3: Plugin duration name registry (plugin_names.rs)
Task 4: Consolidate histogram dispatch (host_state.rs, snapshot.rs)
Task 5: Pre-allocated scope labels (labels.rs, host_state.rs)
Task 6: Align reserved label sets (context.rs)
Task 7: Remove dead instrument accessors (instruments.rs)
Task 8: DRY RecordingHostImports (host_ffi.rs, validate.rs)
```

Tasks 1-2 are independent. Task 3 must precede Task 4. Tasks 5-8 are independent of each other and of 1-4.

---

### Task 1: Cache instruments in OnceLock statics

**Files:**
- Modify: `crates/rapidbyte-metrics/src/instruments.rs`

This is the highest-impact performance fix. Each accessor currently calls `global::meter("rapidbyte")` + `.build()` per invocation (2 Mutex + 1 RwLock). Since the global provider is set once at startup, we cache instruments in `OnceLock`.

- [ ] **Step 1: Change macro to use OnceLock caching**

Replace the 4 `define_*` macros with cached versions. Example for `define_counter_u64`:

```rust
macro_rules! define_counter_u64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> Counter<u64> {
            static INSTANCE: OnceLock<Counter<u64>> = OnceLock::new();
            INSTANCE
                .get_or_init(|| meter().u64_counter($metric_name).build())
                .clone()
        }
    };
}
```

Do the same for `define_histogram_f64`, `define_gauge_f64`, `define_updown_i64`. Add `use std::sync::OnceLock;` to the file.

**Important:** The `OnceLock` captures the instrument from the provider that's active at first-call time. Since `init()` sets the global provider before any instrument is accessed, this is safe. The comment on line 3 ("resolved from the current global meter provider on each call") should be updated to reflect the caching.

- [ ] **Step 2: Update module doc comment in instruments.rs**

Change line 3 from:
```rust
//! Instruments are resolved from the current global meter provider on each call.
//! This keeps late-installed fallback providers observable during pipeline runs.
```
to:
```rust
//! Instruments are resolved from the global meter provider on first access and
//! cached in `OnceLock` statics. The provider must be installed (via `init()`)
//! before any instrument is accessed.
```

- [ ] **Step 3: Update the existing test**

The test `pipeline_instruments_follow_the_current_global_provider` (line 155) tests that switching providers changes instrument behavior. With `OnceLock` caching, instruments are locked to the first provider. This test should be updated: verify that instruments resolve to the provider active at first call, and that subsequent provider changes don't affect cached instruments (this is the desired behavior).

- [ ] **Step 4: Build and test**

```bash
cargo test -p rapidbyte-metrics && cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 5: Commit**

```bash
git commit -am "perf(metrics): cache OTel instruments in OnceLock statics"
```

---

### Task 2: Shared test provider helpers

**Files:**
- Create: `crates/rapidbyte-metrics/src/test_support.rs`
- Modify: `crates/rapidbyte-metrics/src/lib.rs`
- Modify: `crates/rapidbyte-metrics/src/instruments.rs` (test module)
- Modify: `crates/rapidbyte-metrics/src/snapshot.rs` (test module)
- Modify: `crates/rapidbyte-runtime/src/host_state.rs` (test module)
- Modify: `crates/rapidbyte-controller/src/server.rs` (test module)

- [ ] **Step 1: Create test_support.rs**

```rust
//! Shared test utilities for OTel metric testing.

use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
};

use crate::snapshot::SnapshotReader;

/// Create a test meter provider wired to a [`SnapshotReader`].
#[must_use]
pub fn snapshot_test_provider() -> (SdkMeterProvider, SnapshotReader) {
    let reader = SnapshotReader::new();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.build_reader())
        .build();
    (provider, reader)
}

/// Create a test meter provider wired to an in-memory exporter for raw metric inspection.
#[must_use]
pub fn exporter_test_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporterBuilder::new().build();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    (provider, exporter)
}
```

- [ ] **Step 2: Export from lib.rs behind cfg(test) OR unconditionally (downstream test crates need it)**

Since downstream crates (`rapidbyte-runtime`, `rapidbyte-controller`) need these in their `#[cfg(test)]` modules, export unconditionally but document as test-only:

In `lib.rs`, add:
```rust
pub mod test_support;
```

And update the module doc table.

- [ ] **Step 3: Replace local test providers in 4 files**

- `instruments.rs`: Replace local `test_provider()` with `crate::test_support::snapshot_test_provider()`
- `snapshot.rs`: Replace local `test_provider()` with `crate::test_support::snapshot_test_provider()`
- `host_state.rs`: Replace local `metric_test_provider()` with `rapidbyte_metrics::test_support::exporter_test_provider()`
- `server.rs`: Replace local `metric_test_provider()` with `rapidbyte_metrics::test_support::exporter_test_provider()`

- [ ] **Step 4: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 5: Commit**

```bash
git commit -am "refactor(metrics): extract shared test provider helpers"
```

---

### Task 3: Plugin duration name registry

**Files:**
- Create: `crates/rapidbyte-metrics/src/plugin_names.rs`
- Modify: `crates/rapidbyte-metrics/src/lib.rs`

This creates a single source of truth for the builtin plugin duration metric name ↔ OTel instrument mapping, currently duplicated across `host_state.rs` (8 match arms), `snapshot.rs` (`is_builtin_plugin_duration_name`, `builtin_plugin_duration_snapshot_name`).

- [ ] **Step 1: Create plugin_names.rs**

```rust
//! Single source of truth for built-in plugin duration metric names.
//!
//! Maps plugin-side histogram names (e.g. `"source_connect_secs"`) to their
//! OTel instrument names (e.g. `"plugin.source_connect_duration"`) and
//! snapshot field names.

use opentelemetry::metrics::Histogram;

/// A built-in plugin duration metric entry.
pub struct BuiltinDuration {
    /// Plugin-side name (e.g. `"source_connect_secs"`).
    pub plugin_name: &'static str,
    /// OTel instrument name (e.g. `"plugin.source_connect_duration"`).
    pub instrument_name: &'static str,
    /// Canonical snapshot field name (e.g. `"source_connect_secs"`).
    /// Usually same as `plugin_name`, except `"dest_arrow_decode_secs"` → `"dest_decode_secs"`.
    pub snapshot_name: &'static str,
}

/// All built-in plugin duration metrics. Used by:
/// - `host_state.rs` to dispatch histogram recordings
/// - `snapshot.rs` to map OTel instrument names back to snapshot fields
pub const BUILTIN_DURATIONS: &[BuiltinDuration] = &[
    BuiltinDuration { plugin_name: "source_connect_secs",      instrument_name: "plugin.source_connect_duration",  snapshot_name: "source_connect_secs" },
    BuiltinDuration { plugin_name: "source_query_secs",        instrument_name: "plugin.source_query_duration",    snapshot_name: "source_query_secs" },
    BuiltinDuration { plugin_name: "source_fetch_secs",        instrument_name: "plugin.source_fetch_duration",    snapshot_name: "source_fetch_secs" },
    BuiltinDuration { plugin_name: "source_arrow_encode_secs", instrument_name: "plugin.source_encode_duration",   snapshot_name: "source_arrow_encode_secs" },
    BuiltinDuration { plugin_name: "dest_connect_secs",        instrument_name: "plugin.dest_connect_duration",    snapshot_name: "dest_connect_secs" },
    BuiltinDuration { plugin_name: "dest_flush_secs",          instrument_name: "plugin.dest_flush_duration",      snapshot_name: "dest_flush_secs" },
    BuiltinDuration { plugin_name: "dest_commit_secs",         instrument_name: "plugin.dest_commit_duration",     snapshot_name: "dest_commit_secs" },
    BuiltinDuration { plugin_name: "dest_arrow_decode_secs",   instrument_name: "plugin.dest_decode_duration",     snapshot_name: "dest_arrow_decode_secs" },
];

/// Look up a built-in duration by plugin-side name.
#[must_use]
pub fn find_by_plugin_name(name: &str) -> Option<&'static BuiltinDuration> {
    BUILTIN_DURATIONS.iter().find(|d| d.plugin_name == name)
}

/// Look up a built-in duration by OTel instrument name.
#[must_use]
pub fn find_by_instrument_name(name: &str) -> Option<&'static BuiltinDuration> {
    BUILTIN_DURATIONS.iter().find(|d| d.instrument_name == name)
}

/// Check if a raw name (plugin-side or snapshot-side) is a built-in plugin duration.
#[must_use]
pub fn is_builtin_name(name: &str) -> bool {
    BUILTIN_DURATIONS.iter().any(|d| d.plugin_name == name || d.snapshot_name == name)
}
```

- [ ] **Step 2: Export from lib.rs**

Add `pub mod plugin_names;` and update the module doc table.

- [ ] **Step 3: Build and test**

```bash
cargo test -p rapidbyte-metrics
```

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(metrics): add plugin duration name registry as single source of truth"
```

---

### Task 4: Consolidate histogram dispatch and snapshot name maps

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`
- Modify: `crates/rapidbyte-metrics/src/snapshot.rs`

- [ ] **Step 1: Replace 8 match arms in histogram_record_impl with registry lookup**

In `host_state.rs`, replace the 8-arm match (lines 816-846) + default with:

```rust
if let Some(builtin) = rapidbyte_metrics::plugin_names::find_by_plugin_name(name.as_str()) {
    // Resolve the instrument by OTel name via the plugin module
    let instrument = opentelemetry::global::meter("rapidbyte")
        .f64_histogram(builtin.instrument_name)
        .build();
    instrument.record(value, &labels);
} else {
    match rapidbyte_metrics::instruments::plugin::custom_histogram(&name) {
        Ok(histogram) => histogram.record(value, &labels),
        Err(err) => {
            tracing::debug!(metric = %name, "custom histogram skipped: {err}");
        }
    }
}
```

Note: With Task 1's OnceLock caching, the `meter().build()` call here would not benefit from caching (different instrument names each time). But these are plugin-side builtin instruments which ARE already defined in `instruments::plugin` module. Better approach — look up from the existing accessors via a helper:

```rust
fn builtin_plugin_histogram(instrument_name: &str) -> Option<opentelemetry::metrics::Histogram<f64>> {
    // This reuses the cached OnceLock instruments from Task 1
    match instrument_name {
        "plugin.source_connect_duration" => Some(rapidbyte_metrics::instruments::plugin::source_connect_duration()),
        "plugin.source_query_duration" => Some(rapidbyte_metrics::instruments::plugin::source_query_duration()),
        "plugin.source_fetch_duration" => Some(rapidbyte_metrics::instruments::plugin::source_fetch_duration()),
        "plugin.source_encode_duration" => Some(rapidbyte_metrics::instruments::plugin::source_encode_duration()),
        "plugin.dest_connect_duration" => Some(rapidbyte_metrics::instruments::plugin::dest_connect_duration()),
        "plugin.dest_flush_duration" => Some(rapidbyte_metrics::instruments::plugin::dest_flush_duration()),
        "plugin.dest_commit_duration" => Some(rapidbyte_metrics::instruments::plugin::dest_commit_duration()),
        "plugin.dest_decode_duration" => Some(rapidbyte_metrics::instruments::plugin::dest_decode_duration()),
        _ => None,
    }
}
```

Then the dispatch becomes:
```rust
if let Some(builtin) = rapidbyte_metrics::plugin_names::find_by_plugin_name(name.as_str()) {
    if let Some(hist) = builtin_plugin_histogram(builtin.instrument_name) {
        hist.record(value, &labels);
    }
} else { /* custom_histogram fallback */ }
```

- [ ] **Step 2: Replace snapshot.rs name functions with registry**

In `snapshot.rs`, replace `is_builtin_plugin_duration_name` and `builtin_plugin_duration_snapshot_name` with calls to the registry:

```rust
fn builtin_plugin_duration_snapshot_name(name: &str) -> Option<&'static str> {
    rapidbyte_metrics::plugin_names::find_by_instrument_name(name)
        .map(|d| d.snapshot_name)
}

fn is_builtin_plugin_duration_name(name: &str) -> bool {
    rapidbyte_metrics::plugin_names::is_builtin_name(name)
}
```

- [ ] **Step 3: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 4: Commit**

```bash
git commit -am "refactor(metrics): consolidate histogram dispatch via plugin name registry"
```

---

### Task 5: Pre-allocated scope labels

**Files:**
- Modify: `crates/rapidbyte-metrics/src/labels.rs`
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`

Currently `parse_bounded_labels` deserializes JSON → Map → Vec<KeyValue> on every metric call. For built-in metrics, the plugin always sends the same label keys. We can pre-build the scope labels once per `ComponentHostState` and reuse them.

- [ ] **Step 1: Add a `ScopeLabels` struct to labels.rs**

```rust
/// Pre-built label set for a specific metric scope (pipeline + plugin + stream + run + shard).
/// Avoids JSON parsing and allocation on every metric emission for built-in metrics.
#[derive(Clone)]
pub struct ScopeLabels {
    labels: Vec<KeyValue>,
}

impl ScopeLabels {
    /// Build scope labels for a specific pipeline/plugin/stream context.
    #[must_use]
    pub fn new(pipeline: &str, plugin: &str, stream: &str, run: Option<&str>, shard: usize) -> Self {
        let mut labels = vec![
            KeyValue::new(PIPELINE, pipeline.to_owned()),
            KeyValue::new(PLUGIN, plugin.to_owned()),
            KeyValue::new(STREAM, stream.to_owned()),
        ];
        if let Some(run) = run {
            labels.push(KeyValue::new(RUN, run.to_owned()));
        }
        if shard > 0 {
            labels.push(KeyValue::new(SHARD, shard.to_string()));
        }
        Self { labels }
    }

    /// Return the pre-built labels as a slice for OTel instrument calls.
    #[must_use]
    pub fn as_slice(&self) -> &[KeyValue] {
        &self.labels
    }
}
```

- [ ] **Step 2: Pre-build ScopeLabels in ComponentHostState**

In `host_state.rs`, add a `scope_labels: rapidbyte_metrics::labels::ScopeLabels` field to `ComponentHostState` (or `HostTimings`). Build it once during construction from the identity fields.

- [ ] **Step 3: Use ScopeLabels for built-in counter/histogram dispatch**

In `counter_add_impl`, for built-in names (`records_read`, `records_written`, `bytes_read`, `bytes_written`), use `self.scope_labels.as_slice()` instead of parsing JSON + calling `ensure_metric_scope_labels`. Only fall through to `parse_bounded_labels` for custom metrics (the `_` arm).

Same pattern for `histogram_record_impl` built-in durations.

- [ ] **Step 4: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 5: Commit**

```bash
git commit -am "perf(metrics): pre-allocate scope labels to avoid per-emission JSON parsing"
```

---

### Task 6: Align reserved label sets

**Files:**
- Modify: `crates/rapidbyte-sdk/src/context.rs`

- [ ] **Step 1: Add `shard` to SDK reserved labels**

Change `is_reserved_metric_label` (line 192) from:
```rust
fn is_reserved_metric_label(label: &str) -> bool {
    matches!(label, "pipeline" | "run" | "plugin" | "stream")
}
```
to:
```rust
fn is_reserved_metric_label(label: &str) -> bool {
    matches!(label, "pipeline" | "run" | "plugin" | "stream" | "shard")
}
```

This aligns with the host's `ensure_metric_scope_labels` which strips all 5.

- [ ] **Step 2: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 3: Commit**

```bash
git commit -am "fix(sdk): align reserved metric labels with host (add shard)"
```

---

### Task 7: Remove dead instrument accessors

**Files:**
- Modify: `crates/rapidbyte-metrics/src/instruments.rs`

- [ ] **Step 1: Remove 12 unused instrument accessors**

Delete these function definitions (the macros that generate them):

**pipeline module:** `duration`, `run_total`, `run_errors`
**host module:** `module_load_duration`
**controller module:** `preview_store_size`, `tasks_assigned`, `lease_revocations`, `lease_renewals`, `reconciliation_timeouts`, `state_persist_errors`
**agent module:** `spool_disk_bytes`, `flight_request_duration`

Also remove the corresponding histogram bucket views in `lib.rs` if any reference these names (check `pipeline.duration` view).

- [ ] **Step 2: Remove unused type imports from module headers**

After removing the accessors, check if `Gauge`, `Histogram`, `Counter`, `UpDownCounter` imports are still needed in each sub-module. Remove unused ones.

- [ ] **Step 3: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

- [ ] **Step 4: Commit**

```bash
git commit -am "chore(metrics): remove 12 dead instrument accessors"
```

---

### Task 8: DRY RecordingHostImports

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `plugins/transforms/validate/src/validate.rs`

The validate plugin's `RecordingHostImports` (~55 lines) is nearly identical to the SDK's version but uses an instance field instead of a global. The SDK version already supports all 3 metric types; the validate version only records counters.

- [ ] **Step 1: Add a `StubHostImports` to the SDK's test support**

In `host_ffi.rs`, below the existing `RecordingHostImports`, add a simpler `StubHostImports` that can be constructed with an `Arc<Mutex<Vec<MetricCall>>>`:

```rust
/// Minimal no-op host imports stub that records metric calls to a shared vec.
/// For use in plugin tests that need to verify metric emissions without
/// pulling in the full RecordingHostImports test infrastructure.
#[cfg(not(target_arch = "wasm32"))]
pub struct StubHostImports {
    pub metric_calls: Arc<Mutex<Vec<MetricCall>>>,
}
```

Implement `HostImports` for it with all methods as no-ops except the 3 metric methods which push to `metric_calls` (same as the validate plugin's version, but supporting all 3 types).

- [ ] **Step 2: Update validate plugin to use SDK's StubHostImports**

In `validate.rs`, replace the local `RecordingHostImports` struct and impl (~55 lines) with:
```rust
use rapidbyte_sdk::host_ffi::test_support::StubHostImports;
```

- [ ] **Step 3: Build and test**

```bash
cargo test --workspace 2>&1 | grep -E "FAILED|^test result"
```

The validate plugin tests should still pass since `StubHostImports` provides the same recording behavior.

- [ ] **Step 4: Commit**

```bash
git commit -am "refactor(sdk): DRY RecordingHostImports with shared StubHostImports"
```

---

## Verification

After all tasks are complete:

1. `cargo test --workspace` — all tests pass
2. `cargo clippy --workspace --all-targets -- -D warnings` — no warnings
3. Verify no remaining duplicated test providers:
```bash
grep -rn "fn metric_test_provider\|fn test_provider" crates/ --include="*.rs"
```
Should show only `crates/rapidbyte-metrics/src/test_support.rs`.
4. Verify no remaining local name maps:
```bash
grep -rn "is_builtin_plugin_duration_name\|builtin_plugin_duration_snapshot_name" crates/ --include="*.rs"
```
Should show only `crates/rapidbyte-metrics/src/snapshot.rs` (thin wrappers calling the registry).
5. `just bench postgres --profile small` — benchmarks still work with cached instruments
