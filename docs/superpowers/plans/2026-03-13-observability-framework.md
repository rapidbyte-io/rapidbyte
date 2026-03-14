# Observability Framework Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hand-rolled timing accumulators with OpenTelemetry-based observability across all Rapidbyte components.

**Architecture:** New `rapidbyte-metrics` crate owns OTel initialization, instrument registration, and Prometheus export. Existing timing types (`HostTimings`, `PipelineResult`) become thin wrappers over OTel instruments. Typed WIT calls replace JSON-over-WIT metric boundary. Controller and agent get domain + transport metrics.

**Tech Stack:** `opentelemetry` + `opentelemetry_sdk` + `opentelemetry-otlp` + `opentelemetry-prometheus` + `prometheus` + `tracing-opentelemetry` + `dashmap`

**Spec:** `docs/superpowers/specs/2026-03-13-observability-framework-design.md`

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `crates/rapidbyte-metrics/Cargo.toml` | Crate manifest with OTel dependencies |
| `crates/rapidbyte-metrics/src/lib.rs` | `init()`, `OtelGuard`, module re-exports |
| `crates/rapidbyte-metrics/src/instruments.rs` | All `OnceLock`-based instrument accessors (pipeline, host, plugin, controller, agent, process) |
| `crates/rapidbyte-metrics/src/labels.rs` | Bounded label key constants and `parse_bounded_labels()` |
| `crates/rapidbyte-metrics/src/snapshot.rs` | `InMemoryMetricReader` wrapper, `snapshot_pipeline_result()` |
| `crates/rapidbyte-metrics/src/views.rs` | Histogram bucket configuration via OTel `View` |
| `crates/rapidbyte-metrics/src/cache.rs` | `DashMap`-based dynamic instrument cache for custom plugin metrics |
| `crates/rapidbyte-metrics/src/grpc_layer.rs` | Custom `tower::Layer` for gRPC RED metrics |

### Modified files

| File | What changes |
|------|-------------|
| `Cargo.toml` (workspace) | Add `rapidbyte-metrics` to members, add OTel workspace deps |
| `wit/rapidbyte-plugin.wit` | Remove `metric`, add `counter-add`, `gauge-set`, `histogram-record` |
| `crates/rapidbyte-sdk/src/context.rs` | Remove `metric()`, add `counter()`, `gauge()`, `histogram()` + `_with_labels` variants |
| `crates/rapidbyte-sdk/src/host_ffi.rs` | Remove `metric()`, add `counter_add()`, `gauge_set()`, `histogram_record()` |
| `crates/rapidbyte-sdk/Cargo.toml` | No changes (types dep already present) |
| `crates/rapidbyte-runtime/Cargo.toml` | Add `rapidbyte-metrics` dep |
| `crates/rapidbyte-runtime/src/host_state.rs` | Replace `HostTimings` body, replace `metric_impl()`, update `emit_batch_impl()`, `next_batch_impl()` |
| `crates/rapidbyte-runtime/src/bindings.rs` | Remove `metric` dispatch, add `counter_add`, `gauge_set`, `histogram_record` in `impl_host_trait_for_world!` macro |
| `crates/rapidbyte-engine/Cargo.toml` | Add `rapidbyte-metrics` dep |
| `crates/rapidbyte-engine/src/orchestrator.rs` | Replace `finalize_run()`, remove `SourceTimingMaxima`/`DestTimingMaxima` |
| `crates/rapidbyte-engine/src/runner.rs` | Record `ReadSummary`/`WriteSummary` into OTel after plugin returns |
| `crates/rapidbyte-controller/Cargo.toml` | Add `rapidbyte-metrics` dep |
| `crates/rapidbyte-controller/src/pipeline_service.rs` | Add run submission/completion domain metrics |
| `crates/rapidbyte-controller/src/agent_service.rs` | Add heartbeat, agent, lease domain metrics |
| `crates/rapidbyte-controller/src/server.rs` | Add background task metrics, gRPC layer, Prometheus endpoint |
| `crates/rapidbyte-agent/Cargo.toml` | Add `rapidbyte-metrics` dep |
| `crates/rapidbyte-agent/src/executor.rs` | (no changes — task lifecycle metrics are in worker.rs) |
| `crates/rapidbyte-agent/src/flight.rs` | Add Flight request metrics |
| `crates/rapidbyte-agent/src/spool.rs` | Add spool metrics (entries, evictions, spill) |
| `crates/rapidbyte-agent/src/worker.rs` | Wire gRPC metrics layer, add Prometheus endpoint |
| `crates/rapidbyte-cli/Cargo.toml` | Add `rapidbyte-metrics` dep |
| `crates/rapidbyte-cli/src/logging.rs` | Compose OTel tracing layer alongside fmt layer (preserve `.with_target(false)`) |
| `crates/rapidbyte-cli/src/commands/run.rs` | Record process CPU metrics to OTel |
| `crates/rapidbyte-types/src/metric.rs` | Remove `Metric`, `MetricValue`; keep `ReadPerf`, `WritePerf`, summaries |
| `crates/rapidbyte-types/src/lib.rs` | Update prelude to remove `Metric`, `MetricValue` |
| `crates/rapidbyte-sdk/src/lib.rs` | Remove `pub use rapidbyte_types::metric;` re-export of `Metric`/`MetricValue` |
| `crates/rapidbyte-cli/src/main.rs` | Call `metrics::init()`, pass `OtelGuard` to `logging::init()` |
| `plugins/sources/postgres/src/metrics.rs` | Migrate `ctx.metric()` → `ctx.histogram()` / `ctx.counter()` |
| `plugins/destinations/postgres/src/metrics.rs` | Migrate `ctx.metric()` → `ctx.histogram()` |
| `plugins/destinations/postgres/src/writer.rs` | Migrate `ctx.metric()` → `ctx.histogram()` |
| `plugins/destinations/postgres/src/session.rs` | Migrate `ctx.metric()` → `ctx.counter()` |
| `plugins/transforms/validate/src/transform.rs` | Migrate `ctx.metric()` → `ctx.counter()` / `ctx.histogram()` |

---

## Chunk 1: rapidbyte-metrics Crate

### Task 1: Create crate skeleton and workspace wiring

**Files:**
- Create: `crates/rapidbyte-metrics/Cargo.toml`
- Create: `crates/rapidbyte-metrics/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add OTel workspace dependencies**

In `Cargo.toml` (workspace root), add to `[workspace.dependencies]`:

```toml
opentelemetry = "0.28"
opentelemetry_sdk = { version = "0.28", features = ["rt-tokio", "metrics"] }
opentelemetry-otlp = { version = "0.28", features = ["metrics", "grpc-tonic"] }
opentelemetry-prometheus = "0.28"
prometheus = "0.13"
tracing-opentelemetry = "0.29"
dashmap = "6"
```

Add `"crates/rapidbyte-metrics"` to `[workspace].members` list (after `"crates/rapidbyte-engine"`).

- [ ] **Step 2: Create Cargo.toml for rapidbyte-metrics**

Create `crates/rapidbyte-metrics/Cargo.toml`:

```toml
[package]
name = "rapidbyte-metrics"
version.workspace = true
edition.workspace = true

[dependencies]
rapidbyte-types = { path = "../rapidbyte-types" }
opentelemetry = { workspace = true }
opentelemetry_sdk = { workspace = true }
opentelemetry-otlp = { workspace = true }
opentelemetry-prometheus = { workspace = true }
prometheus = { workspace = true }
tracing = { workspace = true }
dashmap = { workspace = true }

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
```

- [ ] **Step 3: Create lib.rs skeleton**

Create `crates/rapidbyte-metrics/src/lib.rs`:

```rust
//! OpenTelemetry-based observability for Rapidbyte.
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | `cache`      | DashMap instrument cache for dynamic plugin metrics |
//! | `grpc_layer` | Tower layer for gRPC RED metrics |
//! | `instruments`| OnceLock instrument accessors (pipeline, host, plugin, ...) |
//! | `labels`     | Bounded label keys and parsing |
//! | `snapshot`   | InMemoryMetricReader and PipelineResult bridge |
//! | `views`      | Histogram bucket configuration |

#![warn(clippy::pedantic)]

pub mod cache;
pub mod grpc_layer;
pub mod instruments;
pub mod labels;
pub mod snapshot;
pub mod views;

// Stub modules so it compiles
// (each will be fleshed out in subsequent tasks)
```

Create empty stub files so the crate compiles:
- `crates/rapidbyte-metrics/src/cache.rs` — `//! Dynamic instrument cache.`
- `crates/rapidbyte-metrics/src/grpc_layer.rs` — `//! gRPC metrics tower layer.`
- `crates/rapidbyte-metrics/src/instruments.rs` — `//! OTel instrument accessors.`
- `crates/rapidbyte-metrics/src/labels.rs` — `//! Bounded label keys.`
- `crates/rapidbyte-metrics/src/snapshot.rs` — `//! Metric snapshot for PipelineResult.`
- `crates/rapidbyte-metrics/src/views.rs` — `//! Histogram bucket views.`

- [ ] **Step 4: Verify crate compiles**

Run: `cargo check -p rapidbyte-metrics`
Expected: compiles with no errors

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/ Cargo.toml Cargo.lock
git commit -m "feat(metrics): add rapidbyte-metrics crate skeleton"
```

---

### Task 2: Implement labels module

**Files:**
- Modify: `crates/rapidbyte-metrics/src/labels.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test for label parsing**

In `crates/rapidbyte-metrics/src/labels.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_labels() {
        let json = r#"{"pipeline":"sync","stream":"users"}"#;
        let labels = parse_bounded_labels(json);
        assert_eq!(labels.len(), 2);
        assert!(labels.iter().any(|kv| kv.key.as_str() == "pipeline"));
        assert!(labels.iter().any(|kv| kv.key.as_str() == "stream"));
    }

    #[test]
    fn unknown_labels_are_dropped() {
        let json = r#"{"pipeline":"sync","unknown_key":"val"}"#;
        let labels = parse_bounded_labels(json);
        assert_eq!(labels.len(), 1);
        assert!(labels.iter().any(|kv| kv.key.as_str() == "pipeline"));
    }

    #[test]
    fn malformed_json_returns_empty() {
        let labels = parse_bounded_labels("not json");
        assert!(labels.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- labels`
Expected: FAIL — `parse_bounded_labels` not defined

- [ ] **Step 3: Implement labels module**

```rust
//! Bounded label keys and parsing.

use opentelemetry::KeyValue;
use std::collections::HashSet;
use std::sync::LazyLock;

pub const PIPELINE: &str = "pipeline";
pub const STREAM: &str = "stream";
pub const SOURCE: &str = "source";
pub const DESTINATION: &str = "destination";
pub const SHARD: &str = "shard";
pub const STATUS: &str = "status";
pub const METHOD: &str = "method";
pub const REASON: &str = "reason";
pub const PLUGIN: &str = "plugin";
pub const AGENT_ID: &str = "agent_id";

static ALLOWED_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        PIPELINE, STREAM, SOURCE, DESTINATION, SHARD, STATUS, METHOD, REASON, PLUGIN, AGENT_ID,
    ])
});

/// Parse a JSON label string, keeping only keys in the bounded set.
/// Unknown keys are silently dropped. Malformed JSON returns empty.
#[must_use]
pub fn parse_bounded_labels(json: &str) -> Vec<KeyValue> {
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json) else {
        tracing::debug!("malformed metric labels JSON: {json}");
        return Vec::new();
    };

    map.into_iter()
        .filter_map(|(k, v)| {
            if ALLOWED_KEYS.contains(k.as_str()) {
                let val = match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                Some(KeyValue::new(k, val))
            } else {
                tracing::debug!("dropping unknown metric label key: {k}");
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    // ... tests from step 1 ...
}
```

Note: add `serde_json = { workspace = true }` to `crates/rapidbyte-metrics/Cargo.toml` dependencies.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-metrics -- labels`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/
git commit -m "feat(metrics): implement bounded label parsing"
```

---

### Task 3: Implement views module (histogram buckets)

**Files:**
- Modify: `crates/rapidbyte-metrics/src/views.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_buckets_are_sub_millisecond_focused() {
        let buckets = fast_duration_buckets();
        assert!(buckets[0] < 0.001, "first bucket should be sub-ms");
        assert!(buckets.last().unwrap() <= &1.0);
    }

    #[test]
    fn slow_buckets_cover_minutes() {
        let buckets = slow_duration_buckets();
        assert!(buckets.last().unwrap() >= &300.0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- views`
Expected: FAIL

- [ ] **Step 3: Implement views module**

```rust
//! Histogram bucket configuration for different duration scales.

use opentelemetry_sdk::metrics::{Instrument, Stream};

/// Sub-millisecond operations: compress, decompress, emit_batch, next_batch.
#[must_use]
pub fn fast_duration_buckets() -> Vec<f64> {
    vec![0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
}

/// Normal operations: plugin connect, query, flush, commit, module load.
#[must_use]
pub fn normal_duration_buckets() -> Vec<f64> {
    vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
}

/// Long operations: pipeline duration, task duration.
#[must_use]
pub fn slow_duration_buckets() -> Vec<f64> {
    vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
}

/// Create a view that applies custom buckets to instruments matching a name prefix.
pub fn bucket_view(
    prefix: &'static str,
    buckets: Vec<f64>,
) -> Box<dyn Fn(&Instrument) -> Option<Stream> + Send + Sync> {
    Box::new(move |instrument: &Instrument| {
        if instrument.name.starts_with(prefix) {
            let mut stream = Stream::new();
            stream.aggregation = Some(
                opentelemetry_sdk::metrics::Aggregation::ExplicitBucketHistogram {
                    boundaries: buckets.clone(),
                    record_min_max: true,
                },
            );
            Some(stream)
        } else {
            None
        }
    })
}
```

**Important:** The exact `View` API varies across `opentelemetry_sdk` versions. The code above shows the intent — instruments with names starting with `host.` get fast buckets, `plugin.` get normal buckets, `pipeline.duration` / `agent.task_duration` get slow buckets. During implementation, consult the actual `opentelemetry_sdk` 0.28 API docs. The `Stream` struct may use a builder pattern or different field names. Use `context7` MCP to fetch the exact API if needed.

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-metrics -- views`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/src/views.rs
git commit -m "feat(metrics): add histogram bucket views for duration scales"
```

---

### Task 4: Implement instrument accessors

**Files:**
- Modify: `crates/rapidbyte-metrics/src/instruments.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_instruments_are_stable_references() {
        // Calling the accessor twice returns the same reference
        let a = pipeline::records_read();
        let b = pipeline::records_read();
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn host_instruments_are_stable_references() {
        let a = host::emit_batch_duration();
        let b = host::emit_batch_duration();
        assert!(std::ptr::eq(a, b));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- instruments`
Expected: FAIL

- [ ] **Step 3: Implement instruments module**

```rust
//! OTel instrument accessors using OnceLock.
//!
//! All instruments are lazily registered on first access via the OTel global meter provider.
//! If `metrics::init()` has not been called, instruments use the no-op global provider.

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};

const METER_NAME: &str = "rapidbyte";

fn meter() -> opentelemetry::metrics::Meter {
    global::meter(METER_NAME)
}

macro_rules! define_counter_u64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Counter<u64> {
            static INST: OnceLock<Counter<u64>> = OnceLock::new();
            INST.get_or_init(|| meter().u64_counter($metric_name).build())
        }
    };
}

macro_rules! define_histogram_f64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Histogram<f64> {
            static INST: OnceLock<Histogram<f64>> = OnceLock::new();
            INST.get_or_init(|| meter().f64_histogram($metric_name).build())
        }
    };
}

macro_rules! define_gauge_f64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Gauge<f64> {
            static INST: OnceLock<Gauge<f64>> = OnceLock::new();
            INST.get_or_init(|| meter().f64_gauge($metric_name).build())
        }
    };
}

macro_rules! define_updown_i64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static UpDownCounter<i64> {
            static INST: OnceLock<UpDownCounter<i64>> = OnceLock::new();
            INST.get_or_init(|| meter().i64_up_down_counter($metric_name).build())
        }
    };
}

pub mod pipeline {
    use super::*;
    define_counter_u64!(records_read, "pipeline.records_read");
    define_counter_u64!(records_written, "pipeline.records_written");
    define_counter_u64!(bytes_read, "pipeline.bytes_read");
    define_counter_u64!(bytes_written, "pipeline.bytes_written");
    define_histogram_f64!(duration, "pipeline.duration");
    define_counter_u64!(run_total, "pipeline.run_total");
    define_counter_u64!(run_errors, "pipeline.run_errors");
}

pub mod host {
    use super::*;
    define_histogram_f64!(emit_batch_duration, "host.emit_batch_duration");
    define_histogram_f64!(next_batch_duration, "host.next_batch_duration");
    define_histogram_f64!(next_batch_wait_duration, "host.next_batch_wait_duration");
    define_histogram_f64!(next_batch_process_duration, "host.next_batch_process_duration");
    define_histogram_f64!(compress_duration, "host.compress_duration");
    define_histogram_f64!(decompress_duration, "host.decompress_duration");
    define_histogram_f64!(module_load_duration, "host.module_load_duration");
}

pub mod plugin {
    use super::*;
    define_histogram_f64!(source_connect_duration, "plugin.source_connect_duration");
    define_histogram_f64!(source_query_duration, "plugin.source_query_duration");
    define_histogram_f64!(source_fetch_duration, "plugin.source_fetch_duration");
    define_histogram_f64!(source_encode_duration, "plugin.source_encode_duration");
    define_histogram_f64!(dest_connect_duration, "plugin.dest_connect_duration");
    define_histogram_f64!(dest_flush_duration, "plugin.dest_flush_duration");
    define_histogram_f64!(dest_commit_duration, "plugin.dest_commit_duration");
    define_histogram_f64!(dest_decode_duration, "plugin.dest_decode_duration");

    // Dynamic plugin metrics — see cache module
    // Note: these re-exports require Task 5 (cache) to be complete.
    // If implementing Task 4 before Task 5, add stub functions in cache.rs first.
    pub use crate::cache::{custom_counter, custom_gauge, custom_histogram};
}

pub mod controller {
    use super::*;
    define_updown_i64!(active_runs, "controller.active_runs");
    define_updown_i64!(active_agents, "controller.active_agents");
    define_gauge_f64!(preview_store_size, "controller.preview_store_size");
    define_counter_u64!(runs_submitted, "controller.runs_submitted");
    define_counter_u64!(runs_completed, "controller.runs_completed");
    define_counter_u64!(tasks_assigned, "controller.tasks_assigned");
    define_counter_u64!(tasks_completed, "controller.tasks_completed");
    define_counter_u64!(lease_grants, "controller.lease_grants");
    define_counter_u64!(lease_revocations, "controller.lease_revocations");
    define_counter_u64!(lease_renewals, "controller.lease_renewals");
    define_counter_u64!(heartbeat_received, "controller.heartbeat_received");
    define_counter_u64!(heartbeat_timeouts, "controller.heartbeat_timeouts");
    define_counter_u64!(reconciliation_sweeps, "controller.reconciliation_sweeps");
    define_counter_u64!(reconciliation_timeouts, "controller.reconciliation_timeouts");
    define_histogram_f64!(state_persist_duration, "controller.state_persist_duration");
    define_counter_u64!(state_persist_errors, "controller.state_persist_errors");
}

pub mod agent {
    use super::*;
    define_updown_i64!(active_tasks, "agent.active_tasks");
    define_gauge_f64!(spool_entries, "agent.spool_entries");
    define_gauge_f64!(spool_disk_bytes, "agent.spool_disk_bytes");
    define_counter_u64!(tasks_received, "agent.tasks_received");
    define_counter_u64!(tasks_completed, "agent.tasks_completed");
    define_histogram_f64!(task_duration, "agent.task_duration");
    define_counter_u64!(records_processed, "agent.records_processed");
    define_counter_u64!(bytes_processed, "agent.bytes_processed");
    define_counter_u64!(flight_requests, "agent.flight_requests");
    define_histogram_f64!(flight_request_duration, "agent.flight_request_duration");
    define_counter_u64!(flight_batches_served, "agent.flight_batches_served");
    define_counter_u64!(previews_stored, "agent.previews_stored");
    define_counter_u64!(previews_evicted, "agent.previews_evicted");
    define_counter_u64!(preview_spill_to_disk, "agent.preview_spill_to_disk");
}

pub mod process {
    use super::*;
    define_gauge_f64!(cpu_seconds, "process.cpu_seconds");
    define_gauge_f64!(peak_rss_bytes, "process.peak_rss_bytes");
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-metrics -- instruments`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/src/instruments.rs
git commit -m "feat(metrics): add OnceLock instrument accessors for all metric modules"
```

---

### Task 5: Implement dynamic instrument cache

**Files:**
- Modify: `crates/rapidbyte-metrics/src/cache.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_counter_returns_same_instrument_for_same_name() {
        let a = custom_counter("my_plugin.rows");
        let b = custom_counter("my_plugin.rows");
        // Both are valid counters (we can't compare identity easily,
        // but calling both should not panic)
        a.add(1, &[]);
        b.add(1, &[]);
    }

    #[test]
    fn different_names_return_different_instruments() {
        let _ = custom_counter("metric_a");
        let _ = custom_counter("metric_b");
        // Should not panic — two distinct instruments created
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- cache`
Expected: FAIL

- [ ] **Step 3: Implement cache module**

```rust
//! DashMap-based instrument cache for dynamic plugin metrics.

use std::sync::OnceLock;

use dashmap::DashMap;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

const PLUGIN_METER: &str = "rapidbyte.plugin";

fn plugin_meter() -> opentelemetry::metrics::Meter {
    global::meter(PLUGIN_METER)
}

static COUNTERS: OnceLock<DashMap<String, Counter<u64>>> = OnceLock::new();
static GAUGES: OnceLock<DashMap<String, Gauge<f64>>> = OnceLock::new();
static HISTOGRAMS: OnceLock<DashMap<String, Histogram<f64>>> = OnceLock::new();

fn counters() -> &'static DashMap<String, Counter<u64>> {
    COUNTERS.get_or_init(DashMap::new)
}

fn gauges() -> &'static DashMap<String, Gauge<f64>> {
    GAUGES.get_or_init(DashMap::new)
}

fn histograms() -> &'static DashMap<String, Histogram<f64>> {
    HISTOGRAMS.get_or_init(DashMap::new)
}

/// Get or create a counter for the given metric name.
pub fn custom_counter(name: &str) -> Counter<u64> {
    counters()
        .entry(name.to_owned())
        .or_insert_with(|| plugin_meter().u64_counter(name).build())
        .clone()
}

/// Get or create a gauge for the given metric name.
pub fn custom_gauge(name: &str) -> Gauge<f64> {
    gauges()
        .entry(name.to_owned())
        .or_insert_with(|| plugin_meter().f64_gauge(name).build())
        .clone()
}

/// Get or create a histogram for the given metric name.
pub fn custom_histogram(name: &str) -> Histogram<f64> {
    histograms()
        .entry(name.to_owned())
        .or_insert_with(|| plugin_meter().f64_histogram(name).build())
        .clone()
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-metrics -- cache`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/src/cache.rs
git commit -m "feat(metrics): add DashMap instrument cache for dynamic plugin metrics"
```

---

### Task 6: Implement init() and OtelGuard

**Files:**
- Modify: `crates/rapidbyte-metrics/src/lib.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Note: init() sets a process-global meter provider. Tests that call init()
    // must run sequentially. Use a single test that exercises both code paths,
    // or use #[serial] from serial_test crate if splitting.
    #[test]
    fn init_returns_guard_and_prometheus_text_does_not_panic() {
        // This test does NOT set OTEL_EXPORTER_OTLP_ENDPOINT,
        // so only the Prometheus exporter is active (no OTLP).
        let guard = init("test-service").expect("init should succeed");
        // prometheus_text() should not panic
        let _text = guard.prometheus_text();
        drop(guard);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- tests::init`
Expected: FAIL

- [ ] **Step 3: Implement init() and OtelGuard**

Update `crates/rapidbyte-metrics/src/lib.rs`:

```rust
//! OpenTelemetry-based observability for Rapidbyte.
//!
//! Call [`init`] at process start to configure the metrics and tracing pipeline.
//! The returned [`OtelGuard`] flushes pending exports on drop.

#![warn(clippy::pedantic)]

pub mod cache;
pub mod grpc_layer;
pub mod instruments;
pub mod labels;
pub mod snapshot;
pub mod views;

use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::Resource;

/// Guard that owns the OTel providers, snapshot reader, and flushes on drop.
pub struct OtelGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    prometheus_registry: prometheus::Registry,
    snapshot_reader: snapshot::SnapshotReader,
}

impl OtelGuard {
    #[must_use]
    pub fn tracer_provider(&self) -> &TracerProvider {
        &self.tracer_provider
    }

    #[must_use]
    pub fn meter_provider(&self) -> &SdkMeterProvider {
        &self.meter_provider
    }

    #[must_use]
    pub fn snapshot_reader(&self) -> &snapshot::SnapshotReader {
        &self.snapshot_reader
    }

    /// Render Prometheus text exposition format.
    #[must_use]
    pub fn prometheus_text(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.prometheus_registry.gather();
        encoder.encode_to_string(&metric_families).unwrap_or_default()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.meter_provider.shutdown() {
            tracing::warn!("failed to shutdown meter provider: {e}");
        }
        if let Err(e) = self.tracer_provider.shutdown() {
            tracing::warn!("failed to shutdown tracer provider: {e}");
        }
    }
}

/// Initialize OTel metrics + tracing pipeline.
///
/// Sets the global meter provider so that instrument accessors in [`instruments`]
/// work correctly. Must be called before pipeline execution.
///
/// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` to optionally enable OTLP export.
///
/// # Errors
///
/// Returns an error if the Prometheus exporter or OTLP exporter fails to initialize.
pub fn init(service_name: &str) -> Result<OtelGuard> {
    let resource = Resource::builder()
        .with_attributes([KeyValue::new(
            "service.name",
            service_name.to_owned(),
        )])
        .build();

    // Prometheus exporter
    let prometheus_registry = prometheus::Registry::new();
    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus_registry.clone())
        .build()?;

    // Create snapshot reader for PipelineResult bridge (CLI display)
    let snapshot_reader = snapshot::SnapshotReader::new();
    let snapshot_periodic_reader = snapshot_reader.build_reader();

    let mut meter_builder = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(prometheus_exporter)
        .with_reader(snapshot_periodic_reader);  // register snapshot reader

    // Register histogram views for different duration scales
    // (exact API may vary — adapt to opentelemetry_sdk version)

    // Optional OTLP metric exporter
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_exporter)
            .build();
        meter_builder = meter_builder.with_reader(reader);
    }

    let meter_provider = meter_builder.build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Traces
    let tracer_provider = if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let otlp_span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()?;
        TracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(otlp_span_exporter)
            .build()
    } else {
        TracerProvider::builder().with_resource(resource).build()
    };

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        prometheus_registry,
        snapshot_reader,
    })
}
```

Note: add `anyhow = { workspace = true }` to `crates/rapidbyte-metrics/Cargo.toml` dependencies.

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-metrics -- tests::init`
Expected: PASS

- [ ] **Step 5: Verify full crate compiles and all tests pass**

Run: `cargo test -p rapidbyte-metrics`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-metrics/
git commit -m "feat(metrics): implement init(), OtelGuard, and Prometheus export"
```

---

### Task 7: Implement snapshot module

**Files:**
- Modify: `crates/rapidbyte-metrics/src/snapshot.rs`
- Test: inline `#[cfg(test)]`

This task implements the `InMemoryMetricReader` wrapper and `snapshot_pipeline_result()` function that bridges OTel metrics back to `PipelineResult` for CLI display.

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_returns_default_when_no_metrics_recorded() {
        let reader = SnapshotReader::new();
        let result = reader.snapshot_pipeline_result("test-pipeline");
        assert_eq!(result.records_read, 0);
        assert_eq!(result.records_written, 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics -- snapshot`
Expected: FAIL

- [ ] **Step 3: Implement snapshot module**

```rust
//! InMemoryMetricReader wrapper and PipelineResult bridge.

use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::metrics::PeriodicReader;
use rapidbyte_types::metric::{ReadPerf, WritePerf};

use crate::labels;

// Re-use engine result types
// Note: rapidbyte-metrics depends on rapidbyte-types but NOT on rapidbyte-engine.
// PipelineResult lives in engine. We define a lightweight snapshot struct here
// that the engine can convert from.

/// Snapshot of pipeline metrics from OTel instruments.
/// The engine converts this to `PipelineResult` by adding non-instrument fields
/// (wasm_overhead, retry_count, parallelism, stream_metrics).
#[derive(Debug, Clone, Default)]
pub struct PipelineMetricsSnapshot {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub pipeline_duration_secs: f64,
    // Source timings (from plugin histograms)
    pub source_connect_secs: f64,
    pub source_query_secs: f64,
    pub source_fetch_secs: f64,
    pub source_encode_secs: f64,
    // Dest timings (from plugin histograms)
    pub dest_connect_secs: f64,
    pub dest_flush_secs: f64,
    pub dest_commit_secs: f64,
    pub dest_decode_secs: f64,
    // Host timings (from host histograms — sum of recorded values)
    pub emit_batch_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
    pub next_batch_nanos: u64,
    pub next_batch_wait_nanos: u64,
    pub next_batch_process_nanos: u64,
    pub decompress_nanos: u64,
    pub next_batch_count: u64,
}

/// Wraps an `InMemoryMetricExporter` for point-in-time snapshots.
pub struct SnapshotReader {
    exporter: InMemoryMetricExporter,
}

impl SnapshotReader {
    /// Create a new snapshot reader.
    /// The caller must register the returned `PeriodicReader` on the `SdkMeterProvider`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            exporter: InMemoryMetricExporter::default(),
        }
    }

    /// Get the periodic reader to register on the meter provider.
    pub fn build_reader(&self) -> PeriodicReader {
        PeriodicReader::builder(self.exporter.clone()).build()
    }

    /// Force a collect and return a pipeline metrics snapshot,
    /// filtered by the given pipeline label value.
    ///
    /// Note: `InMemoryMetricExporter` requires the `PeriodicReader` to have
    /// flushed. Call `meter_provider.force_flush()` before this method
    /// to ensure all pending metrics are available.
    pub fn snapshot_pipeline_result(&self, _pipeline: &str) -> PipelineMetricsSnapshot {
        // Reads metrics that have been flushed by the PeriodicReader.
        // Caller must force_flush() the meter_provider before calling this.
        let metrics = self.exporter.get_finished_metrics().unwrap_or_default();

        // Walk metrics and extract values filtered by pipeline label.
        // This is a best-effort extraction — fields not found remain at default (0).
        let mut snap = PipelineMetricsSnapshot::default();

        for resource_metrics in &metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    // Match metric name to snapshot field and extract sum/last value.
                    // Implementation will use metric.data downcast to Sum/Histogram/Gauge
                    // and filter data points by pipeline label attribute.
                    extract_metric_value(&metric.name, &metric.data, _pipeline, &mut snap);
                }
            }
        }

        snap
    }
}

impl Default for SnapshotReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract a metric value into the snapshot struct by matching instrument name.
fn extract_metric_value(
    name: &str,
    data: &dyn std::any::Any,
    _pipeline: &str,
    snap: &mut PipelineMetricsSnapshot,
) {
    // This function matches instrument names to snapshot fields.
    // The exact implementation depends on the opentelemetry_sdk data model.
    // During implementation, downcast `data` to the appropriate type
    // (Sum<u64>, Histogram<f64>, etc.) and read data points.
    //
    // Stub: to be completed during implementation with actual OTel SDK types.
    let _ = (name, data, _pipeline, snap);
}
```

Note: The `extract_metric_value` function body is a stub. During implementation, the developer will need to:
1. Downcast `data` to `opentelemetry_sdk::metrics::data::Sum<u64>` / `Histogram<f64>` / etc.
2. Iterate data points, filter by pipeline label attribute
3. Map values to the snapshot struct fields

This is the most OTel-SDK-version-sensitive code. The plan intentionally leaves the inner extraction as a stub with clear intent, because the exact SDK types may differ from what we can specify here.

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-metrics -- snapshot`
Expected: PASS (default snapshot returns zeros)

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-metrics/src/snapshot.rs
git commit -m "feat(metrics): add snapshot reader for PipelineResult bridge"
```

---

### Task 8: Verify full Chunk 1 — crate compiles and all tests pass

Note: `grpc_layer.rs` is still a stub at this point. It will be implemented in Chunk 4 (Task 20).

- [ ] **Step 1: Run full crate tests**

Run: `cargo test -p rapidbyte-metrics`
Expected: all tests PASS

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p rapidbyte-metrics -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Verify workspace still builds**

Run: `cargo check --workspace`
Expected: compiles (new crate exists but no other crate depends on it yet)

- [ ] **Step 4: Commit if any fixups needed**

```bash
git add crates/rapidbyte-metrics/
git commit -m "chore(metrics): clippy and build fixups"
```

---

## Chunk 2: WIT + SDK + Plugin Migration

### Task 9: Update WIT interface

**Files:**
- Modify: `wit/rapidbyte-plugin.wit`

- [ ] **Step 1: Remove old `metric` function from WIT**

In `wit/rapidbyte-plugin.wit`, find:
```wit
metric: func(payload-json: string) -> result<_, plugin-error>;
```
Remove this line.

- [ ] **Step 2: Add typed metric functions**

In the same `interface host` block, add:
```wit
    counter-add: func(name: string, value: u64, labels-json: string) -> result<_, plugin-error>;
    gauge-set: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;
    histogram-record: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;
```

- [ ] **Step 3: Verify WIT parses**

Run: `cargo check -p rapidbyte-runtime` (this will fail until bindings are updated, but WIT parse errors surface immediately)
Expected: WIT parse succeeds (Rust compile may fail — that's expected, addressed in next tasks)

- [ ] **Step 4: Commit**

```bash
git add wit/rapidbyte-plugin.wit
git commit -m "feat(wit): replace metric JSON call with typed counter-add, gauge-set, histogram-record"
```

---

### Task 10: Update SDK host_ffi

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`

- [ ] **Step 1: Read current host_ffi.rs**

Read `crates/rapidbyte-sdk/src/host_ffi.rs` to understand the current `metric()` function signature and the `HostImports` trait.

- [ ] **Step 2: Remove `metric()` from HostImports trait**

Remove the `metric` method from the `HostImports` trait and from both `WasmHostImports` and `StubHostImports` implementations.

- [ ] **Step 3: Add typed metric functions to HostImports**

Add to the `HostImports` trait. Note: the WIT functions take 3 params (name, value, labels_json). The plugin_id and stream_name are injected client-side into labels_json by `Context::merge_default_labels()`:
```rust
fn counter_add(&self, name: &str, value: u64, labels_json: &str) -> Result<(), PluginError>;
fn gauge_set(&self, name: &str, value: f64, labels_json: &str) -> Result<(), PluginError>;
fn histogram_record(&self, name: &str, value: f64, labels_json: &str) -> Result<(), PluginError>;
```

Implement for `WasmHostImports` (calls wit-bindgen generated functions) and `StubHostImports` (no-ops that return `Ok(())`).

- [ ] **Step 4: Add public free functions**

Following the existing pattern (e.g., `pub fn metric(...)` at line 574), add corresponding public free functions that delegate to `host_imports()`:
```rust
pub fn counter_add(name: &str, value: u64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().counter_add(name, value, labels_json)
}

pub fn gauge_set(name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().gauge_set(name, value, labels_json)
}

pub fn histogram_record(name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().histogram_record(name, value, labels_json)
}
```

These are what `Context` methods call (Task 11).

- [ ] **Step 5: Verify SDK compiles**

Run: `cargo check -p rapidbyte-sdk`
Expected: may still fail until Context is updated (next task)

**Note on Tasks 9-12:** These four tasks form an atomic chain. The workspace will not compile until all four are complete. Intermediate "verify compiles" steps in Tasks 10-11 are expected to fail — they exist to catch syntax errors in the specific file being edited, not full workspace compilation. Task 12 Step 5 is the first point where the full chain should compile. Consider squashing Tasks 9-12 into a single commit for CI/bisectability.

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_ffi.rs
git commit -m "feat(sdk): replace metric FFI with typed counter_add, gauge_set, histogram_record"
```

---

### Task 11: Update SDK Context

**Files:**
- Modify: `crates/rapidbyte-sdk/src/context.rs`
- Test: update existing test in `context.rs`

- [ ] **Step 1: Remove `Context::metric()` method**

In `crates/rapidbyte-sdk/src/context.rs`, remove:
```rust
pub fn metric(&self, m: &Metric) -> Result<(), PluginError> {
    host_ffi::metric(&self.plugin_id, &self.stream_name, m)
}
```

- [ ] **Step 2: Add typed metric methods**

Add to `impl Context`:
```rust
pub fn counter(&self, name: &str, value: u64) {
    self.counter_with_labels(name, value, &[]);
}

pub fn counter_with_labels(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
    let labels_json = self.merge_default_labels(labels);
    let _ = host_ffi::counter_add(name, value, &labels_json);
}

pub fn gauge(&self, name: &str, value: f64) {
    self.gauge_with_labels(name, value, &[]);
}

pub fn gauge_with_labels(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
    let labels_json = self.merge_default_labels(labels);
    let _ = host_ffi::gauge_set(name, value, &labels_json);
}

pub fn histogram(&self, name: &str, value: f64) {
    self.histogram_with_labels(name, value, &[]);
}

pub fn histogram_with_labels(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
    let labels_json = self.merge_default_labels(labels);
    let _ = host_ffi::histogram_record(name, value, &labels_json);
}

fn merge_default_labels(&self, extra: &[(&str, &str)]) -> String {
    let mut map = serde_json::Map::new();
    map.insert("plugin".to_owned(), serde_json::Value::String(self.plugin_id.clone()));
    map.insert("stream".to_owned(), serde_json::Value::String(self.stream_name.clone()));
    for (k, v) in extra {
        map.insert((*k).to_owned(), serde_json::Value::String((*v).to_owned()));
    }
    serde_json::Value::Object(map).to_string()
}
```

- [ ] **Step 3: Update test_metric_delegates test**

Replace the old `test_metric_delegates` test with:
```rust
#[test]
fn test_typed_metric_delegates() {
    let ctx = Context::new("test-plugin", "users");
    // Should not panic — calls StubHostImports which returns Ok(())
    ctx.counter("records_read", 42);
    ctx.histogram("source_connect_secs", 0.5);
    ctx.gauge("rows_per_second", 1000.0);
}
```

- [ ] **Step 4: Run SDK tests**

Run: `cargo test -p rapidbyte-sdk`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/context.rs crates/rapidbyte-sdk/src/host_ffi.rs
git commit -m "feat(sdk): add typed counter/gauge/histogram methods to Context"
```

---

### Task 12: Update host-side bindings in runtime

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`
- Modify: `crates/rapidbyte-runtime/Cargo.toml`

This task wires the new WIT functions to the host runtime. The old `metric_impl()` is replaced with three typed handlers. The `bindings.rs` file contains the `impl_host_trait_for_world!` macro that dispatches WIT host imports to `*_impl` methods — it must be updated to dispatch the new functions.

- [ ] **Step 1: Add rapidbyte-metrics dependency to runtime**

In `crates/rapidbyte-runtime/Cargo.toml`, add:
```toml
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
```

- [ ] **Step 2: Remove old metric_impl()**

In `crates/rapidbyte-runtime/src/host_state.rs`, remove the `metric_impl()` function (lines ~656-697) and its JSON parsing of `Metric`/`MetricValue`.

Remove the import:
```rust
use rapidbyte_types::metric::{Metric, MetricValue};
```

- [ ] **Step 3: Add typed metric host import implementations**

Add three new functions:

```rust
pub(crate) fn counter_add_impl(&self, name: String, value: u64, labels_json: String) -> Result<(), PluginError> {
    let labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
    match name.as_str() {
        "records_read" => rapidbyte_metrics::instruments::pipeline::records_read().add(value, &labels),
        "records_written" => rapidbyte_metrics::instruments::pipeline::records_written().add(value, &labels),
        _ => rapidbyte_metrics::instruments::plugin::custom_counter(&name).add(value, &labels),
    }
    Ok(())
}

pub(crate) fn gauge_set_impl(&self, name: String, value: f64, labels_json: String) -> Result<(), PluginError> {
    let labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
    rapidbyte_metrics::instruments::plugin::custom_gauge(&name).record(value, &labels);
    Ok(())
}

pub(crate) fn histogram_record_impl(&self, name: String, value: f64, labels_json: String) -> Result<(), PluginError> {
    let labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
    match name.as_str() {
        "source_connect_secs" => rapidbyte_metrics::instruments::plugin::source_connect_duration().record(value, &labels),
        "source_query_secs" => rapidbyte_metrics::instruments::plugin::source_query_duration().record(value, &labels),
        "source_fetch_secs" => rapidbyte_metrics::instruments::plugin::source_fetch_duration().record(value, &labels),
        "source_arrow_encode_secs" => rapidbyte_metrics::instruments::plugin::source_encode_duration().record(value, &labels),
        "dest_connect_secs" => rapidbyte_metrics::instruments::plugin::dest_connect_duration().record(value, &labels),
        "dest_flush_secs" => rapidbyte_metrics::instruments::plugin::dest_flush_duration().record(value, &labels),
        "dest_commit_secs" => rapidbyte_metrics::instruments::plugin::dest_commit_duration().record(value, &labels),
        "dest_arrow_decode_secs" => rapidbyte_metrics::instruments::plugin::dest_decode_duration().record(value, &labels),
        _ => rapidbyte_metrics::instruments::plugin::custom_histogram(&name).record(value, &labels),
    }
    Ok(())
}
```

- [ ] **Step 4: Wire into WIT bindings**

Update `crates/rapidbyte-runtime/src/bindings.rs`. The `impl_host_trait_for_world!` macro (~line 202-207) dispatches `fn metric(...)` to `self.metric_impl(...)`. Replace this with three dispatches:
```rust
fn counter_add(&mut self, name: String, value: u64, labels_json: String) -> Result<(), PluginError> {
    self.counter_add_impl(name, value, labels_json)
}
fn gauge_set(&mut self, name: String, value: f64, labels_json: String) -> Result<(), PluginError> {
    self.gauge_set_impl(name, value, labels_json)
}
fn histogram_record(&mut self, name: String, value: f64, labels_json: String) -> Result<(), PluginError> {
    self.histogram_record_impl(name, value, labels_json)
}
```

- [ ] **Step 5: Verify runtime compiles**

Run: `cargo check -p rapidbyte-runtime`
Expected: compiles

- [ ] **Step 6: Run runtime tests**

Run: `cargo test -p rapidbyte-runtime`
Expected: PASS (existing tests don't directly test metric_impl)

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-runtime/
git commit -m "feat(runtime): replace metric_impl with typed counter/gauge/histogram host imports"
```

---

### Task 13: Migrate plugins

**Files:**
- Modify: `plugins/sources/postgres/src/metrics.rs`
- Modify: `plugins/destinations/postgres/src/metrics.rs`
- Modify: `plugins/destinations/postgres/src/writer.rs`
- Modify: `plugins/destinations/postgres/src/session.rs`
- Modify: `plugins/transforms/validate/src/transform.rs`

- [ ] **Step 1: Read current plugin metric usage**

Read these files to find all `ctx.metric(...)` calls:
- `plugins/sources/postgres/src/metrics.rs`
- `plugins/destinations/postgres/src/metrics.rs`
- `plugins/destinations/postgres/src/writer.rs`
- `plugins/destinations/postgres/src/session.rs`
- `plugins/transforms/validate/src/transform.rs`

- [ ] **Step 2: Migrate source plugin**

In `plugins/sources/postgres/src/metrics.rs`:

Duration observations (`MetricValue::Gauge(v)`) → `ctx.histogram()`:
- `source_connect_secs` → `ctx.histogram("source_connect_secs", v)`
- `source_query_secs` → `ctx.histogram("source_query_secs", v)`
- `source_fetch_secs` → `ctx.histogram("source_fetch_secs", v)`
- `source_arrow_encode_secs` → `ctx.histogram("source_arrow_encode_secs", v)`

Counter observations (`MetricValue::Counter(v)`) → `ctx.counter()`:
- `records_read` → `ctx.counter("records_read", v)`
- `bytes_read` → `ctx.counter("bytes_read", v)`

Remove `use rapidbyte_types::metric::{Metric, MetricValue}` imports.

- [ ] **Step 3: Migrate destination plugin**

In `plugins/destinations/postgres/src/metrics.rs`, `writer.rs`, `session.rs`:

Duration observations → `ctx.histogram()`:
- `dest_connect_secs`, `dest_flush_secs`, `dest_commit_secs`, `dest_arrow_decode_secs`

Counter observations → `ctx.counter()`:
- `records_written` → `ctx.counter("records_written", v)`
- `bytes_written` → `ctx.counter("bytes_written", v)`

- [ ] **Step 4: Migrate validate transform plugin**

In `plugins/transforms/validate/src/transform.rs`, migrate metric calls similarly.

- [ ] **Step 5: Build all plugins**

Run:
```bash
cd plugins/sources/postgres && cargo build
cd plugins/destinations/postgres && cargo build
cd plugins/transforms/validate && cargo build
```
Expected: all compile

- [ ] **Step 6: Commit**

```bash
git add plugins/
git commit -m "feat(plugins): migrate metric calls to typed ctx.counter/histogram"
```

---

### Task 14: Remove Metric and MetricValue from types crate

**Files:**
- Modify: `crates/rapidbyte-types/src/metric.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/src/context.rs` (remove `use crate::metric::Metric` import)

- [ ] **Step 1: Remove Metric and MetricValue**

In `crates/rapidbyte-types/src/metric.rs`, remove:
- `MetricValue` enum
- `Metric` struct

Keep: `ReadPerf`, `WritePerf`, `ReadSummary`, `WriteSummary`, `TransformSummary`.

- [ ] **Step 2: Update types prelude**

In `crates/rapidbyte-types/src/lib.rs`, remove `Metric` and `MetricValue` from the prelude re-export line.

- [ ] **Step 3: Update SDK re-exports**

In `crates/rapidbyte-sdk/src/lib.rs`, update the `pub use rapidbyte_types::metric;` re-export — the module still exists (it has summary types) but `Metric`/`MetricValue` are gone.

In `crates/rapidbyte-sdk/src/context.rs`, remove `use crate::metric::Metric;` import if present.

- [ ] **Step 4: Remove tests for deleted types**

Remove the `metric_counter_roundtrip` test from `metric.rs` tests.

- [ ] **Step 5: Verify workspace compiles**

Run: `cargo check --workspace`
Expected: compiles (all references to `Metric`/`MetricValue` should be gone from prior tasks)

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/
git commit -m "refactor(types): remove Metric and MetricValue, keep summary types"
```

---

## Chunk 3: Host Runtime Refactor

### Task 15: Replace HostTimings

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`

- [ ] **Step 1: Replace HostTimings struct body**

Replace the 16-field accumulator struct with:

```rust
/// Records host-side operation timings directly into OTel instruments.
#[derive(Debug, Clone)]
pub struct HostTimings {
    labels: Vec<opentelemetry::KeyValue>,
}

impl Default for HostTimings {
    fn default() -> Self {
        Self { labels: Vec::new() }
    }
}

impl HostTimings {
    #[must_use]
    pub fn new(pipeline: &str, stream: &str, shard: usize) -> Self {
        use opentelemetry::KeyValue;
        Self {
            labels: vec![
                KeyValue::new("pipeline", pipeline.to_owned()),
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("shard", shard.to_string()),
            ],
        }
    }

    pub fn record_emit_batch(&self, duration: std::time::Duration) {
        rapidbyte_metrics::instruments::host::emit_batch_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_next_batch(
        &self,
        total: std::time::Duration,
        wait: std::time::Duration,
        process: std::time::Duration,
    ) {
        rapidbyte_metrics::instruments::host::next_batch_duration()
            .record(total.as_secs_f64(), &self.labels);
        rapidbyte_metrics::instruments::host::next_batch_wait_duration()
            .record(wait.as_secs_f64(), &self.labels);
        rapidbyte_metrics::instruments::host::next_batch_process_duration()
            .record(process.as_secs_f64(), &self.labels);
    }

    pub fn record_compress(&self, duration: std::time::Duration) {
        rapidbyte_metrics::instruments::host::compress_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_decompress(&self, duration: std::time::Duration) {
        rapidbyte_metrics::instruments::host::decompress_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }
}
```

- [ ] **Step 2: Update emit_batch_impl()**

In `emit_batch_impl()` (~lines 427-480):

1. The existing code uses `compress_elapsed_nanos: u64` (not a `Duration`). Refactor the local variable to `compress_duration: Duration` using `Duration::from_nanos(compress_elapsed_nanos)`, or compute compression timing with `Instant::elapsed()` directly.

2. Replace the mutex-locked accumulation:
```rust
// Old:
let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
t.emit_batch_count += 1;
t.compress_nanos += compress_elapsed_nanos;
// New:
self.checkpoints.timings.record_emit_batch(fn_start.elapsed());
let compress_duration = Duration::from_nanos(compress_elapsed_nanos);
if compress_duration > Duration::ZERO {
    self.checkpoints.timings.record_compress(compress_duration);
}
```

**Note on emit_batch_count:** The old code tracked `emit_batch_count += 1` explicitly. OTel histograms inherently track count in their bucket metadata, so this is preserved. The `snapshot_pipeline_result()` function should extract histogram count for `SourceTiming.emit_count`.

The `timings` field changes from `Arc<Mutex<HostTimings>>` to just `HostTimings` (no mutex needed — OTel instruments are internally thread-safe).

- [ ] **Step 3: Update next_batch_impl()**

In `next_batch_impl()` (~lines 483-531), replace manual nanos accumulation with:
```rust
self.checkpoints.timings.record_next_batch(total_elapsed, wait_elapsed, process_elapsed);
if decompress_duration > Duration::ZERO {
    self.checkpoints.timings.record_decompress(decompress_duration);
}
```

- [ ] **Step 4: Update CheckpointCollector, HostStateBuilder, and AggregatedStreamResults**

Change `CheckpointCollector` to hold `HostTimings` directly instead of `Arc<Mutex<HostTimings>>`:
```rust
pub(crate) struct CheckpointCollector {
    // ...
    pub timings: HostTimings,  // was: Arc<Mutex<HostTimings>>
}
```

Update `HostStateBuilder` to accept `HostTimings` directly (remove Arc/Mutex wrapping).

In the engine crate, `AggregatedStreamResults` has `src_timings: HostTimings` and `dst_timings: HostTimings` fields that were used for cross-stream aggregation (summing nanos across shards). Since timing data now flows to OTel instruments (which handle aggregation), these fields will be removed — but **defer this removal to Task 17** where `finalize_run()` is also updated. Removing them here would break `finalize_run()` which still references them. For now, just update the type from `Arc<Mutex<HostTimings>>` to `HostTimings`.

- [ ] **Step 5: Write tests for new HostTimings**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_timings_new_sets_labels() {
        let ht = HostTimings::new("my-pipeline", "users", 0);
        assert_eq!(ht.labels.len(), 3);
    }

    #[test]
    fn host_timings_default_has_no_labels() {
        let ht = HostTimings::default();
        assert!(ht.labels.is_empty());
    }

    #[test]
    fn record_methods_do_not_panic() {
        let ht = HostTimings::new("test", "stream", 0);
        ht.record_emit_batch(Duration::from_millis(5));
        ht.record_next_batch(
            Duration::from_millis(10),
            Duration::from_millis(7),
            Duration::from_millis(3),
        );
        ht.record_compress(Duration::from_micros(500));
        ht.record_decompress(Duration::from_micros(300));
    }
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p rapidbyte-runtime`
Expected: PASS

- [ ] **Step 7: Verify runtime compiles**

Run: `cargo check -p rapidbyte-runtime`
Expected: may have compile errors in runner.rs (engine crate) — those are addressed in the next task

- [ ] **Step 8: Commit**

```bash
git add crates/rapidbyte-runtime/
git commit -m "refactor(runtime): replace HostTimings accumulators with OTel recording methods"
```

---

### Task 16: Update runner.rs for new HostTimings

**Files:**
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml`

- [ ] **Step 1: Add rapidbyte-metrics dependency to engine**

In `crates/rapidbyte-engine/Cargo.toml`:
```toml
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
```

- [ ] **Step 2: Update run_source_stream()**

Currently creates `Arc<Mutex<HostTimings>>` and clones after run. Change to:
- Create `HostTimings::new(pipeline, stream, shard)` directly
- Pass to builder (no Arc/Mutex wrapping)
**Note on ReadSummary.perf:** In the current codebase, `ReadSummary.perf` is always `None` (set at runner.rs line 209). Source plugins report timing via `ctx.metric()` (now `ctx.histogram()`) calls during execution, which go directly to OTel instruments via the host runtime. Similarly, `records_read` and `bytes_read` are already reported by the plugin via `ctx.counter()` calls (which hit `counter_add_impl()` → `pipeline::records_read().add()`). Do NOT re-record them from `summary.records_read` in the runner — that would double-count.

The runner changes for source are minimal:
- Remove `Arc<Mutex<HostTimings>>` wrapping → pass `HostTimings::new(...)` directly
- Remove the post-run `HostTimings` extraction (no longer needed — data is in OTel)
- Keep `ReadSummary` return for records/bytes count (used by orchestrator for non-OTel purposes)

- [ ] **Step 3: Update run_destination_stream()**

Same pattern: remove `Arc<Mutex<HostTimings>>`, create `HostTimings::new(...)`, record `WriteSummary` into OTel after plugin returns.

- [ ] **Step 4: Update SourceRunResult / DestRunResult**

Change `host_timings: HostTimings` field — since `HostTimings` no longer accumulates state, it's just a label holder. The orchestrator may still pass it around for identification, but the actual metric data lives in OTel now.

Consider whether `SourceRunResult` still needs `host_timings` at all, or if it can be removed. If `finalize_run()` (next task) no longer reads from it, remove the field.

- [ ] **Step 5: Verify engine compiles**

Run: `cargo check -p rapidbyte-engine`
Expected: may fail until orchestrator is updated (next task)

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-engine/
git commit -m "refactor(engine): update runner to record summaries into OTel instruments"
```

---

### Task 17: Update finalize_run() and DryRunResult path in orchestrator

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`

**Important context:** `finalize_run()` is a standalone `async fn`, not a method. The snapshot reader must be passed as a parameter. Also, `build_source_timing()` is called from both `finalize_run()` and the dry-run path (~line 599). Both call sites must be updated.

**Threading the SnapshotReader:** The `OtelGuard` (created in `main.rs` via `init()`) holds the `SnapshotReader`. The orchestrator needs access to it. Thread it through: `main.rs` → `run_pipeline()` (or `Orchestrator::new()`) → `finalize_run()`. Add `snapshot_reader: &SnapshotReader` as a field on the orchestrator or pass it directly to `run_pipeline()`. Also remove `AggregatedStreamResults.src_timings/dst_timings` fields here (deferred from Task 15).

- [ ] **Step 1: Read current finalize_run() and build_source_timing()**

Read `crates/rapidbyte-engine/src/orchestrator.rs`:
- Lines 142-317: `SourceTimingMaxima`, `DestTimingMaxima`, `observe_source_timing`, `build_source_timing`, `observe_dest_timing`, `build_dest_timing`
- Lines 1534-1666: `finalize_run()`
- Lines ~599-604: dry-run call to `build_source_timing()`
- Lines ~2728-2806, ~2946-2949: tests referencing maxima structs

- [ ] **Step 2: Remove SourceTimingMaxima / DestTimingMaxima and helpers**

Remove:
- `SourceTimingMaxima` struct and its `observe_source_timing()` method
- `DestTimingMaxima` struct and its `observe_dest_timing()` method
- `build_source_timing()` and `build_dest_timing()` free functions
- The aggregation loop in `AggregatedStreamResults` that sums `HostTimings` fields

- [ ] **Step 3: Replace finalize_run() aggregation**

Add `snapshot_reader: &rapidbyte_metrics::snapshot::SnapshotReader` as a parameter to `finalize_run()`. Before calling `snapshot_pipeline_result()`, call `meter_provider.force_flush()` to ensure all pending metrics are available.

**Nanos-to-seconds note:** OTel histograms record in seconds (f64). But `SourceTiming` has fields like `emit_nanos: u64` and `compress_nanos: u64`. The snapshot must convert seconds back to nanos: `(snap.emit_batch_secs * 1_000_000_000.0) as u64`. Similarly, histogram count (for `emit_count`) must be extracted from the histogram data point metadata.

```rust
async fn finalize_run(
    // ... existing params ...
    snapshot_reader: &rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
) -> PipelineResult {
    // Force flush so snapshot reader has all data
    let _ = meter_provider.force_flush();
    let snap = snapshot_reader.snapshot_pipeline_result(&pipeline_name);

    let counts = PipelineCounts {
        records_read: snap.records_read,
        records_written: snap.records_written,
        bytes_read: snap.bytes_read,
        bytes_written: snap.bytes_written,
    };

    let source = SourceTiming {
        duration_secs: max_source_duration,  // from stream results, same as before
        module_load_ms: source_module_load_ms,  // from LoadedModules, same as before
        connect_secs: snap.source_connect_secs,
        query_secs: snap.source_query_secs,
        fetch_secs: snap.source_fetch_secs,
        arrow_encode_secs: snap.source_encode_secs,
        emit_nanos: snap.emit_batch_nanos,  // converted: (secs * 1e9) as u64
        compress_nanos: snap.compress_nanos,  // converted: (secs * 1e9) as u64
        emit_count: snap.emit_count,  // from histogram count metadata
    };

    let dest = DestTiming {
        duration_secs: max_dest_duration,  // from stream results
        module_load_ms: dest_module_load_ms,  // from LoadedModules
        connect_secs: snap.dest_connect_secs,
        flush_secs: snap.dest_flush_secs,
        commit_secs: snap.dest_commit_secs,
        arrow_decode_secs: snap.dest_decode_secs,
        vm_setup_secs: max_vm_setup_secs,  // from stream results, NOT OTel
        recv_secs: max_recv_secs,  // from stream results, NOT OTel
        recv_nanos: snap.next_batch_nanos,  // converted
        recv_wait_nanos: snap.next_batch_wait_nanos,  // converted
        recv_process_nanos: snap.next_batch_process_nanos,  // converted
        decompress_nanos: snap.decompress_nanos,  // converted
        recv_count: snap.next_batch_count,  // from histogram count metadata
    };

    // Fields NOT from OTel
    let stream_metrics = collect_stream_shard_metrics(&stream_results);
    let wasm_overhead_secs = /* same derived calculation as before */;

    PipelineResult { counts, source, dest, ... }
}
```

- [ ] **Step 4: Update dry-run call site**

The dry-run path (~line 599) also calls `build_source_timing()`. Replace this with the same snapshot-based approach, or pass the snapshot reader through the dry-run code path.

- [ ] **Step 5: Record module_load_ms to OTel**

Add `rapidbyte_metrics::instruments::host::module_load_duration().record(ms as f64 / 1000.0, &labels)` at the point where modules are loaded (in runner.rs or orchestrator.rs). This ensures the snapshot can extract it if needed, but `finalize_run` should still read it from `LoadedModules` directly since module loading is a one-time event per pipeline.

- [ ] **Step 6: Remove or update orchestrator tests that reference deleted types**

Lines ~2728-2806 and ~2946-2949 in orchestrator.rs have tests using `SourceTimingMaxima`, `DestTimingMaxima`, `observe_source_timing`, `observe_dest_timing`. Remove these tests since the types no longer exist. Add replacement tests:

```rust
#[test]
fn finalize_run_produces_pipeline_result_from_snapshot() {
    // Setup: create a SnapshotReader, record some metrics, verify
    // that finalize_run produces a PipelineResult with correct values.
    // This is more of an integration test — may need to init OTel in test.
}
```

- [ ] **Step 7: Verify engine compiles**

Run: `cargo check -p rapidbyte-engine`
Expected: compiles

- [ ] **Step 8: Run engine tests**

Run: `cargo test -p rapidbyte-engine`
Expected: PASS

- [ ] **Step 9: Run workspace tests**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add crates/rapidbyte-engine/
git commit -m "refactor(engine): replace finalize_run aggregation with OTel metric snapshot"
```

---

## Chunk 4: Controller/Agent Instrumentation

### Task 18: Add controller domain metrics

**Files:**
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`

**Important context:** The instrumentation points are spread across three files:
- `pipeline_service.rs`: `submit_pipeline`, `get_run`, `cancel_run`, `list_runs` — where `runs_submitted`, `runs_completed`, `active_runs` metrics go
- `agent_service.rs`: `register_agent`, `heartbeat`, `poll_task`, `complete_task` — where `heartbeat_received`, `heartbeat_timeouts`, `active_agents`, `lease_grants`, `tasks_assigned`, `tasks_completed` metrics go
- `server.rs`: background tasks (reap dead agents, expire leases, reconciliation sweep, preview cleanup) — where `reconciliation_sweeps`, `state_persist_duration` metrics go

- [ ] **Step 1: Add rapidbyte-metrics and opentelemetry dependencies**

In `crates/rapidbyte-controller/Cargo.toml`:
```toml
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
opentelemetry = { workspace = true }
```

(`opentelemetry` is needed for `KeyValue`. Alternatively, `rapidbyte-metrics` can re-export it — if a `pub use opentelemetry::KeyValue;` is added to `rapidbyte-metrics/src/lib.rs` in Task 1, then only `rapidbyte-metrics` is needed here.)

- [ ] **Step 2: Add metric calls in pipeline_service.rs**

```rust
use opentelemetry::KeyValue;

// In submit_pipeline():
rapidbyte_metrics::instruments::controller::runs_submitted()
    .add(1, &[KeyValue::new("status", "accepted")]);
rapidbyte_metrics::instruments::controller::active_runs().add(1, &[]);

// In complete_task() or wherever run completion is handled:
rapidbyte_metrics::instruments::controller::runs_completed()
    .add(1, &[KeyValue::new("status", "ok")]);
rapidbyte_metrics::instruments::controller::active_runs().add(-1, &[]);
```

- [ ] **Step 3: Add metric calls in agent_service.rs**

```rust
use opentelemetry::KeyValue;

// In heartbeat():
rapidbyte_metrics::instruments::controller::heartbeat_received()
    .add(1, &[KeyValue::new("agent_id", agent_id.clone())]);

// In register_agent():
rapidbyte_metrics::instruments::controller::active_agents().add(1, &[]);

// In poll_task() on lease grant:
rapidbyte_metrics::instruments::controller::lease_grants().add(1, &[]);
```

- [ ] **Step 4: Add metric calls in server.rs background tasks**

```rust
// In reap_dead_agents background task (on heartbeat timeout):
rapidbyte_metrics::instruments::controller::heartbeat_timeouts().add(1, &[]);
rapidbyte_metrics::instruments::controller::active_agents().add(-1, &[]);

// In reconciliation sweep background task:
rapidbyte_metrics::instruments::controller::reconciliation_sweeps().add(1, &[]);

// Around state persist calls:
let start = Instant::now();
// ... persist call ...
rapidbyte_metrics::instruments::controller::state_persist_duration()
    .record(start.elapsed().as_secs_f64(), &[]);
```

- [ ] **Step 5: Verify compiles**

Run: `cargo check -p rapidbyte-controller`
Expected: compiles

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-controller/
git commit -m "feat(controller): add domain metrics instrumentation"
```

---

### Task 19: Add agent domain metrics

**Files:**
- Modify: `crates/rapidbyte-agent/Cargo.toml`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`

**Important context:** Task lifecycle metrics (`tasks_received`, `active_tasks`, `tasks_completed`, `task_duration`) belong in `worker.rs::process_task()` (~lines 311-442), NOT `executor.rs`. The executor receives YAML and produces a result; it has no notion of "task receipt." The worker orchestrates the full task lifecycle.

- [ ] **Step 1: Add rapidbyte-metrics and opentelemetry dependencies**

In `crates/rapidbyte-agent/Cargo.toml`:
```toml
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
opentelemetry = { workspace = true }
```

- [ ] **Step 2: Add task lifecycle metrics in worker.rs**

In `worker.rs::process_task()`:
```rust
use opentelemetry::KeyValue;

// At start of process_task():
rapidbyte_metrics::instruments::agent::tasks_received().add(1, &[]);
rapidbyte_metrics::instruments::agent::active_tasks().add(1, &[]);
let task_start = Instant::now();

// At end of process_task() (both success and error paths):
rapidbyte_metrics::instruments::agent::tasks_completed()
    .add(1, &[KeyValue::new("status", "ok")]);  // or "error"
rapidbyte_metrics::instruments::agent::active_tasks().add(-1, &[]);
rapidbyte_metrics::instruments::agent::task_duration()
    .record(task_start.elapsed().as_secs_f64(), &[KeyValue::new("pipeline", pipeline_name.clone())]);
```

- [ ] **Step 3: Add flight metrics**

In `flight.rs::do_get()`:
```rust
use opentelemetry::KeyValue;

// At start of do_get:
rapidbyte_metrics::instruments::agent::flight_requests()
    .add(1, &[KeyValue::new("method", "do_get")]);

// After encoding batches (count batches before encoding):
let batch_count = batches.len() as u64;
rapidbyte_metrics::instruments::agent::flight_batches_served()
    .add(batch_count, &[KeyValue::new("stream", payload.stream_name.clone())]);
```

Note: Use `payload.stream_name` (from ticket verification), not a local `stream_name` variable.

- [ ] **Step 4: Add spool metrics**

Instrument **inside** `PreviewSpool` methods (since `entries` is private). In `spool.rs`:

```rust
// Inside store() method, after inserting:
rapidbyte_metrics::instruments::agent::previews_stored().add(1, &[]);
rapidbyte_metrics::instruments::agent::spool_entries()
    .record(self.entries.len() as f64, &[]);

// Inside evict_stale() method, after removing:
if count > 0 {
    rapidbyte_metrics::instruments::agent::previews_evicted()
        .add(count as u64, &[KeyValue::new("reason", "ttl")]);
    rapidbyte_metrics::instruments::agent::spool_entries()
        .record(self.entries.len() as f64, &[]);
}

// Inside store_stream(), when spill occurs:
rapidbyte_metrics::instruments::agent::preview_spill_to_disk().add(1, &[]);
```

- [ ] **Step 5: Verify compiles**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles

- [ ] **Step 6: Run agent tests**

Run: `cargo test -p rapidbyte-agent`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-agent/
git commit -m "feat(agent): add domain metrics for worker, flight, and spool"
```

---

### Task 20: Implement gRPC metrics tower layer

**Files:**
- Modify: `crates/rapidbyte-metrics/Cargo.toml`
- Modify: `crates/rapidbyte-metrics/src/grpc_layer.rs`
- Modify: `crates/rapidbyte-metrics/src/instruments.rs`
- Modify: `Cargo.toml` (workspace root)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Add http dependency**

Add `http = "1"` to workspace `[workspace.dependencies]` in root `Cargo.toml`. Then add to `crates/rapidbyte-metrics/Cargo.toml`:
```toml
http = { workspace = true }
tower = { workspace = true }
```

(`tower` is already a workspace dep at v0.5 with "full" features.)

- [ ] **Step 2: Add grpc_request_duration instrument**

In `crates/rapidbyte-metrics/src/instruments.rs`, add a `grpc` module with a dedicated histogram:

```rust
pub mod grpc {
    use opentelemetry::metrics::Histogram;
    use std::sync::OnceLock;

    static REQUEST_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();

    pub fn request_duration() -> &'static Histogram<f64> {
        REQUEST_DURATION.get_or_init(|| {
            opentelemetry::global::meter("rapidbyte")
                .f64_histogram("grpc.request.duration")
                .with_unit("s")
                .with_description("Duration of gRPC requests")
                .build()
        })
    }
}
```

- [ ] **Step 3: Write failing compile-time test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_compiles_with_tower() {
        // Compile-time check that GrpcMetricsLayer implements tower::Layer
        let layer = GrpcMetricsLayer;
        fn assert_layer<L: tower::Layer<tower::util::BoxService<(), (), ()>>>(_l: &L) {}
        assert_layer(&layer);
    }
}
```

Note: This is intentionally a compile-time type check, not a behavioral test. A full integration test (init OTel, send request, verify histogram) requires tonic test harness and is better suited to e2e tests.

- [ ] **Step 4: Run test to verify it fails**

Run: `cargo test -p rapidbyte-metrics grpc`
Expected: FAIL (GrpcMetricsLayer not defined)

- [ ] **Step 5: Implement GrpcMetricsLayer**

```rust
//! Custom tower layer for gRPC RED metrics.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::KeyValue;
use tower::{Layer, Service};

use crate::instruments;

/// Tower layer that records gRPC request metrics.
#[derive(Debug, Clone, Default)]
pub struct GrpcMetricsLayer;

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService { inner }
    }
}

/// Tower service wrapper that records per-request metrics.
#[derive(Debug, Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let method = req.uri().path().to_owned();
        let start = Instant::now();
        // Standard tower clone pattern: swap ready service out, call on the ready clone
        let mut inner = std::mem::replace(&mut self.inner, self.inner.clone());

        Box::pin(async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();
            let status = match &result {
                Ok(_) => "ok",
                Err(_) => "error",
            };
            let labels = [
                KeyValue::new("method", method),
                KeyValue::new("status", status),
            ];

            instruments::grpc::request_duration().record(elapsed, &labels);

            result
        })
    }
}
```

**Implementation notes:**
- The `S::Error: std::fmt::Display` bound may need to be relaxed to `std::fmt::Debug` or `Into<Box<dyn std::error::Error>>` depending on tonic's internal error types. Adjust at implementation time if the compiler rejects it.
- The exact tower Service bounds may need adjustment to match tonic's body types.

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p rapidbyte-metrics grpc`
Expected: PASS

- [ ] **Step 7: Verify compiles**

Run: `cargo check -p rapidbyte-metrics`
Expected: compiles

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/rapidbyte-metrics/
git commit -m "feat(metrics): add gRPC metrics tower layer with dedicated instrument"
```

---

### Task 21: Wire GrpcMetricsLayer into controller and agent servers

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

The controller uses `tonic::transport::Server::builder()` in `server.rs` (~line 492). The agent also uses `tonic::transport::Server::builder()` in `worker.rs` (~line 113) to serve the Flight service via `FlightServiceServer` (from `arrow-flight`). Both accept `.layer()` calls since tonic's Server builder supports tower layers.

- [ ] **Step 1: Add layer to controller tonic server**

In `crates/rapidbyte-controller/src/server.rs`, find the `Server::builder()` call (~line 492) and add the layer:
```rust
let mut server = Server::builder()
    .layer(rapidbyte_metrics::grpc_layer::GrpcMetricsLayer);
// ... rest of TLS config and .add_service() calls stay the same ...
```

- [ ] **Step 2: Add layer to agent Flight server**

In `crates/rapidbyte-agent/src/worker.rs`, find the `tonic::transport::Server::builder()` call (~line 113) and add the layer:
```rust
let mut flight_server = tonic::transport::Server::builder()
    .layer(rapidbyte_metrics::grpc_layer::GrpcMetricsLayer);
// ... rest of TLS config and .add_service(flight_svc.into_server()) stays the same ...
```

Note: The Flight server uses Arrow Flight protocol, so the gRPC method paths in metric labels will be Arrow Flight paths (e.g., `/arrow.flight.protocol.FlightService/DoGet`) rather than standard Rapidbyte RPC paths. This is expected and useful for monitoring.

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p rapidbyte-controller -p rapidbyte-agent`
Expected: compiles

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-agent/src/worker.rs
git commit -m "feat(controller,agent): wire gRPC metrics layer into servers"
```

---

## Chunk 5: CLI + Export + Cleanup

### Task 22: Update CLI logging with OTel tracing layer

**Files:**
- Modify: `crates/rapidbyte-cli/Cargo.toml`
- Modify: `crates/rapidbyte-cli/src/logging.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Important context:** `logging::init()` is called from `main.rs` (~line 237), not from `commands/run.rs`. The current signature is `pub fn init(verbosity: Verbosity, log_level: &str)` and it uses `tracing_subscriber::fmt()` (not `registry()` + layers). It uses `.with_target(false)` and `.with_writer(std::io::stderr)` — both must be preserved.

- [ ] **Step 1: Add dependencies**

In `crates/rapidbyte-cli/Cargo.toml`:
```toml
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
tracing-opentelemetry = { workspace = true }
```

- [ ] **Step 2: Update logging.rs init() to accept OtelGuard**

The current `init()` uses `tracing_subscriber::fmt()` builder which calls `.init()` directly. Rewrite it to use `tracing_subscriber::registry()` with explicit layers so we can compose the OTel layer:

```rust
pub fn init(verbosity: Verbosity, log_level: &str, otel_guard: Option<&rapidbyte_metrics::OtelGuard>) {
    let env_filter = default_filter(verbosity, log_level);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)  // preserved from original
        .with_writer(std::io::stderr);

    if let Some(guard) = otel_guard {
        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(guard.tracer_provider().tracer("rapidbyte"));
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }
}
```

Add necessary imports: `use tracing_subscriber::prelude::*;`

- [ ] **Step 3: Update main.rs to call metrics::init() and pass guard**

In `crates/rapidbyte-cli/src/main.rs` (~line 237), change:
```rust
// Old:
logging::init(verbosity, &cli.log_level);

// New:
let otel_guard = rapidbyte_metrics::init("rapidbyte-cli").ok();
logging::init(verbosity, &cli.log_level, otel_guard.as_ref());
```

There is exactly one call site at line 237 in `main.rs`. No other files call `logging::init()`.

- [ ] **Step 4: Record process CPU metrics to OTel**

In `commands/run.rs`, after computing `ProcessCpuMetrics` (which returns `Option`), add:
```rust
if let Some(cpu) = &cpu_metrics {
    rapidbyte_metrics::instruments::process::cpu_seconds()
        .record(cpu.cpu_secs, &[]);
}
if let Some(rss) = peak_rss_mb {
    rapidbyte_metrics::instruments::process::peak_rss_bytes()
        .record(rss * 1024.0 * 1024.0, &[]);
}
```

- [ ] **Step 5: Update logging tests (if needed)**

Check `logging.rs` tests — existing tests test `default_filter_for_env()`, not `init()` directly, so they likely need no changes. If any test does call `init()`, pass `None` for the otel_guard parameter.

- [ ] **Step 6: Verify CLI compiles and tests pass**

Run: `cargo test -p rapidbyte-cli`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-cli/
git commit -m "feat(cli): integrate OTel tracing layer and process metrics"
```

---

### Task 23: Add Prometheus HTTP endpoint to controller and agent

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`

**Important context — port conflicts:** The controller's default gRPC port is `9090` and the agent's default Flight port is `9091`. The Prometheus `/metrics` endpoint must use **different** ports. Use `9190` for controller and `9191` for agent (or make configurable). These ports should eventually be added to `ControllerConfig` and `AgentConfig`, but hard-coding is acceptable for the initial implementation.

**Approach:** Use a minimal raw-TCP handler (`tokio::net::TcpListener` + `tokio::io::AsyncWriteExt`) instead of pulling in `hyper`/`axum`. The `/metrics` endpoint returns plaintext with no routing complexity — a raw HTTP response is the simplest option that avoids new dependency declarations.

- [ ] **Step 1: Add metrics init and Prometheus handler to controller**

In `crates/rapidbyte-controller/src/server.rs`, in the `run()` function immediately after `validate_signing_key_config()` and before `initialize_metadata_store()`, add:

```rust
let otel_guard = Arc::new(rapidbyte_metrics::init("rapidbyte-controller")?);

// Spawn Prometheus metrics HTTP server on separate port (not 9090 — that's gRPC)
let metrics_guard = otel_guard.clone();
tokio::spawn(async move {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9190").await.unwrap();
    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let guard = metrics_guard.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let body = guard.prometheus_text();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body,
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
});
```

- [ ] **Step 2: Add metrics init and Prometheus handler to agent**

In `crates/rapidbyte-agent/src/worker.rs`, in the `run()` function after `validate_signing_key_config()` (~line 98) and before binding the Flight listener (~line 102), add the same pattern but on port `9191`:

```rust
let otel_guard = Arc::new(rapidbyte_metrics::init("rapidbyte-agent")?);

// Spawn Prometheus metrics HTTP server (not 9091 — that's Flight)
let metrics_guard = otel_guard.clone();
tokio::spawn(async move {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9191").await.unwrap();
    // ... same accept loop as controller ...
});
```

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p rapidbyte-controller -p rapidbyte-agent`
Expected: compiles

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-agent/src/worker.rs
git commit -m "feat(controller,agent): add Prometheus /metrics HTTP endpoint on ports 9190/9191"
```

---

### Task 24: Final cleanup and workspace verification

- [ ] **Step 1: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all tests PASS

- [ ] **Step 2: Run clippy on workspace**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt --check`
Expected: no formatting issues. If there are issues, run `cargo fmt` and stage the formatted files.

- [ ] **Step 4: Verify all plugins build**

Run:
```bash
cd plugins/sources/postgres && cargo build
cd plugins/destinations/postgres && cargo build
cd plugins/transforms/validate && cargo build
cd plugins/transforms/sql && cargo build
```
Expected: all compile (validate and sql may also import SDK types affected by the migration)

- [ ] **Step 5: Final commit (if any remaining changes)**

Stage only the specific files that were modified during cleanup. Do **not** use `git add -A` — it can accidentally include untracked files (secrets, build artifacts, etc.). Instead:

```bash
git diff --name-only  # check what changed
git add <specific files from diff output>
git commit -m "chore: final cleanup for observability framework"
```

If no files changed during cleanup (all prior tasks committed cleanly), skip this step.
