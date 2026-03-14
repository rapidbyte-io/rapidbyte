# Observability Framework Design

**Date:** 2026-03-13
**Status:** Draft
**Scope:** New `rapidbyte-metrics` crate, OTel integration across all components, WIT interface changes, metrics refactor

---

## Overview

Unified observability framework for Rapidbyte using OpenTelemetry as the single metrics and tracing API. Replaces the current hand-rolled timing accumulators and JSON-over-WIT metric boundary with OTel-native instruments and typed WIT calls.

**Goals:**
- Single source of truth for all metrics (OTel instruments)
- Prometheus scrape endpoint for long-lived processes (controller, agent)
- OTLP export for traces and metrics (env-var driven, zero config)
- Typed plugin metric boundary (no JSON parsing on hot path)
- Bounded label cardinality (metrics safe for Prometheus, high-cardinality data on spans)
- Clean removal of legacy metric accumulation code

**Non-goals:**
- Backward compatibility with old metric types or WIT interface
- Pipeline YAML config for observability (OTel env vars only)
- Trace context propagation into WASM plugins (future work)

---

## Design Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Metrics API | OTel unified API + Prometheus exporter |
| 2 | Plugin boundary | Typed WIT interface (`counter-add`, `gauge-set`, `histogram-record`) |
| 3 | Label strategy | Two-tier: bounded labels on metrics, high-cardinality on spans |
| 4 | Refactor scope | Replace: existing types become thin wrappers around OTel instruments |
| 5 | Tracing | Keep `tracing` crate, bridge via `tracing-opentelemetry` |
| 6 | Controller/agent | Both RED transport metrics + domain-level metrics |
| 7 | Configuration | OTel env var conventions (`OTEL_*`), no YAML config |
| 8 | Backward compat | None required. Clean break. |

---

## 1. New `rapidbyte-metrics` Crate

### Position in dependency graph

```
types (leaf)
  +-- metrics  -> types
  +-- state    -> types
  +-- runtime  -> types, state, metrics
  +-- sdk      -> types
  +-- engine   -> types, runtime, state, metrics
  |   +-- dev  -> engine, runtime, types, state, metrics
  |   +-- cli  -> engine, runtime, types, metrics
controller -> metrics
agent      -> metrics
```

`dev` depends on `metrics` directly because it runs pipelines and needs to call `metrics::init()`.

### Dependencies

- `opentelemetry` (API + SDK)
- `opentelemetry_sdk` (meter provider, tracer provider, resource)
- `opentelemetry-otlp` (OTLP exporter for metrics + traces)
- `opentelemetry-prometheus` (Prometheus exporter bridge)
- `prometheus` (registry, text encoder)
- `tracing-opentelemetry` (tracing -> OTel span bridge)

### Public API

```rust
/// Initialize OTel metrics + tracing pipeline.
/// Reads OTEL_* env vars for exporter config.
/// Returns a guard that flushes on drop.
///
/// MUST be called before any instrument accessor functions.
/// Sets the global MeterProvider used by OnceLock instrument registration.
pub fn init(service_name: &str) -> Result<OtelGuard>;

pub struct OtelGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    prometheus_registry: prometheus::Registry,
}

impl OtelGuard {
    pub fn tracer_provider(&self) -> &TracerProvider;
    pub fn meter_provider(&self) -> &SdkMeterProvider;

    /// Render Prometheus text exposition format from the registry.
    /// Uses `prometheus::TextEncoder` to gather and encode metrics.
    pub fn prometheus_text(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.prometheus_registry.gather();
        encoder.encode_to_string(&metric_families).unwrap_or_default()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Flush pending metric/trace exports
    }
}
```

### Initialization and global meter

`init()` sets the OTel global meter provider. All `OnceLock`-based instrument accessors obtain their `Meter` from `opentelemetry::global::meter("rapidbyte")`. This means:

- If `init()` is not called, instruments use the no-op global provider (safe but silent).
- `init()` must be called once at process start (before pipeline execution) for real export.

```rust
pub fn init(service_name: &str) -> Result<OtelGuard> {
    let resource = Resource::new(vec![
        KeyValue::new("service.name", service_name.to_owned()),
    ]);

    // Prometheus exporter: creates a Registry and wires it as a metric reader
    let prometheus_registry = prometheus::Registry::new();
    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus_registry.clone())
        .build()?;

    let mut meter_provider_builder = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(prometheus_exporter);

    // OTLP exporter (optional, env-driven)
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        meter_provider_builder = meter_provider_builder
            .with_reader(PeriodicReader::builder(otlp_exporter).build());
    }

    let meter_provider = meter_provider_builder.build();
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

    Ok(OtelGuard { tracer_provider, meter_provider, prometheus_registry })
}
```

### Instrument modules

Each module exposes lazily-registered instruments via `OnceLock`. Instruments obtain their `Meter` from the OTel global provider:

```rust
use std::sync::OnceLock;
use opentelemetry::global;

pub mod pipeline {
    pub fn records_read() -> &'static Counter<u64> {
        static INST: OnceLock<Counter<u64>> = OnceLock::new();
        INST.get_or_init(|| global::meter("rapidbyte").u64_counter("pipeline.records_read").build())
    }
    pub fn records_written() -> &'static Counter<u64>;
    pub fn bytes_read() -> &'static Counter<u64>;
    pub fn bytes_written() -> &'static Counter<u64>;
    pub fn duration() -> &'static Histogram<f64>;
    pub fn run_total() -> &'static Counter<u64>;
    pub fn run_errors() -> &'static Counter<u64>;
}

pub mod host {
    pub fn emit_batch_duration() -> &'static Histogram<f64>;
    pub fn next_batch_duration() -> &'static Histogram<f64>;
    pub fn next_batch_wait_duration() -> &'static Histogram<f64>;
    pub fn next_batch_process_duration() -> &'static Histogram<f64>;
    pub fn compress_duration() -> &'static Histogram<f64>;
    pub fn decompress_duration() -> &'static Histogram<f64>;
    pub fn module_load_duration() -> &'static Histogram<f64>;
}

pub mod plugin {
    pub fn source_connect_duration() -> &'static Histogram<f64>;
    pub fn source_query_duration() -> &'static Histogram<f64>;
    pub fn source_fetch_duration() -> &'static Histogram<f64>;
    pub fn source_encode_duration() -> &'static Histogram<f64>;
    pub fn dest_connect_duration() -> &'static Histogram<f64>;
    pub fn dest_flush_duration() -> &'static Histogram<f64>;
    pub fn dest_commit_duration() -> &'static Histogram<f64>;
    pub fn dest_decode_duration() -> &'static Histogram<f64>;

    /// Dynamic plugin metrics use a DashMap cache to avoid re-registering
    /// instruments on every WIT call. Keyed by metric name.
    pub fn custom_counter(name: &str) -> Counter<u64>;
    pub fn custom_gauge(name: &str) -> Gauge<f64>;
    pub fn custom_histogram(name: &str) -> Histogram<f64>;
}

pub mod controller {
    pub fn active_runs() -> &'static UpDownCounter<i64>;
    pub fn active_agents() -> &'static UpDownCounter<i64>;
    pub fn preview_store_size() -> &'static Gauge<f64>;  // f64: OTel Rust SDK has no Gauge<u64>
    pub fn runs_submitted() -> &'static Counter<u64>;
    pub fn runs_completed() -> &'static Counter<u64>;
    pub fn tasks_assigned() -> &'static Counter<u64>;
    pub fn tasks_completed() -> &'static Counter<u64>;
    pub fn lease_grants() -> &'static Counter<u64>;
    pub fn lease_revocations() -> &'static Counter<u64>;
    pub fn lease_renewals() -> &'static Counter<u64>;
    pub fn heartbeat_received() -> &'static Counter<u64>;
    pub fn heartbeat_timeouts() -> &'static Counter<u64>;
    pub fn reconciliation_sweeps() -> &'static Counter<u64>;
    pub fn reconciliation_timeouts() -> &'static Counter<u64>;
    pub fn state_persist_duration() -> &'static Histogram<f64>;
    pub fn state_persist_errors() -> &'static Counter<u64>;
}

pub mod agent {
    pub fn active_tasks() -> &'static UpDownCounter<i64>;
    pub fn spool_entries() -> &'static Gauge<f64>;      // f64: OTel Rust SDK has no Gauge<u64>
    pub fn spool_disk_bytes() -> &'static Gauge<f64>;   // f64: OTel Rust SDK has no Gauge<u64>
    pub fn tasks_received() -> &'static Counter<u64>;
    pub fn tasks_completed() -> &'static Counter<u64>;
    pub fn task_duration() -> &'static Histogram<f64>;
    pub fn records_processed() -> &'static Counter<u64>;
    pub fn bytes_processed() -> &'static Counter<u64>;
    pub fn flight_requests() -> &'static Counter<u64>;
    pub fn flight_request_duration() -> &'static Histogram<f64>;
    pub fn flight_batches_served() -> &'static Counter<u64>;
    pub fn previews_stored() -> &'static Counter<u64>;
    pub fn previews_evicted() -> &'static Counter<u64>;
    pub fn preview_spill_to_disk() -> &'static Counter<u64>;
}

pub mod process {
    pub fn cpu_seconds() -> &'static Gauge<f64>;
    pub fn peak_rss_bytes() -> &'static Gauge<f64>;
}
```

### Custom instrument cache

Dynamic plugin metrics (`custom_counter`, `custom_gauge`, `custom_histogram`) use a `DashMap<String, Instrument>` to cache instruments by name. This avoids re-registering on every WIT call:

```rust
use dashmap::DashMap;
use std::sync::OnceLock;

static CUSTOM_COUNTERS: OnceLock<DashMap<String, Counter<u64>>> = OnceLock::new();

pub fn custom_counter(name: &str) -> Counter<u64> {
    let map = CUSTOM_COUNTERS.get_or_init(DashMap::new);
    map.entry(name.to_owned())
        .or_insert_with(|| global::meter("rapidbyte.plugin").u64_counter(name).build())
        .clone()
}
```

### Histogram bucket configuration

Different duration scales require different histogram bucket boundaries. Configure via OTel `View` on the meter provider:

```rust
// Sub-millisecond operations (compress, decompress, emit_batch, next_batch)
let fast_buckets = vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0];

// Normal operations (plugin connect, query, flush, commit, module load)
let normal_buckets = vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

// Long operations (pipeline duration, task duration)
let slow_buckets = vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0];
```

Views are registered on the `SdkMeterProvider` during `init()`, matching instrument names by prefix pattern (e.g., `host.*` -> fast_buckets, `plugin.*` -> normal_buckets, `pipeline.duration` -> slow_buckets).

### Label keys (bounded set)

```rust
pub mod labels {
    pub const PIPELINE: &str = "pipeline";
    pub const STREAM: &str = "stream";
    pub const SOURCE: &str = "source";
    pub const DESTINATION: &str = "destination";
    pub const SHARD: &str = "shard";
    pub const STATUS: &str = "status";    // "ok" | "error" | "cancelled"
    pub const METHOD: &str = "method";    // gRPC method name
    pub const REASON: &str = "reason";    // "ttl" | "broken_storage"
    pub const AGENT_ID: &str = "agent_id"; // bounded by fleet size
}
```

### Label validation

When parsing labels from plugin WIT calls (`labels-json` parameter), the host validates label keys against the bounded set. Unknown label keys are silently dropped with a `tracing::debug!` log. This prevents cardinality explosion from misbehaving plugins while avoiding hard errors that would disrupt pipeline execution.

### PipelineResult snapshot

The OTel Rust SDK does not expose a "read current value" API on instruments. To bridge OTel metrics to `PipelineResult` for CLI display, we use an `InMemoryMetricReader`:

```rust
/// Registered as an additional reader on the MeterProvider during init().
/// Allows collecting a point-in-time snapshot of all metrics.
struct SnapshotReader {
    reader: InMemoryMetricReader,
}

impl SnapshotReader {
    /// Force a collect and return all metric data points.
    /// Filter by pipeline label to isolate per-pipeline results.
    fn collect_for_pipeline(&self, pipeline: &str) -> PipelineResult {
        let mut resource_metrics = ResourceMetrics::default();
        self.reader.collect(&mut resource_metrics).ok();
        // Walk resource_metrics.scope_metrics[].metrics[].data
        // Filter data points by pipeline label
        // Map instrument names to PipelineResult fields
        build_pipeline_result(pipeline, &resource_metrics)
    }
}
```

The `InMemoryMetricReader` is registered alongside the Prometheus and optional OTLP readers during `init()`. For multi-pipeline scenarios, each pipeline's metrics are isolated by the `pipeline` label — the snapshot filters data points by matching this label value.

```rust
pub fn snapshot_pipeline_result(
    pipeline: &str,
    snapshot_reader: &SnapshotReader,
) -> PipelineResult {
    snapshot_reader.collect_for_pipeline(pipeline)
}
```

---

## 2. WIT Interface Changes

The three new functions are **added to** the existing `interface host` block in `rapidbyte-plugin.wit`. Only the old `metric` function is removed. All other host imports (~20 functions) remain unchanged.

### Remove

```wit
metric: func(payload-json: string) -> result<_, plugin-error>;
```

### Add (to existing `interface host` block)

```wit
    // Typed metric calls (replace `metric`)
    counter-add: func(name: string, value: u64, labels-json: string) -> result<_, plugin-error>;
    gauge-set: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;
    histogram-record: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;
```

Labels are passed as JSON string (`{"stream":"users","pipeline":"sync"}`) — keeps WIT boundary simple, host validates against bounded label set (unknown keys silently dropped).

### Host-side implementation

Each typed call routes to the appropriate OTel instrument. These functions do **not** need the `Mutex<HostTimings>` lock — OTel instruments are internally thread-safe. This is a performance improvement over the current `metric_impl()` which holds a mutex:

```rust
fn counter_add_impl(name: &str, value: u64, labels_json: &str) {
    let labels = parse_bounded_labels(labels_json);
    match name {
        "records_read" => metrics::pipeline::records_read().add(value, &labels),
        "records_written" => metrics::pipeline::records_written().add(value, &labels),
        _ => metrics::plugin::custom_counter(name).add(value, &labels),
    }
}

fn histogram_record_impl(name: &str, value: f64, labels_json: &str) {
    let labels = parse_bounded_labels(labels_json);
    match name {
        "source_connect_secs" => metrics::plugin::source_connect_duration().record(value, &labels),
        "source_query_secs" => metrics::plugin::source_query_duration().record(value, &labels),
        "source_fetch_secs" => metrics::plugin::source_fetch_duration().record(value, &labels),
        "source_arrow_encode_secs" => metrics::plugin::source_encode_duration().record(value, &labels),
        "dest_connect_secs" => metrics::plugin::dest_connect_duration().record(value, &labels),
        "dest_flush_secs" => metrics::plugin::dest_flush_duration().record(value, &labels),
        "dest_commit_secs" => metrics::plugin::dest_commit_duration().record(value, &labels),
        "dest_arrow_decode_secs" => metrics::plugin::dest_decode_duration().record(value, &labels),
        _ => metrics::plugin::custom_histogram(name).record(value, &labels),
    }
}

fn gauge_set_impl(name: &str, value: f64, labels_json: &str) {
    let labels = parse_bounded_labels(labels_json);
    metrics::plugin::custom_gauge(name).record(value, &labels);
}
```

---

## 3. Plugin SDK Changes

### Remove

- `Context::metric(&self, m: &Metric)`
- `host_ffi::metric()` function
- `Metric` struct, `MetricValue` enum from `rapidbyte-types`

### Add

```rust
impl Context {
    pub fn counter(&self, name: &str, value: u64) {
        self.counter_with_labels(name, value, &[]);
    }

    pub fn counter_with_labels(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        let merged = self.merge_default_labels(labels);
        let _ = host_ffi::counter_add(name, value, &merged);
    }

    pub fn gauge(&self, name: &str, value: f64) {
        self.gauge_with_labels(name, value, &[]);
    }

    pub fn gauge_with_labels(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let merged = self.merge_default_labels(labels);
        let _ = host_ffi::gauge_set(name, value, &merged);
    }

    pub fn histogram(&self, name: &str, value: f64) {
        self.histogram_with_labels(name, value, &[]);
    }

    pub fn histogram_with_labels(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let merged = self.merge_default_labels(labels);
        let _ = host_ffi::histogram_record(name, value, &merged);
    }

    /// Injects {plugin, stream} on every metric call.
    fn merge_default_labels(&self, extra: &[(&str, &str)]) -> String {
        // JSON serialize plugin_id + stream_name + extra labels
    }
}
```

### Plugin migration table

Old `ctx.metric()` calls map to new typed calls as follows. Note: the old code used `MetricValue::Gauge` for duration observations — these are semantically histograms (distribution of durations), not gauges (last-value):

| Old call | New call | Rationale |
|----------|----------|-----------|
| `ctx.metric(&Metric{name:"source_connect_secs", value: Gauge(v), ..})` | `ctx.histogram("source_connect_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"source_query_secs", value: Gauge(v), ..})` | `ctx.histogram("source_query_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"source_fetch_secs", value: Gauge(v), ..})` | `ctx.histogram("source_fetch_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"source_arrow_encode_secs", value: Gauge(v), ..})` | `ctx.histogram("source_arrow_encode_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"dest_connect_secs", value: Gauge(v), ..})` | `ctx.histogram("dest_connect_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"dest_flush_secs", value: Gauge(v), ..})` | `ctx.histogram("dest_flush_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"dest_commit_secs", value: Gauge(v), ..})` | `ctx.histogram("dest_commit_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"dest_arrow_decode_secs", value: Gauge(v), ..})` | `ctx.histogram("dest_arrow_decode_secs", v)` | Duration observation → histogram |
| `ctx.metric(&Metric{name:"rows_per_second", value: Gauge(v), ..})` | `ctx.gauge("rows_per_second", v)` | Point-in-time rate → gauge |
| `ctx.metric(&Metric{name: custom, value: Counter(v), ..})` | `ctx.counter(custom, v)` | Direct mapping |

---

## 4. HostTimings Replacement

### Current (remove)

16 raw `u64`/`f64` accumulator fields with manual `+=` in each host import, behind a `Mutex`.

### New

`HostTimings` becomes a label-holder that records directly to OTel instruments. No mutex needed — OTel instruments are internally thread-safe. This removes lock contention on the batch emission hot path.

```rust
pub struct HostTimings {
    labels: Vec<KeyValue>,
}

impl HostTimings {
    pub fn new(pipeline: &str, stream: &str, shard: usize) -> Self {
        Self {
            labels: vec![
                KeyValue::new("pipeline", pipeline.to_owned()),
                KeyValue::new("stream", stream.to_owned()),
                KeyValue::new("shard", shard.to_string()),
            ],
        }
    }

    pub fn record_emit_batch(&self, duration: Duration) {
        metrics::host::emit_batch_duration().record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_next_batch(&self, total: Duration, wait: Duration, process: Duration) {
        metrics::host::next_batch_duration().record(total.as_secs_f64(), &self.labels);
        metrics::host::next_batch_wait_duration().record(wait.as_secs_f64(), &self.labels);
        metrics::host::next_batch_process_duration().record(process.as_secs_f64(), &self.labels);
    }

    pub fn record_compress(&self, duration: Duration) {
        metrics::host::compress_duration().record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_decompress(&self, duration: Duration) {
        metrics::host::decompress_duration().record(duration.as_secs_f64(), &self.labels);
    }
}
```

Call sites in `host_state.rs` change from:
```rust
self.timings.emit_batch_nanos += elapsed;
self.timings.emit_batch_count += 1;
```
To:
```rust
self.timings.record_emit_batch(start.elapsed());
```

### Plugin-reported timing fallback

Currently the orchestrator's `build_source_timing()` / `build_dest_timing()` prefer `ReadPerf`/`WritePerf` from plugin summaries but fall back to accumulated `HostTimings` fields. Post-migration, plugin-reported timings flow through typed WIT calls directly to OTel histograms. The `snapshot_pipeline_result()` function reads these histogram values from the `InMemoryMetricReader`, so the fallback path is replaced by: always read from OTel instruments, which contain data regardless of whether the plugin used `ReadPerf` or individual `histogram-record` calls. Both paths record to the same OTel instruments.

---

## 5. PipelineResult & CLI Output

### PipelineResult

Struct definition unchanged. Populated via `metrics::snapshot_pipeline_result()` instead of manual aggregation in `finalize_run()`.

### finalize_run() replacement

Current ~130 lines of manual maxima/sum aggregation collapses to:

```rust
fn finalize_run(&self) -> PipelineResult {
    let mut result = metrics::snapshot_pipeline_result(
        &self.pipeline_name,
        &self.snapshot_reader,
    );
    result.stream_metrics = self.collect_stream_shard_metrics();
    result
}
```

The `snapshot_reader` is passed to the orchestrator at construction time (created during `init()`).

### Multi-pipeline isolation

OTel instruments are global singletons. Per-pipeline isolation is achieved via the `pipeline` label on every metric recording. The `InMemoryMetricReader` snapshot returns all metrics; `snapshot_pipeline_result()` filters data points by the `pipeline` label value to produce per-pipeline results. For the CLI (which runs a single pipeline), there is exactly one `pipeline` label value, so no ambiguity.

### Removed

- `SourceTimingMaxima` / `DestTimingMaxima` helper structs
- Manual nanos-to-secs conversion math
- ~80 lines of per-field maxima accumulation

### Unchanged

- `PipelineResult` struct
- `SourceTiming`, `DestTiming` as view types (populated from OTel reads)
- `StreamShardMetric` (per-shard skew, still collected directly)
- `ReadSummary` / `WriteSummary` / `TransformSummary` (plugin return values, runner records into OTel)
- CLI `print_compact()`, `print_verbose()`, `print_diagnostic()`
- Bench `@@BENCH_JSON@@` output format

---

## 6. Tracing Integration

### Approach

Keep `tracing` crate as authoring API. Bridge to OTel via `tracing-opentelemetry` layer.

### CLI logging.rs changes

```rust
pub fn init(verbosity: Verbosity, log_level: &str, otel_guard: &OtelGuard) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr);

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(otel_guard.tracer_provider().tracer("rapidbyte"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();
}
```

### Key spans

```rust
// orchestrator.rs
#[tracing::instrument(skip_all, fields(pipeline = %name))]
pub async fn run_pipeline(...)

// runner.rs
#[tracing::instrument(skip_all, fields(pipeline, stream, plugin.type, plugin.name))]
fn run_source_plugin(...)

// host_state.rs (debug level to avoid hot-path overhead)
#[tracing::instrument(level = "debug", skip_all, fields(stream))]
fn emit_batch_impl(...)
```

### Trace context propagation

- **Across gRPC (CLI -> controller -> agent):** Yes, via tonic interceptor that injects/extracts W3C `traceparent` in gRPC metadata. Use `opentelemetry::propagation::TextMapPropagator` with a tower layer.
- **Into WASM plugins:** No. Plugins are in-process. Parent span captures wall-clock time. Future work if distributed plugin execution is needed.

---

## 7. Controller & Agent Metrics

### Transport layer (automatic via tower middleware)

There is no off-the-shelf `OtelGrpcLayer` crate. Implement as a custom `tower::Layer` that wraps each gRPC request in a span and records RED metrics:

```rust
/// Custom tower layer for gRPC observability.
/// Records: request count (counter), request duration (histogram),
/// error count (counter), per gRPC method and status code.
pub struct GrpcMetricsLayer;

impl<S> tower::Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService { inner }
    }
}
```

Wire into server:

```rust
Server::builder()
    .layer(GrpcMetricsLayer)
    .add_service(pipeline_service)
    .add_service(agent_service)
    .serve(addr)
```

Provides per-method: request count, duration histogram, error count — following OTel gRPC semantic conventions (`rpc.server.duration`, `rpc.grpc.status_code`).

### Controller domain metrics

| Metric | Type | Labels | Instrumentation point |
|--------|------|--------|-----------------------|
| `active_runs` | UpDownCounter | - | submit +1, complete/fail -1 |
| `active_agents` | UpDownCounter | - | register +1, timeout -1 |
| `preview_store_size` | Gauge | - | after store/remove |
| `runs_submitted` | Counter | status | on submit |
| `runs_completed` | Counter | status | on complete/fail/cancel |
| `tasks_assigned` | Counter | pipeline | on poll_task grant |
| `tasks_completed` | Counter | pipeline, status | on complete_task |
| `lease_grants` | Counter | - | on lease grant |
| `lease_revocations` | Counter | - | on revocation |
| `lease_renewals` | Counter | - | on renewal |
| `heartbeat_received` | Counter | agent_id | on heartbeat |
| `heartbeat_timeouts` | Counter | - | on timeout detection |
| `reconciliation_sweeps` | Counter | - | on sweep tick |
| `reconciliation_timeouts` | Counter | - | on reconciliation timeout |
| `state_persist_duration` | Histogram | - | around persist call |
| `state_persist_errors` | Counter | - | on persist failure |

### Agent domain metrics

| Metric | Type | Labels | Instrumentation point |
|--------|------|--------|-----------------------|
| `active_tasks` | UpDownCounter | - | receive +1, complete -1 |
| `spool_entries` | Gauge | - | after store/evict |
| `spool_disk_bytes` | Gauge | - | after spill/evict |
| `tasks_received` | Counter | - | on task receipt |
| `tasks_completed` | Counter | status | on completion |
| `task_duration` | Histogram | pipeline | on completion |
| `records_processed` | Counter | pipeline, stream | during execution |
| `bytes_processed` | Counter | pipeline, stream | during execution |
| `flight_requests` | Counter | method | on Flight request |
| `flight_request_duration` | Histogram | method | around Flight handler |
| `flight_batches_served` | Counter | stream | on do_get batch send |
| `previews_stored` | Counter | - | on preview store |
| `previews_evicted` | Counter | reason | on eviction |
| `preview_spill_to_disk` | Counter | - | on spill |

---

## 8. Prometheus Endpoint & Export

### Long-lived processes (controller, agent)

Prometheus scrape endpoint on separate HTTP port (default `:9090/metrics`). Uses the `prometheus::Registry` from `OtelGuard`:

```rust
async fn metrics_handler(State(guard): State<Arc<OtelGuard>>) -> String {
    guard.prometheus_text()
}
```

### Short-lived processes (CLI)

No Prometheus endpoint. Metrics export via OTLP only if `OTEL_EXPORTER_OTLP_ENDPOINT` is set. `PipelineResult` snapshot serves CLI display and bench output.

### Configuration

All via standard OTel env vars:

| Env var | Purpose |
|---------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Enables OTLP export (metrics + traces) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` (default) or `http/protobuf` |
| `OTEL_SERVICE_NAME` | Auto-set by `init()`, overridable |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes |
| `OTEL_METRICS_EXPORTER` | `otlp`, `prometheus`, `none` |
| `OTEL_TRACES_EXPORTER` | `otlp`, `none` |

When no env vars set: in-memory only, zero overhead from unused exporters.

---

## 9. Code Removal

### Types removed

- `Metric` struct (`rapidbyte-types/src/metric.rs`)
- `MetricValue` enum (`rapidbyte-types/src/metric.rs`)
- `SourceTimingMaxima` / `DestTimingMaxima` (`engine/src/orchestrator.rs`)
- 16 raw accumulator fields from `HostTimings` body

### Functions removed

- `metric_impl()` switch statement (`runtime/src/host_state.rs`)
- `Context::metric()` (`sdk/src/context.rs`)
- `host_ffi::metric()` (`sdk/src/host_ffi.rs`)
- Manual aggregation in `finalize_run()` (~80 lines)

### WIT removed

- `metric: func(payload-json: string) -> result<_, plugin-error>`

---

## 10. Implementation Order

1. Add `rapidbyte-metrics` crate (instruments, `init()`, `OtelGuard`, `InMemoryMetricReader`, snapshot, histogram views)
2. Update WIT + SDK (typed metric calls, remove old `metric`, rebuild plugins)
3. Replace `HostTimings` (OTel recording methods, remove mutex)
4. Replace `metric_impl()` (typed host import handlers: `counter_add_impl`, `gauge_set_impl`, `histogram_record_impl`)
5. Update `finalize_run()` (metric snapshot via `InMemoryMetricReader`, remove maxima structs)
6. Add controller/agent domain metrics (instrument calls at each code location)
7. Update CLI `logging.rs` (OTel tracing layer)
8. Add Prometheus endpoint (controller, agent — HTTP handler on separate port)
9. Add gRPC metrics tower layer (custom `GrpcMetricsLayer` for RED metrics)
10. Remove dead code (`Metric`, `MetricValue`, old WIT function, maxima structs)
11. Update tests (verify `PipelineResult` from snapshot, verify Prometheus output, plugin SDK migration)
