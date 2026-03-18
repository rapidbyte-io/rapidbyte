# Engine Hexagonal Refactor Design

**Date:** 2026-03-18
**Status:** Approved
**Scope:** Rewrite `rapidbyte-engine` using hexagonal architecture, extract misplaced concerns to proper crates, eliminate `rapidbyte-state` crate.

## Goals

- Rewrite engine internals from scratch using hexagonal architecture (domain / application / adapter layers)
- Extract config types, parsing, and validation to `rapidbyte-pipeline-config`
- Extract Arrow IPC codec to `rapidbyte-types`
- Eliminate `rapidbyte-state` crate — trait moves to `types`, Postgres implementation becomes engine adapter
- Drop SQLite state backend support — Postgres only, matching the controller
- Clean cut: no legacy code, no bridge layers, no `v2_` prefixes, no `allow(dead_code)`
- Breaking changes to public API are acceptable
- All code must be coherent, justified, and pass `clippy::pedantic` without suppression

## Crate Boundary Changes

### Before

```
rapidbyte-engine (big, mixed concerns)
├── config/          → parsing, validation, types
├── plugin/          → resolution, loading, sandbox
├── runner/          → WASM invocation
├── pipeline/        → execution, planning, scheduling
├── finalizers/      → checkpoint, metrics, DLQ
├── arrow.rs         → IPC codec
├── orchestrator.rs  → top-level entry points
├── error.rs         → PipelineError + retry policy
├── outcome.rs       → result types
└── progress.rs      → progress events
```

### After

```
rapidbyte-types (leaf)
├── ... existing protocol types ...
├── arrow.rs                        ← MOVED from engine (IPC codec)
└── state.rs                        ← StateBackend trait (moved from rapidbyte-state)

rapidbyte-pipeline-config (expanded)
├── ... existing substitution ...
├── types.rs                        ← MOVED from engine/config/types.rs
├── parser.rs                       ← MOVED from engine/config/parser.rs
└── validator.rs                    ← MOVED from engine/config/validator.rs

rapidbyte-engine (lean orchestration, hexagonal)
├── domain/
│   ├── ports/                      ← trait definitions
│   ├── error.rs                    ← factual errors only
│   ├── retry.rs                    ← RetryPolicy, BackoffClass
│   ├── outcome.rs                  ← execution results
│   └── progress.rs                 ← progress events
├── application/
│   ├── context.rs                  ← DI container (EngineContext)
│   ├── run.rs                      ← run_pipeline use case
│   ├── check.rs                    ← check_pipeline use case
│   ├── discover.rs                 ← discover_plugin use case
│   └── testing.rs                  ← fakes for all ports
└── adapter/
    ├── wasm_runner.rs              ← impl PluginRunner (Wasmtime)
    ├── registry_resolver.rs        ← impl PluginResolver (OCI registry)
    └── postgres/                   ← impl CursorRepository, RunRecordRepository, DlqRepository, StateBackend
migrations/
    └── 0001_engine_initial.sql     ← SQLx versioned migration (cursors, runs, DLQ tables)
```

### Crate Removed

`rapidbyte-state` — trait moves to `rapidbyte-types::state`, Postgres implementation becomes an engine adapter module. SQLite support is dropped entirely.

## Domain Layer

### Ports

Six port traits define the engine's boundaries. All are `Send + Sync`. Async port traits use `#[async_trait]` for object safety (`Arc<dyn Trait>`), matching the controller pattern.

#### PluginRunner

Executes a plugin for a single stream. The WASM runtime is the only real implementation; tests use fakes.

```rust
#[async_trait]
pub trait PluginRunner: Send + Sync {
    async fn run_source(&self, ctx: &SourceRunParams) -> Result<SourceOutcome, PipelineError>;
    async fn run_transform(&self, ctx: &TransformRunParams) -> Result<TransformOutcome, PipelineError>;
    async fn run_destination(&self, ctx: &DestinationRunParams) -> Result<DestinationOutcome, PipelineError>;
    async fn validate_plugin(&self, ctx: &ValidateParams) -> Result<CheckComponentStatus, PipelineError>;
    async fn discover(&self, ctx: &DiscoverParams) -> Result<Vec<DiscoveredStream>, PipelineError>;
}
```

#### PluginResolver

Resolves plugin references to loadable modules with validated manifests.

```rust
#[async_trait]
pub trait PluginResolver: Send + Sync {
    async fn resolve(
        &self,
        plugin_ref: &str,
        expected_kind: PluginKind,
        config_json: Option<&serde_json::Value>,
    ) -> Result<ResolvedPlugin, PipelineError>;
}
```

#### Focused State Repositories

Three focused traits replace the monolithic `StateBackend` at the orchestration level. All are async, using `sqlx` with Postgres — matching the controller's async repository pattern.

Note: these are engine-internal orchestration ports. The monolithic `StateBackend` trait (moved to `rapidbyte-types`) continues to serve the runtime's host imports during WASM execution. The engine's `PgBackend` adapter implements *both* the focused repository traits (for orchestration) and the `StateBackend` trait (for runtime host imports). See the "Dual-Trait Pattern" section under Adapter Layer.

```rust
#[async_trait]
pub trait CursorRepository: Send + Sync {
    async fn get(&self, pipeline: &PipelineId, stream: &StreamName) -> Result<Option<CursorState>, RepositoryError>;
    async fn set(&self, pipeline: &PipelineId, stream: &StreamName, cursor: &CursorState) -> Result<(), RepositoryError>;
    async fn compare_and_set(
        &self, pipeline: &PipelineId, stream: &StreamName,
        expected: Option<&str>, new_value: &str,
    ) -> Result<bool, RepositoryError>;
}

#[async_trait]
pub trait RunRecordRepository: Send + Sync {
    async fn start(&self, pipeline: &PipelineId, stream: &StreamName) -> Result<i64, RepositoryError>;
    async fn complete(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait DlqRepository: Send + Sync {
    async fn insert(&self, pipeline: &PipelineId, run_id: i64, records: &[DlqRecord]) -> Result<u64, RepositoryError>;
}
```

#### ProgressReporter

Receives progress events during pipeline execution. Synchronous — events are fire-and-forget.

```rust
pub trait ProgressReporter: Send + Sync {
    fn report(&self, event: ProgressEvent);
}
```

#### MetricsSnapshot

Provides access to OpenTelemetry metric snapshots for computing timing breakdowns (`SourceTiming`, `DestTiming`, WASM overhead). The real implementation reads from `rapidbyte-metrics::snapshot::SnapshotReader`; tests return empty/fixed snapshots.

```rust
pub trait MetricsSnapshot: Send + Sync {
    fn snapshot_for_run(&self) -> RunMetricsSnapshot;
}
```

### Error Types

`PipelineError` is factual — it carries what happened, not what to do about it. The `Plugin` variant wraps the existing `PluginError` from `rapidbyte-types::error` (which carries `retryable`, `safe_to_retry`, `ErrorCategory`, `code`, `scope`, `commit_state`, `retry_after_ms`, etc.) rather than flattening its fields. This preserves the full error structure and avoids a lossy conversion boundary.

```rust
pub enum PipelineError {
    /// Error originating from a plugin. Wraps the existing PluginError from rapidbyte-types.
    Plugin(PluginError),
    /// Infrastructure failure (WASM load, state backend, channel closed). Never retryable.
    Infrastructure(anyhow::Error),
    /// Graceful cancellation (via CancellationToken). Distinct from Plugin/Infrastructure
    /// so callers (CLI, agent) can distinguish cancellation from real failures for reporting.
    Cancelled,
}

impl PipelineError {
    pub fn infra(msg: impl Into<String>) -> Self { ... }
    pub fn plugin_error(&self) -> Option<&PluginError> { ... }

    /// Extract retry-relevant fields from the wrapped PluginError for RetryPolicy.
    pub fn retry_params(&self) -> Option<RetryParams> { ... }
}

/// Extracted retry-relevant fields, passed to RetryPolicy::should_retry().
pub struct RetryParams {
    pub retryable: bool,
    pub safe_to_retry: bool,
    pub backoff_class: BackoffClass,
    pub retry_after: Option<Duration>,
    pub commit_state: CommitState,
}
```

The `WasmPluginRunner` adapter converts runtime-level errors to `PipelineError::Plugin(PluginError)` using the existing `source_error_to_sdk()` / `dest_error_to_sdk()` / `transform_error_to_sdk()` conversion functions. The `BackoffClass` is derived from `PluginError::category` at the conversion boundary.

`RepositoryError` follows the controller pattern:

```rust
pub enum RepositoryError {
    Conflict(String),
    Other(Box<dyn Error + Send + Sync>),
}
```

### Retry Policy

Pure business rules — no I/O, fully unit-testable.

```rust
pub enum BackoffClass { Fast, Normal, Slow }

pub enum RetryDecision {
    Retry { delay: Duration },
    GiveUp { reason: String },
}

pub struct RetryPolicy { max_attempts: u32 }

impl RetryPolicy {
    pub fn should_retry(
        &self, attempt: u32, retryable: bool, safe_to_retry: bool,
        backoff_class: BackoffClass, retry_after_hint: Option<Duration>,
    ) -> RetryDecision { ... }
}
```

Backoff computation: `base_ms * 2^(attempt-1)`, capped at 60s. Base values: Fast=100ms, Normal=1s, Slow=5s.

### Outcome Types

Clean data containers, no business logic.

```rust
pub enum PipelineOutcome { Run(PipelineResult), DryRun(DryRunResult) }

pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source_timing: SourceTiming,
    pub dest_timing: DestTiming,
    pub stream_results: Vec<StreamResult>,
    pub retry_count: u32,
}

pub struct DryRunResult { pub streams: Vec<DryRunStreamResult> }
pub struct DryRunStreamResult { pub stream_name: String, pub batches: Vec<RecordBatch> }
pub struct CheckResult { pub components: Vec<CheckComponentStatus> }
pub enum CheckStatus { Ok, Warning(String), Error(String) }
```

### Progress Events

```rust
pub enum Phase { Resolving, Loading, Running, Finalizing }

pub enum ProgressEvent {
    PhaseChanged { phase: Phase },
    StreamStarted { stream: String },
    BatchEmitted { stream: String, rows: u64 },
    StreamCompleted { stream: String },
    RetryScheduled { attempt: u32, delay: Duration },
}
```

## Application Layer

### DI Container

```rust
pub struct EngineConfig {
    pub max_retries: u32,
    pub channel_capacity: usize,
    pub default_parallelism: PipelineParallelism,
}

pub struct EngineContext {
    pub runner: Arc<dyn PluginRunner>,
    pub resolver: Arc<dyn PluginResolver>,
    pub cursors: Arc<dyn CursorRepository>,
    pub runs: Arc<dyn RunRecordRepository>,
    pub dlq: Arc<dyn DlqRepository>,
    pub progress: Arc<dyn ProgressReporter>,
    pub metrics: Arc<dyn MetricsSnapshot>,
    pub config: EngineConfig,
}
```

### Use Cases

Free functions taking `&EngineContext`, same pattern as controller's `register()`, `submit_pipeline()`.

#### run_pipeline

The core orchestration use case. Owns the retry loop.

```rust
pub async fn run_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
    options: &ExecutionOptions,
    cancel: CancellationToken,
) -> Result<PipelineOutcome, PipelineError> { ... }
```

The `CancellationToken` (from `tokio_util::sync`) enables graceful shutdown. The orchestrator checks `cancel.is_cancelled()` at phase boundaries (before resolve, before execute, between retry attempts). When cancelled, returns `PipelineError::Cancelled`.

Internal flow:
1. Resolve plugins via `ctx.resolver`
2. Load cursors from `ctx.cursors`, build stream plans
3. Execute streams with parallelism control (semaphore)
4. On error: consult `RetryPolicy::should_retry()` — retry or give up
5. On success: finalize (aggregate metrics, correlate checkpoints via `ctx.runs`, `ctx.dlq`)

Stream planning, execution, and finalization are private helper functions within `run.rs`.

#### check_pipeline

```rust
pub async fn check_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
) -> Result<CheckResult, PipelineError> { ... }
```

Resolves plugins, validates manifests + config schemas, calls `runner.validate_plugin()` for each component.

#### discover_plugin

```rust
pub async fn discover_plugin(
    ctx: &EngineContext,
    source_ref: &str,
    config_json: Option<&serde_json::Value>,
) -> Result<Vec<DiscoveredStream>, PipelineError> { ... }
```

### Testing Infrastructure

All fakes live in `application/testing.rs`. No mocking libraries — fakes are written once and reused.

**Visibility:** Gated behind `#[cfg(any(test, feature = "test-support"))]`. Intra-crate unit tests get fakes via `#[cfg(test)]`; external integration test crates and benchmarks enable the `test-support` feature. Fakes are never compiled in release builds.

```rust
pub struct TestContext {
    pub ctx: EngineContext,
    pub runner: Arc<FakePluginRunner>,
    pub resolver: Arc<FakePluginResolver>,
    pub cursors: Arc<FakeCursorRepository>,
    pub runs: Arc<FakeRunRecordRepository>,
    pub dlq: Arc<FakeDlqRepository>,
    pub progress: Arc<FakeProgressReporter>,
    pub metrics: Arc<FakeMetricsSnapshot>,
}

pub fn fake_context() -> TestContext { ... }
```

- `FakePluginRunner` uses `VecDeque` for enqueued results
- `FakeCursorRepository`, `FakeRunRecordRepository`, `FakeDlqRepository` use `HashMap`
- `FakeProgressReporter` captures events in `Vec<ProgressEvent>` for assertions

## Adapter Layer

### WasmPluginRunner

Wraps `rapidbyte-runtime`. Builds Wasmtime host state, instantiates components, calls plugin lifecycle (open/run/close), extracts outcomes.

```rust
pub struct WasmPluginRunner {
    runtime: WasmRuntime,
}

impl PluginRunner for WasmPluginRunner { ... }
```

Receives `Arc<dyn StateBackend>` (from `rapidbyte-types`) at construction time for runtime host imports.

### RegistryPluginResolver

Wraps `rapidbyte-registry`. Resolves OCI references, loads manifests, validates protocol version + kind + config schema.

```rust
pub struct RegistryPluginResolver {
    registry_config: Option<RegistryConfig>,
}

impl PluginResolver for RegistryPluginResolver { ... }
```

### ChannelProgressReporter

Adapter that sends progress events over an `mpsc` channel. Used by CLI and agent to bridge progress events to their respective UI/streaming layers.

```rust
pub struct ChannelProgressReporter {
    tx: mpsc::UnboundedSender<ProgressEvent>,
}

impl ChannelProgressReporter {
    pub fn new(tx: mpsc::UnboundedSender<ProgressEvent>) -> Self { Self { tx } }
}

impl ProgressReporter for ChannelProgressReporter {
    fn report(&self, event: ProgressEvent) {
        let _ = self.tx.send(event); // Best-effort, dropped if receiver closed
    }
}
```

### OtelMetricsSnapshot

Adapter wrapping `rapidbyte-metrics` snapshot reader.

```rust
pub struct OtelMetricsSnapshot {
    reader: SnapshotReader,
}

impl MetricsSnapshot for OtelMetricsSnapshot { ... }
```

### Postgres State Adapter

Postgres-only. SQLite support is dropped — Postgres is the single state backend, matching the controller.

#### Dual-Trait Pattern

`PgBackend` implements **two sets of traits**:

1. **Focused repository ports** (`CursorRepository`, `RunRecordRepository`, `DlqRepository`) — used by the engine's orchestration layer for stream planning, finalization, and DLQ handling. These are async, using `sqlx`.
2. **Monolithic `StateBackend` trait** (from `rapidbyte-types`) — used by the runtime's host imports during WASM plugin execution (cursor reads/writes, DLQ inserts within plugin calls). This is synchronous; `PgBackend` uses a dedicated blocking `postgres::Client` connection for this trait.

Both trait sets are backed by the same Postgres database. The `as_state_backend()` method returns an `Arc<dyn StateBackend>` that the engine passes to `WasmPluginRunner` at construction time.

```rust
pub struct PgBackend {
    pool: PgPool,            // async (sqlx) — for repository ports
    sync_pool: ClientPool,   // sync connection pool — for StateBackend trait
}

// ClientPool: small pool of sync postgres::Client connections (1-8, based on parallelism).
// Needed because multiple WASM plugin streams may call StateBackend concurrently
// during parallel stream execution. Carried over from the existing PostgresStateBackend.
//
// Temporal ordering guarantees correctness between async and sync paths:
// orchestration reads (async) happen before/after WASM execution (sync), never concurrently.

impl PgBackend {
    pub async fn connect(connstr: &str) -> Result<Self, PipelineError> { ... }

    /// Run SQLx migrations (creates/updates tables).
    pub async fn migrate(&self) -> Result<(), PipelineError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Returns an Arc<dyn StateBackend> for runtime host imports.
    pub fn as_state_backend(&self) -> Arc<dyn StateBackend> { ... }
}

// Focused repository ports (for engine orchestration, async)
impl CursorRepository for PgBackend { ... }
impl RunRecordRepository for PgBackend { ... }
impl DlqRepository for PgBackend { ... }

// Monolithic trait (for runtime host imports, sync)
impl StateBackend for PgBackend { ... }
```

#### Migrations

SQLx versioned migrations, same pattern as the controller. Stored in `crates/rapidbyte-engine/migrations/`.

```
crates/rapidbyte-engine/migrations/
└── 0001_initial.sql
```

`0001_engine_initial.sql` creates the state tables (migrated from `rapidbyte-state`). Column names match the existing `CursorState`, `RunStats`, and `DlqRecord` types from `rapidbyte-types`. Timestamps upgraded from `TEXT` (ISO 8601 strings) to `TIMESTAMPTZ` (native Postgres).

Note: the filename is prefixed with `engine_` to avoid migration checksum collisions with the controller's `0001_initial.sql` if both crates connect to the same Postgres database. SQLx tracks applied migrations in `_sqlx_migrations` and would error on checksum mismatch if two crates share a migration number with different content.

```sql
CREATE TABLE IF NOT EXISTS sync_cursors (
    pipeline     TEXT NOT NULL,
    stream       TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    stream          TEXT NOT NULL,
    status          TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    records_read    BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    bytes_read      BIGINT DEFAULT 0,
    bytes_written   BIGINT DEFAULT 0,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS dlq_records (
    id             BIGSERIAL PRIMARY KEY,
    pipeline       TEXT NOT NULL,
    run_id         BIGINT NOT NULL REFERENCES sync_runs(id),
    stream_name    TEXT NOT NULL,
    record_json    TEXT NOT NULL,
    error_message  TEXT NOT NULL,
    error_category TEXT NOT NULL,
    failed_at      TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_pipeline ON sync_runs(pipeline, stream);
CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_run ON dlq_records(pipeline, run_id);
```

Migrations are applied at startup via `backend.migrate()` before constructing `EngineContext`. SQLx tracks applied migrations in `_sqlx_migrations` table — each migration runs exactly once.

### Wiring Example

Callers (CLI, agent) construct `EngineContext` with real adapters:

```rust
let backend = PgBackend::connect(&connstr).await?;
backend.migrate().await?;
let state_backend = backend.as_state_backend();
let backend = Arc::new(backend);

let runner = Arc::new(WasmPluginRunner::new(runtime, state_backend));

let ctx = EngineContext {
    runner,
    resolver: Arc::new(RegistryPluginResolver::new(registry_config)),
    cursors: Arc::clone(&backend) as Arc<dyn CursorRepository>,
    runs: Arc::clone(&backend) as Arc<dyn RunRecordRepository>,
    dlq: Arc::clone(&backend) as Arc<dyn DlqRepository>,
    progress: Arc::new(ChannelProgressReporter::new(tx)),
    metrics: Arc::new(OtelMetricsSnapshot::new(snapshot_reader)),
    config: engine_config,
};

let cancel = CancellationToken::new();
let outcome = run_pipeline(&ctx, &pipeline, &options, cancel.clone()).await?;
```

## Dependency Graph After Refactor

```
types (leaf — gains StateBackend trait + Arrow IPC)
  │
  ├── secrets → (leaf, no internal deps)
  │
  ├── pipeline-config → types, secrets
  │     (gains config types, parser, validator from engine)
  │
  ├── registry → types
  │
  ├── runtime → types
  │     (uses StateBackend trait from types, receives impl via Arc<dyn>)
  │
  ├── metrics → types
  │
  └── engine → types, runtime, pipeline-config, registry, secrets, metrics
  │     domain:       error, retry, outcome, progress, port traits
  │     application:  run/check/discover use cases, EngineContext DI
  │     adapter:      WasmPluginRunner, RegistryPluginResolver, PgBackend
  │
  ├── controller → types, pipeline-config, secrets, metrics (unchanged)
  ├── agent → engine, types, pipeline-config
  ├── cli → engine, types, pipeline-config, controller, dev
  └── dev → engine, runtime, types
```

### Crate Removed

`rapidbyte-state` — eliminated entirely.

## Complete Engine Module Layout

```
crates/rapidbyte-engine/
├── migrations/
│   └── 0001_engine_initial.sql     # sync_cursors, sync_runs, dlq_records tables
├── src/
│   ├── lib.rs
├── domain/
│   ├── mod.rs
│   ├── ports/
│   │   ├── mod.rs
│   │   ├── runner.rs               # PluginRunner
│   │   ├── resolver.rs             # PluginResolver
│   │   ├── cursor.rs               # CursorRepository
│   │   ├── run_record.rs           # RunRecordRepository
│   │   ├── dlq.rs                  # DlqRepository
│   │   └── metrics.rs              # MetricsSnapshot
│   │   # Note: ProgressReporter lives in domain/progress.rs alongside Phase/ProgressEvent
│   ├── error.rs                    # PipelineError, RepositoryError, RetryParams
│   ├── retry.rs                    # RetryPolicy, BackoffClass, RetryDecision
│   ├── outcome.rs                  # PipelineOutcome, PipelineResult, CheckResult, etc.
│   └── progress.rs                 # Phase, ProgressEvent, ProgressReporter trait
├── application/
│   ├── mod.rs
│   ├── context.rs                  # EngineContext, EngineConfig
│   ├── run.rs                      # run_pipeline() + helpers
│   ├── check.rs                    # check_pipeline()
│   ├── discover.rs                 # discover_plugin()
│   └── testing.rs                  # Fakes + fake_context()
└── adapter/
    ├── mod.rs
    ├── wasm_runner.rs              # WasmPluginRunner
    ├── registry_resolver.rs        # RegistryPluginResolver
    ├── progress.rs                 # ChannelProgressReporter
    ├── metrics.rs                  # OtelMetricsSnapshot
    └── postgres/
        ├── mod.rs                  # PgBackend (connect, migrate, as_state_backend)
        ├── cursor.rs               # impl CursorRepository
        ├── run_record.rs           # impl RunRecordRepository
        ├── dlq.rs                  # impl DlqRepository
        └── state_backend.rs        # impl StateBackend (sync, for runtime host imports)
```

## Public API Surface

### Top-Level Re-exports

```rust
pub use application::context::{EngineContext, EngineConfig};
pub use application::run::run_pipeline;
pub use application::check::check_pipeline;
pub use application::discover::discover_plugin;
pub use domain::error::PipelineError;
pub use domain::outcome::{
    CheckResult, CheckStatus, DryRunResult, DryRunStreamResult,
    ExecutionOptions, PipelineOutcome, PipelineResult,
};
pub use domain::progress::{Phase, ProgressEvent};
pub use domain::ports::{
    CursorRepository, DlqRepository, MetricsSnapshot, PluginResolver,
    PluginRunner, RunRecordRepository,
};
pub use domain::progress::ProgressReporter;
```

### Consumer Migration

| Consumer | Before | After |
|----------|--------|-------|
| CLI | `rapidbyte_engine::orchestrator::run_pipeline` | `rapidbyte_engine::run_pipeline` |
| CLI | `rapidbyte_engine::config::parser::parse_pipeline` | `rapidbyte_pipeline_config::parse_pipeline` |
| CLI | `rapidbyte_engine::config::validator::validate_pipeline` | `rapidbyte_pipeline_config::validate_pipeline` |
| Agent | `rapidbyte_engine::config::parser` | `rapidbyte_pipeline_config::parse_pipeline` |
| Dev | `rapidbyte_engine::arrow::ipc_to_record_batches` | `rapidbyte_types::arrow::ipc_to_record_batches` |
| Benchmarks | `rapidbyte_engine::runner::*` (low-level) | `rapidbyte_engine::run_pipeline` (orchestrator) |

### What's Removed From Public API

- `runner/` module — private, inside `WasmPluginRunner`
- `plugin/` module — private, inside `RegistryPluginResolver`
- `finalizers/` module — private, inside `run.rs`
- `pipeline/` module — private, inside `run.rs`
- `arrow` module — moved to `rapidbyte-types`
- `config/` module — moved to `rapidbyte-pipeline-config`

## Config Changes

When config types move to `rapidbyte-pipeline-config`, `StateConfig` is simplified since Postgres is the only backend:

```rust
// Before (in engine/config/types.rs)
pub struct StateConfig {
    pub backend: StateBackendKind,  // Sqlite | Postgres
    pub connection: Option<String>,
}

// After (in rapidbyte-pipeline-config/types.rs)
pub struct StateConfig {
    pub connection: String,  // Postgres connection string, required
}
```

- The `backend` field is removed — there is only Postgres
- `connection` becomes required (no default to SQLite)
- Existing pipeline YAML files with `state.backend: sqlite` will fail validation with a clear error
- Pipeline YAML files omitting `state:` entirely will fail validation (connection is required)
- The YAML format simplifies to `state: { connection: "postgres://..." }`

## Breaking Changes

1. All `rapidbyte_engine::config::*` imports → `rapidbyte_pipeline_config::*`
2. All `rapidbyte_engine::arrow::*` imports → `rapidbyte_types::arrow::*`
3. Benchmarks rewritten to use `run_pipeline()` instead of `run_*_stream()`
4. CLI/agent must construct `EngineContext` with adapters (explicit DI, no implicit wiring)
5. `rapidbyte-state` crate removed from workspace; `StateBackend` trait moves to `rapidbyte-types`
6. SQLite state backend dropped — Postgres is the only supported state backend
7. Engine's `runner/`, `plugin/`, `pipeline/`, `finalizers/` modules no longer public
8. `run_pipeline()` signature gains `cancel: CancellationToken` parameter
9. `PipelineError::Plugin` wraps `PluginError` (struct variant → tuple variant)
10. `ProgressEvent` variants renamed: `PhaseChange` → `PhaseChanged`, `Retry` → `RetryScheduled`; new `StreamStarted` variant added
11. `CheckResult` simplified from per-component-type fields to `Vec<CheckComponentStatus>` (each entry carries component name and kind)
12. State backend requires Postgres — engine runs SQLx migrations at startup via `PgBackend::migrate()`
