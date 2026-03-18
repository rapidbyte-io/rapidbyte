# Engine Hexagonal Refactor Design

**Date:** 2026-03-18
**Status:** Approved
**Scope:** Rewrite `rapidbyte-engine` using hexagonal architecture, extract misplaced concerns to proper crates, eliminate `rapidbyte-state` crate.

## Goals

- Rewrite engine internals from scratch using hexagonal architecture (domain / application / adapter layers)
- Extract config types, parsing, and validation to `rapidbyte-pipeline-config`
- Extract Arrow IPC codec to `rapidbyte-types`
- Eliminate `rapidbyte-state` crate — trait moves to `types`, implementations become engine adapters
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
    ├── sqlite/                     ← impl CursorRepository, RunRecordRepository, DlqRepository
    └── postgres/                   ← impl CursorRepository, RunRecordRepository, DlqRepository
```

### Crate Removed

`rapidbyte-state` — trait moves to `rapidbyte-types::state`, SQLite/Postgres implementations become engine adapter modules.

## Domain Layer

### Ports

Six port traits define the engine's boundaries. All are `Send + Sync`.

#### PluginRunner

Executes a plugin for a single stream. The WASM runtime is the only real implementation; tests use fakes.

```rust
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

Three focused traits replace the monolithic `StateBackend` at the orchestration level.

```rust
pub trait CursorRepository: Send + Sync {
    fn get(&self, pipeline: &PipelineId, stream: &StreamName) -> Result<Option<CursorState>, RepositoryError>;
    fn set(&self, pipeline: &PipelineId, stream: &StreamName, cursor: &CursorState) -> Result<(), RepositoryError>;
    fn compare_and_set(
        &self, pipeline: &PipelineId, stream: &StreamName,
        expected: Option<&str>, new_value: &str,
    ) -> Result<bool, RepositoryError>;
}

pub trait RunRecordRepository: Send + Sync {
    fn start(&self, pipeline: &PipelineId, stream: &StreamName) -> Result<i64, RepositoryError>;
    fn complete(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> Result<(), RepositoryError>;
}

pub trait DlqRepository: Send + Sync {
    fn insert(&self, pipeline: &PipelineId, run_id: i64, records: &[DlqRecord]) -> Result<u64, RepositoryError>;
}
```

#### ProgressReporter

Receives progress events during pipeline execution. Synchronous — events are fire-and-forget.

```rust
pub trait ProgressReporter: Send + Sync {
    fn report(&self, event: ProgressEvent);
}
```

### Error Types

`PipelineError` is factual — it carries what happened, not what to do about it.

```rust
pub enum PipelineError {
    Plugin {
        kind: PluginKind,
        message: String,
        retryable: bool,
        safe_to_retry: bool,
        backoff_class: BackoffClass,
        retry_after: Option<Duration>,
        commit_state: CommitState,
    },
    Infrastructure(anyhow::Error),
}
```

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
) -> Result<PipelineOutcome, PipelineError> { ... }
```

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

```rust
pub struct TestContext {
    pub ctx: EngineContext,
    pub runner: Arc<FakePluginRunner>,
    pub resolver: Arc<FakePluginResolver>,
    pub cursors: Arc<FakeCursorRepository>,
    pub runs: Arc<FakeRunRecordRepository>,
    pub dlq: Arc<FakeDlqRepository>,
    pub progress: Arc<FakeProgressReporter>,
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

### State Backend Adapters

Single struct per backend implements all three repository traits.

#### SQLite

```rust
pub struct SqliteBackend {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteBackend {
    pub fn open(path: &Path) -> Result<Self, PipelineError> { ... }
    pub fn in_memory() -> Result<Self, PipelineError> { ... }
    pub fn as_state_backend(&self) -> Arc<dyn StateBackend> { ... }
}

impl CursorRepository for SqliteBackend { ... }
impl RunRecordRepository for SqliteBackend { ... }
impl DlqRepository for SqliteBackend { ... }
```

#### Postgres

```rust
pub struct PgBackend {
    pool: PgPool,
}

impl CursorRepository for PgBackend { ... }
impl RunRecordRepository for PgBackend { ... }
impl DlqRepository for PgBackend { ... }
```

### Wiring Example

Callers (CLI, agent) construct `EngineContext` with real adapters:

```rust
let backend = SqliteBackend::open(&db_path)?;
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
    config: engine_config,
};
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
  │     adapter:      WasmPluginRunner, RegistryPluginResolver,
  │                   SqliteBackend, PgBackend
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
crates/rapidbyte-engine/src/
├── lib.rs
├── domain/
│   ├── mod.rs
│   ├── ports/
│   │   ├── mod.rs
│   │   ├── runner.rs               # PluginRunner
│   │   ├── resolver.rs             # PluginResolver
│   │   ├── cursor.rs               # CursorRepository
│   │   ├── run_record.rs           # RunRecordRepository
│   │   ├── dlq.rs                  # DlqRepository
│   │   └── progress.rs             # ProgressReporter
│   ├── error.rs                    # PipelineError, RepositoryError
│   ├── retry.rs                    # RetryPolicy, BackoffClass, RetryDecision
│   ├── outcome.rs                  # PipelineOutcome, PipelineResult, CheckResult, etc.
│   └── progress.rs                 # Phase, ProgressEvent
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
    ├── sqlite/
    │   ├── mod.rs                  # SqliteBackend
    │   ├── cursor.rs
    │   ├── run_record.rs
    │   └── dlq.rs
    └── postgres/
        ├── mod.rs                  # PgBackend
        ├── cursor.rs
        ├── run_record.rs
        └── dlq.rs
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
    CursorRepository, DlqRepository, PluginResolver,
    PluginRunner, ProgressReporter, RunRecordRepository,
};
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

## Breaking Changes

1. All `rapidbyte_engine::config::*` imports → `rapidbyte_pipeline_config::*`
2. All `rapidbyte_engine::arrow::*` imports → `rapidbyte_types::arrow::*`
3. Benchmarks rewritten to use `run_pipeline()` instead of `run_*_stream()`
4. CLI/agent must construct `EngineContext` with adapters (explicit DI, no implicit wiring)
5. `rapidbyte-state` crate removed from workspace
6. Engine's `runner/`, `plugin/`, `pipeline/`, `finalizers/` modules no longer public
