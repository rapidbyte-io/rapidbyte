# Agent Hexagonal Refactor Design

## Overview

Rewrite `rapidbyte-agent` from scratch using hexagonal architecture, matching the patterns established in `rapidbyte-engine` and `rapidbyte-controller`. Remove the entire preview/Flight/ticket/spool subsystem (dead code). No legacy bridges, no `allow(dead_code)`, no tech debt carried forward.

## Goals

- Hexagonal architecture: domain (ports, types, errors), application (context, use-cases, fakes), adapter (gRPC, engine, metrics, clock)
- Single way of doing things — consistent with engine/controller conventions
- All code justified — no dead code, no unused features
- Full testability via port fakes — no mock gRPC servers needed
- Breaking changes are fine — this is the new default

## What Gets Removed

| File | Lines | Reason |
|------|-------|--------|
| `flight.rs` | 588 | Preview Flight server — dead feature |
| `spool.rs` | 532 | Preview spool (memory + disk) — dead feature |
| `ticket.rs` | 313 | HMAC ticket signing/verification — only used by preview |
| `auth.rs` | 44 | Replaced by adapter-internal auth |
| `proto.rs` | 13 | Proto wrapper moves into adapter |

Total removed: ~1,490 lines of dead/legacy code.

Dependencies removed: `arrow-flight`, `hmac`, `sha2`.

Config fields removed: `flight_listen`, `flight_advertise`, `signing_key`, `preview_ttl`, `flight_tls`, `allow_insecure_default_signing_key`.

## Directory Structure

```
crates/rapidbyte-agent/src/
├── lib.rs                          # Module table, canonical re-exports
├── domain/
│   ├── mod.rs                      # Re-exports
│   ├── error.rs                    # AgentError enum
│   ├── task.rs                     # TaskOutcomeKind, TaskErrorInfo, CommitState, TaskMetrics, TaskExecutionResult
│   ├── progress.rs                 # ProgressSnapshot value object
│   └── ports/
│       ├── mod.rs                  # Port re-exports
│       ├── controller.rs           # ControllerGateway trait + supporting types
│       ├── executor.rs             # PipelineExecutor trait
│       ├── progress.rs             # ProgressCollector trait
│       ├── metrics.rs              # MetricsProvider trait
│       └── clock.rs                # Clock trait
├── application/
│   ├── mod.rs                      # Re-exports
│   ├── context.rs                  # AgentContext (DI container), AgentAppConfig
│   ├── worker.rs                   # run_agent use-case (register, poll loop, shutdown)
│   ├── execute.rs                  # execute_task use-case (parse, run, report)
│   ├── heartbeat.rs                # heartbeat_loop use-case
│   └── testing.rs                  # Fakes for all 5 ports + TestContext factory
├── adapter/
│   ├── mod.rs                      # Re-exports
│   ├── agent_factory.rs            # build_agent_context (composition root)
│   ├── grpc_controller.rs          # ControllerGateway → gRPC client
│   ├── engine_executor.rs          # PipelineExecutor → rapidbyte-engine
│   ├── channel_progress.rs         # ProgressCollector → AtomicProgressCollector
│   ├── metrics.rs                  # MetricsProvider → OTel
│   └── clock.rs                    # Clock → SystemClock
├── proto.rs                        # Generated protobuf types (adapter-internal)
└── build.rs                        # Protobuf code generation
```

## Domain Layer

### Error Types (`domain/error.rs`)

```rust
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(#[from] anyhow::Error),

    #[error("controller error: {0}")]
    Controller(String),

    #[error("cancelled")]
    Cancelled,
}
```

### Task Types (`domain/task.rs`)

```rust
pub enum TaskOutcomeKind {
    Completed,
    Failed(TaskErrorInfo),
    Cancelled,
}

pub struct TaskErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub safe_to_retry: bool,
    pub commit_state: CommitState,
}

pub enum CommitState {
    BeforeCommit,
    AfterCommitUnknown,
    AfterCommitConfirmed,
}

pub struct TaskMetrics {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub elapsed_seconds: f64,
    pub cursors_advanced: u64,
}

pub struct TaskExecutionResult {
    pub outcome: TaskOutcomeKind,
    pub metrics: TaskMetrics,
}
```

### Progress Types (`domain/progress.rs`)

```rust
#[derive(Debug, Clone, Default)]
pub struct ProgressSnapshot {
    pub message: Option<String>,
    pub progress_pct: Option<f64>,
}
```

## Port Definitions

### ControllerGateway (`domain/ports/controller.rs`)

Single outbound port for all controller communication. Four methods covering the full agent-controller protocol.

```rust
#[async_trait]
pub trait ControllerGateway: Send + Sync {
    async fn register(&self, config: &RegistrationConfig) -> Result<String, AgentError>;
    async fn poll(&self, agent_id: &str, wait_seconds: u32) -> Result<Option<TaskAssignment>, AgentError>;
    async fn heartbeat(&self, request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError>;
    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError>;
}
```

Supporting domain types (not protobuf-derived):

```rust
pub struct RegistrationConfig {
    pub max_tasks: u32,
}

pub struct TaskAssignment {
    pub task_id: String,
    pub run_id: String,
    pub pipeline_yaml: Vec<u8>,
    pub lease_epoch: u64,
}

pub struct HeartbeatPayload {
    pub agent_id: String,
    pub tasks: Vec<TaskHeartbeat>,
}

pub struct TaskHeartbeat {
    pub task_id: String,
    pub lease_epoch: u64,
    pub progress: ProgressSnapshot,
}

pub struct HeartbeatResponse {
    pub cancel_task_ids: Vec<String>,
}

pub struct CompletionPayload {
    pub agent_id: String,
    pub task_id: String,
    pub run_id: String,
    pub lease_epoch: u64,
    pub result: TaskExecutionResult,
}
```

### PipelineExecutor (`domain/ports/executor.rs`)

Takes parsed `PipelineConfig`, not raw YAML. Application layer owns parsing.

```rust
#[async_trait]
pub trait PipelineExecutor: Send + Sync {
    async fn execute(
        &self,
        config: &PipelineConfig,
        cancel: CancellationToken,
        progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError>;
}
```

### ProgressCollector (`domain/ports/progress.rs`)

Sync port — no I/O. Read-side interface for heartbeat use-case.

```rust
pub trait ProgressCollector: Send + Sync {
    fn latest(&self) -> ProgressSnapshot;
    fn reset(&self);
}
```

### MetricsProvider (`domain/ports/metrics.rs`)

Provides OTel handles to the executor adapter.

```rust
pub trait MetricsProvider: Send + Sync {
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader;
    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider;
}
```

### Clock (`domain/ports/clock.rs`)

Monotonic time for backoff and intervals.

```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> std::time::Instant;
}
```

## Application Layer

### DI Container (`application/context.rs`)

```rust
pub struct AgentContext {
    pub gateway: Arc<dyn ControllerGateway>,
    pub executor: Arc<dyn PipelineExecutor>,
    pub progress: Arc<dyn ProgressCollector>,
    pub metrics: Arc<dyn MetricsProvider>,
    pub clock: Arc<dyn Clock>,
    pub config: AgentAppConfig,
}

pub struct AgentAppConfig {
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
    pub completion_retry_delay: Duration,
    pub max_completion_retries: u32,
}
```

### Use Cases

Three use-cases matching the three concurrent activities:

**`run_agent` (`application/worker.rs`)** — Top-level entry point. Registers with controller, spawns heartbeat loop, runs poll loop, handles graceful shutdown.

**`execute_task` (`application/execute.rs`)** — Single task lifecycle: parse YAML into `PipelineConfig`, execute via `PipelineExecutor` port, handle cancellation semantics (pre/post-commit), report completion to controller with retry logic.

**`heartbeat_loop` (`application/heartbeat.rs`)** — Periodic heartbeat: reads latest progress from `ProgressCollector`, sends to controller, processes cancel directives.

### Shared State

`ActiveLeaseMap = Arc<RwLock<HashMap<String, LeaseEntry>>>` shared between worker, heartbeat, and task execution. Each active task registers its lease entry (run_id, lease_epoch, cancel_token). Heartbeat reads all entries. Cancel directives trigger the corresponding `CancellationToken`.

```rust
pub struct LeaseEntry {
    pub run_id: String,
    pub lease_epoch: u64,
    pub cancel: CancellationToken,
    pub progress: Arc<dyn ProgressCollector>,
}
```

### Completion Retry Logic

Lives in `application/execute.rs`. Retries on transient controller errors (Unavailable, DeadlineExceeded). Stops immediately on non-retryable errors (Unauthenticated, PermissionDenied, NotFound, InvalidArgument, Aborted, FailedPrecondition). Respects shutdown signal.

### Progress Bridging

`execute_task` spawns a bridge task that reads `ProgressEvent` from the engine's `mpsc::UnboundedReceiver` and calls `AtomicProgressCollector::update()`. The heartbeat reads via the `ProgressCollector::latest()` trait method. Bridge is aborted and progress reset when execution completes.

## Adapter Layer

### GrpcControllerGateway (`adapter/grpc_controller.rs`)

- Implements `ControllerGateway`
- Connects via tonic `Channel` with optional TLS
- All proto ↔ domain conversion lives here
- Bearer auth applied to every request
- Error classification: maps tonic `Status` codes to `AgentError`

### EngineExecutor (`adapter/engine_executor.rs`)

- Implements `PipelineExecutor`
- Calls `rapidbyte_engine::build_run_context` then `run_pipeline`
- Converts `PipelineResult` → `TaskMetrics`
- Converts `PipelineError` → `TaskOutcomeKind` (including pre/post-commit detection via `retry_params()`)
- `PipelineError::Cancelled` → `AgentError::Cancelled`

### AtomicProgressCollector (`adapter/channel_progress.rs`)

- Implements `ProgressCollector` (read-side)
- Concrete `update()` method (write-side, used by bridge task)
- Backed by `RwLock<ProgressSnapshot>`

### OtelMetricsProvider (`adapter/metrics.rs`)

- Implements `MetricsProvider`
- Wraps `SnapshotReader` and `SdkMeterProvider`

### SystemClock (`adapter/clock.rs`)

- Implements `Clock`
- Returns `Instant::now()`

### build_agent_context (`adapter/agent_factory.rs`)

Composition root. Constructs all adapters and wires `AgentContext`:

1. Connect `GrpcControllerGateway` (with TLS + auth)
2. Build `OtelMetricsProvider` from OTel guard
3. Build `EngineExecutor` with registry config + metrics
4. Create `AtomicProgressCollector`
5. Create `SystemClock`
6. Assemble `AgentContext`
7. Return `(AgentContext, RegistrationConfig)`

## Testing

### Fakes (`application/testing.rs`)

Feature-gated: `#[cfg(any(test, feature = "test-support"))]`

| Fake | Port | Pattern |
|------|------|---------|
| `FakeControllerGateway` | `ControllerGateway` | FIFO result queues + recorded calls |
| `FakePipelineExecutor` | `PipelineExecutor` | FIFO result queue |
| `FakeProgressCollector` | `ProgressCollector` | `RwLock<ProgressSnapshot>` |
| `FakeMetricsProvider` | `MetricsProvider` | No-op |
| `FakeClock` | `Clock` | Controllable `Mutex<Instant>` with `advance()` |

### TestContext Factory

```rust
pub fn fake_context() -> TestContext
```

Returns `TestContext` with both `AgentContext` (trait objects) and typed fake references for test assertions. Same pattern as engine's `fake_context()` and controller's `fake_context()`.

### Test Coverage Targets

- **worker.rs**: Registration failure, poll returning tasks, graceful shutdown, max concurrent tasks
- **execute.rs**: Invalid YAML → failed outcome, successful execution, cancellation (pre/post-commit), completion retry on transient error, completion stops on non-retryable error
- **heartbeat.rs**: Progress forwarding, cancel directive handling, shutdown

## Public API (`lib.rs`)

```rust
pub mod domain;
pub mod application;
pub mod adapter;

pub use application::{run_agent, AgentContext, AgentAppConfig};
pub use adapter::{build_agent_context, AgentConfig, ClientTlsConfig};
pub use domain::{AgentError, TaskExecutionResult, TaskOutcomeKind};
```

CLI usage:

```rust
let config = build_cli_agent_config(args)?;
let (ctx, registration) = rapidbyte_agent::build_agent_context(&config, otel_guard).await?;
rapidbyte_agent::run_agent(&ctx, registration, shutdown_token).await?;
```

## Dependency Changes

### Removed

- `arrow-flight` — preview Flight server removed
- `hmac`, `sha2` — ticket signing removed
- `arrow` — no longer needed (was only for preview batches)

### Retained

- `rapidbyte-engine`, `rapidbyte-pipeline-config`, `rapidbyte-types` — core pipeline execution
- `rapidbyte-metrics`, `rapidbyte-registry`, `rapidbyte-runtime`, `rapidbyte-secrets` — forwarded to engine
- `tonic`, `prost`, `prost-types` — gRPC client
- `tokio`, `tokio-util` — async runtime
- `anyhow`, `thiserror` — error handling
- `tracing` — logging
- `opentelemetry`, `opentelemetry_sdk` — metrics
- `uuid` — agent ID generation
- `async-trait` — async port traits

### Added

None.

## Conventions

- `#![warn(clippy::pedantic)]` — no suppressions unless justified with a comment
- No `#[allow(dead_code)]` — all code must be reachable
- Module doc table in `lib.rs`
- Top-level re-exports for common public types
- `pub(crate)` for internal types
- Error types use `thiserror`, public APIs return typed errors
- Port traits use `#[async_trait]` where async, plain traits where sync
- Adapters follow naming: `GrpcControllerGateway`, `EngineExecutor`, `AtomicProgressCollector`, `OtelMetricsProvider`, `SystemClock`
- Tests use `fake_context()` pattern — no mock gRPC servers
