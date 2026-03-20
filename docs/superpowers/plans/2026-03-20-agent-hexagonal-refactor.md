# Agent Hexagonal Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `rapidbyte-agent` from scratch using hexagonal architecture, removing all preview/Flight/ticket dead code.

**Architecture:** Domain layer (ports, types, errors) → Application layer (DI context, use-cases, fakes) → Adapter layer (gRPC client, engine executor, progress, metrics, clock, factory). Matches `rapidbyte-engine` and `rapidbyte-controller` conventions exactly.

**Tech Stack:** Rust, async-trait, tokio, tonic (gRPC client), rapidbyte-engine, rapidbyte-pipeline-config, rapidbyte-metrics, opentelemetry

**Spec:** `docs/superpowers/specs/2026-03-20-agent-hexagonal-refactor-design.md`

---

### Task 1: Strip old source files and update Cargo.toml

**Files:**
- Delete: `crates/rapidbyte-agent/src/worker.rs`
- Delete: `crates/rapidbyte-agent/src/executor.rs`
- Delete: `crates/rapidbyte-agent/src/progress.rs`
- Delete: `crates/rapidbyte-agent/src/flight.rs`
- Delete: `crates/rapidbyte-agent/src/spool.rs`
- Delete: `crates/rapidbyte-agent/src/ticket.rs`
- Delete: `crates/rapidbyte-agent/src/auth.rs`
- Delete: `crates/rapidbyte-agent/src/proto.rs`
- Modify: `crates/rapidbyte-agent/Cargo.toml`
- Modify: `crates/rapidbyte-agent/src/lib.rs`

- [ ] **Step 1: Delete all old source files**

```bash
rm crates/rapidbyte-agent/src/worker.rs \
   crates/rapidbyte-agent/src/executor.rs \
   crates/rapidbyte-agent/src/progress.rs \
   crates/rapidbyte-agent/src/flight.rs \
   crates/rapidbyte-agent/src/spool.rs \
   crates/rapidbyte-agent/src/ticket.rs \
   crates/rapidbyte-agent/src/auth.rs \
   crates/rapidbyte-agent/src/proto.rs
```

- [ ] **Step 2: Update Cargo.toml — remove dead dependencies, add async-trait**

Replace `crates/rapidbyte-agent/Cargo.toml` with:

```toml
[package]
name = "rapidbyte-agent"
version.workspace = true
edition.workspace = true

[dependencies]
rapidbyte-types = { path = "../rapidbyte-types" }
rapidbyte-engine = { path = "../rapidbyte-engine" }
rapidbyte-pipeline-config = { path = "../rapidbyte-pipeline-config" }
rapidbyte-secrets = { path = "../rapidbyte-secrets" }
rapidbyte-metrics = { path = "../rapidbyte-metrics" }
rapidbyte-registry = { path = "../rapidbyte-registry" }
opentelemetry = { workspace = true }
opentelemetry_sdk = { workspace = true }
tonic = { workspace = true }
prost = { workspace = true }
prost-types = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
uuid = { workspace = true }
tracing = { workspace = true }
anyhow = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }

[build-dependencies]
tonic-build = { workspace = true }
protoc-bin-vendored = { workspace = true }

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
```

Note: `rapidbyte-runtime` removed (not directly used by agent — engine handles it). `arrow`, `arrow-flight`, `hmac`, `sha2`, `bytes`, `tokio-stream` removed.

- [ ] **Step 3: Write skeleton lib.rs**

Replace `crates/rapidbyte-agent/src/lib.rs` with:

```rust
//! Agent worker for Rapidbyte — polls the controller, executes pipelines, reports results.
//!
//! # Crate structure
//!
//! | Module        | Responsibility                              |
//! |---------------|---------------------------------------------|
//! | `domain`      | Port traits, domain types, errors            |
//! | `application` | DI context, use-case orchestration, fakes    |
//! | `adapter`     | gRPC client, engine executor, metrics, clock |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod domain;
```

- [ ] **Step 4: Create empty module stubs so it compiles**

Create `crates/rapidbyte-agent/src/domain/mod.rs`:
```rust
pub mod error;
pub mod ports;
pub mod progress;
pub mod task;
```

Create `crates/rapidbyte-agent/src/domain/error.rs`:
```rust
// Placeholder — implemented in Task 2
```

Create `crates/rapidbyte-agent/src/domain/task.rs`:
```rust
// Placeholder — implemented in Task 2
```

Create `crates/rapidbyte-agent/src/domain/progress.rs`:
```rust
// Placeholder — implemented in Task 2
```

Create `crates/rapidbyte-agent/src/domain/ports/mod.rs`:
```rust
// Placeholder — implemented in Task 3
```

Create `crates/rapidbyte-agent/src/application/mod.rs`:
```rust
// Placeholder — implemented in Task 5
```

Create `crates/rapidbyte-agent/src/adapter/mod.rs`:
```rust
// Placeholder — implemented in Task 7
```

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles with no errors (empty modules are valid Rust)

- [ ] **Step 6: Commit**

```bash
git add -A crates/rapidbyte-agent/
git commit -m "refactor(agent): strip old code, set up hexagonal module skeleton"
```

---

### Task 2: Domain layer — types and errors

**Files:**
- Create: `crates/rapidbyte-agent/src/domain/error.rs`
- Create: `crates/rapidbyte-agent/src/domain/task.rs`
- Create: `crates/rapidbyte-agent/src/domain/progress.rs`
- Modify: `crates/rapidbyte-agent/src/domain/mod.rs`

- [ ] **Step 1: Write domain error type**

Replace `crates/rapidbyte-agent/src/domain/error.rs`:

```rust
//! Agent-level error types.

/// Errors that can occur during agent operations.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// Pipeline YAML could not be parsed or validated.
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),

    /// Pipeline execution failed with an infrastructure error.
    #[error("execution failed: {0}")]
    ExecutionFailed(#[from] anyhow::Error),

    /// Controller communication failed.
    #[error("controller error: {0}")]
    Controller(String),

    /// Task was cancelled before or during execution.
    #[error("cancelled")]
    Cancelled,
}
```

- [ ] **Step 2: Write domain task types**

Replace `crates/rapidbyte-agent/src/domain/task.rs`:

```rust
//! Task execution result types.

pub use rapidbyte_types::prelude::CommitState;

/// Terminal outcome of a task execution.
#[derive(Debug)]
pub enum TaskOutcomeKind {
    Completed,
    Failed(TaskErrorInfo),
    Cancelled,
}

/// Error details for a failed task.
#[derive(Debug)]
pub struct TaskErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    /// Domain-internal only — used for commit-state decisions, not sent over the wire.
    pub safe_to_retry: bool,
    pub commit_state: CommitState,
}

/// Execution metrics collected during a pipeline run.
#[derive(Debug, Default)]
pub struct TaskMetrics {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub elapsed_seconds: f64,
    pub cursors_advanced: u64,
}

/// Combined result of executing a single task.
#[derive(Debug)]
pub struct TaskExecutionResult {
    pub outcome: TaskOutcomeKind,
    pub metrics: TaskMetrics,
}
```

- [ ] **Step 3: Write domain progress types**

Replace `crates/rapidbyte-agent/src/domain/progress.rs`:

```rust
//! Progress snapshot for heartbeat reporting.

/// Latest progress snapshot for a running task, included in the next heartbeat.
#[derive(Debug, Clone, Default)]
pub struct ProgressSnapshot {
    pub message: Option<String>,
    pub progress_pct: Option<f64>,
}
```

- [ ] **Step 4: Update domain mod.rs with re-exports**

Replace `crates/rapidbyte-agent/src/domain/mod.rs`:

```rust
//! Domain model: port traits, types, and errors for the agent.

pub mod error;
pub mod ports;
pub mod progress;
pub mod task;
```

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-agent/src/domain/
git commit -m "feat(agent): add domain types — error, task, progress"
```

---

### Task 3: Domain layer — port traits

**Files:**
- Create: `crates/rapidbyte-agent/src/domain/ports/controller.rs`
- Create: `crates/rapidbyte-agent/src/domain/ports/executor.rs`
- Create: `crates/rapidbyte-agent/src/domain/ports/progress.rs`
- Create: `crates/rapidbyte-agent/src/domain/ports/metrics.rs`
- Create: `crates/rapidbyte-agent/src/domain/ports/clock.rs`
- Modify: `crates/rapidbyte-agent/src/domain/ports/mod.rs`

- [ ] **Step 1: Write ControllerGateway port**

Create `crates/rapidbyte-agent/src/domain/ports/controller.rs`:

```rust
//! Outbound port for communicating with the controller.

use async_trait::async_trait;

use crate::domain::error::AgentError;
use crate::domain::progress::ProgressSnapshot;
use crate::domain::task::TaskExecutionResult;

/// What the agent tells the controller at registration.
#[derive(Debug, Clone)]
pub struct RegistrationConfig {
    pub max_tasks: u32,
}

/// A task assigned by the controller.
#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: String,
    pub run_id: String,
    pub pipeline_yaml: String,
    pub lease_epoch: u64,
    pub attempt: u32,
}

/// Heartbeat request payload.
#[derive(Debug, Clone)]
pub struct HeartbeatPayload {
    pub agent_id: String,
    pub tasks: Vec<TaskHeartbeat>,
}

/// Per-task heartbeat data.
#[derive(Debug, Clone)]
pub struct TaskHeartbeat {
    pub task_id: String,
    pub lease_epoch: u64,
    pub progress: ProgressSnapshot,
}

/// Heartbeat response from the controller.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub directives: Vec<TaskDirective>,
}

/// Per-task directive from the controller.
#[derive(Debug, Clone)]
pub struct TaskDirective {
    pub task_id: String,
    pub acknowledged: bool,
    pub cancel_requested: bool,
    pub lease_expires_at: Option<u64>,
}

/// Task completion report.
#[derive(Debug, Clone)]
pub struct CompletionPayload {
    pub agent_id: String,
    pub task_id: String,
    pub lease_epoch: u64,
    pub result: TaskExecutionResult,
}

/// Registry configuration received from the controller on registration.
#[derive(Debug, Clone, Default)]
pub struct ControllerRegistryInfo {
    pub url: Option<String>,
    pub insecure: bool,
}

/// Registration response from the controller.
#[derive(Debug, Clone)]
pub struct RegistrationResponse {
    pub agent_id: String,
    pub registry: Option<ControllerRegistryInfo>,
}

/// Outbound port for all controller communication.
#[async_trait]
pub trait ControllerGateway: Send + Sync {
    /// Register this agent with the controller.
    async fn register(
        &self,
        config: &RegistrationConfig,
    ) -> Result<RegistrationResponse, AgentError>;

    /// Long-poll for a task assignment. Returns `None` if no task available.
    async fn poll(&self, agent_id: &str) -> Result<Option<TaskAssignment>, AgentError>;

    /// Send heartbeat with progress and active leases.
    async fn heartbeat(&self, request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError>;

    /// Report task completion (success, failure, or cancellation).
    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError>;
}
```

Note: `TaskExecutionResult` needs `Clone`. Add `#[derive(Clone)]` to `TaskOutcomeKind`, `TaskErrorInfo`, `TaskMetrics`, `TaskExecutionResult` in `domain/task.rs`.

- [ ] **Step 2: Add Clone derives to task types**

Update `crates/rapidbyte-agent/src/domain/task.rs` — add `Clone` to all four types:

```rust
#[derive(Debug, Clone)]
pub enum TaskOutcomeKind { ... }

#[derive(Debug, Clone)]
pub struct TaskErrorInfo { ... }

#[derive(Debug, Clone, Default)]
pub struct TaskMetrics { ... }

#[derive(Debug, Clone)]
pub struct TaskExecutionResult { ... }
```

- [ ] **Step 3: Write PipelineExecutor port**

Create `crates/rapidbyte-agent/src/domain/ports/executor.rs`:

```rust
//! Outbound port for pipeline execution.

use async_trait::async_trait;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::task::TaskExecutionResult;

/// Outbound port for executing a parsed pipeline.
#[async_trait]
pub trait PipelineExecutor: Send + Sync {
    /// Execute a validated pipeline configuration.
    ///
    /// The `progress_tx` channel receives engine progress events for
    /// forwarding to the heartbeat loop.
    async fn execute(
        &self,
        config: &PipelineConfig,
        cancel: CancellationToken,
        progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError>;
}
```

- [ ] **Step 4: Write ProgressCollector port**

Create `crates/rapidbyte-agent/src/domain/ports/progress.rs`:

```rust
//! Outbound port for progress collection.

use crate::domain::progress::ProgressSnapshot;

/// Read-side interface for collecting progress from a running task.
///
/// The heartbeat loop reads the latest snapshot; the execute use-case
/// resets it between tasks.
pub trait ProgressCollector: Send + Sync {
    /// Get the latest progress snapshot for heartbeating.
    fn latest(&self) -> ProgressSnapshot;

    /// Reset progress (when starting a new task).
    fn reset(&self);
}
```

- [ ] **Step 5: Write MetricsProvider port**

Create `crates/rapidbyte-agent/src/domain/ports/metrics.rs`:

```rust
//! Outbound port for metrics infrastructure.

/// Provides OTel handles needed by the executor adapter.
pub trait MetricsProvider: Send + Sync {
    /// Snapshot reader for forwarding to the engine.
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader;

    /// Meter provider for OTel instrumentation.
    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider;
}
```

- [ ] **Step 6: Write Clock port**

Create `crates/rapidbyte-agent/src/domain/ports/clock.rs`:

```rust
//! Time abstraction for testability.

use std::time::Instant;

/// Monotonic clock for backoff and interval timing.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}
```

- [ ] **Step 7: Update ports/mod.rs with re-exports**

Replace `crates/rapidbyte-agent/src/domain/ports/mod.rs`:

```rust
//! Port traits for the agent's hexagonal architecture.
//!
//! | Module       | Trait                | Purpose                            |
//! |--------------|----------------------|------------------------------------|
//! | `clock`      | `Clock`              | Monotonic time abstraction         |
//! | `controller` | `ControllerGateway`  | Controller communication           |
//! | `executor`   | `PipelineExecutor`   | Pipeline execution                 |
//! | `metrics`    | `MetricsProvider`    | OTel metrics handles               |
//! | `progress`   | `ProgressCollector`  | Progress snapshot for heartbeating |

pub mod clock;
pub mod controller;
pub mod executor;
pub mod metrics;
pub mod progress;

pub use clock::Clock;
pub use controller::{
    CompletionPayload, ControllerGateway, ControllerRegistryInfo, HeartbeatPayload,
    HeartbeatResponse, RegistrationConfig, RegistrationResponse, TaskAssignment, TaskDirective,
    TaskHeartbeat,
};
pub use executor::PipelineExecutor;
pub use metrics::MetricsProvider;
pub use progress::ProgressCollector;
```

- [ ] **Step 8: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 9: Commit**

```bash
git add crates/rapidbyte-agent/src/domain/ports/
git commit -m "feat(agent): add port traits — controller, executor, progress, metrics, clock"
```

---

### Task 4: Application layer — context, config, shared state

**Files:**
- Create: `crates/rapidbyte-agent/src/application/context.rs`
- Modify: `crates/rapidbyte-agent/src/application/mod.rs`

- [ ] **Step 1: Write AgentContext and AgentAppConfig**

Create `crates/rapidbyte-agent/src/application/context.rs`:

```rust
//! DI container and configuration for the agent application layer.

use std::sync::Arc;
use std::time::Duration;

use crate::domain::ports::{
    Clock, ControllerGateway, MetricsProvider, PipelineExecutor, ProgressCollector,
};

/// Application-level configuration (derived from infrastructure config at startup).
#[derive(Debug, Clone)]
pub struct AgentAppConfig {
    /// Maximum number of concurrent tasks.
    pub max_tasks: u32,
    /// Interval between heartbeat RPCs.
    pub heartbeat_interval: Duration,
    /// Seconds the server holds a long-poll before returning no-task.
    pub poll_wait_seconds: u32,
    /// Delay between completion retry attempts.
    pub completion_retry_delay: Duration,
    /// Maximum number of completion retry attempts before giving up.
    pub max_completion_retries: u32,
}

impl Default for AgentAppConfig {
    fn default() -> Self {
        Self {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
            completion_retry_delay: Duration::from_secs(1),
            max_completion_retries: 60,
        }
    }
}

/// Application-level dependency container.
///
/// Holds `Arc`-wrapped trait objects for every external dependency the
/// agent needs. Constructed once at startup by the adapter factory.
pub struct AgentContext {
    /// Controller communication (register, poll, heartbeat, complete).
    pub gateway: Arc<dyn ControllerGateway>,
    /// Pipeline execution.
    pub executor: Arc<dyn PipelineExecutor>,
    /// Progress snapshot for heartbeating.
    pub progress: Arc<dyn ProgressCollector>,
    /// OTel metrics handles.
    pub metrics: Arc<dyn MetricsProvider>,
    /// Monotonic clock for timing.
    pub clock: Arc<dyn Clock>,
    /// Application configuration.
    pub config: AgentAppConfig,
}
```

- [ ] **Step 2: Update application/mod.rs**

Replace `crates/rapidbyte-agent/src/application/mod.rs`:

```rust
//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;

pub use context::{AgentAppConfig, AgentContext};
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/application/
git commit -m "feat(agent): add application context — AgentContext, AgentAppConfig"
```

---

### Task 5: Application layer — test fakes and TestContext

**Files:**
- Create: `crates/rapidbyte-agent/src/application/testing.rs`
- Modify: `crates/rapidbyte-agent/src/application/mod.rs`

- [ ] **Step 1: Write all fakes and TestContext factory**

Create `crates/rapidbyte-agent/src/application/testing.rs`:

```rust
//! In-memory fakes for all agent port traits and a [`TestContext`] factory.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::clock::Clock;
use crate::domain::ports::controller::{
    CompletionPayload, ControllerGateway, HeartbeatPayload, HeartbeatResponse,
    RegistrationConfig, RegistrationResponse, TaskAssignment,
};
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::ports::metrics::MetricsProvider;
use crate::domain::ports::progress::ProgressCollector;
use crate::domain::progress::ProgressSnapshot;
use crate::domain::task::TaskExecutionResult;

use super::context::{AgentAppConfig, AgentContext};

// ---------------------------------------------------------------------------
// FakeControllerGateway
// ---------------------------------------------------------------------------

pub struct FakeControllerGateway {
    register_results: Mutex<VecDeque<Result<RegistrationResponse, AgentError>>>,
    poll_results: Mutex<VecDeque<Result<Option<TaskAssignment>, AgentError>>>,
    heartbeat_results: Mutex<VecDeque<Result<HeartbeatResponse, AgentError>>>,
    complete_results: Mutex<VecDeque<Result<(), AgentError>>>,
    completed_payloads: Mutex<Vec<CompletionPayload>>,
}

impl FakeControllerGateway {
    #[must_use]
    pub fn new() -> Self {
        Self {
            register_results: Mutex::new(VecDeque::new()),
            poll_results: Mutex::new(VecDeque::new()),
            heartbeat_results: Mutex::new(VecDeque::new()),
            complete_results: Mutex::new(VecDeque::new()),
            completed_payloads: Mutex::new(Vec::new()),
        }
    }

    pub fn enqueue_register(&self, result: Result<RegistrationResponse, AgentError>) {
        self.register_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_poll(&self, result: Result<Option<TaskAssignment>, AgentError>) {
        self.poll_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_heartbeat(&self, result: Result<HeartbeatResponse, AgentError>) {
        self.heartbeat_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_complete(&self, result: Result<(), AgentError>) {
        self.complete_results.lock().unwrap().push_back(result);
    }

    /// Returns all completion payloads that were reported.
    pub fn completed_payloads(&self) -> Vec<CompletionPayload> {
        self.completed_payloads.lock().unwrap().clone()
    }
}

#[async_trait]
impl ControllerGateway for FakeControllerGateway {
    async fn register(&self, _config: &RegistrationConfig) -> Result<RegistrationResponse, AgentError> {
        self.register_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no register result enqueued")
    }

    async fn poll(&self, _agent_id: &str) -> Result<Option<TaskAssignment>, AgentError> {
        self.poll_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no poll result enqueued")
    }

    async fn heartbeat(&self, _request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError> {
        self.heartbeat_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no heartbeat result enqueued")
    }

    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError> {
        self.completed_payloads.lock().unwrap().push(request);
        self.complete_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no complete result enqueued")
    }
}

// ---------------------------------------------------------------------------
// FakePipelineExecutor
// ---------------------------------------------------------------------------

pub struct FakePipelineExecutor {
    results: Mutex<VecDeque<Result<TaskExecutionResult, AgentError>>>,
}

impl FakePipelineExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            results: Mutex::new(VecDeque::new()),
        }
    }

    pub fn enqueue(&self, result: Result<TaskExecutionResult, AgentError>) {
        self.results.lock().unwrap().push_back(result);
    }
}

#[async_trait]
impl PipelineExecutor for FakePipelineExecutor {
    async fn execute(
        &self,
        _config: &PipelineConfig,
        _cancel: CancellationToken,
        _progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError> {
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakePipelineExecutor: no result enqueued")
    }
}

// ---------------------------------------------------------------------------
// FakeProgressCollector
// ---------------------------------------------------------------------------

pub struct FakeProgressCollector {
    snapshot: RwLock<ProgressSnapshot>,
}

impl FakeProgressCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(ProgressSnapshot::default()),
        }
    }

    pub fn set(&self, snapshot: ProgressSnapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }
}

impl ProgressCollector for FakeProgressCollector {
    fn latest(&self) -> ProgressSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    fn reset(&self) {
        *self.snapshot.write().unwrap() = ProgressSnapshot::default();
    }
}

// ---------------------------------------------------------------------------
// FakeMetricsProvider
// ---------------------------------------------------------------------------

pub struct FakeMetricsProvider {
    reader: rapidbyte_metrics::snapshot::SnapshotReader,
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl FakeMetricsProvider {
    #[must_use]
    pub fn new() -> Self {
        let reader = rapidbyte_metrics::snapshot::SnapshotReader::new();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.build_reader())
            .build();
        Self { reader, provider }
    }
}

impl MetricsProvider for FakeMetricsProvider {
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader {
        &self.reader
    }

    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider {
        &self.provider
    }
}

// ---------------------------------------------------------------------------
// FakeClock
// ---------------------------------------------------------------------------

pub struct FakeClock {
    now: Mutex<Instant>,
}

impl FakeClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            now: Mutex::new(Instant::now()),
        }
    }

    pub fn advance(&self, duration: std::time::Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// TestContext
// ---------------------------------------------------------------------------

/// Bundles `AgentContext` with typed references to each fake for test assertions.
pub struct TestContext {
    pub ctx: AgentContext,
    pub gateway: Arc<FakeControllerGateway>,
    pub executor: Arc<FakePipelineExecutor>,
    pub progress: Arc<FakeProgressCollector>,
    pub clock: Arc<FakeClock>,
}

/// Create a fully-wired test context with in-memory fakes.
#[must_use]
pub fn fake_context() -> TestContext {
    let gateway = Arc::new(FakeControllerGateway::new());
    let executor = Arc::new(FakePipelineExecutor::new());
    let progress = Arc::new(FakeProgressCollector::new());
    let metrics = Arc::new(FakeMetricsProvider::new());
    let clock = Arc::new(FakeClock::new());

    let ctx = AgentContext {
        gateway: Arc::clone(&gateway) as Arc<dyn ControllerGateway>,
        executor: Arc::clone(&executor) as Arc<dyn PipelineExecutor>,
        progress: Arc::clone(&progress) as Arc<dyn ProgressCollector>,
        metrics: Arc::new(FakeMetricsProvider::new()),
        clock: Arc::clone(&clock) as Arc<dyn Clock>,
        config: AgentAppConfig::default(),
    };

    // Suppress unused-variable warning for metrics local
    drop(metrics);

    TestContext {
        ctx,
        gateway,
        executor,
        progress,
        clock,
    }
}
```

- [ ] **Step 2: Update application/mod.rs to include testing module**

Add to `crates/rapidbyte-agent/src/application/mod.rs`:

```rust
//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
#[cfg(test)]
pub mod testing;

pub use context::{AgentAppConfig, AgentContext};
```

- [ ] **Step 3: Verify compilation and test**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/application/
git commit -m "feat(agent): add test fakes and TestContext factory"
```

---

### Task 6: Application layer — execute_task use-case (TDD)

**Files:**
- Create: `crates/rapidbyte-agent/src/application/execute.rs`
- Modify: `crates/rapidbyte-agent/src/application/mod.rs`

- [ ] **Step 1: Write execute_task with tests**

Create `crates/rapidbyte-agent/src/application/execute.rs`:

```rust
//! Task execution use-case: parse YAML, run pipeline, report completion.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{CompletionPayload, ControllerGateway, TaskAssignment};
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::ports::progress::ProgressCollector;
use crate::domain::task::{
    CommitState, TaskErrorInfo, TaskExecutionResult, TaskMetrics, TaskOutcomeKind,
};

use super::context::AgentContext;

/// Execute a single task: parse YAML, run via executor port, report to controller.
pub async fn execute_task(
    ctx: &AgentContext,
    agent_id: &str,
    assignment: &TaskAssignment,
    progress_collector: &Arc<crate::adapter::AtomicProgressCollector>,
    cancel: CancellationToken,
) {
    info!(
        task_id = assignment.task_id,
        run_id = assignment.run_id,
        attempt = assignment.attempt,
        lease_epoch = assignment.lease_epoch,
        "Received task"
    );

    // 1. Parse YAML → PipelineConfig
    let config = match parse_pipeline(&assignment.pipeline_yaml).await {
        Ok(c) => c,
        Err(e) => {
            let result = TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "INVALID_PIPELINE".into(),
                    message: e,
                    retryable: false,
                    safe_to_retry: true,
                    commit_state: CommitState::BeforeCommit,
                }),
                metrics: TaskMetrics::default(),
            };
            report_completion(ctx, agent_id, assignment, result).await;
            return;
        }
    };

    // 2. Check for early cancellation
    if cancel.is_cancelled() {
        let result = TaskExecutionResult {
            outcome: TaskOutcomeKind::Cancelled,
            metrics: TaskMetrics::default(),
        };
        report_completion(ctx, agent_id, assignment, result).await;
        return;
    }

    // 3. Set up progress channel and execute
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(bridge_progress(progress_collector.clone(), progress_rx));

    let result = ctx
        .executor
        .execute(&config, cancel, progress_tx)
        .await;

    bridge.abort();
    ctx.progress.reset();

    // 4. Convert to TaskExecutionResult
    let result = match result {
        Ok(r) => r,
        Err(AgentError::Cancelled) => TaskExecutionResult {
            outcome: TaskOutcomeKind::Cancelled,
            metrics: TaskMetrics::default(),
        },
        Err(e) => TaskExecutionResult {
            outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                code: "EXECUTION_ERROR".into(),
                message: e.to_string(),
                retryable: true,
                safe_to_retry: false,
                commit_state: CommitState::AfterCommitUnknown,
            }),
            metrics: TaskMetrics::default(),
        },
    };

    // 5. Report to controller
    report_completion(ctx, agent_id, assignment, result).await;
}

async fn parse_pipeline(yaml: &str) -> Result<rapidbyte_pipeline_config::PipelineConfig, String> {
    let config = rapidbyte_pipeline_config::parser::parse_pipeline(
        yaml,
        &rapidbyte_secrets::SecretProviders::new(),
    )
    .await
    .map_err(|e| format!("{e:#}"))?;

    rapidbyte_pipeline_config::validator::validate_pipeline(&config)
        .map_err(|e| format!("{e:#}"))?;

    Ok(config)
}

/// Bridge engine progress events to the concrete `AtomicProgressCollector`.
///
/// Note: this function takes the concrete adapter type, not the port trait,
/// because it needs the write-side `update()` method which is not part of
/// the `ProgressCollector` port (read-only interface for heartbeat).
pub(crate) async fn bridge_progress(
    collector: Arc<crate::adapter::AtomicProgressCollector>,
    mut rx: mpsc::UnboundedReceiver<rapidbyte_engine::ProgressEvent>,
) {
    use rapidbyte_engine::ProgressEvent;

    while let Some(event) = rx.recv().await {
        let snapshot = match &event {
            ProgressEvent::BatchEmitted { bytes, .. } => crate::domain::progress::ProgressSnapshot {
                message: Some(format!("processing ({bytes} bytes)")),
                progress_pct: None,
            },
            ProgressEvent::StreamCompleted { stream } => crate::domain::progress::ProgressSnapshot {
                message: Some(format!("stream {stream} completed")),
                progress_pct: None,
            },
            ProgressEvent::PhaseChanged { phase } => crate::domain::progress::ProgressSnapshot {
                message: Some(format!("{phase:?}").to_lowercase()),
                progress_pct: None,
            },
            ProgressEvent::StreamStarted { stream } => crate::domain::progress::ProgressSnapshot {
                message: Some(format!("stream {stream} starting")),
                progress_pct: None,
            },
            ProgressEvent::RetryScheduled { .. } => continue,
        };
        collector.update(snapshot);
    }
}

/// Report task completion to the controller with retry logic.
pub(crate) async fn report_completion(
    ctx: &AgentContext,
    agent_id: &str,
    assignment: &TaskAssignment,
    result: TaskExecutionResult,
) {
    let payload = CompletionPayload {
        agent_id: agent_id.to_owned(),
        task_id: assignment.task_id.clone(),
        lease_epoch: assignment.lease_epoch,
        result,
    };

    for attempt in 0..ctx.config.max_completion_retries {
        match ctx.gateway.complete(payload.clone()).await {
            Ok(()) => {
                info!(task_id = assignment.task_id, "Task completion reported");
                return;
            }
            Err(ref e) if is_retryable_controller_error(e) => {
                warn!(
                    task_id = assignment.task_id,
                    attempt,
                    error = %e,
                    "Completion failed, retrying"
                );
                tokio::time::sleep(ctx.config.completion_retry_delay).await;
            }
            Err(e) => {
                warn!(
                    task_id = assignment.task_id,
                    error = %e,
                    "Completion failed with non-retryable error, giving up"
                );
                return;
            }
        }
    }

    warn!(
        task_id = assignment.task_id,
        "Completion retries exhausted"
    );
}

fn is_retryable_controller_error(e: &AgentError) -> bool {
    match e {
        AgentError::Controller(msg) => {
            // Non-retryable gRPC codes mapped to message prefixes by the adapter
            !msg.starts_with("unauthenticated:")
                && !msg.starts_with("permission_denied:")
                && !msg.starts_with("not_found:")
                && !msg.starts_with("invalid_argument:")
                && !msg.starts_with("aborted:")
                && !msg.starts_with("failed_precondition:")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_context;
    use crate::domain::ports::controller::{
        HeartbeatResponse, RegistrationResponse, TaskAssignment,
    };
    use crate::domain::task::{TaskExecutionResult, TaskMetrics, TaskOutcomeKind};

    fn test_assignment() -> TaskAssignment {
        TaskAssignment {
            task_id: "task-1".into(),
            run_id: "run-1".into(),
            pipeline_yaml: String::new(), // invalid — will fail parse
            lease_epoch: 1,
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn invalid_yaml_reports_failed_outcome() {
        let t = fake_context();
        t.gateway.enqueue_complete(Ok(()));

        let assignment = test_assignment();
        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(&t.ctx, "agent-1", &assignment, &pc, CancellationToken::new()).await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        match &payloads[0].result.outcome {
            TaskOutcomeKind::Failed(info) => {
                assert_eq!(info.code, "INVALID_PIPELINE");
                assert!(!info.retryable);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_cancelled_token_reports_cancelled() {
        let t = fake_context();
        t.gateway.enqueue_complete(Ok(()));

        let token = CancellationToken::new();
        token.cancel();

        // Use valid-ish YAML so parse succeeds but cancellation is caught
        let mut assignment = test_assignment();
        assignment.pipeline_yaml = "version: '1.0'\npipeline: test\nsource:\n  use: postgres\n  config:\n    host: localhost\n  streams:\n    - name: users\n      sync_mode: full_refresh\ndestination:\n  use: postgres\n  config:\n    host: localhost\n  write_mode: append\n".into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(&t.ctx, "agent-1", &assignment, &pc, token).await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Cancelled
        ));
    }

    #[tokio::test]
    async fn successful_execution_reports_completed() {
        let t = fake_context();
        t.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics {
                records_written: 42,
                ..TaskMetrics::default()
            },
        }));
        t.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = "version: '1.0'\npipeline: test\nsource:\n  use: postgres\n  config:\n    host: localhost\n  streams:\n    - name: users\n      sync_mode: full_refresh\ndestination:\n  use: postgres\n  config:\n    host: localhost\n  write_mode: append\n".into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(&t.ctx, "agent-1", &assignment, &pc, CancellationToken::new()).await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Completed
        ));
        assert_eq!(payloads[0].result.metrics.records_written, 42);
    }

    #[tokio::test]
    async fn completion_retry_on_transient_error() {
        let t = fake_context();
        t.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics::default(),
        }));
        // First attempt: transient failure. Second: success.
        t.gateway
            .enqueue_complete(Err(AgentError::Controller("unavailable: server down".into())));
        t.gateway.enqueue_complete(Ok(()));

        let mut ctx = fake_context();
        ctx.ctx.config.completion_retry_delay = std::time::Duration::from_millis(1);
        ctx.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics::default(),
        }));
        ctx.gateway
            .enqueue_complete(Err(AgentError::Controller("unavailable: down".into())));
        ctx.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = "version: '1.0'\npipeline: test\nsource:\n  use: postgres\n  config:\n    host: localhost\n  streams:\n    - name: users\n      sync_mode: full_refresh\ndestination:\n  use: postgres\n  config:\n    host: localhost\n  write_mode: append\n".into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(&ctx.ctx, "agent-1", &assignment, &pc, CancellationToken::new()).await;

        let payloads = ctx.gateway.completed_payloads();
        assert_eq!(payloads.len(), 2); // First attempt failed, second succeeded
    }

    #[tokio::test]
    async fn completion_stops_on_non_retryable_error() {
        let mut ctx = fake_context();
        ctx.ctx.config.completion_retry_delay = std::time::Duration::from_millis(1);
        ctx.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics::default(),
        }));
        ctx.gateway.enqueue_complete(Err(AgentError::Controller(
            "unauthenticated: bad token".into(),
        )));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = "version: '1.0'\npipeline: test\nsource:\n  use: postgres\n  config:\n    host: localhost\n  streams:\n    - name: users\n      sync_mode: full_refresh\ndestination:\n  use: postgres\n  config:\n    host: localhost\n  write_mode: append\n".into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(&ctx.ctx, "agent-1", &assignment, &pc, CancellationToken::new()).await;

        // Only 1 attempt — no retry on unauthenticated
        let payloads = ctx.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
    }
}
```

- [ ] **Step 2: Update application/mod.rs**

```rust
//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub mod execute;
#[cfg(test)]
pub mod testing;

pub use context::{AgentAppConfig, AgentContext};
pub use execute::execute_task;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-agent -- execute`
Expected: All execute tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/application/
git commit -m "feat(agent): add execute_task use-case with completion retry"
```

---

### Task 7: Application layer — heartbeat_loop use-case (TDD)

**Files:**
- Create: `crates/rapidbyte-agent/src/application/heartbeat.rs`
- Modify: `crates/rapidbyte-agent/src/application/mod.rs`

- [ ] **Step 1: Write heartbeat_loop with tests**

Create `crates/rapidbyte-agent/src/application/heartbeat.rs`:

```rust
//! Heartbeat loop use-case: periodic progress reporting and cancel handling.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::domain::ports::controller::{
    ControllerGateway, HeartbeatPayload, TaskHeartbeat,
};
use crate::domain::ports::progress::ProgressCollector;
use crate::domain::progress::ProgressSnapshot;

use super::context::AgentAppConfig;

/// Active lease entry tracked by the worker.
pub struct LeaseEntry {
    pub run_id: String,
    pub lease_epoch: u64,
    pub cancel: CancellationToken,
    pub progress: Arc<dyn ProgressCollector>,
}

/// Shared map of active task leases.
pub type ActiveLeaseMap = Arc<RwLock<HashMap<String, LeaseEntry>>>;

/// Run the periodic heartbeat loop until shutdown is signalled.
pub async fn heartbeat_loop(
    gateway: &dyn ControllerGateway,
    agent_id: &str,
    config: &AgentAppConfig,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(config.heartbeat_interval);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _tick = ticker.tick() => {}
        }

        let tasks = build_heartbeats(&active_leases).await;

        let payload = HeartbeatPayload {
            agent_id: agent_id.to_owned(),
            tasks,
        };

        match gateway.heartbeat(payload).await {
            Ok(response) => {
                for directive in response.directives {
                    if directive.cancel_requested {
                        warn!(task_id = directive.task_id, "Received cancel directive");
                        let leases = active_leases.read().await;
                        if let Some(entry) = leases.get(&directive.task_id) {
                            entry.cancel.cancel();
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Heartbeat failed");
            }
        }
    }
}

async fn build_heartbeats(active_leases: &ActiveLeaseMap) -> Vec<TaskHeartbeat> {
    let leases = active_leases.read().await;
    leases
        .iter()
        .map(|(task_id, entry)| TaskHeartbeat {
            task_id: task_id.clone(),
            lease_epoch: entry.lease_epoch,
            progress: entry.progress.latest(),
        })
        .collect()
}
```

- [ ] **Step 2: Update application/mod.rs**

```rust
//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub mod execute;
pub mod heartbeat;
#[cfg(test)]
pub mod testing;

pub use context::{AgentAppConfig, AgentContext};
pub use execute::execute_task;
pub use heartbeat::{heartbeat_loop, ActiveLeaseMap, LeaseEntry};
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-agent`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/application/
git commit -m "feat(agent): add heartbeat_loop use-case with cancel handling"
```

---

### Task 8: Application layer — worker use-case (run_agent)

**Files:**
- Create: `crates/rapidbyte-agent/src/application/worker.rs`
- Modify: `crates/rapidbyte-agent/src/application/mod.rs`

- [ ] **Step 1: Write run_agent orchestrating register, poll, heartbeat, shutdown**

Create `crates/rapidbyte-agent/src/application/worker.rs`:

```rust
//! Worker lifecycle use-case: register, poll loop, heartbeat, graceful shutdown.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{RegistrationConfig, TaskAssignment};

use super::context::AgentContext;
use super::execute::execute_task;
use super::heartbeat::{heartbeat_loop, ActiveLeaseMap, LeaseEntry};

/// Run the agent worker loop.
///
/// Registers with the controller, spawns heartbeat and worker pool,
/// then polls for tasks until shutdown is signalled.
pub async fn run_agent(
    ctx: &AgentContext,
    registration: RegistrationConfig,
    adapters: &crate::adapter::AgentAdapters,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    // 1. Register with controller
    let response = ctx.gateway.register(&registration).await?;
    let agent_id = response.agent_id;
    info!(agent_id, "Registered with controller");

    // 2. Update registry config from controller response
    if let Some(registry_info) = response.registry {
        use crate::adapter::engine_executor::EngineExecutor;
        let new_config = rapidbyte_registry::RegistryConfig {
            insecure: registry_info.insecure,
            default_registry: rapidbyte_registry::normalize_registry_url_option(
                registry_info.url.as_deref(),
            ),
            ..Default::default()
        };
        adapters.engine_executor.update_registry_config(new_config).await;
        info!(agent_id, "Updated registry config from controller");
    }

    // 3. Shared active lease tracking
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

    // 3. Spawn heartbeat loop
    let hb_gateway = ctx.gateway.clone();
    let hb_agent_id = agent_id.clone();
    let hb_config = ctx.config.clone();
    let hb_leases = active_leases.clone();
    let hb_shutdown = shutdown.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            hb_gateway.as_ref(),
            &hb_agent_id,
            &hb_config,
            hb_leases,
            hb_shutdown,
        )
        .await;
    });

    // 4. Run worker pool: max_tasks concurrent poll→execute loops
    let pool_result = run_worker_pool(
        ctx,
        &agent_id,
        active_leases.clone(),
        shutdown.clone(),
    )
    .await;

    // 5. Graceful shutdown: cancel heartbeat, wait for it
    shutdown.cancel();
    let _ = heartbeat_handle.await;

    if let Err(e) = &pool_result {
        error!(error = %e, "Worker pool exited with error");
    }

    info!("Agent stopped");
    pool_result
}

async fn run_worker_pool(
    ctx: &AgentContext,
    agent_id: &str,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    let mut workers = JoinSet::new();

    for _ in 0..ctx.config.max_tasks.max(1) {
        let gateway = ctx.gateway.clone();
        let executor = ctx.executor.clone();
        let progress = ctx.progress.clone();
        let metrics = ctx.metrics.clone();
        let clock = ctx.clock.clone();
        let config = ctx.config.clone();
        let agent_id = agent_id.to_owned();
        let leases = active_leases.clone();
        let shutdown = shutdown.clone();

        workers.spawn(async move {
            let worker_ctx = AgentContext {
                gateway,
                executor,
                progress,
                metrics,
                clock,
                config,
            };
            worker_loop(&worker_ctx, &agent_id, leases, shutdown).await
        });
    }

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(AgentError::ExecutionFailed(e.into())),
        }
    }

    Ok(())
}

async fn worker_loop(
    ctx: &AgentContext,
    agent_id: &str,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let assignment = match ctx.gateway.poll(agent_id).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                continue;
            }
            Err(e) => {
                warn!(error = %e, "Poll failed");
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let cancel = CancellationToken::new();
        let child_cancel = cancel.clone();

        // Register lease
        active_leases.write().await.insert(
            assignment.task_id.clone(),
            LeaseEntry {
                run_id: assignment.run_id.clone(),
                lease_epoch: assignment.lease_epoch,
                cancel: cancel.clone(),
                progress: ctx.progress.clone(),
            },
        );

        // Link task cancellation to shutdown
        let shutdown_for_cancel = shutdown.clone();
        let cancel_for_shutdown = cancel.clone();
        tokio::spawn(async move {
            shutdown_for_cancel.cancelled().await;
            cancel_for_shutdown.cancel();
        });

        execute_task(ctx, agent_id, &assignment, progress_collector, child_cancel).await;

        // Remove lease after execution
        active_leases.write().await.remove(&assignment.task_id);
    }
}
```

- [ ] **Step 2: Update application/mod.rs with worker**

```rust
//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub mod execute;
pub mod heartbeat;
#[cfg(test)]
pub mod testing;
pub mod worker;

pub use context::{AgentAppConfig, AgentContext};
pub use execute::execute_task;
pub use heartbeat::{heartbeat_loop, ActiveLeaseMap, LeaseEntry};
pub use worker::run_agent;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-agent/src/application/
git commit -m "feat(agent): add run_agent worker loop use-case"
```

---

### Task 9: Adapter layer — clock, metrics, progress

**Files:**
- Create: `crates/rapidbyte-agent/src/adapter/clock.rs`
- Create: `crates/rapidbyte-agent/src/adapter/metrics.rs`
- Create: `crates/rapidbyte-agent/src/adapter/channel_progress.rs`
- Modify: `crates/rapidbyte-agent/src/adapter/mod.rs`

- [ ] **Step 1: Write SystemClock adapter**

Create `crates/rapidbyte-agent/src/adapter/clock.rs`:

```rust
//! System clock adapter.

use std::time::Instant;

use crate::domain::ports::clock::Clock;

/// Real monotonic clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
```

- [ ] **Step 2: Write OtelMetricsProvider adapter**

Create `crates/rapidbyte-agent/src/adapter/metrics.rs`:

```rust
//! OTel metrics adapter.

use crate::domain::ports::metrics::MetricsProvider;

/// Wraps the OTel guard's metric handles.
pub struct OtelMetricsProvider {
    reader: rapidbyte_metrics::snapshot::SnapshotReader,
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl OtelMetricsProvider {
    #[must_use]
    pub fn new(guard: &rapidbyte_metrics::OtelGuard) -> Self {
        Self {
            reader: guard.snapshot_reader().clone(),
            provider: guard.meter_provider().clone(),
        }
    }
}

impl MetricsProvider for OtelMetricsProvider {
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader {
        &self.reader
    }

    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider {
        &self.provider
    }
}
```

- [ ] **Step 3: Write AtomicProgressCollector adapter**

Create `crates/rapidbyte-agent/src/adapter/channel_progress.rs`:

```rust
//! Progress collector backed by a read-write lock.

use std::sync::RwLock;

use crate::domain::ports::progress::ProgressCollector;
use crate::domain::progress::ProgressSnapshot;

/// Thread-safe progress collector with read/write sides.
///
/// The read side (trait impl) is used by the heartbeat loop.
/// The write side (`update`) is used by the progress bridge task.
pub struct AtomicProgressCollector {
    snapshot: RwLock<ProgressSnapshot>,
}

impl AtomicProgressCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(ProgressSnapshot::default()),
        }
    }

    /// Update the snapshot from an engine progress event.
    ///
    /// Called by the bridge task in `execute_task`.
    pub fn update(&self, snapshot: ProgressSnapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }
}

impl ProgressCollector for AtomicProgressCollector {
    fn latest(&self) -> ProgressSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    fn reset(&self) {
        *self.snapshot.write().unwrap() = ProgressSnapshot::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_and_latest_round_trips() {
        let collector = AtomicProgressCollector::new();
        assert!(collector.latest().message.is_none());

        collector.update(ProgressSnapshot {
            message: Some("hello".into()),
            progress_pct: Some(0.5),
        });

        let snap = collector.latest();
        assert_eq!(snap.message.as_deref(), Some("hello"));
        assert_eq!(snap.progress_pct, Some(0.5));
    }

    #[test]
    fn reset_clears_snapshot() {
        let collector = AtomicProgressCollector::new();
        collector.update(ProgressSnapshot {
            message: Some("hello".into()),
            progress_pct: Some(0.5),
        });
        collector.reset();
        assert!(collector.latest().message.is_none());
    }
}
```

- [ ] **Step 4: Update adapter/mod.rs**

Replace `crates/rapidbyte-agent/src/adapter/mod.rs`:

```rust
//! Concrete adapter implementations for agent port traits.
//!
//! | Module              | Adapter                    | Port               |
//! |---------------------|----------------------------|--------------------|
//! | `channel_progress`  | `AtomicProgressCollector`  | `ProgressCollector` |
//! | `clock`             | `SystemClock`              | `Clock`            |
//! | `metrics`           | `OtelMetricsProvider`      | `MetricsProvider`  |

pub mod channel_progress;
pub mod clock;
pub mod metrics;

pub use channel_progress::AtomicProgressCollector;
pub use clock::SystemClock;
pub use metrics::OtelMetricsProvider;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-agent`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-agent/src/adapter/
git commit -m "feat(agent): add clock, metrics, progress adapters"
```

---

### Task 10: Adapter layer — GrpcControllerGateway

**Files:**
- Create: `crates/rapidbyte-agent/src/adapter/grpc_controller.rs`
- Create: `crates/rapidbyte-agent/src/adapter/proto.rs`
- Modify: `crates/rapidbyte-agent/src/adapter/mod.rs`

- [ ] **Step 1: Write proto module wrapper**

Create `crates/rapidbyte-agent/src/adapter/proto.rs`:

```rust
//! Generated protobuf types — adapter-internal only.

pub mod rapidbyte {
    pub mod v1 {
        tonic::include_proto!("rapidbyte.v1");
    }
}
```

- [ ] **Step 2: Write GrpcControllerGateway**

Create `crates/rapidbyte-agent/src/adapter/grpc_controller.rs`:

```rust
//! gRPC adapter for the ControllerGateway port.

use async_trait::async_trait;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint,
};
use tracing::info;

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{
    CompletionPayload, ControllerGateway, ControllerRegistryInfo, HeartbeatPayload,
    HeartbeatResponse, RegistrationConfig, RegistrationResponse, TaskAssignment,
    TaskDirective, TaskHeartbeat,
};
use crate::domain::task::{CommitState, TaskOutcomeKind};

use super::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use super::proto::rapidbyte::v1::{
    self as pb, complete_task_request, poll_task_response, AgentCapabilities,
    CompleteTaskRequest, HeartbeatRequest, PollTaskRequest, RegisterRequest,
    RunMetrics, TaskCancelled, TaskCompleted, TaskFailed,
};

/// TLS configuration for connecting to the controller.
#[derive(Clone)]
pub struct ClientTlsConfig {
    pub ca_cert_pem: Vec<u8>,
    pub domain_name: Option<String>,
}

/// gRPC implementation of [`ControllerGateway`].
pub struct GrpcControllerGateway {
    channel: Channel,
    auth_token: Option<String>,
}

impl GrpcControllerGateway {
    /// Connect to the controller at the given URL.
    pub async fn connect(
        url: &str,
        tls: Option<&ClientTlsConfig>,
        auth_token: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let channel = connect_channel(url, tls).await?;
        Ok(Self { channel, auth_token })
    }
}

#[async_trait]
impl ControllerGateway for GrpcControllerGateway {
    async fn register(
        &self,
        config: &RegistrationConfig,
    ) -> Result<RegistrationResponse, AgentError> {
        let agent_id = uuid::Uuid::new_v4().to_string();
        let mut client = AgentServiceClient::new(self.channel.clone());
        let request = RegisterRequest {
            agent_id: agent_id.clone(),
            capabilities: Some(AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: config.max_tasks,
            }),
        };
        let response = client
            .register(apply_auth(request, &self.auth_token)?)
            .await
            .map_err(|s| AgentError::Controller(format_status(&s)))?
            .into_inner();

        let registry = response.registry.map(|r| ControllerRegistryInfo {
            url: if r.url.is_empty() { None } else { Some(r.url) },
            insecure: r.insecure,
        });

        Ok(RegistrationResponse {
            agent_id,
            registry,
        })
    }

    async fn poll(
        &self,
        agent_id: &str,
    ) -> Result<Option<TaskAssignment>, AgentError> {
        let mut client = AgentServiceClient::new(self.channel.clone());
        let request = PollTaskRequest {
            agent_id: agent_id.to_owned(),
        };
        let response = client
            .poll_task(apply_auth(request, &self.auth_token)?)
            .await
            .map_err(|s| AgentError::Controller(format_status(&s)))?
            .into_inner();

        Ok(match response.result {
            Some(poll_task_response::Result::Assignment(task)) => Some(TaskAssignment {
                task_id: task.task_id,
                run_id: task.run_id,
                pipeline_yaml: task.pipeline_yaml,
                lease_epoch: task.lease_epoch,
                attempt: task.attempt,
            }),
            Some(poll_task_response::Result::NoTask(_)) | None => None,
        })
    }

    async fn heartbeat(
        &self,
        request: HeartbeatPayload,
    ) -> Result<HeartbeatResponse, AgentError> {
        let mut client = AgentServiceClient::new(self.channel.clone());
        let proto_tasks: Vec<pb::TaskHeartbeat> = request
            .tasks
            .into_iter()
            .map(|t| pb::TaskHeartbeat {
                task_id: t.task_id,
                lease_epoch: t.lease_epoch,
                progress_message: t.progress.message,
                progress_pct: t.progress.progress_pct,
            })
            .collect();

        let proto_request = HeartbeatRequest {
            agent_id: request.agent_id,
            tasks: proto_tasks,
        };

        let response = client
            .heartbeat(apply_auth(proto_request, &self.auth_token)?)
            .await
            .map_err(|s| AgentError::Controller(format_status(&s)))?
            .into_inner();

        Ok(HeartbeatResponse {
            directives: response
                .directives
                .into_iter()
                .map(|d| TaskDirective {
                    task_id: d.task_id,
                    acknowledged: d.acknowledged,
                    cancel_requested: d.cancel_requested,
                    lease_expires_at: if d.lease_expires_at == 0 {
                        None
                    } else {
                        Some(d.lease_expires_at)
                    },
                })
                .collect(),
        })
    }

    async fn complete(
        &self,
        request: CompletionPayload,
    ) -> Result<(), AgentError> {
        let mut client = AgentServiceClient::new(self.channel.clone());

        let outcome = match &request.result.outcome {
            TaskOutcomeKind::Completed => {
                let m = &request.result.metrics;
                complete_task_request::Outcome::Completed(TaskCompleted {
                    metrics: Some(RunMetrics {
                        rows_read: m.records_read,
                        rows_written: m.records_written,
                        bytes_read: m.bytes_read,
                        bytes_written: m.bytes_written,
                        #[allow(
                            clippy::cast_possible_truncation,
                            clippy::cast_sign_loss
                        )]
                        duration_ms: (m.elapsed_seconds * 1000.0).max(0.0) as u64,
                    }),
                })
            }
            TaskOutcomeKind::Failed(info) => {
                let commit_state = match info.commit_state {
                    CommitState::BeforeCommit => pb::CommitState::BeforeCommit as i32,
                    CommitState::AfterCommitUnknown => {
                        pb::CommitState::AfterCommitUnknown as i32
                    }
                    CommitState::AfterCommitConfirmed => {
                        pb::CommitState::AfterCommitConfirmed as i32
                    }
                };
                complete_task_request::Outcome::Failed(TaskFailed {
                    error_code: info.code.clone(),
                    error_message: info.message.clone(),
                    retryable: info.retryable,
                    commit_state,
                })
            }
            TaskOutcomeKind::Cancelled => {
                complete_task_request::Outcome::Cancelled(TaskCancelled {})
            }
        };

        let proto_request = CompleteTaskRequest {
            agent_id: request.agent_id,
            task_id: request.task_id,
            lease_epoch: request.lease_epoch,
            outcome: Some(outcome),
        };

        client
            .complete_task(apply_auth(proto_request, &self.auth_token)?)
            .await
            .map_err(|s| AgentError::Controller(format_status(&s)))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn connect_channel(
    url: &str,
    tls: Option<&ClientTlsConfig>,
) -> Result<Channel, anyhow::Error> {
    let mut endpoint = Endpoint::from_shared(url.to_string())?;
    if url.starts_with("https://") || tls.is_some() {
        let mut tls_config = TonicClientTlsConfig::new();
        if let Some(tls) = tls {
            if !tls.ca_cert_pem.is_empty() {
                tls_config =
                    tls_config.ca_certificate(Certificate::from_pem(tls.ca_cert_pem.clone()));
            }
            if let Some(domain_name) = &tls.domain_name {
                tls_config = tls_config.domain_name(domain_name.clone());
            }
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint.connect().await?)
}

fn apply_auth<T>(
    request: T,
    auth_token: &Option<String>,
) -> Result<tonic::Request<T>, AgentError> {
    let mut req = tonic::Request::new(request);
    if let Some(token) = auth_token {
        let value = format!("Bearer {token}")
            .parse()
            .map_err(|_| AgentError::Controller("invalid bearer token".into()))?;
        req.metadata_mut()
            .insert("authorization", value);
    }
    Ok(req)
}

/// Format a tonic Status with code prefix for retryability classification.
fn format_status(status: &tonic::Status) -> String {
    let code = match status.code() {
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::NotFound => "not_found",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::Aborted => "aborted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        _ => return status.message().to_string(),
    };
    format!("{code}: {}", status.message())
}
```

- [ ] **Step 3: Update adapter/mod.rs**

Add to `crates/rapidbyte-agent/src/adapter/mod.rs`:

```rust
//! Concrete adapter implementations for agent port traits.

pub mod channel_progress;
pub mod clock;
pub mod grpc_controller;
pub mod metrics;
mod proto;

pub use channel_progress::AtomicProgressCollector;
pub use clock::SystemClock;
pub use grpc_controller::{ClientTlsConfig, GrpcControllerGateway};
pub use metrics::OtelMetricsProvider;
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/adapter/
git commit -m "feat(agent): add GrpcControllerGateway adapter with proto conversion"
```

---

### Task 11: Adapter layer — EngineExecutor and factory

**Files:**
- Create: `crates/rapidbyte-agent/src/adapter/engine_executor.rs`
- Create: `crates/rapidbyte-agent/src/adapter/agent_factory.rs`
- Modify: `crates/rapidbyte-agent/src/adapter/mod.rs`

- [ ] **Step 1: Write EngineExecutor adapter**

Create `crates/rapidbyte-agent/src/adapter/engine_executor.rs`:

```rust
//! Engine executor adapter — bridges PipelineExecutor port to rapidbyte-engine.

use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_engine::domain::error::PipelineError;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::ports::metrics::MetricsProvider;
use crate::domain::task::{
    CommitState, TaskErrorInfo, TaskExecutionResult, TaskMetrics, TaskOutcomeKind,
};

/// Implements [`PipelineExecutor`] by calling `rapidbyte_engine::run_pipeline`.
pub struct EngineExecutor {
    registry_config: RwLock<rapidbyte_registry::RegistryConfig>,
    metrics: Arc<dyn MetricsProvider>,
}

impl EngineExecutor {
    #[must_use]
    pub fn new(
        registry_config: rapidbyte_registry::RegistryConfig,
        metrics: Arc<dyn MetricsProvider>,
    ) -> Self {
        Self {
            registry_config: RwLock::new(registry_config),
            metrics,
        }
    }

    /// Update registry config after receiving it from the controller.
    pub async fn update_registry_config(&self, config: rapidbyte_registry::RegistryConfig) {
        *self.registry_config.write().await = config;
    }
}

#[async_trait]
impl PipelineExecutor for EngineExecutor {
    async fn execute(
        &self,
        config: &PipelineConfig,
        cancel: CancellationToken,
        progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError> {
        let registry_config = self.registry_config.read().await.clone();

        let engine_ctx =
            rapidbyte_engine::build_run_context(config, Some(progress_tx), &registry_config)
                .await
                .map_err(|e| AgentError::ExecutionFailed(e.into()))?;

        let start = std::time::Instant::now();

        let result = rapidbyte_engine::run_pipeline(&engine_ctx, config, cancel).await;

        let elapsed = start.elapsed().as_secs_f64();

        match result {
            Ok(pipeline_result) => Ok(TaskExecutionResult {
                outcome: TaskOutcomeKind::Completed,
                metrics: TaskMetrics {
                    records_read: pipeline_result.counts.records_read,
                    records_written: pipeline_result.counts.records_written,
                    bytes_read: pipeline_result.counts.bytes_read,
                    bytes_written: pipeline_result.counts.bytes_written,
                    elapsed_seconds: elapsed,
                    cursors_advanced: 0,
                },
            }),
            Err(PipelineError::Cancelled) => Err(AgentError::Cancelled),
            Err(ref e) if is_pre_commit_cancellation(e) => Err(AgentError::Cancelled),
            Err(e) => Ok(TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(convert_pipeline_error(&e)),
                metrics: TaskMetrics {
                    elapsed_seconds: elapsed,
                    ..TaskMetrics::default()
                },
            }),
        }
    }
}

fn is_pre_commit_cancellation(error: &PipelineError) -> bool {
    match error {
        PipelineError::Plugin(pe) => {
            pe.code == "CANCELLED"
                && matches!(pe.commit_state, Some(CommitState::BeforeCommit))
        }
        PipelineError::Infrastructure(_) => false,
        PipelineError::Cancelled => true,
    }
}

fn convert_pipeline_error(error: &PipelineError) -> TaskErrorInfo {
    match error {
        PipelineError::Plugin(pe) => TaskErrorInfo {
            code: pe.code.clone(),
            message: pe.message.clone(),
            retryable: pe.retryable,
            safe_to_retry: pe.safe_to_retry,
            commit_state: pe.commit_state.unwrap_or(CommitState::BeforeCommit),
        },
        PipelineError::Infrastructure(e) => TaskErrorInfo {
            code: "INFRASTRUCTURE".into(),
            message: format!("{e:#}"),
            retryable: false,
            safe_to_retry: false,
            commit_state: CommitState::BeforeCommit,
        },
        PipelineError::Cancelled => TaskErrorInfo {
            code: "CANCELLED".into(),
            message: "pipeline cancelled".into(),
            retryable: true,
            safe_to_retry: true,
            commit_state: CommitState::BeforeCommit,
        },
    }
}
```

- [ ] **Step 2: Write AgentConfig and build_agent_context factory**

Create `crates/rapidbyte-agent/src/adapter/agent_factory.rs`:

```rust
//! Composition root: wires all adapters into an AgentContext.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::application::context::{AgentAppConfig, AgentContext};
use crate::domain::ports::controller::RegistrationConfig;

use super::channel_progress::AtomicProgressCollector;
use super::clock::SystemClock;
use super::grpc_controller::{ClientTlsConfig, GrpcControllerGateway};
use super::engine_executor::EngineExecutor;
use super::metrics::OtelMetricsProvider;

/// Full infrastructure configuration for agent startup.
#[derive(Clone)]
pub struct AgentConfig {
    pub controller_url: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
    pub auth_token: Option<String>,
    pub controller_tls: Option<ClientTlsConfig>,
    pub metrics_listen: Option<String>,
    pub registry_url: Option<String>,
    pub registry_insecure: bool,
    pub trust_policy: String,
    pub trusted_key_pems: Vec<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller_url: "http://[::]:9090".into(),
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
            auth_token: None,
            controller_tls: None,
            metrics_listen: None,
            registry_url: None,
            registry_insecure: false,
            trust_policy: "skip".into(),
            trusted_key_pems: Vec::new(),
        }
    }
}

/// Build the agent context from infrastructure config.
///
/// Connects to the controller, initialises metrics, and wires all adapters.
/// Concrete adapter handles returned alongside the context.
///
/// These are needed by the application layer for operations that go beyond
/// the port trait interface (e.g., `EngineExecutor::update_registry_config`,
/// `AtomicProgressCollector::update`).
pub struct AgentAdapters {
    pub engine_executor: Arc<EngineExecutor>,
    pub progress_collector: Arc<AtomicProgressCollector>,
}

pub async fn build_agent_context(
    config: &AgentConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
) -> Result<(AgentContext, RegistrationConfig, AgentAdapters), anyhow::Error> {
    // Start Prometheus endpoint if configured
    if let Some(ref addr) = config.metrics_listen {
        info!("Prometheus metrics endpoint at {addr}");
        let listener = rapidbyte_metrics::bind_prometheus(addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            otel_guard.clone(),
            listener,
        ));
    }

    let gateway = Arc::new(
        GrpcControllerGateway::connect(
            &config.controller_url,
            config.controller_tls.as_ref(),
            config.auth_token.clone(),
        )
        .await?,
    );

    let metrics: Arc<dyn crate::domain::ports::metrics::MetricsProvider> =
        Arc::new(OtelMetricsProvider::new(&otel_guard));

    let trust_policy =
        rapidbyte_registry::TrustPolicy::from_str_name(&config.trust_policy)
            .unwrap_or(rapidbyte_registry::TrustPolicy::Skip);

    let registry_config = rapidbyte_registry::RegistryConfig {
        insecure: config.registry_insecure,
        default_registry: rapidbyte_registry::normalize_registry_url_option(
            config.registry_url.as_deref(),
        ),
        trust_policy,
        trusted_key_pems: config.trusted_key_pems.clone(),
        ..Default::default()
    };

    let executor = Arc::new(EngineExecutor::new(registry_config, metrics.clone()));
    let progress = Arc::new(AtomicProgressCollector::new());
    let clock = Arc::new(SystemClock);

    let ctx = AgentContext {
        gateway,
        executor,
        progress,
        metrics,
        clock,
        config: AgentAppConfig {
            max_tasks: config.max_tasks,
            heartbeat_interval: config.heartbeat_interval,
            poll_wait_seconds: config.poll_wait_seconds,
            ..AgentAppConfig::default()
        },
    };

    let registration = RegistrationConfig {
        max_tasks: config.max_tasks,
    };

    let adapters = AgentAdapters {
        engine_executor: executor,
        progress_collector: progress,
    };

    Ok((ctx, registration, adapters))
}
```

- [ ] **Step 3: Update adapter/mod.rs**

```rust
//! Concrete adapter implementations for agent port traits.

pub mod agent_factory;
pub mod channel_progress;
pub mod clock;
pub mod engine_executor;
pub mod grpc_controller;
pub mod metrics;
mod proto;

pub use agent_factory::{build_agent_context, AgentAdapters, AgentConfig};
pub use channel_progress::AtomicProgressCollector;
pub use clock::SystemClock;
pub use engine_executor::EngineExecutor;
pub use grpc_controller::{ClientTlsConfig, GrpcControllerGateway};
pub use metrics::OtelMetricsProvider;
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p rapidbyte-agent`
Expected: compiles successfully

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/adapter/
git commit -m "feat(agent): add EngineExecutor adapter and agent_factory composition root"
```

---

### Task 12: Update lib.rs re-exports and CLI integration

**Files:**
- Modify: `crates/rapidbyte-agent/src/lib.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

- [ ] **Step 1: Update lib.rs with canonical re-exports**

Replace `crates/rapidbyte-agent/src/lib.rs`:

```rust
//! Agent worker for Rapidbyte — polls the controller, executes pipelines, reports results.
//!
//! # Crate structure
//!
//! | Module        | Responsibility                              |
//! |---------------|---------------------------------------------|
//! | `domain`      | Port traits, domain types, errors            |
//! | `application` | DI context, use-case orchestration, fakes    |
//! | `adapter`     | gRPC client, engine executor, metrics, clock |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod domain;

// ---------------------------------------------------------------------------
// Public re-exports — canonical API surface
// ---------------------------------------------------------------------------

// Application layer
pub use application::{run_agent, AgentAppConfig, AgentContext};

// Adapter layer
pub use adapter::{build_agent_context, AgentAdapters, AgentConfig, ClientTlsConfig};

// Domain types
pub use domain::error::AgentError;
pub use domain::task::{TaskExecutionResult, TaskOutcomeKind};
```

- [ ] **Step 2: Update CLI agent command**

Replace `crates/rapidbyte-cli/src/commands/agent.rs` to use the new public API. Remove all flight/preview/signing-key parameters.

```rust
//! Agent worker subcommand.

use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

pub async fn execute(
    controller: &str,
    max_tasks: u32,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    metrics_listen: Option<&str>,
    otel_guard: rapidbyte_metrics::OtelGuard,
) -> Result<()> {
    let config = build_config(
        controller,
        max_tasks,
        auth_token,
        controller_ca_cert,
        controller_tls_domain,
        metrics_listen,
    )?;

    let otel_guard = Arc::new(otel_guard);
    let (ctx, registration, adapters) =
        rapidbyte_agent::build_agent_context(&config, otel_guard).await?;

    let shutdown = CancellationToken::new();

    // Wire signal handlers
    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("SIGINT received, stopping agent...");
        signal_token.cancel();
    });

    rapidbyte_agent::run_agent(&ctx, registration, &adapters, shutdown).await?;
    Ok(())
}

fn build_config(
    controller: &str,
    max_tasks: u32,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    metrics_listen: Option<&str>,
) -> Result<rapidbyte_agent::AgentConfig> {
    let mut config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        max_tasks,
        ..Default::default()
    };
    config.auth_token = auth_token.map(str::to_owned);
    if controller_ca_cert.is_some() || controller_tls_domain.is_some() {
        config.controller_tls = Some(rapidbyte_agent::ClientTlsConfig {
            ca_cert_pem: match controller_ca_cert {
                Some(p) => std::fs::read(p)?,
                None => Vec::new(),
            },
            domain_name: controller_tls_domain.map(str::to_owned),
        });
    }
    config.metrics_listen = metrics_listen.map(str::to_owned);
    Ok(config)
}
```

- [ ] **Step 3: Update CLI main.rs — remove flight/signing-key args from Agent command**

Modify the `Agent` variant in the `Commands` enum in `crates/rapidbyte-cli/src/main.rs`:
- Remove: `flight_listen`, `flight_advertise`, `signing_key`, `allow_insecure_signing_key`, `flight_tls_cert`, `flight_tls_key`
- Keep: `controller`, `max_tasks`, `auth_token`, `controller_ca_cert`, `controller_tls_domain`, `metrics_listen`
- Update the match arm to pass the reduced args to `commands::agent::execute`

- [ ] **Step 4: Verify full workspace compiles**

Run: `cargo check --workspace`
Expected: compiles with no errors

- [ ] **Step 5: Run all agent tests**

Run: `cargo test -p rapidbyte-agent`
Expected: all tests pass

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -p rapidbyte-agent -- -D warnings`
Expected: no warnings (no `allow(dead_code)`, no pedantic violations)

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-agent/ crates/rapidbyte-cli/
git commit -m "refactor(agent): complete hexagonal refactor — update lib.rs and CLI integration"
```

---

### Task 13: Final verification and cleanup

**Files:**
- All agent crate files
- CLI integration

- [ ] **Step 1: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all tests pass

- [ ] **Step 2: Run clippy on full workspace**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no warnings

- [ ] **Step 3: Verify no dead code or allow directives**

Run: `grep -r "allow(dead_code)" crates/rapidbyte-agent/`
Expected: no matches

Run: `grep -r "#\[allow" crates/rapidbyte-agent/`
Expected: only justified clippy allows (e.g., `cast_possible_truncation` in the gRPC adapter for duration_ms conversion)

- [ ] **Step 4: Verify deleted files are gone**

```bash
ls crates/rapidbyte-agent/src/flight.rs \
   crates/rapidbyte-agent/src/spool.rs \
   crates/rapidbyte-agent/src/ticket.rs \
   crates/rapidbyte-agent/src/auth.rs 2>&1
```
Expected: all "No such file or directory"

- [ ] **Step 5: Final commit if any cleanup was needed**

```bash
git add -A crates/rapidbyte-agent/
git commit -m "chore(agent): final cleanup after hexagonal refactor"
```
