# Controller Hexagonal Refactor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite rapidbyte-controller using hexagonal architecture with Postgres as sole source of truth, enabling multi-instance deployment.

**Architecture:** Single crate with domain/application/adapter module layers. Domain defines port traits, application orchestrates use cases, adapters implement Postgres (sqlx), gRPC (tonic), and external services. All state flows through Postgres — no in-memory state containers.

**Tech Stack:** Rust, sqlx (Postgres), tonic (gRPC), tokio, Postgres LISTEN/NOTIFY for events

**Spec:** `docs/superpowers/specs/2026-03-17-controller-hexagonal-refactor-design.md`

---

## Important Notes for Implementers

**Compilation strategy:** After Task 1 (proto replacement) and Task 2 (new lib.rs +
delete old files), the crate will only compile incrementally as domain types are added.
Use `cargo check -p rapidbyte-controller` to verify, not `cargo build --workspace`.
Other crates (CLI, agent) will break until Chunk 5.

**Shared fake state:** All in-memory test fakes (Task 11) must share backing storage.
`FakePipelineStore` holds `Arc<Mutex<HashMap>>` references to the SAME maps used by
`FakeRunRepository` and `FakeTaskRepository`. This way, `store.submit_run()` inserts
into the same maps that `runs.find_by_id()` reads from.

**Lease epoch generation:** The `TaskRepository` port trait includes `next_lease_epoch()`
so the application layer can generate epochs without knowing about Postgres sequences.
The fake returns an incrementing counter.

**SecretProviders and OtelGuard:** `server::run()` takes `ControllerConfig`,
`SecretProviders`, and `Arc<OtelGuard>` as parameters (same as current). `SecretProviders`
is passed into `VaultSecretResolver`. `OtelGuard` is held for metrics/Prometheus lifetime.
`ControllerConfig` does not embed these — they're separate params.

**Metrics instrumentation:** Each use case should call the appropriate metric counter
after successful operations (e.g., `runs_submitted().add(1)` in submit, `lease_grants().add(1)`
in poll_task). Add these calls inline when implementing each use case.

---

## Chunk 1: Foundation — Proto, Domain Types, Errors

### Task 1: Replace proto and restructure crate

**Files:**
- Modify: `proto/rapidbyte/v1/controller.proto`
- Modify: `crates/rapidbyte-controller/src/lib.rs`
- Delete: all old source files (`scheduler.rs`, `run_state.rs`, `registry.rs`, `lease.rs`,
  `preview.rs`, `watcher.rs`, `state.rs`, `terminal.rs`, `middleware.rs`, `store/`,
  `services/`, `background/`)
- Create: stub module files for new structure

- [ ] **Step 1: Replace controller.proto with new contract**

Replace the entire contents of `proto/rapidbyte/v1/controller.proto` with the proto
contract defined in the spec (Section "Proto Contract"). This is the full 10-RPC,
5-state design with multi-task heartbeat, cancel_requested, RunDetail/RunSummary, etc.

- [ ] **Step 2: Delete all old source files**

Remove: `scheduler.rs`, `run_state.rs`, `registry.rs`, `lease.rs`, `preview.rs`,
`watcher.rs`, `state.rs`, `terminal.rs`, `middleware.rs`, and the directories `store/`,
`services/`, `background/`.

- [ ] **Step 3: Replace lib.rs with new module structure**

```rust
pub mod domain;
pub mod application;
pub mod adapter;
pub mod config;
pub mod proto;

mod server;
pub use server::run;
```

- [ ] **Step 4: Create stub files so crate compiles**

Create minimal stubs for all declared modules:
- `domain/mod.rs`: empty (will be populated in Tasks 2-8)
- `application/mod.rs`: empty
- `adapter/mod.rs` with `pub mod grpc; pub mod postgres;`
- `adapter/grpc/mod.rs`: empty
- `adapter/postgres/mod.rs`: empty
- `config.rs`: minimal `pub struct ControllerConfig {}` placeholder
- `server.rs`: minimal `pub async fn run(_config: ControllerConfig) -> anyhow::Result<()> { todo!() }`

- [ ] **Step 5: Verify proto compiles and crate structure is valid**

Run: `cargo check -p rapidbyte-controller`
Expected: Compiles (all modules are stubs with `todo!()`). Other crates will fail.

- [ ] **Step 6: Commit**

```bash
git add -A crates/rapidbyte-controller/ proto/rapidbyte/v1/controller.proto
git commit -m "refactor(controller): replace proto, delete old code, scaffold hexagonal structure"
```

### Task 2: Create domain error types

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/mod.rs`
- Create: `crates/rapidbyte-controller/src/domain/error.rs`

- [ ] **Step 1: Write tests for DomainError Display**

Create `crates/rapidbyte-controller/src/domain/error.rs` with the `DomainError` enum
and `#[cfg(test)]` module. Test that each variant produces a meaningful error message via
`Display`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    #[error("invalid transition from {from} to {to}")]
    InvalidTransition { from: &'static str, to: &'static str },
    #[error("lease mismatch: expected epoch {expected}, got {got}")]
    LeaseMismatch { expected: u64, got: u64 },
    #[error("agent mismatch: expected {expected}, got {got}")]
    AgentMismatch { expected: String, got: String },
    #[error("lease expired")]
    LeaseExpired,
    #[error("retries exhausted: attempt {attempt} of {max}")]
    RetriesExhausted { attempt: u32, max: u32 },
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),
}
```

Tests: verify `to_string()` for each variant.

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller domain::error`
Expected: PASS

- [ ] **Step 3: Create domain/mod.rs**

```rust
pub mod error;
```

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-controller/src/domain/
git commit -m "feat(controller): add domain error types"
```

### Task 3: Create Lease value object

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/lease.rs`

- [ ] **Step 1: Write failing tests for Lease**

Test cases:
- `new()` creates a lease with given epoch and expiry
- `is_expired()` returns true when now > expires_at
- `is_expired()` returns false when now < expires_at
- `extend()` keeps same epoch but updates expires_at

Use `chrono::Utc::now()` and `chrono::Duration` for test timestamps.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-controller domain::lease`
Expected: FAIL

- [ ] **Step 3: Implement Lease**

```rust
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    epoch: u64,
    expires_at: DateTime<Utc>,
}

impl Lease {
    pub fn new(epoch: u64, expires_at: DateTime<Utc>) -> Self { ... }
    pub fn epoch(&self) -> u64 { ... }
    pub fn expires_at(&self) -> DateTime<Utc> { ... }
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool { ... }
    pub fn extend(&self, duration: Duration, now: DateTime<Utc>) -> Self { ... }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-controller domain::lease`
Expected: PASS

- [ ] **Step 5: Add to domain/mod.rs and commit**

```bash
git commit -m "feat(controller): add Lease value object"
```

### Task 4: Create Run aggregate with state machine

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/run.rs`

- [ ] **Step 1: Write failing tests for Run state machine**

Test ALL valid transitions:
- Pending → Running via `start()`
- Running → Completed via `complete(metrics)`
- Running → Failed via `fail(error)`
- Running → Cancelled via `cancel()`
- Pending → Cancelled via `cancel()`
- Running → Pending via `retry()` (increments attempt)

Test ALL invalid transitions return `DomainError::InvalidTransition`:
- Completed → anything
- Failed → anything
- Cancelled → anything
- Pending → Completed (must go through Running)
- Pending → Failed (must go through Running)

Test decision methods:
- `can_retry_after_error()`: true when retryable + BeforeCommit + attempts remaining
- `can_retry_after_error()`: false when AfterCommitUnknown
- `can_retry_after_error()`: false when not retryable
- `can_retry_after_error()`: false when attempts exhausted
- `can_retry_after_timeout()`: true when attempts remaining
- `can_retry_after_timeout()`: false when attempts exhausted
- `request_cancel()` sets flag without changing state
- `is_cancel_requested()` returns the flag

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-controller domain::run`

- [ ] **Step 3: Implement Run aggregate**

Define `RunState` enum (Pending, Running, Completed, Failed, Cancelled), `RunError`,
`RunMetrics`, `CommitState`, `ExecutionOptions`, and the `Run` struct with all fields
from the spec. Implement state transition methods that return `Result<(), DomainError>`.

Key types needed:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState { Pending, Running, Completed, Failed, Cancelled }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitState { BeforeCommit, AfterCommitUnknown, AfterCommitConfirmed }

#[derive(Debug, Clone)]
pub struct RunError { pub code: String, pub message: String }

#[derive(Debug, Clone)]
pub struct RunMetrics {
    pub rows_read: u64, pub rows_written: u64,
    pub bytes_read: u64, pub bytes_written: u64,
    pub duration_ms: u64,
}
```

The `Run` struct fields: id (String), idempotency_key (Option<String>), pipeline_name
(String), pipeline_yaml (String), state (RunState), current_attempt (u32), max_retries
(u32), timeout_seconds (Option<u64>), cancel_requested (bool), error (Option<RunError>),
metrics (Option<RunMetrics>), created_at (DateTime<Utc>), updated_at (DateTime<Utc>).

Constructor: `Run::new(id, idempotency_key, pipeline_name, pipeline_yaml, max_retries, timeout_seconds, now)`

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-controller domain::run`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(controller): add Run aggregate with state machine"
```

### Task 5: Create Task aggregate with state machine

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/task.rs`

- [ ] **Step 1: Write failing tests for Task state machine**

Test valid transitions:
- Pending → Running via `assign(agent_id, lease)`
- Running → Completed via `complete()`
- Running → Failed via `fail()`
- Running → Cancelled via `cancel()`
- Running → TimedOut via `timeout()`

Test invalid transitions return DomainError::InvalidTransition.

Test `validate_lease()`:
- Matching agent_id + epoch → Ok
- Wrong agent_id → DomainError::AgentMismatch
- Wrong epoch → DomainError::LeaseMismatch
- No lease (Pending task) → DomainError::LeaseExpired

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement Task aggregate**

`TaskState` enum: Pending, Running, Completed, Failed, Cancelled, TimedOut.
`Task` struct with fields: id, run_id, attempt, state, agent_id, lease, created_at,
updated_at.
Constructor: `Task::new(id, run_id, attempt, now)`.

- [ ] **Step 4: Run tests, verify pass**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(controller): add Task aggregate with state machine"
```

### Task 6: Create Agent aggregate

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/agent.rs`

- [ ] **Step 1: Write tests**

- `touch()` updates `last_seen_at`
- `is_alive()` returns true when within timeout
- `is_alive()` returns false when past timeout

- [ ] **Step 2: Implement Agent aggregate**

```rust
pub struct AgentCapabilities {
    pub plugins: Vec<String>,
    pub max_concurrent_tasks: u32,
}

pub struct Agent {
    id: String,
    capabilities: AgentCapabilities,
    last_seen_at: DateTime<Utc>,
    registered_at: DateTime<Utc>,
}
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): add Agent aggregate"
```

### Task 7: Create DomainEvent enum

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/event.rs`

- [ ] **Step 1: Define DomainEvent**

```rust
#[derive(Debug, Clone)]
pub enum DomainEvent {
    RunStateChanged { run_id: String, state: RunState, attempt: u32 },
    ProgressReported { run_id: String, message: String, pct: Option<f64> },
    RunCompleted { run_id: String, metrics: RunMetrics },
    RunFailed { run_id: String, error: RunError },
    RunCancelled { run_id: String },
}
```

No tests needed — this is a data-only enum with no behavior.

- [ ] **Step 2: Commit**

```bash
git commit -m "feat(controller): add DomainEvent enum"
```

### Task 8: Create port traits

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/ports/mod.rs`
- Create: `crates/rapidbyte-controller/src/domain/ports/repository.rs`
- Create: `crates/rapidbyte-controller/src/domain/ports/pipeline_store.rs`
- Create: `crates/rapidbyte-controller/src/domain/ports/event_bus.rs`
- Create: `crates/rapidbyte-controller/src/domain/ports/secrets.rs`
- Create: `crates/rapidbyte-controller/src/domain/ports/clock.rs`

- [ ] **Step 1: Define all port traits**

Define each trait as specified in the spec. All repository traits use `#[async_trait]`.
Each trait is `Send + Sync`.

Define supporting types:
- `RepositoryError` (wraps `Box<dyn std::error::Error + Send + Sync>`)
- `EventBusError` (same pattern)
- `SecretError` (wraps String)
- `EventStream` type alias (Pin<Box<dyn Stream<Item = DomainEvent> + Send>>)
- `RunFilter` and `Pagination` structs for `RunRepository::list`

Important: `TaskRepository` must include `async fn next_lease_epoch(&self) -> Result<u64, RepositoryError>`
so the application layer can generate lease epochs without knowing about Postgres sequences.
The `PgTaskRepository` implements this via `SELECT nextval('lease_epoch_seq')`.
The `FakeTaskRepository` implements this with an `AtomicU64` counter.

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p rapidbyte-controller`
Expected: Compiles (traits are just signatures, no implementation needed yet)

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(controller): add port traits (repositories, event bus, secrets, clock)"
```

### Task 9: Create application error types and context

**Files:**
- Create: `crates/rapidbyte-controller/src/application/mod.rs`
- Create: `crates/rapidbyte-controller/src/application/error.rs`
- Create: `crates/rapidbyte-controller/src/application/context.rs`

- [ ] **Step 1: Define AppError**

```rust
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },
    #[error("run already exists: {run_id}")]
    AlreadyExists { run_id: String },
    #[error("repository error: {0}")]
    Repository(#[from] RepositoryError),
    #[error("event bus error: {0}")]
    EventBus(#[from] EventBusError),
    #[error("secret resolution failed: {0}")]
    SecretResolution(String),
}
```

- [ ] **Step 2: Define AppContext and AppConfig**

Per spec — `AppContext` holds Arc<dyn Trait> for all ports plus `AppConfig`.

- [ ] **Step 3: Verify compilation, commit**

```bash
git commit -m "feat(controller): add AppError, AppContext, AppConfig"
```

---

## Chunk 2: Shared Crate Extraction and Application Layer

### Task 10: Extract rapidbyte-pipeline-config crate

**Files:**
- Create: `crates/rapidbyte-pipeline-config/Cargo.toml`
- Create: `crates/rapidbyte-pipeline-config/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)
- Modify: `crates/rapidbyte-engine/Cargo.toml`
- Modify: `crates/rapidbyte-engine/src/config/parser.rs`
- Modify: `crates/rapidbyte-controller/Cargo.toml`

- [ ] **Step 1: Create new crate**

Create `crates/rapidbyte-pipeline-config/Cargo.toml` with dependencies: `anyhow`,
`regex`, `serde`, `serde_yaml`, `tokio`, `rapidbyte-secrets`.

Create `crates/rapidbyte-pipeline-config/src/lib.rs` — move from
`rapidbyte-engine/src/config/parser.rs`:
- `reject_malformed_refs()`
- `substitute_secrets()`
- `substitute_variables()`
- `contains_secret_refs()`
- Internal helpers: `apply_replacements()`, `collect_secret_replacements()`
- Static regex matchers
- `MatchReplacement` struct
- All tests from the parser module

Add new function with tests: `extract_pipeline_name(yaml_str: &str) -> Result<String>`
(parses YAML, extracts `pipeline` field as string, returns "unknown" if missing).

- [ ] **Step 2: Update engine to re-export from new crate**

In `rapidbyte-engine/Cargo.toml`, add dep on `rapidbyte-pipeline-config`.
In `rapidbyte-engine/src/config/parser.rs`, replace moved functions with re-exports:
```rust
pub use rapidbyte_pipeline_config::{reject_malformed_refs, substitute_secrets, ...};
```

- [ ] **Step 3: Add workspace member and update controller dep**

Add `"crates/rapidbyte-pipeline-config"` to workspace members in root `Cargo.toml`.
In `crates/rapidbyte-controller/Cargo.toml`, add `rapidbyte-pipeline-config` dep
(replacing `rapidbyte-engine` dep).

- [ ] **Step 4: Run workspace tests**

Run: `cargo test --workspace`
Expected: All existing engine tests pass via re-exports.

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor: extract rapidbyte-pipeline-config crate"
```

### Task 11: Create in-memory test fakes

**Files:**
- Create: `crates/rapidbyte-controller/src/application/testing.rs`

- [ ] **Step 1: Implement in-memory fakes for all port traits**

Create `FakeRunRepository`, `FakeTaskRepository`, `FakeAgentRepository`,
`FakePipelineStore`, `FakeEventBus`, `FakeSecretResolver`, `FakeClock`.

**Critical:** All fakes share a common backing store so cross-aggregate operations are
observable. Create a `FakeStorage` struct that holds the shared `Arc<Mutex<HashMap>>`
maps for runs, tasks, and agents. `FakeRunRepository`, `FakeTaskRepository`, and
`FakePipelineStore` all hold references to the SAME `FakeStorage`. This way,
`store.submit_run()` inserts into the same maps that `runs.find_by_id()` reads from.

`FakeEventBus` uses `tokio::sync::broadcast`. `FakeClock` holds an
`Arc<Mutex<DateTime<Utc>>>` that tests can advance. `FakeTaskRepository::next_lease_epoch()`
uses an internal `AtomicU64` counter.

Create a `fake_context()` helper that builds an `AppContext` with all fakes
pre-wired (using shared `FakeStorage`), plus accessors to inspect state
(e.g., `get_published_events()`, `get_run()`, `get_task()`).

Mark the module `#[cfg(test)]` so it's only compiled for tests.

- [ ] **Step 2: Verify compilation**

Run: `cargo test -p rapidbyte-controller --no-run`
Expected: Compiles

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(controller): add in-memory test fakes for port traits"
```

### Task 12: Implement submit_pipeline use case

**Files:**
- Create: `crates/rapidbyte-controller/src/application/submit.rs`

- [ ] **Step 1: Write failing tests**

Test cases:
- Happy path: submit creates run + task, returns run_id
- Idempotency: submitting same key returns existing run_id with already_exists=true
- Invalid YAML: returns AppError containing DomainError::InvalidPipeline
- Verify RunStateChanged event is published

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Implement submit_pipeline**

```rust
pub async fn submit_pipeline(
    ctx: &AppContext,
    idempotency_key: Option<String>,
    pipeline_yaml: String,
    max_retries: u32,
    timeout_seconds: Option<u64>,
) -> Result<SubmitResult, AppError>
```

Flow per spec: check idempotency, validate YAML and extract pipeline name using
`rapidbyte_pipeline_config::extract_pipeline_name()` (from Task 10), create Run + Task,
save via `store.submit_run()`, publish event.

- [ ] **Step 4: Run tests, verify pass**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(controller): implement submit_pipeline use case"
```

### Task 13: Implement poll_task use case

**Files:**
- Create: `crates/rapidbyte-controller/src/application/poll.rs`

- [ ] **Step 1: Write failing tests**

Test cases:
- Happy path: agent polls, gets pending task assignment
- No tasks available: returns None
- Unknown agent: returns AppError::NotFound
- Agent liveness updated on poll
- Lease epoch and expiry set on assignment
- Run transitions to Running
- RunStateChanged event published
- Secret resolution failure: task is released (timed out), run retried if possible
- timeout_seconds from run overrides default lease duration

- [ ] **Step 2: Implement poll_task**

```rust
pub async fn poll_task(
    ctx: &AppContext,
    agent_id: &str,
) -> Result<Option<TaskAssignment>, AppError>
```

Flow per spec. Note: `poll_and_assign` on the fake does
`SELECT ... FOR UPDATE SKIP LOCKED` semantics — the fake just grabs the first pending
task. Secret resolution calls `ctx.secrets.resolve()`.

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement poll_task use case"
```

### Task 14: Implement heartbeat use case

**Files:**
- Create: `crates/rapidbyte-controller/src/application/heartbeat.rs`

- [ ] **Step 1: Write failing tests**

Test cases:
- Agent liveness updated
- Lease extended for active task
- Stale lease rejected (acknowledged=false in directive)
- Cancel requested when run has cancel_requested=true
- Progress event published (ephemeral)
- Multiple tasks heartbeated in one call

- [ ] **Step 2: Implement heartbeat**

```rust
pub async fn heartbeat(
    ctx: &AppContext,
    agent_id: &str,
    tasks: Vec<TaskHeartbeatInput>,
) -> Result<Vec<TaskDirectiveOutput>, AppError>
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement heartbeat use case"
```

### Task 15: Implement complete_task use case

**Files:**
- Create: `crates/rapidbyte-controller/src/application/complete.rs`

- [ ] **Step 1: Write failing tests**

Test cases per outcome:
- **Completed**: task → Completed, run → Completed, metrics stored, event published
- **Failed + retryable**: task → Failed, run → Pending, new task created, event published
- **Failed + terminal (not retryable)**: task → Failed, run → Failed, event published
- **Failed + terminal (retries exhausted)**: same as above
- **Failed + after_commit**: not retryable regardless of retryable flag
- **Cancelled**: task → Cancelled, run → Cancelled, event published
- **Stale lease**: returns error (lease validation fails)

- [ ] **Step 2: Implement complete_task**

```rust
pub async fn complete_task(
    ctx: &AppContext,
    agent_id: &str,
    task_id: &str,
    lease_epoch: u64,
    outcome: TaskOutcome,
) -> Result<(), AppError>
```

Where `TaskOutcome` is a domain enum:
```rust
pub enum TaskOutcome {
    Completed { metrics: RunMetrics },
    Failed { error: TaskError },
    Cancelled,
}
pub struct TaskError {
    pub code: String, pub message: String,
    pub retryable: bool, pub commit_state: CommitState,
}
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement complete_task use case"
```

### Task 16: Implement cancel_run use case

**Files:**
- Create: `crates/rapidbyte-controller/src/application/cancel.rs`

- [ ] **Step 1: Write failing tests**

Test cases:
- Cancel pending run: immediate transition to Cancelled, task cancelled
- Cancel running run: sets cancel_requested flag, state stays Running
- Cancel already terminal: returns CancelResult { accepted: false }
- Cancel non-existent run: AppError::NotFound

- [ ] **Step 2: Implement cancel_run**

```rust
pub async fn cancel_run(
    ctx: &AppContext,
    run_id: &str,
) -> Result<CancelResult, AppError>
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement cancel_run use case"
```

### Task 17: Implement query use cases (get_run, list_runs, watch_run)

**Files:**
- Create: `crates/rapidbyte-controller/src/application/query.rs`

- [ ] **Step 1: Write tests**

- get_run: returns run by id, NotFound if missing
- list_runs: returns filtered, paginated list
- watch_run: subscribe returns event stream, first event is current state

- [ ] **Step 2: Implement query functions**

```rust
pub async fn get_run(ctx: &AppContext, run_id: &str) -> Result<Run, AppError>
pub async fn list_runs(ctx: &AppContext, filter: RunFilter, pagination: Pagination) -> Result<RunPage, AppError>
pub async fn watch_run(ctx: &AppContext, run_id: &str) -> Result<(Run, EventStream), AppError>
```

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement query use cases"
```

### Task 18: Implement register/deregister use cases

**Files:**
- Create: `crates/rapidbyte-controller/src/application/register.rs`

- [ ] **Step 1: Write tests**

- Register: creates agent, upsert on re-register
- Deregister: deletes agent, timeouts running tasks, retries/fails affected runs
- Deregister unknown agent: AppError::NotFound

- [ ] **Step 2: Implement**

```rust
pub async fn register(ctx: &AppContext, agent_id: &str, capabilities: AgentCapabilities) -> Result<RegistryInfo, AppError>
pub async fn deregister(ctx: &AppContext, agent_id: &str) -> Result<(), AppError>
```

The deregister flow: find_running_by_agent_id, timeout each, retry or fail runs, delete
agent. Per spec.

- [ ] **Step 3: Run tests, verify pass, commit**

```bash
git commit -m "feat(controller): implement register/deregister use cases"
```

### Task 19: Implement background use cases

**Files:**
- Create: `crates/rapidbyte-controller/src/application/background/mod.rs`
- Create: `crates/rapidbyte-controller/src/application/background/lease_sweep.rs`
- Create: `crates/rapidbyte-controller/src/application/background/agent_reaper.rs`

- [ ] **Step 1: Write tests for sweep_expired_leases**

- Expired task → timed out, run retried (if attempts remain)
- Expired task → timed out, run failed (if retries exhausted)
- No expired tasks → no-op

- [ ] **Step 2: Implement sweep_expired_leases**

```rust
pub async fn sweep_expired_leases(ctx: &AppContext) -> Result<(), AppError>
```

Uses `tasks.find_expired_leases(now)`, then for each: timeout task, check
`run.can_retry_after_timeout()`, retry or fail, save via `store.timeout_and_retry()`.

- [ ] **Step 3: Write tests for reap_stale_agents**

- Stale agent → deregistered, tasks released
- No stale agents → no-op

- [ ] **Step 4: Implement reap_stale_agents**

```rust
pub async fn reap_stale_agents(ctx: &AppContext) -> Result<(), AppError>
```

Uses `agents.find_stale(timeout, now)`, then calls deregister logic for each.

- [ ] **Step 5: Run all tests, commit**

```bash
git commit -m "feat(controller): implement background use cases (lease sweep, agent reaper)"
```

---

## Chunk 3: Postgres Adapters

### Task 20: Update Cargo.toml and add migration

**Files:**
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Create: `crates/rapidbyte-controller/migrations/0001_initial.sql`

- [ ] **Step 1: Update Cargo.toml**

Replace dependencies:
- Remove: `hmac`, `sha2`, `bytes`, `tokio-postgres`, `tokio-rustls`, `rustls`,
  `rustls-native-certs`, `rapidbyte-engine`
- Add: `sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "chrono", "migrate"] }`
- Add: `serde = { workspace = true, features = ["derive"] }`
- Add: `serde_json = { workspace = true }`
- Keep: `tonic`, `prost`, `prost-types`, `tokio`, `tokio-stream`, `tower`, `async-trait`,
  `thiserror`, `chrono`, `uuid`, `tracing`, `rapidbyte-secrets`, `rapidbyte-metrics`

- [ ] **Step 2: Create migration file**

Create `migrations/0001_initial.sql` with the full schema from the spec (CREATE SEQUENCE,
CREATE TABLE runs/tasks/agents, all indexes).

- [ ] **Step 3: Remove old migration file**

Delete: `crates/rapidbyte-controller/src/store/migrations/0001_controller_metadata.sql`

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p rapidbyte-controller`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(controller): update deps (sqlx), add new migration schema"
```

### Task 22: Implement PgRunRepository

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/postgres/run.rs`

- [ ] **Step 1: Implement PgRunRepository**

Implement the `RunRepository` trait using sqlx queries:
- `find_by_id`: `SELECT * FROM runs WHERE id = $1`
- `find_by_idempotency_key`: `SELECT * FROM runs WHERE idempotency_key = $1`
- `save`: `INSERT ... ON CONFLICT (id) DO UPDATE SET ...`
- `list`: Dynamic query with optional state filter + cursor pagination

Include row mapping from sqlx Row to domain `Run` type. Use `sqlx::FromRow` derive
where possible, or manual mapping for enum fields (RunState from TEXT).

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p rapidbyte-controller`

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(controller): implement PgRunRepository"
```

### Task 23: Implement PgTaskRepository

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/postgres/task.rs`

- [ ] **Step 1: Implement PgTaskRepository**

Key methods:
- `poll_and_assign`: The critical query — `SELECT ... FOR UPDATE SKIP LOCKED` + UPDATE
  in one transaction. Takes agent_id and Lease, returns assigned Task.
- `find_expired_leases`: `SELECT * FROM tasks WHERE state = 'running' AND lease_expires_at < $1`
- `find_running_by_agent_id`: `SELECT * FROM tasks WHERE state = 'running' AND agent_id = $1`
- `find_by_id`, `save`, `find_by_run_id`: standard CRUD

The `poll_and_assign` method:
```rust
async fn poll_and_assign(&self, agent_id: &str, lease: Lease) -> Result<Option<Task>, RepositoryError> {
    let mut tx = self.pool.begin().await?;
    let row = sqlx::query("SELECT * FROM tasks WHERE state = 'pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED")
        .fetch_optional(&mut *tx).await?;
    // If found: UPDATE state, agent_id, lease fields, COMMIT
    // Return mapped Task
}
```

Include `next_lease_epoch()` method:
```rust
pub async fn next_lease_epoch(&self) -> Result<u64, RepositoryError> {
    let epoch: i64 = sqlx::query_scalar("SELECT nextval('lease_epoch_seq')")
        .fetch_one(&self.pool).await?;
    Ok(epoch as u64)
}
```

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement PgTaskRepository"
```

### Task 24: Implement PgAgentRepository

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/postgres/agent.rs`

- [ ] **Step 1: Implement PgAgentRepository**

Standard CRUD plus:
- `find_stale`: `SELECT * FROM agents WHERE last_seen_at < $1`
- `save`: upsert with `ON CONFLICT (id) DO UPDATE`
- `delete`: `DELETE FROM agents WHERE id = $1`

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement PgAgentRepository"
```

### Task 25: Implement PgPipelineStore

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/postgres/store.rs`

- [ ] **Step 1: Implement PgPipelineStore**

Each method wraps multiple operations in a transaction:

```rust
async fn submit_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
    let mut tx = self.pool.begin().await?;
    // INSERT run
    // INSERT task
    tx.commit().await?;
    Ok(())
}

async fn complete_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
    let mut tx = self.pool.begin().await?;
    // UPDATE task SET state = 'completed', updated_at
    // UPDATE run SET state = 'completed', metrics fields, updated_at
    tx.commit().await?;
    Ok(())
}
```

Implement all 7 methods from the PipelineStore trait: `submit_run`, `complete_run`,
`fail_run`, `fail_and_retry`, `cancel_run`, `cancel_pending_run`, `timeout_and_retry`.

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement PgPipelineStore (cross-aggregate transactions)"
```

### Task 26: Implement PgEventBus

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/postgres/event_bus.rs`

- [ ] **Step 1: Implement PgEventBus**

Structure:
```rust
pub struct PgEventBus {
    pool: PgPool,
    subscribers: Arc<RwLock<HashMap<String, broadcast::Sender<DomainEvent>>>>,
}
```

Methods:
- `publish()`: Serialize `DomainEvent` to JSON, call `SELECT pg_notify('run_events', $1)`
- `subscribe()`: Create or get broadcast channel for run_id, return receiver as EventStream
- `start_listener()`: Spawn background task that runs `LISTEN run_events` on dedicated
  connection, deserializes notifications, routes to appropriate broadcast channel. On
  reconnect, notify subscribers.

Use `sqlx::postgres::PgListener` for the LISTEN connection.

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement PgEventBus (LISTEN/NOTIFY)"
```

### Task 27: Implement external adapters (secrets, clock)

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/secrets.rs`
- Create: `crates/rapidbyte-controller/src/adapter/clock.rs`

- [ ] **Step 1: Implement VaultSecretResolver**

Wraps `rapidbyte_pipeline_config::substitute_secrets()` and
`rapidbyte_secrets::SecretProviders`. Implements `SecretResolver` port.

- [ ] **Step 2: Implement SystemClock**

```rust
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> { Utc::now() }
}
```

- [ ] **Step 3: Wire adapter/mod.rs**

```rust
pub mod grpc;
pub mod postgres;
pub mod secrets;
pub mod clock;
```

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(controller): implement VaultSecretResolver and SystemClock adapters"
```

---

## Chunk 4: gRPC Adapters and Server Wiring

### Task 28: Implement gRPC convert module

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/grpc/convert.rs`

- [ ] **Step 1: Implement proto ↔ domain conversions**

Pure functions:
- `to_status(err: AppError) -> tonic::Status` — maps error types to gRPC codes
- `run_to_detail(run: &Run) -> proto::RunDetail`
- `run_to_summary(run: &Run) -> proto::RunSummary`
- `domain_event_to_run_event(event: &DomainEvent) -> proto::RunEvent`
- `to_execution_options(proto: proto::ExecutionOptions) -> (u32, Option<u64>)`
  (returns max_retries, timeout_seconds)

- [ ] **Step 2: Write tests for conversions**

Test key mappings:
- Each AppError variant maps to correct Status code
- RunState domain → proto enum
- DomainEvent → RunEvent proto

- [ ] **Step 3: Run tests, commit**

```bash
git commit -m "feat(controller): implement gRPC proto/domain conversion layer"
```

### Task 29: Implement PipelineGrpcService

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/grpc/pipeline.rs`

- [ ] **Step 1: Implement PipelineService tonic trait**

```rust
pub struct PipelineGrpcService {
    ctx: Arc<AppContext>,
}
```

Each method: convert request → call use case → convert response. 3-5 lines per method.
`watch_run` maps the domain EventStream to a `ResponseStream<RunEvent>`.

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement PipelineGrpcService"
```

### Task 30: Implement AgentGrpcService

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/grpc/agent.rs`

- [ ] **Step 1: Implement AgentService tonic trait**

Same pattern as PipelineGrpcService. `heartbeat` maps `Vec<TaskHeartbeat>` proto to
domain inputs, calls use case, maps directives back to `Vec<TaskDirective>` proto.

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(controller): implement AgentGrpcService"
```

### Task 31: Implement auth middleware

**Files:**
- Create: `crates/rapidbyte-controller/src/adapter/grpc/auth.rs`

- [ ] **Step 1: Implement BearerAuthInterceptor**

Port the existing auth interceptor concept. Tower interceptor that checks
`authorization` header against configured bearer tokens. If `allow_unauthenticated`
is set, pass through.

- [ ] **Step 2: Commit**

```bash
git commit -m "feat(controller): implement BearerAuthInterceptor"
```

### Task 32: Implement config and server composition root

**Files:**
- Create: `crates/rapidbyte-controller/src/config.rs`
- Create: `crates/rapidbyte-controller/src/server.rs`

- [ ] **Step 1: Implement ControllerConfig**

```rust
pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub database_url: String,
    pub auth: AuthConfig,
    pub app: AppConfig,
    pub tls: Option<TlsConfig>,
    pub metrics_listen: Option<SocketAddr>,
}
```

Include `validate()` function.

- [ ] **Step 2: Implement server::run()**

The composition root per spec:
1. Connect to Postgres (PgPool)
2. Run migrations
3. Build adapters (repositories, store, event bus, secrets, clock)
4. Start LISTEN listener
5. Compose AppContext
6. Spawn background tasks (lease_sweep, agent_reaper)
7. Build gRPC services
8. Configure TLS/auth
9. Start tonic server

```rust
pub async fn run(
    config: ControllerConfig,
    otel_guard: Arc<OtelGuard>,
    secrets: SecretProviders,
) -> anyhow::Result<()> { ... }
```

`SecretProviders` is passed into `VaultSecretResolver` adapter. `OtelGuard` is held
alive for Prometheus metrics lifetime. Metrics endpoint is spawned if
`config.metrics_listen` is set (same as current behavior).

Note: `anyhow::Result` is used only at this top-level boundary (the entry point). All
internal code uses typed errors.

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p rapidbyte-controller`
Expected: Full crate compiles

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(controller): implement config and server composition root"
```

---

## Chunk 5: CLI Updates and Integration

### Task 33: Update CLI controller command

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`

- [ ] **Step 1: Update controller command args**

Update the CLI argument struct to match `ControllerConfig`. Remove: `signing_key`,
`allow_insecure_signing_key`, `reconciliation_timeout_seconds`. The `metadata_database_url`
becomes `database_url` (required, not optional — Postgres is always needed now).

- [ ] **Step 2: Update the execute function**

Map CLI args to `ControllerConfig`, call `rapidbyte_controller::run(config)`.

- [ ] **Step 3: Verify compilation, commit**

```bash
git commit -m "feat(cli): update controller command for new config"
```

### Task 34: Update CLI distributed_run command

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`

- [ ] **Step 1: Update for new proto types**

- `SubmitPipelineRequest`: use `string pipeline_yaml` instead of `bytes pipeline_yaml_utf8`
- `ExecutionOptions`: remove `dry_run`, `limit`. Add `max_retries`, `timeout_seconds`.
- `WatchRunRequest`/`RunEvent`: update to match new proto shapes (oneof detail, RunState)
- Remove all preview-related code (PreviewAccess, StreamPreview, Flight ticket handling)

- [ ] **Step 2: Verify compilation, commit**

```bash
git commit -m "feat(cli): update distributed_run for new proto contract"
```

### Task 35: Update CLI status, watch, list_runs commands

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/status.rs`
- Modify: `crates/rapidbyte-cli/src/commands/watch.rs`
- Modify: `crates/rapidbyte-cli/src/commands/list_runs.rs`

- [ ] **Step 1: Update status command**

`GetRunResponse` now wraps `RunDetail`. Update field access:
`resp.run` instead of flat fields. Add `cancel_requested`, `attempt`, `max_retries`.
Remove preview-related display.

- [ ] **Step 2: Update watch command**

`RunEvent` now uses oneof detail. Update match arms for `ProgressDetail`,
`CompletionDetail`, `FailureDetail`.

- [ ] **Step 3: Update list_runs command**

`ListRunsResponse` now returns `RunSummary` (not `RunSummary` with same name but
different shape). Update display. Add cursor-based pagination if needed.

- [ ] **Step 4: Verify compilation, commit**

```bash
git commit -m "feat(cli): update status/watch/list_runs for new proto"
```

### Task 36: Update agent crate proto

**Files:**
- Modify: `crates/rapidbyte-agent/build.rs` (if needed)
- Modify: `crates/rapidbyte-agent/src/proto.rs` (if needed)
- Modify: agent source files that use old proto types

- [ ] **Step 1: Check agent impact**

The agent compiles its own copy of the proto. Since we replaced `controller.proto`,
the agent's generated types change too. Update agent code that uses: `RegisterAgentRequest`
→ `RegisterRequest`, `HeartbeatRequest` (repeated `TaskHeartbeat` instead of
`ActiveLease`), `CompleteTaskRequest` (oneof outcome), `ReportProgressRequest` (removed),
etc.

- [ ] **Step 2: Update agent code for new proto shapes**

This is the biggest change: heartbeat now sends `repeated TaskHeartbeat` and receives
`repeated TaskDirective`. Progress reporting is folded into heartbeat. Preview-related
fields removed from CompleteTask. Trust policy fields removed from RegisterResponse
(agent code that reads `trust_policy` and `trusted_key_pems` must be stripped — these
fields no longer exist in the proto).

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p rapidbyte-agent`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(agent): update for new controller proto contract"
```

### Task 37: Full workspace compilation and test

- [ ] **Step 1: Build entire workspace**

Run: `cargo build --workspace`
Expected: All crates compile

- [ ] **Step 2: Run all tests**

Run: `cargo test --workspace`
Expected: All tests pass (domain + application tests via fakes, existing engine tests
via re-exports)

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings. No `allow(dead_code)` anywhere.

- [ ] **Step 4: Commit any fixes**

```bash
git commit -m "fix: resolve workspace-wide compilation and clippy issues"
```

---

## Chunk 6: Cleanup and Verification

### Task 38: Remove dead code and old files

- [ ] **Step 1: Verify no old module references remain**

Search for any remaining references to removed modules: `scheduler`, `run_state`,
`registry`, `preview`, `watcher`, `state` (as module), `terminal`, `store` (old).

Run: `grep -r "rapidbyte_controller::scheduler\|rapidbyte_controller::run_state\|rapidbyte_controller::preview\|rapidbyte_controller::watcher\|rapidbyte_controller::state\|rapidbyte_controller::terminal" crates/`
Expected: No results

- [ ] **Step 2: Remove old store migration directory if still present**

Delete `crates/rapidbyte-controller/src/store/` directory entirely if not already done.

- [ ] **Step 3: Verify no allow(dead_code) or clippy suppressions**

Run: `grep -r "allow(dead_code)\|allow(unused" crates/rapidbyte-controller/src/`
Expected: No results

- [ ] **Step 4: Final clippy and test**

Run: `cargo clippy -p rapidbyte-controller -- -D warnings && cargo test -p rapidbyte-controller`
Expected: Clean

- [ ] **Step 5: Commit**

```bash
git commit -m "chore: remove dead code, verify clean clippy"
```

### Task 39: Update CLAUDE.md

- [ ] **Step 1: Update project structure in CLAUDE.md**

Update the crate dependency graph to include `rapidbyte-pipeline-config` and reflect
the new controller architecture. Remove references to old controller internals
(ControllerState, snapshot-restore, etc.).

- [ ] **Step 2: Commit**

```bash
git commit -m "docs: update CLAUDE.md for controller hexagonal refactor"
```
