# Controller Hexagonal Refactor Design

**Date:** 2026-03-17
**Status:** Draft
**Scope:** Complete rewrite of `rapidbyte-controller` crate using hexagonal architecture

## Context

The current `rapidbyte-controller` (~5,350 lines) is a distributed pipeline orchestration
control plane with gRPC services, in-memory state machines, optional Postgres durability,
and background cleanup tasks. It has accumulated significant structural issues:

- `ControllerState` is a god object (13 public fields, directly accessed everywhere)
- Snapshot-restore rollback pattern is pervasive and duplicates state machine knowledge
- Service handlers mix domain logic, persistence, retry decisions, and metrics
- No port/adapter boundaries — Postgres, gRPC, and domain logic are tightly coupled
- `store/mod.rs` is 1,800 lines of concrete Postgres implementation
- In-memory-first state model (`Arc<RwLock<HashMap>>`) prevents multi-instance deployment

## Goals

- Multi-instance deployment behind a load balancer (Postgres is source of truth)
- Clean hexagonal architecture with enforced layer boundaries
- Single way of doing things — no legacy code, no bridge patterns
- All code justified — no `allow(dead_code)`, no clippy suppressions
- Breaking proto changes are acceptable (coordinated CLI + controller + agent deploy)
- Remove preview system and dry_run entirely (can be re-added later)
- ~45% code reduction (estimated ~3,000 lines from ~5,350)

## Non-Goals

- Caching layer (Redis/Moka) — not needed; Postgres handles the workload
- Preview/dry-run — removed entirely, can be re-added later with a proxy-based design
- Multiple database backends — Postgres only
- Long-polling or streaming task assignment — simple poll-based model

## Architecture

Single crate (`rapidbyte-controller`), organized by hexagonal layers via modules:

```
domain/         Pure business logic, zero framework deps
    |
    v
application/    Use cases — orchestrate domain + ports
    |
    v
adapter/        Infrastructure — gRPC, Postgres, secrets
    |
    v
server.rs       Composition root — wires everything
```

Strict rule: arrows only point downward. `domain/` never imports from `application/` or
`adapter/`. `application/` never imports from `adapter/`.

## RPC Design

10 RPCs total, split across two services. The current proto has 10 RPCs (5 PipelineService
+ 5 AgentService). The new design keeps 10 RPCs but changes the composition: removes
`ReportProgress` (folded into Heartbeat) and adds `Deregister` (clean agent shutdown).

**Breaking change:** This is a wire-incompatible proto rewrite. All field numbers, types,
and message shapes change. Requires coordinated deployment of CLI, controller, and agent.

### PipelineService (client-facing)

| RPC | Purpose |
|-----|---------|
| `SubmitPipeline` | Submit pipeline YAML + idempotency key. Returns run_id |
| `GetRun` | Get current run state, metrics, error |
| `WatchRun` | Server-stream of run events via PG LISTEN/NOTIFY |
| `CancelRun` | Request cancellation. Agent learns via HeartbeatResponse |
| `ListRuns` | Paginated list with filters |

### AgentService (agent-facing)

| RPC | Purpose |
|-----|---------|
| `Register` | Announce agent: capabilities, capacity. Returns agent_id |
| `Deregister` | Clean shutdown: release assigned tasks back to queue |
| `PollTask` | Get work. Returns full payload with resolved secrets, lease, registry info |
| `Heartbeat` | Liveness + optional progress + lease renewal. Response carries cancellation signal |
| `CompleteTask` | Report outcome: completed (metrics), failed (error + retry info), or cancelled |

### Changed RPCs

| RPC | Change |
|-----|--------|
| `ReportProgress` | Removed — folded into `Heartbeat` as optional progress payload |
| `Deregister` | Added — clean agent shutdown, releases assigned tasks back to queue |
| `Heartbeat` | Changed — carries optional per-task progress, supports multi-task lease reporting; response carries cancellation signal and renewed lease epochs |
| `PollTask` | Changed — returns full payload with resolved secrets (no separate Dispatch step). `wait_seconds` long-poll removed for simplicity |
| `Register` | Changed — response includes registry info; renamed from `RegisterAgent` |
| `CompleteTask` | Changed — outcome is oneof (completed/failed/cancelled), preview and backend_run_id removed |

Note: The current codebase has no `Dispatch`, `GetSecret`, or `ReportRunning` RPCs in the
proto despite earlier discussion suggesting their existence. The internal `dispatch.rs` is
a helper module within the `PollTask` handler, not a separate RPC.

## Run State Machine

```
Pending --> Running --> Completed (with metrics)
   ^            |--> Failed (with error)
   |            '--> Cancelled
   '-- (retry: task failed but retryable, new attempt enqueued)
```

5 states (down from 11). Removed: Assigned, PreviewReady, TimedOut, Reconciling,
RecoveryFailed, Cancelling.

- **Assigned** — distinction not useful to clients. PollTask transitions directly to Running.
- **PreviewReady** — preview system removed entirely.
- **TimedOut** — a timeout is Failed with error_code indicating lease expiry.
- **Reconciling/RecoveryFailed** — with Postgres as source of truth and lease expiry,
  the system self-heals. No crash-recovery state needed.
- **Cancelling** — replaced by a `cancel_requested` boolean flag on the runs table.
  `cancel_run` sets the flag but does NOT change `state`. The run stays `Running`.
  The heartbeat use case checks the flag and returns `cancel_requested: true` in
  the response. The agent then completes with `TaskCancelled`, which transitions
  the run to `Cancelled`. If the agent ignores the cancel and completes normally,
  the run completes normally. If the lease expires, normal timeout handling applies.
  This is cleaner than a separate state — cancellation is a request, not a state.

### Task State Machine

```
Pending --> Running --> Completed
                  |--> Failed
                  |--> Cancelled
                  '--> TimedOut (lease expired)
```

Tasks are internal — clients see runs only. The `attempt` number on RunDetail tells clients
about retries without exposing task IDs.

## Proto Contract

```protobuf
syntax = "proto3";
package rapidbyte.v1;

import "google/protobuf/timestamp.proto";

// --- Client-facing ---

service PipelineService {
  rpc SubmitPipeline(SubmitPipelineRequest) returns (SubmitPipelineResponse);
  rpc GetRun(GetRunRequest) returns (GetRunResponse);
  rpc WatchRun(WatchRunRequest) returns (stream RunEvent);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc ListRuns(ListRunsRequest) returns (ListRunsResponse);
}

// --- Agent-facing ---

service AgentService {
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
  rpc PollTask(PollTaskRequest) returns (PollTaskResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc CompleteTask(CompleteTaskRequest) returns (CompleteTaskResponse);
}

// --- Enums ---

enum RunState {
  RUN_STATE_UNSPECIFIED = 0;
  RUN_STATE_PENDING = 1;
  RUN_STATE_RUNNING = 2;
  RUN_STATE_COMPLETED = 3;
  RUN_STATE_FAILED = 4;
  RUN_STATE_CANCELLED = 5;
}

enum CommitState {
  COMMIT_STATE_UNSPECIFIED = 0;
  COMMIT_STATE_BEFORE_COMMIT = 1;
  COMMIT_STATE_AFTER_COMMIT_UNKNOWN = 2;
  COMMIT_STATE_AFTER_COMMIT_CONFIRMED = 3;
}

// --- Pipeline Service Messages ---

message SubmitPipelineRequest {
  string pipeline_yaml = 1;           // UTF-8 string (changed from bytes)
  string idempotency_key = 2;
  ExecutionOptions options = 3;
}

message ExecutionOptions {
  uint32 max_retries = 1;             // Default 0 = no retries
  uint64 timeout_seconds = 2;         // Per-task lease timeout override (0 = use server default)
}

message SubmitPipelineResponse {
  string run_id = 1;
  bool already_exists = 2;            // True if idempotency_key matched an existing run
}

message GetRunRequest {
  string run_id = 1;
}

message GetRunResponse {
  RunDetail run = 1;
}

message WatchRunRequest {
  string run_id = 1;
}

// WatchRun sends current state as the first event (catch-up), then streams changes.
// State changes are durable (persisted + NOTIFY). Progress is ephemeral (NOTIFY only).
// Notifications are truncated to fit PG's 8KB NOTIFY payload limit.
message RunEvent {
  string run_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  RunState state = 3;
  uint32 attempt = 4;
  oneof detail {
    ProgressDetail progress = 5;
    CompletionDetail completed = 6;
    FailureDetail failed = 7;
  }
}

message ProgressDetail {
  string message = 1;
  optional double progress_pct = 2;   // 0.0-1.0
}

message CompletionDetail {
  RunMetrics metrics = 1;
}

message FailureDetail {
  string error_code = 1;
  string error_message = 2;
  bool retryable = 3;
  CommitState commit_state = 4;
}

message CancelRunRequest {
  string run_id = 1;
}

message CancelRunResponse {
  bool accepted = 1;                  // False if run is already terminal
}

message ListRunsRequest {
  optional RunState state_filter = 1;
  uint32 page_size = 2;              // Default 20
  string page_token = 3;             // Opaque cursor: base64-encoded (created_at, run_id)
}

message ListRunsResponse {
  repeated RunSummary runs = 1;
  string next_page_token = 2;        // Empty if no more results
}

// RunDetail is the full run view (GetRun, WatchRun).
message RunDetail {
  string run_id = 1;
  string pipeline_name = 2;          // Extracted from YAML during submission
  RunState state = 3;
  bool cancel_requested = 4;         // True if CancelRun was called (run may still be Running)
  uint32 attempt = 5;
  uint32 max_retries = 6;
  optional RunMetrics metrics = 7;
  optional RunError error = 8;
  google.protobuf.Timestamp created_at = 9;
  google.protobuf.Timestamp updated_at = 10;
}

// RunSummary is the compact list view (ListRuns).
message RunSummary {
  string run_id = 1;
  string pipeline_name = 2;
  RunState state = 3;
  uint32 attempt = 4;
  google.protobuf.Timestamp created_at = 5;
}

message RunMetrics {
  uint64 rows_read = 1;
  uint64 rows_written = 2;
  uint64 bytes_read = 3;
  uint64 bytes_written = 4;
  uint64 duration_ms = 5;
}

message RunError {
  string code = 1;
  string message = 2;
}

// --- Agent Service Messages ---

message RegisterRequest {
  string agent_id = 1;               // Agent-generated, stable across restarts
  AgentCapabilities capabilities = 2;
}

message AgentCapabilities {
  repeated string plugins = 1;
  uint32 max_concurrent_tasks = 2;
}

message RegisterResponse {
  RegistryInfo registry = 1;         // OCI registry for plugin pulls
}

message DeregisterRequest {
  string agent_id = 1;
}

message DeregisterResponse {}

message PollTaskRequest {
  string agent_id = 1;
}

message PollTaskResponse {
  oneof result {
    TaskAssignment assignment = 1;
    NoTask no_task = 2;
  }
}

message TaskAssignment {
  string task_id = 1;
  string run_id = 2;
  string pipeline_yaml = 3;          // Secrets already resolved by controller
  ExecutionOptions options = 4;
  uint64 lease_epoch = 5;
  google.protobuf.Timestamp lease_expires_at = 6;
  uint32 attempt = 7;
  // RegistryInfo is sent once via RegisterResponse, not per-task.
}

message RegistryInfo {
  string url = 1;
  bool insecure = 2;
}

message NoTask {}

// Heartbeat supports multi-task agents. Each active lease is reported and renewed.
message HeartbeatRequest {
  string agent_id = 1;
  repeated TaskHeartbeat tasks = 2;   // One entry per active task (supports concurrent tasks)
}

message TaskHeartbeat {
  string task_id = 1;
  uint64 lease_epoch = 2;
  optional string progress_message = 3;
  optional double progress_pct = 4;   // 0.0-1.0
}

message HeartbeatResponse {
  repeated TaskDirective directives = 1;  // Per-task directives (cancel, lease renewal)
}

message TaskDirective {
  string task_id = 1;
  bool acknowledged = 2;             // False if task/lease is stale (agent should stop)
  bool cancel_requested = 3;         // Controller signals agent to cancel this task
  google.protobuf.Timestamp lease_expires_at = 4;  // Extended expiry (epoch unchanged)
}

message CompleteTaskRequest {
  string agent_id = 1;
  string task_id = 2;
  uint64 lease_epoch = 3;
  oneof outcome {
    TaskCompleted completed = 4;
    TaskFailed failed = 5;
    TaskCancelled cancelled = 6;
  }
}

message TaskCompleted {
  RunMetrics metrics = 1;
}

message TaskFailed {
  string error_code = 1;
  string error_message = 2;
  bool retryable = 3;
  CommitState commit_state = 4;       // Retry only allowed when BEFORE_COMMIT
}

message TaskCancelled {}

message CompleteTaskResponse {}
```

## Domain Layer

Pure business logic, no framework dependencies.

### Aggregates

**Run** (`domain/run.rs`):
- Fields: id, idempotency_key, pipeline_name, pipeline_yaml, state, current_attempt,
  max_retries, cancel_requested, error, metrics, created_at, updated_at
- State transition methods: `start()`, `complete(metrics)`, `fail(error)`, `cancel()`,
  `retry()` (returns new attempt number)
- Cancel request: `request_cancel()` sets `cancel_requested = true` without changing state
- Decision methods:
  - `can_retry_after_error(error) -> bool` — checks retryable + commit_state == BeforeCommit
    + current_attempt < max_retries + 1
  - `can_retry_after_timeout() -> bool` — checks current_attempt < max_retries + 1
    (no commit state needed — lease expiry and agent deregistration mean the agent
    never reached commit, so retry is always safe if attempts remain)
  - `is_cancel_requested() -> bool`
- `max_retries` defaults to 0 (proto3 default), meaning no retries unless explicitly set
- `pipeline_name` is extracted from YAML during submission (via rapidbyte-pipeline-config)

**Task** (`domain/task.rs`):
- Fields: id, run_id, attempt, state, agent_id, lease, created_at, updated_at
- Methods: `assign(agent_id, lease)`, `complete()`, `fail()`, `cancel()`, `timeout()`
- Validation: `validate_lease(agent_id, epoch)` — checks agent and epoch match

**Agent** (`domain/agent.rs`):
- Fields: id, capabilities (plugins, max_concurrent_tasks), last_seen_at, registered_at
- Methods: `touch(now)`, `is_alive(now, timeout) -> bool`

**Lease** (`domain/lease.rs`):
- Fields: epoch, expires_at
- Methods: `is_expired(now)`, `extend(duration, now) -> Lease` (keeps same epoch, extends expiry)
- Epoch is assigned once at poll_and_assign time (from Postgres sequence) and stays
  fixed for the lifetime of that task assignment. Heartbeat extends the expiry, not the
  epoch. The agent uses the same epoch for all subsequent heartbeats and CompleteTask.
  New epochs are only generated on task assignment (poll) and reassignment after timeout.

### Domain Events (`domain/event.rs`)

```rust
pub enum DomainEvent {
    RunStateChanged { run_id, state, attempt },
    ProgressReported { run_id, message, pct },
    RunCompleted { run_id, metrics },
    RunFailed { run_id, error },
    RunCancelled { run_id },
}
```

Published through the EventBus port. State changes are durable (persisted + NOTIFY).
Progress is ephemeral (NOTIFY only).

The gRPC adapter maps DomainEvent to proto RunEvent:
- `RunStateChanged` → `RunEvent` with state field set, no detail (pure state change)
- `ProgressReported` → `RunEvent` with `ProgressDetail`
- `RunCompleted` → `RunEvent` with `CompletionDetail`
- `RunFailed` → `RunEvent` with `FailureDetail`
- `RunCancelled` → `RunEvent` with state = CANCELLED, no detail

Note: `cancel_requested` is not a domain event. Clients see it via `GetRun` (the
`RunDetail.cancel_requested` field) or infer it when the run eventually transitions
to Cancelled. This is intentional — cancellation is a request, not a state change.

### Port Traits (`domain/ports/`)

**RunRepository**: find_by_id, find_by_idempotency_key, save, list

**TaskRepository**: find_by_id, save, poll_and_assign(agent_id, lease),
find_expired_leases(now), find_by_run_id

**AgentRepository**: find_by_id, save, delete, find_stale(timeout, now)

**PipelineStore** (cross-aggregate transactions):
- submit_run(run, task) — atomic insert of run + initial task
- complete_run(task, run)
- fail_and_retry(failed_task, run, new_task)
- timeout_and_retry(timed_out_task, run, new_task or None if no retry)

**EventBus**: publish(event), subscribe(run_id) -> EventStream

**SecretResolver**: resolve(yaml) -> Result<String>

**Clock**: now() -> Timestamp

## Application Layer

Use cases orchestrate domain aggregates through port traits. Each takes `&AppContext`.

```rust
pub struct AppContext {
    pub runs: Arc<dyn RunRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub agents: Arc<dyn AgentRepository>,
    pub store: Arc<dyn PipelineStore>,
    pub event_bus: Arc<dyn EventBus>,
    pub secrets: Arc<dyn SecretResolver>,
    pub clock: Arc<dyn Clock>,
    pub config: AppConfig,
}

pub struct AppConfig {
    pub default_lease_duration: Duration,   // Used when run has no timeout_seconds
    pub lease_check_interval: Duration,     // How often sweep_expired_leases runs
    pub agent_reap_timeout: Duration,       // Agent heartbeat timeout before reaping
    pub agent_reap_interval: Duration,      // How often reap_stale_agents runs
    pub default_max_retries: u32,           // Server-side default if client doesn't set
    pub registry: Option<RegistryConfig>,   // OCI registry info sent to agents
}
```

### Use Cases

**submit_pipeline**: Check idempotency key (DB unique constraint handles races), validate
YAML, create Run + Task, save, publish event. No PendingKeyGuard, no TOCTOU dance.

**poll_task**: Verify agent exists, touch liveness, `poll_and_assign` (SELECT FOR UPDATE
SKIP LOCKED), generate lease epoch from Postgres sequence. Lease TTL is determined by:
`run.timeout_seconds` if set, otherwise `config.default_lease_duration`. Resolve secrets
via SecretResolver. Publish event. If secret resolution fails, compensating action
releases the task (timeout the task, retry the run if possible).

**heartbeat**: Touch agent, validate lease if task_id present, renew lease, publish
progress (ephemeral), check cancellation flag on run.

**complete_task**: Validate lease, match on outcome:
- Completed: `task.complete()`, `run.complete(metrics)`, `store.complete_run()`, publish
- Failed + retryable: `task.fail()`, `run.retry()`, create new task,
  `store.fail_and_retry()`, publish
- Failed + terminal: `task.fail()`, `run.fail(error)`, save both, publish
- Cancelled: `task.cancel()`, `run.cancel()`, save both, publish

**cancel_run**: Two cases based on state:
- Pending (no agent assigned): `run.cancel()` (Pending → Cancelled), cancel the pending
  task, save, publish RunCancelled event. Immediate.
- Running (agent executing): `run.request_cancel()` sets `cancel_requested = true`,
  save. Run stays `Running`. Agent discovers cancellation on next heartbeat via
  `cancel_requested: true` in `TaskDirective`. Agent then calls `CompleteTask` with
  `TaskCancelled`, which transitions run to Cancelled. If agent never responds, lease
  expires and normal timeout handling applies.

**get_run / list_runs**: Direct repository reads.

**watch_run**: `event_bus.subscribe(run_id)` returns stream. On subscribe, the current
run state is sent as the first event (catch-up), then live events follow via LISTEN/NOTIFY.

**register**: Create or upsert agent. Returns registry info. Upsert semantics allow agent
re-registration after restart without controller-side cleanup.

**deregister**: Clean agent shutdown flow:
1. Find all tasks currently assigned to the agent (state = Running, agent_id matches)
2. For each task: `task.timeout()` (Running → TimedOut), save
3. For each affected run: if `run.can_retry()`, `run.retry()` + create new pending task.
   Otherwise `run.fail(agent_deregistered)`. Save, publish events.
4. Delete agent record.
This ensures tasks are immediately requeued rather than waiting for lease expiry.

### Background Use Cases

**sweep_expired_leases**: Find tasks with expired leases. For each: `task.timeout()`,
then check `run.can_retry_after_timeout()` (no commit state needed — lease expiry means
agent never committed). If retryable: `run.retry()`, create new pending task,
`store.timeout_and_retry(task, run, new_task)`. If not retryable: `run.fail(lease_expired)`,
`store.timeout_and_retry(task, run, None)`. Publish events.

**reap_stale_agents**: Find agents past heartbeat timeout, deregister each (releasing tasks).

### Key Properties

- No rollback logic — Postgres transactions handle atomicity
- No snapshot-restore — Postgres is source of truth
- Linear control flow — each use case reads top to bottom
- complete.rs goes from 564 lines to ~80

## Adapter Layer

### gRPC Adapters (`adapter/grpc/`)

Thin translation: convert proto to domain types, call use case, convert result to proto.
3-5 lines per RPC method. `convert` module handles all mapping + AppError to Status.

Auth middleware: BearerAuthInterceptor (Tower interceptor, unchanged in concept).

### Postgres Adapters (`adapter/postgres/`)

**PgRunRepository, PgTaskRepository, PgAgentRepository**: sqlx compile-time checked queries.
Each focused on one aggregate.

**PgPipelineStore**: Cross-aggregate transactions (BEGIN + multiple operations + COMMIT).

**PgEventBus**: Publish via `pg_notify('run_events', json_payload)`. Subscribe via a
background listener task (spawned once at startup) that runs `LISTEN run_events` on a
dedicated connection, deserializes each notification payload, and routes to local
`broadcast::channel` per run_id. Notifications exceeding PG's 8KB payload limit are
truncated (progress messages may lose their message field). On subscribe, the current
run state is fetched from the DB and sent as the first event before streaming live
notifications. If the LISTEN connection drops, the listener reconnects and subscribers
are notified of reconnection (clients can re-fetch state). Note: all instances receive
all notifications and discard events for run_ids with no local subscribers. This fan-out
is acceptable at expected scale but could become a bottleneck at very high throughput.

**Lease epoch**: Postgres sequence (`SELECT nextval('lease_epoch_seq')`). Monotonic,
unique across all instances.

**poll_and_assign**: The exact query:
```sql
SELECT * FROM tasks
WHERE state = 'pending'
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
```
This is the core of multi-instance task distribution. Each controller instance grabs the
next available pending task without blocking others. The `FOR UPDATE` lock is held within
the transaction that also updates the task state to `running` and sets agent_id/lease.

### External Adapters

**VaultSecretResolver**: Wraps `rapidbyte-secrets::SecretProviders`, implements
SecretResolver port.

**SystemClock**: `chrono::Utc::now()`. Tests inject FakeClock.

## Database Schema

```sql
CREATE SEQUENCE lease_epoch_seq;

CREATE TABLE runs (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    pipeline_name    TEXT NOT NULL,
    pipeline_yaml    TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending',
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    attempt          INTEGER NOT NULL DEFAULT 1,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    timeout_seconds  BIGINT,
    error_code       TEXT,
    error_message    TEXT,
    rows_read        BIGINT,
    rows_written     BIGINT,
    bytes_read       BIGINT,
    bytes_written    BIGINT,
    duration_ms      BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_runs_state ON runs(state);
CREATE INDEX idx_runs_created_at ON runs(created_at DESC);

CREATE TABLE tasks (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt          INTEGER NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending',
    agent_id         TEXT,
    lease_epoch      BIGINT,
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(run_id, attempt)
);

CREATE INDEX idx_tasks_poll ON tasks(created_at ASC) WHERE state = 'pending';
CREATE INDEX idx_tasks_lease ON tasks(lease_expires_at) WHERE state = 'running';

CREATE TABLE agents (
    id                   TEXT PRIMARY KEY,
    plugins              TEXT[] NOT NULL DEFAULT '{}',
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 1,
    last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_agents_last_seen ON agents(last_seen_at);
```

Key design decisions:
- State as TEXT — readable in queries and debugging, avoids hard-to-migrate PG enums
- `idempotency_key UNIQUE` — replaces entire TOCTOU/PendingKeyGuard complexity
- `ON DELETE CASCADE` on tasks — no orphan management code
- Partial indexes for poll and lease sweep — keeps indexes tight
- `UNIQUE(run_id, attempt)` — enforces one task per attempt at DB level
- Metrics/error denormalized onto runs — at most one of each, no joins needed

## Error Handling

Three error types with clear boundaries:

**DomainError** (domain layer):
- InvalidTransition, LeaseMismatch, AgentMismatch, LeaseExpired, RetriesExhausted,
  InvalidPipeline

**AppError** (application layer):
- Domain(DomainError), NotFound, AlreadyExists, Repository(RepositoryError),
  EventBus(EventBusError), SecretResolution(String)

**tonic::Status** (gRPC adapter):
- Mapped from AppError via convert module
- Domain errors get appropriate codes (failed_precondition, aborted, not_found, etc.)
- Infrastructure errors become opaque internal errors

No `anyhow::Result` in public APIs. No composite error aggregation. No rollback chains.

## Testing Strategy

| Layer | Approach |
|-------|----------|
| Domain (state machines, lease validation, retry decisions) | Pure unit tests, no dependencies |
| Application (use cases) | Unit tests with in-memory fakes for all ports |
| Postgres adapters | Integration tests with real Postgres, feature-gated |
| gRPC handlers | Thin wiring, tested via e2e only |

`cargo test` stays fast (no Docker). Postgres integration tests run in CI or via
`just e2e`.

## New Shared Crate: rapidbyte-pipeline-config

Extract pipeline YAML parsing and secret substitution from `rapidbyte-engine` into a
shared crate. Both `rapidbyte-engine` (local execution) and `rapidbyte-controller`
(secret resolution at dispatch) depend on it. Breaks the current controller -> engine
coupling.

## File Structure

```
crates/rapidbyte-controller/
  Cargo.toml
  build.rs
  migrations/
    0001_initial.sql
  src/
    lib.rs
    domain/
      mod.rs
      run.rs
      task.rs
      agent.rs
      lease.rs
      event.rs
      error.rs
      ports/
        mod.rs
        repository.rs
        pipeline_store.rs
        event_bus.rs
        secrets.rs
        clock.rs
    application/
      mod.rs
      context.rs
      error.rs
      submit.rs
      poll.rs
      heartbeat.rs
      complete.rs
      cancel.rs
      query.rs
      register.rs
      background/
        mod.rs
        lease_sweep.rs
        agent_reaper.rs
    adapter/
      mod.rs
      grpc/
        mod.rs
        pipeline.rs
        agent.rs
        convert.rs
        auth.rs
      postgres/
        mod.rs
        run.rs
        task.rs
        agent.rs
        store.rs
        event_bus.rs
      secrets.rs
      clock.rs
    config.rs
    server.rs
    proto.rs
```

## Removed vs Current

| Current | Refactored |
|---------|-----------|
| 10 RPCs (5+5) | 10 RPCs (5+5, different composition) |
| 11 run states | 5 run states + cancel_requested flag |
| ~5,350 lines | ~3,000 lines (estimated) |
| In-memory first, optional Postgres | Postgres only (source of truth) |
| Arc<RwLock<HashMap>> state containers | Direct Postgres queries |
| Snapshot-restore rollback | Postgres transactions |
| PendingKeyGuard TOCTOU prevention | DB unique constraint |
| AtomicU64 epoch generator | Postgres sequence |
| tokio broadcast for WatchRun | Postgres LISTEN/NOTIFY |
| HMAC ticket signing for preview | Removed (no preview) |
| Preview system (tickets, TTL, cleanup) | Removed entirely |
| Cancelling state | cancel_requested boolean flag |
| 1,800-line monolithic store | ~100-line focused repositories |
| anyhow + Status + typed errors mixed | 3 typed error enums, clear boundaries |
| dry_run + limit in ExecutionOptions | Removed (re-add with preview) |
| bytes pipeline_yaml_utf8 | string pipeline_yaml |
| Flat GetRunResponse fields | Wrapped in RunDetail message |
| ReportProgress RPC | Folded into Heartbeat |
| Single-lease HeartbeatRequest | Multi-task HeartbeatRequest with TaskHeartbeat |

## Prerequisites

**rapidbyte-pipeline-config crate**: Must be created before the controller refactor.
Extract from `rapidbyte-engine::config::parser`:
- `reject_malformed_refs(yaml: &str) -> Result<()>` — validates no malformed secret refs
- `substitute_secrets(yaml: &str, resolver: &dyn SecretResolver) -> Result<String>`
- `extract_pipeline_name(yaml: &str) -> Result<String>` — parses YAML to extract name
Both `rapidbyte-engine` and `rapidbyte-controller` will depend on this new crate.

**CLI updates**: The CLI commands that interact with the controller (`distributed_run`,
`status`, `watch`, `list_runs`, `cancel`) must be updated for the new proto contract.
This is a coordinated deploy — old CLI won't work with new controller and vice versa.

## Build Requirements

**sqlx offline mode**: Use `cargo sqlx prepare` to generate `.sqlx/` metadata files
checked into the repo. This allows `cargo build` without a live Postgres database.
CI runs `cargo sqlx prepare --check` to verify metadata is up to date. Developer
workflow: after modifying any SQL query or adding a migration, run
`cargo sqlx prepare --workspace` against a local Postgres with the current schema.

## Metrics

Preserve the following metric instruments (from `rapidbyte_metrics`):
- `runs_submitted` (counter) — incremented in submit_pipeline use case
- `runs_completed` (counter, with state label) — incremented in complete_task/cancel/timeout
- `active_runs` (gauge) — adjusted on state transitions
- `tasks_completed` (counter, with outcome label) — incremented in complete_task
- `lease_grants` (counter) — incremented in poll_task
- `heartbeat_received` (counter) — incremented in heartbeat
- `active_agents` (gauge) — adjusted on register/deregister/reap

Removed metrics (no longer applicable):
- `state_persist_duration` — no separate persist step, writes are inline
- `reconciliation_sweeps` — no reconciliation state
- `heartbeat_timeouts` — folded into lease expiry

Metrics are recorded in the application layer (use cases), not the domain. Direct
`tracing` and metric counter calls are acceptable — no port trait needed for metrics.

## Cargo Dependencies

Added: sqlx, serde, serde_json, rapidbyte-pipeline-config
Removed: hmac, sha2, bytes, tokio-postgres, tokio-rustls, rustls, rustls-native-certs,
  rapidbyte-engine (replaced by rapidbyte-pipeline-config)
Kept: tonic, prost, prost-types, tokio, tokio-stream, tower, async-trait, thiserror,
  chrono, uuid, tracing, rapidbyte-secrets, rapidbyte-metrics
