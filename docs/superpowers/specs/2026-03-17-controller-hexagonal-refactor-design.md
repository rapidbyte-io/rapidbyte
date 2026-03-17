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
- Breaking proto changes are acceptable
- Remove preview system entirely
- ~60% code reduction (estimated ~2,150 lines from ~5,350)

## Non-Goals

- Caching layer (Redis/Moka) — not needed; Postgres handles the workload
- Preview/dry-run data inspection — removed entirely, can be re-added later
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

10 RPCs total (down from 12), split across two services.

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

### Removed RPCs

| Removed | Why |
|---------|-----|
| `Dispatch` | Folded into `PollTask` — no reason for two round-trips |
| `GetSecret` | Secrets resolved server-side in `PollTask` |
| `ReportRunning` | Implicit: PollTask transitions to Running, first Heartbeat confirms liveness |
| `ReportProgress` | Folded into `Heartbeat` as optional progress payload |

## Run State Machine

```
Pending --> Running --> Completed (with metrics)
   ^            |--> Failed (with error)
   |            '--> Cancelled
   '-- (retry: task failed but retryable, new attempt enqueued)
```

5 states (down from 9). Removed: Assigned, PreviewReady, TimedOut, Reconciling,
RecoveryFailed.

- **Assigned** — distinction not useful to clients. PollTask transitions directly to Running.
- **PreviewReady** — preview system removed entirely.
- **TimedOut** — a timeout is Failed with error_code indicating lease expiry.
- **Reconciling/RecoveryFailed** — with Postgres as source of truth and lease expiry,
  the system self-heals. No crash-recovery state needed.

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
  string pipeline_yaml = 1;
  string idempotency_key = 2;
  ExecutionOptions options = 3;
}

message ExecutionOptions {
  bool dry_run = 1;
  uint32 max_retries = 2;
  uint64 timeout_seconds = 3;
}

message SubmitPipelineResponse {
  string run_id = 1;
  bool already_exists = 2;
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
  optional double progress_pct = 2;
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

message CancelRunResponse {}

message ListRunsRequest {
  optional RunState state_filter = 1;
  uint32 page_size = 2;
  string page_token = 3;
}

message ListRunsResponse {
  repeated RunDetail runs = 1;
  string next_page_token = 2;
}

message RunDetail {
  string run_id = 1;
  RunState state = 2;
  uint32 attempt = 3;
  uint32 max_retries = 4;
  optional RunMetrics metrics = 5;
  optional RunError error = 6;
  google.protobuf.Timestamp created_at = 7;
  google.protobuf.Timestamp updated_at = 8;
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
  string agent_id = 1;
  AgentCapabilities capabilities = 2;
}

message AgentCapabilities {
  repeated string plugins = 1;
  uint32 max_concurrent_tasks = 2;
}

message RegisterResponse {}

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
  string pipeline_yaml = 3;
  ExecutionOptions options = 4;
  uint64 lease_epoch = 5;
  google.protobuf.Timestamp lease_expires_at = 6;
  uint32 attempt = 7;
  RegistryInfo registry = 8;
}

message RegistryInfo {
  string url = 1;
  bool insecure = 2;
}

message NoTask {}

message HeartbeatRequest {
  string agent_id = 1;
  optional string task_id = 2;
  optional uint64 lease_epoch = 3;
  optional string progress_message = 4;
  optional double progress_pct = 5;
}

message HeartbeatResponse {
  bool cancel_requested = 1;
  optional uint64 renewed_lease_epoch = 2;
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
  CommitState commit_state = 4;
}

message TaskCancelled {}

message CompleteTaskResponse {}
```

## Domain Layer

Pure business logic, no framework dependencies.

### Aggregates

**Run** (`domain/run.rs`):
- Fields: id, idempotency_key, pipeline_yaml, options, state, current_attempt,
  max_retries, error, metrics, created_at, updated_at
- State transition methods: `start()`, `complete(metrics)`, `fail(error)`, `cancel()`,
  `retry()` (returns new attempt number)
- Decision method: `can_retry(error) -> bool` — checks retryable + before_commit +
  attempts < max

**Task** (`domain/task.rs`):
- Fields: id, run_id, attempt, state, agent_id, lease, created_at, updated_at
- Methods: `assign(agent_id, lease)`, `complete()`, `fail()`, `cancel()`, `timeout()`
- Validation: `validate_lease(agent_id, epoch)` — checks agent and epoch match

**Agent** (`domain/agent.rs`):
- Fields: id, capabilities (plugins, max_concurrent_tasks), last_seen_at, registered_at
- Methods: `touch(now)`, `is_alive(now, timeout) -> bool`

**Lease** (`domain/lease.rs`):
- Fields: epoch, expires_at
- Methods: `is_expired(now)`, `renew(new_epoch, duration, now) -> Lease`

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

### Port Traits (`domain/ports/`)

**RunRepository**: find_by_id, find_by_idempotency_key, save, list, delete

**TaskRepository**: find_by_id, save, poll_and_assign(agent_id, lease),
find_expired_leases(now), find_by_run_id

**AgentRepository**: find_by_id, save, delete, find_stale(timeout, now)

**PipelineStore** (cross-aggregate transactions):
- complete_run(task, run)
- fail_and_retry(failed_task, run, new_task)
- timeout_and_retry(timed_out_task, run, new_task)

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
```

### Use Cases

**submit_pipeline**: Check idempotency key (DB unique constraint handles races), validate
YAML, create Run + Task, save, publish event. No PendingKeyGuard, no TOCTOU dance.

**poll_task**: Verify agent exists, touch liveness, `poll_and_assign` (SELECT FOR UPDATE
SKIP LOCKED), generate lease epoch from Postgres sequence, resolve secrets, publish event.
If secret resolution fails, compensating action releases the task.

**heartbeat**: Touch agent, validate lease if task_id present, renew lease, publish
progress (ephemeral), check cancellation flag on run.

**complete_task**: Validate lease, match on outcome:
- Completed: `task.complete()`, `run.complete(metrics)`, `store.complete_run()`, publish
- Failed + retryable: `task.fail()`, `run.retry()`, create new task,
  `store.fail_and_retry()`, publish
- Failed + terminal: `task.fail()`, `run.fail(error)`, save both, publish
- Cancelled: `task.cancel()`, `run.cancel()`, save both, publish

**cancel_run**: `run.cancel()`, cancel assigned task if any, save, publish.

**get_run / list_runs**: Direct repository reads.

**watch_run**: `event_bus.subscribe(run_id)` returns stream.

**register / deregister**: Create/delete agent. Deregister releases tasks back to queue.

### Background Use Cases

**sweep_expired_leases**: Find tasks with expired leases, timeout each, retry or fail the
run, publish events.

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

**PgEventBus**: Publish via `pg_notify()`. Subscribe via background `LISTEN` task that
routes notifications to local `broadcast::channel` per run_id.

**Lease epoch**: Postgres sequence (`SELECT nextval('lease_epoch_seq')`). Monotonic,
unique across all instances.

**poll_and_assign**: `SELECT ... FOR UPDATE SKIP LOCKED` — core of multi-instance
task distribution. Each controller grabs next available task without blocking others.

### External Adapters

**VaultSecretResolver**: Wraps `rapidbyte-secrets::SecretProviders`, implements
SecretResolver port.

**SystemClock**: `chrono::Utc::now()`. Tests inject FakeClock.

## Database Schema

```sql
CREATE SEQUENCE lease_epoch_seq;

CREATE TABLE runs (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    pipeline_yaml   TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'pending',
    attempt         INTEGER NOT NULL DEFAULT 1,
    max_retries     INTEGER NOT NULL DEFAULT 0,
    dry_run         BOOLEAN NOT NULL DEFAULT FALSE,
    timeout_seconds BIGINT,
    error_code      TEXT,
    error_message   TEXT,
    rows_read       BIGINT,
    rows_written    BIGINT,
    bytes_read      BIGINT,
    bytes_written   BIGINT,
    duration_ms     BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
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
| 12 RPCs | 10 RPCs |
| 9 run states | 5 run states |
| ~5,350 lines | ~2,150 lines (estimated) |
| In-memory first, optional Postgres | Postgres only |
| Arc<RwLock<HashMap>> state containers | Direct Postgres queries |
| Snapshot-restore rollback | Postgres transactions |
| PendingKeyGuard TOCTOU prevention | DB unique constraint |
| AtomicU64 epoch generator | Postgres sequence |
| tokio broadcast for WatchRun | Postgres LISTEN/NOTIFY |
| HMAC ticket signing for preview | Removed (no preview) |
| 1,800-line monolithic store | ~100-line focused repositories |
| anyhow + Status + typed errors mixed | 3 typed error enums, clear boundaries |

## Cargo Dependencies

Added: sqlx, serde, serde_json, rapidbyte-pipeline-config
Removed: hmac, sha2, bytes, tokio-postgres, tokio-rustls, rustls, rustls-native-certs,
  rapidbyte-engine (replaced by rapidbyte-pipeline-config)
Kept: tonic, prost, prost-types, tokio, tokio-stream, tower, async-trait, thiserror,
  chrono, uuid, tracing, rapidbyte-secrets, rapidbyte-metrics
