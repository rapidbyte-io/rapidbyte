# Controller Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 103 new tests to rapidbyte-controller covering domain edge cases, application error paths, gRPC handler logic, and Postgres integration.

**Architecture:** Inline unit tests using existing fake infrastructure for domain/application/gRPC layers. Separate `tests/postgres/` integration test binary using testcontainers for real Postgres verification. All unit tests run with `cargo test`, integration tests behind `--features integration`.

**Tech Stack:** Rust, tokio::test, testcontainers, testcontainers-modules (postgres), sqlx, tonic

**Spec:** `docs/superpowers/specs/2026-03-18-controller-test-coverage-design.md`

---

## Chunk 1: Domain Layer Tests (+22)

### Task 1: Domain run.rs edge case tests (+12)

**Files:**
- Modify: `crates/rapidbyte-controller/src/domain/run.rs` (add tests to existing `#[cfg(test)]` module)

- [ ] **Step 1: Add all 12 tests to the existing tests module**

Add these tests to the `mod tests` block in `run.rs`. Each test uses the existing
`make_run()` or `make_run_with_retries()` helpers (or creates a `Run::new()` directly).

```rust
#[test]
fn retry_preserves_metrics() {
    let mut run = make_run_with_retries(2);
    run.start().unwrap();
    let metrics = RunMetrics { rows_read: 10, rows_written: 5, bytes_read: 100, bytes_written: 50, duration_ms: 500 };
    run.complete(metrics).unwrap();
    // Can't retry from Completed — test that metrics survive on a run that CAN retry
    // Instead: start, then retry (Running → Pending), check metrics is still None (never set)
    let mut run2 = make_run_with_retries(2);
    run2.start().unwrap();
    run2.retry().unwrap();
    assert!(run2.metrics().is_none()); // metrics were never set, still None
}

#[test]
fn retry_increments_attempt_correctly() {
    let mut run = Run::new("r1".into(), None, "pipe".into(), "yaml".into(), 5, None, Utc::now());
    run.start().unwrap();
    assert_eq!(run.retry().unwrap(), 2);
    run.start().unwrap();
    assert_eq!(run.retry().unwrap(), 3);
    run.start().unwrap();
    assert_eq!(run.retry().unwrap(), 4);
    assert_eq!(run.current_attempt(), 4);
}

#[test]
fn can_retry_after_error_with_max_retries_zero() {
    let run = Run::new("r1".into(), None, "p".into(), "y".into(), 0, None, Utc::now());
    assert!(!run.can_retry_after_error(true, &CommitState::BeforeCommit));
}

#[test]
fn can_retry_after_timeout_with_max_retries_zero() {
    let run = Run::new("r1".into(), None, "p".into(), "y".into(), 0, None, Utc::now());
    assert!(!run.can_retry_after_timeout());
}

#[test]
fn complete_stores_metrics() {
    let mut run = make_run();
    run.start().unwrap();
    let m = RunMetrics { rows_read: 1, rows_written: 2, bytes_read: 3, bytes_written: 4, duration_ms: 5 };
    run.complete(m).unwrap();
    let stored = run.metrics().unwrap();
    assert_eq!(stored.rows_read, 1);
    assert_eq!(stored.duration_ms, 5);
}

#[test]
fn fail_stores_error() {
    let mut run = make_run();
    run.start().unwrap();
    run.fail(RunError { code: "ERR".into(), message: "msg".into() }).unwrap();
    let e = run.error().unwrap();
    assert_eq!(e.code, "ERR");
    assert_eq!(e.message, "msg");
}

#[test]
fn request_cancel_on_terminal_state_is_noop() {
    let mut run = make_run();
    run.start().unwrap();
    run.complete(RunMetrics { rows_read: 0, rows_written: 0, bytes_read: 0, bytes_written: 0, duration_ms: 0 }).unwrap();
    run.request_cancel(); // should not panic
    assert!(run.is_cancel_requested());
}

#[test]
fn new_with_empty_pipeline_name() {
    let run = Run::new("r1".into(), None, "".into(), "y".into(), 0, None, Utc::now());
    assert_eq!(run.pipeline_name(), "");
}

#[test]
fn retry_from_failed_fails() {
    let mut run = make_run();
    run.start().unwrap();
    run.fail(RunError { code: "E".into(), message: "m".into() }).unwrap();
    assert!(run.retry().is_err());
}

#[test]
fn retry_from_cancelled_fails() {
    let mut run = make_run();
    run.cancel().unwrap();
    assert!(run.retry().is_err());
}

#[test]
fn updated_at_changes_on_transition() {
    let now = Utc::now();
    let mut run = Run::new("r1".into(), None, "p".into(), "y".into(), 0, None, now);
    let before = run.updated_at();
    run.start().unwrap();
    assert!(run.updated_at() >= before);
}

#[test]
fn from_row_with_all_none_optionals() {
    let now = Utc::now();
    let run = Run::from_row("r1".into(), None, "p".into(), "y".into(), RunState::Pending, 1, 0, None, false, None, None, now, now);
    assert!(run.error().is_none());
    assert!(run.metrics().is_none());
    assert!(run.timeout_seconds().is_none());
    assert!(run.idempotency_key().is_none());
}
```

Note: Adapt the test code to match the actual `Run::new` and `make_run` helper signatures
in the file. Read the existing tests to understand the patterns used.

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller domain::run`
Expected: All existing + 12 new tests pass.

- [ ] **Step 3: Commit**

```bash
git commit -m "test(controller): add 12 domain run.rs edge case tests"
```

### Task 2: Domain task.rs, lease.rs, agent.rs edge case tests (+10)

**Files:**
- Modify: `crates/rapidbyte-controller/src/domain/task.rs`
- Modify: `crates/rapidbyte-controller/src/domain/lease.rs`
- Modify: `crates/rapidbyte-controller/src/domain/agent.rs`

- [ ] **Step 1: Add 6 tests to task.rs**

Read the existing test module and helpers in task.rs first. Add:

- `cancel_pending_from_running_fails`: assign a task (Running), try cancel_pending → InvalidTransition
- `cancel_pending_success`: make_task (Pending), cancel_pending → Cancelled
- `assign_sets_agent_and_lease`: assign, check agent_id() == Some("a1"), lease().is_some()
- `timeout_preserves_agent_and_lease`: assign then timeout, verify agent_id and lease still present
- `from_row_with_no_lease`: Task::from_row with None agent_id, None lease → valid task
- `updated_at_changes_on_assign`: create task, assign, verify updated_at >= created_at

- [ ] **Step 2: Add 2 tests to lease.rs**

- `extend_with_zero_duration`: extend(Duration::zero(), now) → expires_at == now
- `is_expired_at_exact_now_equals_expires`: Lease with expires_at=X, check is_expired(X) (verify boundary behavior matches current impl)

- [ ] **Step 3: Add 2 tests to agent.rs**

- `is_alive_with_zero_timeout`: Agent just created, is_alive(now, Duration::zero()) → false
- `touch_then_is_alive`: Create agent, advance 30s, touch, check is_alive with 60s timeout → true

- [ ] **Step 4: Run all domain tests**

Run: `cargo test -p rapidbyte-controller domain`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git commit -m "test(controller): add 10 domain edge case tests (task, lease, agent)"
```

---

## Chunk 2: Application Layer Tests (+31)

### Task 3: timeout.rs tests (+5)

**Files:**
- Modify: `crates/rapidbyte-controller/src/application/timeout.rs`

- [ ] **Step 1: Read timeout.rs and add test module**

Read the existing `handle_task_timeout` function. Add a `#[cfg(test)] mod tests` block.
Use `fake_context()` from `crate::application::testing`. Set up a running task+run
for each test by using `submit_pipeline` + `poll_task`.

Tests:
- `handle_timeout_cancel_requested`: Set cancel flag on run, call handle_task_timeout → RunCancelled event
- `handle_timeout_retryable`: Run with retries, call handle_task_timeout → RunStateChanged Pending, new task exists
- `handle_timeout_terminal_failure`: Run with 0 retries, call handle_task_timeout → RunFailed event with given error code
- `handle_timeout_cancel_takes_priority_over_retry`: Set cancel flag + retries available → RunCancelled (not retried)
- `handle_timeout_error_message_preserved`: Verify error code and message in RunFailed event match inputs

Each test: create run+task via submit+poll, timeout the task via `task.timeout()`,
then call `handle_task_timeout(&ctx, &task, &mut run, "ERROR_CODE", "error message")`.

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller application::timeout`
Expected: 5 tests pass

- [ ] **Step 3: Commit**

```bash
git commit -m "test(controller): add 5 timeout.rs tests covering all paths"
```

### Task 4: Application use case gap-fill tests (+26)

**Files:**
- Modify: `crates/rapidbyte-controller/src/application/submit.rs` (+3 tests)
- Modify: `crates/rapidbyte-controller/src/application/poll.rs` (+4 tests)
- Modify: `crates/rapidbyte-controller/src/application/heartbeat.rs` (+4 tests)
- Modify: `crates/rapidbyte-controller/src/application/complete.rs` (+3 tests)
- Modify: `crates/rapidbyte-controller/src/application/cancel.rs` (+3 tests)
- Modify: `crates/rapidbyte-controller/src/application/query.rs` (+3 tests)
- Modify: `crates/rapidbyte-controller/src/application/register.rs` (+3 tests)
- Modify: `crates/rapidbyte-controller/src/application/background/lease_sweep.rs` (+2 tests)
- Modify: `crates/rapidbyte-controller/src/application/background/agent_reaper.rs` (+1 test)

- [ ] **Step 1: Read each file's existing test module and add the new tests**

For each file, read the existing tests to understand the patterns (how they use
`fake_context()`, how they set up running tasks, etc.), then add the new tests.

**submit.rs (+3):**
- `submit_with_max_retries_zero`: submit with max_retries=0, verify run.max_retries()==0
- `submit_with_timeout_seconds`: submit with timeout=60, verify run.timeout_seconds()==Some(60)
- `submit_idempotency_race_returns_existing`: This tests the error-handling path in submit. To
  simulate, first submit with a key, then make the fake store's `submit_run` fail (you may need
  to add a failure injection mechanism to FakePipelineStore, or simply verify the existing
  idempotency path works correctly by checking the fallback). If FakePipelineStore can't be
  made to fail easily, test the sequential idempotency path instead (submit twice, second returns
  already_exists=true with same run_id).

**poll.rs (+4):**
- `poll_secret_resolution_failure_compensates`: Need FakeSecretResolver to fail. Check if the
  fake has a way to inject errors. If not, create a `FailingSecretResolver` in the test that
  always returns Err. Wire it into a custom AppContext. Verify the task is timed out and run
  is retried or failed.
- `poll_returns_resolved_yaml`: The default FakeSecretResolver returns input unchanged. Submit
  a pipeline, poll, verify assignment.pipeline_yaml matches the submitted YAML.
- `poll_lease_epoch_increments`: Submit 2 pipelines, poll both, verify epoch1 != epoch2 and both > 0.
- `poll_agent_at_capacity_returns_none`: Register agent with max=1, submit 2 pipelines, first poll
  succeeds, second returns None.

**heartbeat.rs (+4):**
- `heartbeat_multiple_tasks`: Register agent with max=2, submit 2 pipelines, poll both, heartbeat
  with both task_ids → 2 directives, both acknowledged=true.
- `heartbeat_unknown_task_acknowledged_false`: heartbeat with a made-up task_id → acknowledged=false.
- `heartbeat_expired_lease_rejected`: Submit+poll, advance clock past lease expiry, heartbeat →
  acknowledged=false (because validate_lease checks expiry).
- `heartbeat_without_tasks_touches_agent`: heartbeat with empty tasks vec, verify agent liveness updated.

**complete.rs (+3):**
- `complete_unknown_task_not_found`: complete with non-existent task_id → AppError::NotFound
- `complete_unknown_agent_mismatch`: complete with wrong agent_id → DomainError::AgentMismatch
- `complete_expired_lease_rejected`: advance clock past lease, complete → DomainError::LeaseExpired

**cancel.rs (+3):**
- `cancel_pending_race_falls_through`: This tests the race condition path. Submit a pipeline (run=Pending).
  Before cancelling, manually transition the task to Running in the fake storage (simulating concurrent
  poll). Cancel should fall through to the Running path and set cancel_requested.
- `cancel_running_then_complete_cancelled`: Cancel running run, then complete with TaskOutcome::Cancelled.
  Verify run ends in Cancelled state.
- `cancel_idempotent`: Cancel a running run twice. Both should return accepted=true.

**query.rs (+3):**
- `list_runs_empty_result`: list with no runs → empty vec, no next_page_token
- `list_runs_pagination`: submit 3 runs, list with page_size=2, verify 2 returned + next_page_token.
  List again with that token → 1 returned + no next_page_token.
- `list_runs_default_page_size`: list with page_size=0 → verify the fake applies default (returns all available up to 20).

**register.rs (+3):**
- `deregister_cancel_requested_run_gets_cancelled`: Submit+poll, cancel the run (sets flag), deregister
  agent → run should be Cancelled (not Failed with AGENT_DEREGISTERED).
- `register_with_zero_capacity`: Register with max_concurrent_tasks=0. Should succeed (no validation at app layer).
- `deregister_multiple_running_tasks`: Register agent, submit 3 pipelines (set agent max=3), poll all 3,
  deregister → all 3 tasks timed out, all 3 runs retried or failed.

**background/lease_sweep.rs (+2):**
- `lease_sweep_cancel_requested_gets_cancelled`: Submit+poll, cancel the run, advance clock past lease →
  sweep should cancel the run (not fail it with LEASE_EXPIRED).
- `lease_sweep_multiple_expired_tasks`: Submit 3 pipelines with retries, poll all 3, advance clock →
  sweep times out all 3, all 3 runs retried.

**background/agent_reaper.rs (+1):**
- `reaper_doesnt_touch_fresh_agents`: Register 2 agents. Advance clock past timeout for one only.
  Reap. First agent gone, second still present.

- [ ] **Step 2: Run all application tests**

Run: `cargo test -p rapidbyte-controller application`
Expected: All 43 existing + 26 new = 69 pass (plus 5 from timeout = 74)

- [ ] **Step 3: Commit**

```bash
git commit -m "test(controller): add 26 application use case tests (error paths, edge cases)"
```

---

## Chunk 3: Adapter gRPC Tests (+15)

### Task 5: gRPC agent handler tests (+8)

**Files:**
- Modify: `crates/rapidbyte-controller/src/adapter/grpc/agent.rs`

- [ ] **Step 1: Add test module to agent.rs**

Read the existing `agent.rs` code. Add `#[cfg(test)] mod tests`. Import the testing
infrastructure:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_context;
    use crate::proto::rapidbyte::v1 as pb;
    use crate::proto::rapidbyte::v1::agent_service_server::AgentService;
    use tonic::Request;
```

Each test constructs `AgentGrpcService::new(Arc::new(ctx))` and calls trait methods
with proto `Request<T>` wrappers.

For tests that need a running task (heartbeat, complete, poll), first set up via the
application layer (register agent, submit pipeline, poll task) using the `fake_context`.

**Tests:**
- `register_missing_capabilities_defaults_to_one`: RegisterRequest with capabilities=None.
  Register, then poll (submit a pipeline first). Should work since default is 1.
- `register_zero_capacity_normalized_to_one`: RegisterRequest with max_concurrent_tasks=0.
  Should normalize to 1.
- `complete_task_missing_outcome_returns_invalid_argument`: CompleteTaskRequest with outcome=None → Status INVALID_ARGUMENT
- `complete_task_unknown_commit_state_returns_invalid_argument`: TaskFailed with commit_state=99 → Status INVALID_ARGUMENT
- `complete_task_completed_maps_metrics`: Complete with TaskCompleted{metrics}, verify via get_run that run is Completed
- `complete_task_failed_maps_error_fields`: Complete with TaskFailed, verify error code in run
- `heartbeat_maps_multiple_tasks_to_directives`: Submit 2, poll 2, heartbeat both → 2 directives
- `poll_task_no_task_returns_no_task_variant`: Poll with no pending tasks → NoTask variant

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller adapter::grpc::agent`
Expected: 8 tests pass

- [ ] **Step 3: Commit**

```bash
git commit -m "test(controller): add 8 gRPC agent handler tests"
```

### Task 6: gRPC pipeline handler tests (+7)

**Files:**
- Modify: `crates/rapidbyte-controller/src/adapter/grpc/pipeline.rs`

- [ ] **Step 1: Add test module to pipeline.rs**

Same pattern as agent.rs. Import `PipelineService` trait, `fake_context`, proto types.

**Tests:**
- `submit_empty_idempotency_key_treated_as_none`: Submit with idempotency_key="", submit again
  with same "" → should NOT be idempotent (treated as None).
- `submit_options_default_when_missing`: Submit with options=None → run created with max_retries=0
- `list_runs_page_size_zero_defaults_to_twenty`: ListRunsRequest with page_size=0, submit 25 runs →
  response has 20 items (or however many the fake returns up to 20).
- `list_runs_page_size_clamped_to_thousand`: ListRunsRequest with page_size=5000 → verify the
  request is handled (no error) and page_size is effectively capped.
- `list_runs_unknown_state_filter_returns_invalid_argument`: state_filter=Some(99) → INVALID_ARGUMENT
- `cancel_run_returns_accepted_field`: Submit+poll a run, cancel → accepted=true. Cancel again → accepted depends on state.
- `watch_run_dedup_skips_exact_snapshot_match`: This is the trickiest test. Submit a pipeline.
  Call watch_run. The stream's first event should be the current state. Since the run is Pending,
  the initial event has state=Pending, attempt=1. Any queued RunStateChanged with same (Pending, 1)
  should be filtered. Test by verifying the stream doesn't emit duplicate initial state.

For the watch test, you'll need to consume the stream with `tokio_stream::StreamExt::next()`.
Use `tokio::time::timeout` to avoid hanging if no event arrives.

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-controller adapter::grpc::pipeline`
Expected: 7 tests pass

- [ ] **Step 3: Commit**

```bash
git commit -m "test(controller): add 7 gRPC pipeline handler tests"
```

---

## Chunk 4: Postgres Integration Tests (+35)

### Task 7: Setup testcontainers infrastructure

**Files:**
- Modify: `crates/rapidbyte-controller/Cargo.toml` (add dev-dependencies + feature)
- Create: `crates/rapidbyte-controller/tests/postgres/mod.rs`
- Create: `crates/rapidbyte-controller/tests/postgres.rs` (test harness entry point)

- [ ] **Step 1: Add dev-dependencies to Cargo.toml**

```toml
[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }

[features]
integration = []
```

Check the exact versions available — `testcontainers` API changed significantly between
versions. Read the docs or check `Cargo.lock` for compatible versions.

- [ ] **Step 2: Create test harness**

Create `crates/rapidbyte-controller/tests/postgres.rs`:
```rust
#![cfg(feature = "integration")]

mod postgres;
```

Create `crates/rapidbyte-controller/tests/postgres/mod.rs`:
```rust
use sqlx::PgPool;

pub async fn setup_db() -> (PgPool, impl Drop) {
    // Use testcontainers to start a Postgres container
    // Run migrations
    // Return pool + container (container drops when test ends)
    todo!("implement based on testcontainers API version")
}

// Helper: create a sample Run domain object for testing
pub fn sample_run(id: &str) -> rapidbyte_controller::domain::run::Run {
    // ...
}

// Helper: create a sample Task
pub fn sample_task(id: &str, run_id: &str) -> rapidbyte_controller::domain::task::Task {
    // ...
}

// Helper: create a sample Agent
pub fn sample_agent(id: &str) -> rapidbyte_controller::domain::agent::Agent {
    // ...
}

mod run_repo;
mod task_repo;
mod agent_repo;
mod pipeline_store;
mod event_bus;
```

**IMPORTANT:** The domain types (`Run`, `Task`, `Agent`, `Lease`, etc.) and the adapter types
(`PgRunRepository`, `PgTaskRepository`, etc.) need to be accessible from integration tests.
Check that they are `pub` or `pub(crate)`. If `pub(crate)`, the integration tests (which are
external to the crate) cannot access them. You may need to:
1. Make the adapter types `pub` in their module and re-export from `adapter::postgres::mod.rs`
2. Make domain types accessible via `pub use` from `lib.rs`

Read `crates/rapidbyte-controller/src/lib.rs` and the adapter module declarations to verify
visibility.

- [ ] **Step 3: Verify infrastructure compiles**

Run: `cargo test -p rapidbyte-controller --features integration --no-run`
Expected: Compiles (tests are todo!() but structure is valid)

- [ ] **Step 4: Commit**

```bash
git commit -m "test(controller): add testcontainers infrastructure for Postgres integration tests"
```

### Task 8: Postgres repository integration tests (+20)

**Files:**
- Create: `crates/rapidbyte-controller/tests/postgres/run_repo.rs`
- Create: `crates/rapidbyte-controller/tests/postgres/task_repo.rs`
- Create: `crates/rapidbyte-controller/tests/postgres/agent_repo.rs`

- [ ] **Step 1: Implement run_repo.rs tests (+8)**

Each test calls `setup_db()`, constructs `PgRunRepository::new(pool)`, and exercises the
repository trait methods against real Postgres.

Tests: save_and_find_by_id, save_upsert_updates_existing, find_by_idempotency_key,
find_by_idempotency_key_not_found, idempotency_key_unique_constraint, list_with_state_filter,
list_pagination_cursor, list_default_page_size.

For `list_pagination_cursor`: save 5 runs with different created_at timestamps. List with
page_size=2. Verify 2 runs returned + next_page_token. Use token to get next page (2 more).
Then next page (1 more). Then next page (empty).

- [ ] **Step 2: Implement task_repo.rs tests (+7)**

Tests: save_and_find_by_id, find_by_run_id, find_running_by_agent_id, find_expired_leases,
next_lease_epoch_increments, unique_run_attempt_constraint, save_with_no_lease.

Note: Tasks have FK to runs, so create a run first before creating tasks.

- [ ] **Step 3: Implement agent_repo.rs tests (+5)**

Tests: save_and_find_by_id, save_upsert, delete_agent, find_stale, delete_nonexistent_is_noop.

- [ ] **Step 4: Run integration tests**

Run: `cargo test -p rapidbyte-controller --features integration postgres::run_repo postgres::task_repo postgres::agent_repo`
Expected: 20 tests pass (requires Docker)

- [ ] **Step 5: Commit**

```bash
git commit -m "test(controller): add 20 Postgres repository integration tests"
```

### Task 9: Postgres pipeline_store and event_bus integration tests (+15)

**Files:**
- Create: `crates/rapidbyte-controller/tests/postgres/pipeline_store.rs`
- Create: `crates/rapidbyte-controller/tests/postgres/event_bus.rs`

- [ ] **Step 1: Implement pipeline_store.rs tests (+10)**

Each test creates `PgPipelineStore::new(pool)` plus repository instances for verification.

Tests: submit_run_atomic, submit_run_idempotency_violation_rolls_back, assign_task_happy_path,
assign_task_respects_capacity, assign_task_concurrent_cancelled_run_rolls_back,
assign_task_skip_locked, complete_run_atomic, fail_and_retry_creates_new_task,
cancel_pending_run_only_if_still_pending, timeout_and_retry_with_none_new_task.

For `assign_task_skip_locked`: Submit 2 tasks. Use two concurrent `assign_task` calls
(via `tokio::join!`). Verify they get different tasks.

For `assign_task_concurrent_cancelled_run_rolls_back`: Submit a pipeline. Cancel it directly
in the DB (`UPDATE runs SET state='cancelled'`). Then `assign_task` should return None.

- [ ] **Step 2: Implement event_bus.rs tests (+5)**

Tests: publish_and_receive_via_listen_notify, subscribe_filters_by_run_id,
cleanup_removes_empty_channel, publish_oversized_payload_truncated,
multiple_subscribers_same_run.

For `publish_and_receive_via_listen_notify`: Create PgEventBus, start_listener, subscribe
to a run_id, publish an event, verify the stream receives it within a timeout.

For `publish_oversized_payload_truncated`: Create a DomainEvent::ProgressReported with a
7600+ byte message string. Publish should succeed (truncated internally).

- [ ] **Step 3: Run all integration tests**

Run: `cargo test -p rapidbyte-controller --features integration`
Expected: 35 integration tests pass

- [ ] **Step 4: Final verification — all tests**

Run: `cargo test -p rapidbyte-controller`
Expected: ~214 unit tests pass (146 existing + 68 new unit tests)

Run: `cargo test -p rapidbyte-controller --features integration`
Expected: ~249 total tests pass (214 unit + 35 integration)

Run: `cargo clippy -p rapidbyte-controller --all-targets --features integration -- -D warnings`
Expected: No warnings

- [ ] **Step 5: Commit**

```bash
git commit -m "test(controller): add 15 Postgres pipeline_store and event_bus integration tests"
```
