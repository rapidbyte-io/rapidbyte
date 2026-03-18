# Controller Test Coverage Design

**Date:** 2026-03-18
**Status:** Draft
**Scope:** Comprehensive unit and integration test coverage for `rapidbyte-controller`

## Context

The controller crate was rewritten with hexagonal architecture. Current state:
- 146 tests total (71 domain, 43 application, 32 adapter gRPC)
- Domain layer: good state machine coverage, missing edge cases
- Application layer: happy paths covered, missing error paths and `timeout.rs` (0 tests)
- Adapter gRPC handlers: 0 tests for `agent.rs` and `pipeline.rs` (convert: 25, auth: 7)
- Adapter Postgres: 0 tests across 5 files (~1100 lines)
- Integration tests: none

## Goals

- 103 new tests bringing total to ~249
- Cover all layers: domain edge cases, application error paths, gRPC handlers, Postgres SQL
- Postgres integration tests use testcontainers (real DB, no mocks)
- Unit tests run without infrastructure (`cargo test`)
- Integration tests behind feature flag (`cargo test --features integration`)

## Non-Goals

- CLI command tests (separate scope)
- Performance/load tests
- Distributed multi-controller tests

## Test Organization

```
crates/rapidbyte-controller/
  src/
    domain/*.rs          # +22 inline unit tests (edge cases, boundaries)
    application/*.rs     # +32 inline unit tests (error paths, edge cases)
    adapter/grpc/*.rs    # +15 inline unit tests (handler logic, conversion)
  tests/
    postgres/            # +35 integration tests (testcontainers)
      mod.rs             # Shared setup (container, pool, migrations)
      run_repo.rs
      task_repo.rs
      agent_repo.rs
      pipeline_store.rs
      event_bus.rs
```

## Dependencies

```toml
[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }

[features]
integration = []
```

## Domain Layer Tests (+22)

### run.rs (+12 tests)

| Test | What it verifies |
|------|-----------------|
| `retry_preserves_metrics` | Metrics survive retry (not cleared, only error cleared) |
| `retry_increments_attempt_correctly` | Attempt 3 → retry → attempt 4 |
| `can_retry_after_error_with_max_retries_zero` | max_retries=0 → never retry |
| `can_retry_after_timeout_with_max_retries_zero` | Same boundary for timeout path |
| `complete_stores_metrics` | RunMetrics accessible via getter after complete() |
| `fail_stores_error` | RunError accessible via getter after fail() |
| `request_cancel_on_terminal_state_is_noop` | cancel_requested on Completed run doesn't panic |
| `new_with_empty_pipeline_name` | Empty string is valid at domain level |
| `retry_from_failed_fails` | Failed is terminal, retry returns InvalidTransition |
| `retry_from_cancelled_fails` | Cancelled is terminal |
| `updated_at_changes_on_transition` | updated_at differs from created_at after a transition (uses >= to avoid flakiness from Utc::now() wall clock) |
| `from_row_with_all_none_optionals` | No error, no metrics, no timeout → valid Run |

### task.rs (+6 tests)

| Test | What it verifies |
|------|-----------------|
| `cancel_pending_from_running_fails` | cancel_pending only works from Pending |
| `cancel_pending_success` | Pending → Cancelled via cancel_pending() |
| `assign_sets_agent_and_lease` | Fields populated after assign() |
| `timeout_preserves_agent_and_lease` | State changes but agent/lease remain for audit |
| `from_row_with_no_lease` | None agent, None lease → valid Task |
| `updated_at_changes_on_assign` | updated_at differs from created_at after assign (>= check) |

### lease.rs (+2 tests)

| Test | What it verifies |
|------|-----------------|
| `extend_with_zero_duration` | extend(0, now) sets expires_at = now |
| `is_expired_at_exact_now_equals_expires` | Boundary: now == expires_at → expired (≥ check) |

### agent.rs (+2 tests)

| Test | What it verifies |
|------|-----------------|
| `is_alive_with_zero_timeout` | Zero timeout → always stale |
| `touch_then_is_alive` | touch resets the liveness window |

## Application Layer Tests (+32)

### timeout.rs (+5 tests, currently 0)

| Test | What it verifies |
|------|-----------------|
| `handle_timeout_cancel_requested` | Cancel-requested run → Cancelled, RunCancelled event |
| `handle_timeout_retryable` | Retries remain → Pending, new task, RunStateChanged event |
| `handle_timeout_terminal_failure` | No retries → Failed with given error code/message |
| `handle_timeout_cancel_takes_priority_over_retry` | cancel_requested + retries → Cancelled (not retried) |
| `handle_timeout_error_message_preserved` | Error code/message in RunFailed event match inputs |

### submit.rs (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `submit_with_max_retries_zero` | Run created with 0 retries |
| `submit_with_timeout_seconds` | timeout_seconds flows through to Run |
| `submit_idempotency_race_returns_existing` | Store failure → fallback lookup returns existing run |

### poll.rs (+4 tests)

| Test | What it verifies |
|------|-----------------|
| `poll_secret_resolution_failure_compensates` | Secrets fail → task timed out, run retried/failed |
| `poll_returns_resolved_yaml` | Resolved YAML in assignment (not raw) |
| `poll_lease_epoch_increments` | Each poll gets unique epoch |
| `poll_agent_at_capacity_returns_none` | max=1, 1 running → None |

### heartbeat.rs (+4 tests)

| Test | What it verifies |
|------|-----------------|
| `heartbeat_multiple_tasks` | 2 active tasks → 2 directives |
| `heartbeat_unknown_task_acknowledged_false` | Unknown task_id → acknowledged=false |
| `heartbeat_expired_lease_rejected` | Expired lease → acknowledged=false |
| `heartbeat_without_tasks_touches_agent` | Empty task list → agent liveness updated |

### complete.rs (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `complete_unknown_task_not_found` | Unknown task_id → NotFound |
| `complete_unknown_agent_mismatch` | Wrong agent → AgentMismatch |
| `complete_expired_lease_rejected` | Expired lease → LeaseExpired |

### cancel.rs (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `cancel_pending_race_falls_through` | Pending but task already assigned → sets cancel flag |
| `cancel_running_then_complete_cancelled` | Full lifecycle: cancel → complete(Cancelled) → Cancelled |
| `cancel_idempotent` | Double cancel on running run succeeds both times |

### query.rs (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `list_runs_empty_result` | No runs → empty list, no next_page_token |
| `list_runs_pagination` | Multiple runs → page_token navigation works |
| `list_runs_default_page_size` | page_size=0 uses default 20 (via fake) |

### register.rs (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `deregister_cancel_requested_run_gets_cancelled` | Deregister + cancel_requested → Cancelled |
| `register_with_zero_capacity` | Works (normalization is gRPC layer concern) |
| `deregister_multiple_running_tasks` | 3 tasks all timed out |

### background/ (+3 tests)

| Test | What it verifies |
|------|-----------------|
| `lease_sweep_cancel_requested_gets_cancelled` | Cancel-requested + expired → Cancelled |
| `reaper_doesnt_touch_fresh_agents` | Mix of stale/fresh, only stale reaped |
| `lease_sweep_multiple_expired_tasks` | 3 expired tasks all handled |

## Adapter gRPC Tests (+15)

### adapter/grpc/agent.rs (+8 tests)

Tests construct `AgentGrpcService` with `fake_context()` and call trait methods directly.

| Test | What it verifies |
|------|-----------------|
| `register_missing_capabilities_defaults_to_one` | None capabilities → max=1 |
| `register_zero_capacity_normalized_to_one` | max=0 → 1 |
| `complete_task_missing_outcome_returns_invalid_argument` | No oneof set → error |
| `complete_task_unknown_commit_state_returns_invalid_argument` | Invalid enum → error |
| `complete_task_completed_maps_metrics` | RunMetrics fields flow through |
| `complete_task_failed_maps_error_fields` | Error code/message/retryable/commit_state |
| `heartbeat_maps_multiple_tasks_to_directives` | 1:1 task→directive mapping |
| `poll_task_no_task_returns_no_task_variant` | NoTask proto variant |

### adapter/grpc/pipeline.rs (+7 tests)

| Test | What it verifies |
|------|-----------------|
| `submit_empty_idempotency_key_treated_as_none` | "" → None |
| `submit_options_default_when_missing` | options=None → max_retries=0 |
| `list_runs_page_size_zero_defaults_to_twenty` | Server-side default |
| `list_runs_page_size_clamped_to_thousand` | 5000 → 1000 |
| `list_runs_unknown_state_filter_returns_invalid_argument` | Invalid enum → error |
| `cancel_run_returns_accepted_field` | CancelRunResponse.accepted maps correctly |
| `watch_run_dedup_skips_exact_snapshot_match` | (state, attempt) dedup logic |

## Postgres Integration Tests (+35)

All behind `#[cfg(feature = "integration")]`. Use testcontainers for real Postgres.

### Shared Setup (tests/postgres/mod.rs)

```rust
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

pub async fn setup_db() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    (pool, container)
}
```

### tests/postgres/run_repo.rs (+8 tests)

| Test | What it verifies |
|------|-----------------|
| `save_and_find_by_id` | Round-trip: all fields preserved |
| `save_upsert_updates_existing` | Second save overwrites |
| `find_by_idempotency_key` | Lookup by key works |
| `find_by_idempotency_key_not_found` | Returns None |
| `idempotency_key_unique_constraint` | Duplicate key → error |
| `list_with_state_filter` | Filter by state returns correct subset |
| `list_pagination_cursor` | Cursor-based pagination across multiple pages |
| `list_default_page_size` | page_size=0 → 20 results |

### tests/postgres/task_repo.rs (+7 tests)

| Test | What it verifies |
|------|-----------------|
| `save_and_find_by_id` | Round-trip |
| `find_by_run_id` | Returns all tasks for run |
| `find_running_by_agent_id` | Only Running tasks for agent |
| `find_expired_leases` | Only expired leases returned |
| `next_lease_epoch_increments` | 3 calls → 3 monotonic values |
| `unique_run_attempt_constraint` | Duplicate (run_id, attempt) → error |
| `save_with_no_lease` | None agent/lease round-trips |

### tests/postgres/agent_repo.rs (+5 tests)

| Test | What it verifies |
|------|-----------------|
| `save_and_find_by_id` | Round-trip with plugins TEXT[] |
| `save_upsert` | Update capabilities |
| `delete_agent` | Find returns None after delete |
| `find_stale` | Only stale agents returned |
| `delete_nonexistent_is_noop` | No error |

### tests/postgres/pipeline_store.rs (+10 tests)

| Test | What it verifies |
|------|-----------------|
| `submit_run_atomic` | Both run + task created |
| `submit_run_idempotency_violation_rolls_back` | Constraint violation → neither persisted |
| `assign_task_happy_path` | Task assigned, run Running |
| `assign_task_respects_capacity` | Agent at max → None |
| `assign_task_concurrent_cancelled_run_rolls_back` | Run cancelled → assignment rolled back |
| `assign_task_skip_locked` | Two concurrent assigns get different tasks |
| `complete_run_atomic` | Both task + run updated |
| `fail_and_retry_creates_new_task` | Failed task + new pending task |
| `cancel_pending_run_only_if_still_pending` | WHERE guard works |
| `timeout_and_retry_with_none_new_task` | Terminal timeout, no new task |

### tests/postgres/event_bus.rs (+5 tests)

| Test | What it verifies |
|------|-----------------|
| `publish_and_receive_via_listen_notify` | End-to-end NOTIFY delivery |
| `subscribe_filters_by_run_id` | Other run events not delivered |
| `cleanup_removes_empty_channel` | Dropped receiver → entry removed |
| `publish_oversized_payload_truncated` | >7500 bytes doesn't fail |
| `multiple_subscribers_same_run` | Both receive event |

## Running Tests

```bash
# Unit tests (no DB needed, fast)
cargo test -p rapidbyte-controller

# Integration tests (needs Docker for testcontainers)
cargo test -p rapidbyte-controller --features integration

# All tests
cargo test -p rapidbyte-controller --features integration
```

## Summary

| Category | Existing | New | Total |
|----------|----------|-----|-------|
| Domain | 71 | 22 | 93 |
| Application | 43 | 31 | 74 |
| Adapter gRPC | 32 | 15 | 47 |
| Adapter Postgres | 0 | 35 | 35 |
| **Total** | **146** | **103** | **249** |

Note: The `testing` module is `#[cfg(test)]` which is sufficient for all inline
unit tests. Integration tests in `tests/postgres/` use real `PgPool` and do not
need the fake infrastructure.
