# Persistent Controller State Design

**Date:** 2026-03-13

**Problem**

The distributed controller currently stores run, task, agent, and preview metadata entirely in memory. A controller restart drops that metadata immediately, orphaning in-flight work and forcing operators to resubmit pipelines even when agents and the shared pipeline state backend are still healthy.

**Goal**

Make controller metadata durable in distributed mode so controller restart preserves operational truth, supports bounded recovery of in-flight work, and maintains strict lease fencing without introducing control-plane tech debt.

## Decision Summary

Use a dedicated Postgres-backed controller metadata store inside `rapidbyte-controller`. Keep scheduling and lifecycle rules in Rust domain logic, but persist all controller-owned metadata transactionally. On restart, recover non-terminal work into a reconciliation flow instead of losing state or blindly requeueing tasks.

## Architecture

### Storage Boundary

Add a `ControllerStore` abstraction to `rapidbyte-controller` and route controller handlers through it. The controller should stop depending directly on in-memory `RunStore`, `TaskQueue`, and `AgentRegistry` locks for authoritative metadata.

The abstraction will have:

- A Postgres implementation for distributed mode
- A test-only in-memory implementation for fast unit tests
- In-memory watchers and `Notify` primitives retained only for transport fanout and long-poll wakeups

This keeps persistence concerns isolated from service logic while preserving the existing controller API surface.

### Durable Entities

Persist the following controller-owned entities:

- `controller_runs`: run lifecycle state, timestamps, pipeline name, idempotency key, current attempt, terminal metrics, and last error
- `controller_tasks`: one row per task attempt, including pipeline YAML, execution options, task state, assigned agent, lease epoch, and lease expiry
- `controller_agents`: registered agents, advertised Flight endpoint, capacity, bundle hash, and heartbeat metadata
- `controller_previews`: preview discovery metadata and signed ticket material needed after restart
- `controller_task_history`: append-only assignment/retry/completion/cancellation/recovery audit trail

Mutable tables provide the latest state; the history table provides debuggability and future operator tooling.

## Restart Recovery

### Recovery States

Add explicit persisted recovery states for tasks and runs that were previously in-flight when the controller restarted. The controller should treat those records as `reconciling` rather than immediately failing or requeueing them.

### Recovery Flow

1. Controller starts and loads non-terminal runs/tasks from Postgres.
2. Previously `assigned` or `running` tasks become `reconciling`.
3. Agents that still own live work can reattach by heartbeat/progress/completion using the persisted `task_id` and `lease_epoch`.
4. If reconciliation times out, the controller applies the existing retry-safety policy:
   - Requeue only if retryable, safe to retry, and clearly pre-commit
   - Otherwise mark the run failed with a recovery-specific error for operator action

This preserves operational truth while avoiding duplicate writes after uncertain commit state.

### Fencing

Persist `lease_epoch`, assignment ownership, and lease expiry in the authoritative task record. After restart, the controller must continue rejecting stale heartbeats, progress reports, and completions that do not match the stored active assignment.

## Integration Plan

### Service Layer

Update `PipelineServiceImpl` and `AgentServiceImpl` to call store methods for multi-entity operations instead of mutating independent in-memory structures. The critical operations that must be transactional are:

- Submit pipeline: create run and initial task
- Poll task: claim next pending task and persist assignment
- First progress report: transition task/run into running state
- Complete task: persist task terminal outcome, update run, and enqueue retry if allowed
- Cancel run: persist run and active task cancellation consistently

### Startup

Run controller store initialization and recovery from `server.rs` before accepting traffic. If metadata schema initialization or recovery fails, controller startup must fail closed.

### State Ownership

Keep business rules like lifecycle validation, retry policy, and lease checks in Rust code. SQL should persist state and support queries, not become the source of workflow rules.

## Schema and Migrations

Use checked-in SQL migrations for controller metadata schema creation and evolution. Avoid ad hoc `CREATE TABLE IF NOT EXISTS` strings spread across request handlers. The first migration should create the controller tables, indexes for queue lookup and reconciliation, and any enum/text constraints needed for lifecycle states.

## Error Handling

- Controller startup fails if the metadata store cannot connect or migrate
- Recovery-specific failures surface clearly through `GetRun`, `ListRuns`, and `WatchRun`
- Unknown or stale post-restart task reports remain fenced off rather than tolerated
- Preview metadata can expire independently without corrupting run state

## Testing Strategy

- Unit tests for store-backed lifecycle transitions and recovery logic
- Persistence tests proving restart preserves submitted, assigned, and terminal runs
- Recovery tests proving agent reconciliation succeeds when lease/task identity matches
- Negative tests proving stale lease epochs and wrong-agent reports are rejected after restart
- Integration coverage for controller startup recovery against a temporary Postgres instance or existing project Postgres test harness

## Rollout

Make Postgres-backed controller metadata the default for distributed mode. Keep the in-memory implementation only for unit tests and explicit local-only scenarios. Avoid a partially persistent hybrid because it preserves the hardest restart bugs while increasing complexity.
