# Controller Timeout Atomicity And Startup Preview Pruning Design

**Date:** 2026-03-13

**Problem**

The persistent-controller branch still has two durability gaps:

1. background lease-expiry and reconciliation-timeout handlers persist the task row and run row as separate writes; if the task write succeeds and the run write fails, Postgres can contain a terminal task paired with a non-terminal run, which restart recovery does not currently repair
2. `load_snapshot()` drops already-expired preview rows in memory, but it does not delete those rows from `controller_previews`, so previews that expired before controller startup can remain in durable metadata forever

These are both violations of the branch goal that durable controller metadata is the operational source of truth across restart.

## Decision Summary

Add a transactional metadata-store operation for background timeout transitions, and prune expired durable preview rows during startup snapshot repair before rebuilding in-memory preview state.

This keeps the fix narrow:

- the timeout paths stop relying on compensating rollback for cross-record durability
- startup repair becomes responsible for cleaning stale preview rows that will never appear in memory

## Design

### Transactional Timeout Persistence

Background timeout handling updates one run and, optionally, its current task. That pair must be durably committed together.

Add a store method with a narrow contract, for example:

- `persist_timeout_transition(run: &RunRecord, task: Option<&TaskRecord>)`

Behavior:

1. begin a database transaction
2. upsert the run row
3. if a task is provided, upsert the task row in the same transaction
4. commit only if both writes succeed

Use this in:

- `handle_expired_lease`
- `sweep_reconciliation_timeouts`

The service flow stays otherwise the same:

1. snapshot previous in-memory run/task state
2. apply the timeout transition in memory
3. snapshot the new terminal run/task state
4. call the transactional store method
5. if the transaction fails, roll memory back to the previous snapshots and skip watcher publish
6. publish terminal failure only after durable commit

This removes the durable split-brain case the review identified: restart can no longer see a timed-out task with a still-assigned or still-running run.

### State Layer Wiring

Expose the new store operation through `ControllerState` with a small helper such as:

- `persist_timeout_records(run: &RunRecord, task: Option<&TaskRecord>)`

This keeps `server.rs` independent of transaction mechanics and matches the branch’s existing pattern for `create_run_with_task`, `assign_task`, and `mark_task_running`.

### Startup Preview Pruning

`load_snapshot()` currently reads `controller_previews` and silently filters expired rows with `preview_entry_from_row()`. That means expired rows already present in Postgres before startup are never deleted if they never exist in `PreviewStore`.

The fix should move durable preview expiry enforcement into startup repair:

1. delete expired rows from `controller_previews` with a single durable query:
   - `DELETE FROM controller_previews WHERE expires_at <= NOW()`
2. then load the snapshot
3. then apply run reconciliation repair as today
4. return the repaired snapshot

This belongs in `MetadataStore::load_repaired_snapshot()` because startup repair is already the controller’s durable boot-time normalization point.

Important detail:

- keep the existing in-memory periodic preview cleanup and retry set for previews that expire while the controller is running
- startup pruning is only for expired rows that would otherwise be invisible to the in-memory cleanup loop

### Scope Boundary

Do not broaden this patch into general background-job transactionalization or a generic preview GC framework.

The minimal correct patch is:

- one transactional timeout-persistence helper
- both background timeout handlers cut over to it
- one startup expired-preview prune query in snapshot repair
- focused tests proving both contracts

## Testing Strategy

Add tests before implementation:

- a server test using `FailingMetadataStore` that proves timeout persistence is atomic from the durable-store perspective when the transactional timeout write fails
- a Postgres-backed ignored store test that seeds an already-expired preview row, runs `load_repaired_snapshot()`, and asserts the row is deleted from `controller_previews`

Keep the existing in-memory rollback and preview-cleanup retry tests. Extend them only as needed to assert the new durable behavior.

Verification after implementation:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-cli`
- the ignored Postgres-backed controller store and distributed restart tests

## Rollout

Ship this before merge. Both issues are direct contradictions of the durability guarantees introduced by the persistent controller state work.
