# Controller Timeout Atomicity And Startup Preview Pruning Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make background timeout transitions durably atomic and prune already-expired preview rows from metadata during controller startup.

**Architecture:** Add one transactional metadata-store operation for run/task timeout transitions and route both background timeout handlers through it. Extend startup snapshot repair to delete expired preview rows before loading the repaired snapshot, while leaving the existing in-memory cleanup loop in place for previews that expire at runtime.

**Tech Stack:** Rust, Tokio, tonic, tokio-postgres transactions, controller unit tests, ignored Postgres-backed metadata-store tests.

---

### Task 1: Add the failing timeout atomicity test

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing test**

Add a server test that:

- creates a controller state backed by `FailingMetadataStore`
- seeds a run in `Assigned` or `Reconciling` with an assigned task
- pre-populates the failing store with the original run/task durable snapshots
- injects a failure on the transactional timeout write
- runs the timeout handler
- asserts both in-memory and durable snapshots remain at the pre-timeout states

The important assertion is on the durable store, not only memory:

```rust
assert_eq!(store.persisted_task(&task_id).unwrap().state, TaskState::Assigned);
assert_eq!(store.persisted_run(&run_id).unwrap().state, InternalRunState::Assigned);
```

**Step 2: Run the focused test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller handle_expired_lease_keeps_durable_state_consistent_when_timeout_persist_fails -- --nocapture
```

Expected:
- FAIL because the current timeout path can durably write the task before the run

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "test(controller): cover timeout durability atomicity"
```

### Task 2: Add the failing startup preview-pruning test

**Files:**
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing test**

Add an ignored Postgres-backed store test that:

- creates an isolated schema
- inserts a preview row whose `expires_at` is in the past
- calls `load_repaired_snapshot()`
- asserts the returned snapshot has no preview
- asserts `SELECT COUNT(*) FROM controller_previews` is zero

Use direct SQL insert for the expired row so the test is independent of in-memory preview filtering.

**Step 2: Run the focused test to verify it fails**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_prunes_expired_previews -- --ignored
```

Expected:
- FAIL because startup repair currently filters the expired row out of memory but leaves it in `controller_previews`

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/store/mod.rs
git commit -m "test(controller): cover startup preview pruning"
```

### Task 3: Implement transactional timeout persistence

**Files:**
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`

**Step 1: Write minimal implementation**

Add the new durable store method and wire it through the state layer:

```rust
async fn persist_timeout_transition(
    &self,
    run: &RunRecord,
    task: Option<&TaskRecord>,
) -> anyhow::Result<()>;
```

Implementation requirements:

- Postgres store uses one DB transaction for run + optional task
- `FailingMetadataStore` simulates the same all-or-nothing behavior
- `handle_expired_lease` and `sweep_reconciliation_timeouts` call the new helper with post-transition snapshots
- on error, those handlers roll memory back and skip watcher publish

**Step 2: Run focused tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller handle_expired_lease_keeps_durable_state_consistent_when_timeout_persist_fails -- --nocapture
cargo test -p rapidbyte-controller handle_expired_lease_rolls_back_when_persist_fails -- --nocapture
cargo test -p rapidbyte-controller handle_reconciliation_timeout_rolls_back_when_persist_fails -- --nocapture
```

Expected:
- PASS

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/server.rs
git commit -m "fix(controller): make timeout persistence atomic"
```

### Task 4: Implement startup expired-preview pruning

**Files:**
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write minimal implementation**

Add a store helper that deletes expired preview rows:

```rust
async fn prune_expired_previews(&self) -> anyhow::Result<u64>;
```

Call it at the start of `load_repaired_snapshot()` before loading the snapshot contents.

Keep the existing runtime preview cleanup loop unchanged except for any small refactor needed to share deletion SQL.

**Step 2: Run focused tests to verify they pass**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_prunes_expired_previews -- --ignored
```

Expected:
- PASS

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): prune expired previews at startup"
```

### Task 5: Run full verification and push

**Files:**
- Modify: none expected unless verification reveals defects

**Step 1: Format and lint**

Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- PASS

**Step 2: Run automated tests**

Run:

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-cli
```

Expected:
- PASS

**Step 3: Run ignored Postgres-backed tests**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_transaction_create_run_with_task_commits_both_records -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_preserves_original_recovery_started_at -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_prunes_expired_previews -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored
```

Expected:
- PASS

**Step 4: Push**

```bash
git add docs/plans/2026-03-13-controller-timeout-atomicity-and-startup-preview-pruning-design.md docs/plans/2026-03-13-controller-timeout-atomicity-and-startup-preview-pruning-plan.md crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/server.rs
git commit -m "fix(controller): make timeout durability atomic"
git push
```
