# Controller Background Durability Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make background timeout handlers rollback-safe on durable write failure and ensure expired durable preview rows are retried until deletion succeeds.

**Architecture:** Add snapshot-based rollback to lease-expiry and reconciliation-timeout handlers, and add a preview-delete retry set that survives transient durable delete failures across cleanup ticks. Keep the changes local to the controller background loops and their tests.

**Tech Stack:** Rust, tokio, tonic, controller metadata store, cargo test, cargo clippy

---

### Task 1: Add failing background-timeout rollback tests

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing tests**

Add tests that:

- set up an assigned lease-expiry path with injected task/run persistence failure
- assert the run/task are restored to their pre-timeout state after `handle_expired_lease`
- set up a stale reconciling run with injected persistence failure
- assert `sweep_reconciliation_timeouts` restores run/task state instead of leaving memory terminal

**Step 2: Run the focused tests to verify they fail**

Run:

- `cargo test -p rapidbyte-controller handle_expired_lease_skips_publish_when_persist_fails -- --nocapture`
- `cargo test -p rapidbyte-controller handle_reconciliation_timeout_fails_stale_reconciling_run -- --nocapture`

Expected: FAIL once rollback assertions are added.

**Step 3: Implement the minimal rollback logic**

Snapshot the previous run/task state in the timeout handlers, persist the terminal records, and restore memory on any durable failure.

**Step 4: Run the focused tests to verify they pass**

Run the same commands.

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): roll back background timeout failures"
```

### Task 2: Add failing durable preview-delete retry coverage

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing test**

Add a preview cleanup test that:

- inserts an expired preview and durable preview row
- injects one durable delete failure
- runs cleanup once and asserts the row still exists durably but is queued for retry
- runs cleanup again and asserts the durable row is deleted and the retry set is empty

**Step 2: Run the focused test to verify it fails**

Run: `cargo test -p rapidbyte-controller preview_cleanup_task_removes_expired_entries -- --nocapture`

Expected: FAIL after the retry assertions are added.

**Step 3: Implement the minimal retry mechanism**

Add a preview-delete retry set to controller state, feed newly expired preview IDs into it, and remove IDs only after durable delete success.

**Step 4: Run the focused test to verify it passes**

Run: `cargo test -p rapidbyte-controller preview_cleanup_task_removes_expired_entries -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): retry durable preview cleanup"
```

### Task 3: Full verification and branch update

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Format**

Run: `cargo fmt --all`

**Step 2: Lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS with zero warnings

**Step 3: Run controller tests**

Run: `cargo test -p rapidbyte-controller`

Expected: PASS

**Step 4: Run CLI tests**

Run: `cargo test -p rapidbyte-cli`

Expected: PASS

**Step 5: Run ignored Postgres-backed tests**

Run:

- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_transaction_create_run_with_task_commits_both_records -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_preserves_original_recovery_started_at -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

Expected: PASS

**Step 6: Commit and push**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): make background durability converge"
git push
```
