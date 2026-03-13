# Controller Recovery Timeout And Preview Pruning Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make reconciliation timeout survive repeated controller restarts and ensure expired preview metadata is pruned from durable storage as well as memory.

**Architecture:** Add a boot-time metadata-store repair pass that durably normalizes inflight runs to `Reconciling` without resetting an existing `recovery_started_at`, then update preview cleanup to collect expired preview IDs and delete their durable rows. Cover both behaviors with targeted controller tests and Postgres-backed metadata verification.

**Tech Stack:** Rust, tokio, tonic, tokio-postgres, Postgres metadata store, cargo test, cargo clippy

---

### Task 1: Add failing recovery-timeout persistence tests

**Files:**
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing tests**

Add tests that prove:

- startup normalization stamps `recovery_started_at` only once
- a second load from the repaired durable snapshot keeps the original timestamp
- repeated restart-style normalization still allows reconciliation timeout to fire based on the original timestamp

**Step 2: Run the focused tests to verify they fail**

Run:

- `cargo test -p rapidbyte-controller from_snapshot_marks_inflight_runs_reconciling -- --nocapture`
- `cargo test -p rapidbyte-controller handle_reconciliation_timeout_fails_stale_reconciling_run -- --nocapture`

Expected: FAIL or need extension because current startup repair is not durable.

**Step 3: Implement the minimal store/state repair path**

Add a metadata-store startup repair method, use it from controller initialization, and preserve any existing `recovery_started_at`.

**Step 4: Run the focused tests to verify they pass**

Run the same commands.

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): persist reconciling recovery timestamps"
```

### Task 2: Add failing durable preview-pruning coverage

**Files:**
- Modify: `crates/rapidbyte-controller/src/preview.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`

**Step 1: Write the failing tests**

Add tests that:

- create an expired preview in memory and durable metadata
- run the preview cleanup task or helper
- assert the preview disappears from memory
- assert the durable preview row is deleted too

**Step 2: Run the focused test to verify it fails**

Run: `cargo test -p rapidbyte-controller preview_cleanup_task_removes_expired_entries -- --nocapture`

Expected: FAIL once the durable-delete assertion is added.

**Step 3: Implement the minimal cleanup change**

Expose expired preview IDs from `PreviewStore`, delete the durable rows during cleanup, and keep expiry conservative if durable deletion fails.

**Step 4: Run the focused test to verify it passes**

Run: `cargo test -p rapidbyte-controller preview_cleanup_task_removes_expired_entries -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/preview.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): prune expired preview metadata durably"
```

### Task 3: Full verification and branch update

**Files:**
- Modify: `crates/rapidbyte-controller/src/state.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/preview.rs`
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
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

Expected: PASS

**Step 6: Commit and push**

```bash
git add crates/rapidbyte-controller/src/state.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/preview.rs crates/rapidbyte-controller/src/store/mod.rs
git commit -m "fix(controller): persist recovery timeouts and prune previews"
git push
```
