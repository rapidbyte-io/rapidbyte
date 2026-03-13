# Controller Poll Capacity Atomicity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `poll_task` enforce `max_tasks` atomically so concurrent polls cannot over-assign a single agent.

**Architecture:** Keep the current registry lookup in `poll_task`, but move the authoritative capacity check into `try_claim_task` while holding the task write lock used for assignment. Prove the regression first with a concurrent test, then implement the smallest claim-path change that restores atomic admission.

**Tech Stack:** Rust, Tokio, tonic, controller unit tests, existing scheduler state helpers.

---

### Task 1: Add the failing concurrent capacity test

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write the failing test**

Add a new unit test near the existing capacity tests that:

- registers one agent with `max_tasks = 1`
- submits two pending runs
- issues two `poll_task` requests concurrently for that same agent
- asserts exactly one result is `Task` and one result is `NoTask`
- asserts `active_tasks_for_agent(agent_id) == 1`

Use `tokio::join!` or two spawned tasks so both requests race through the optimistic read-side admission path.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller test_poll_task_concurrent_polls_respect_agent_capacity -- --nocapture
```

Expected:
- FAIL because both polls can currently claim work for the same `max_tasks=1` agent

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs
git commit -m "test(controller): cover concurrent poll capacity"
```

### Task 2: Make capacity enforcement atomic in `try_claim_task`

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write minimal implementation**

Update `try_claim_task` to accept `max_tasks` and enforce the cap while holding the task write lock before peeking or polling.

Keep the rest of the assignment and durability flow unchanged unless the new signature requires small call-site cleanup in `poll_task`.

Minimal implementation shape:

```rust
async fn try_claim_task(
    &self,
    agent_id: &str,
    max_tasks: u32,
) -> Result<Option<TaskAssignment>, Status> {
    let claimed = {
        let mut tasks = self.state.tasks.write().await;
        if tasks.active_tasks_for_agent(agent_id)
            >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
        {
            return Ok(None);
        }
        // existing claim flow
    };
    // existing persistence flow
}
```

**Step 2: Run focused tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-controller test_poll_task_concurrent_polls_respect_agent_capacity -- --nocapture
cargo test -p rapidbyte-controller test_poll_task_respects_agent_capacity -- --nocapture
```

Expected:
- PASS

**Step 3: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs
git commit -m "fix(controller): make poll capacity admission atomic"
```

### Task 3: Run verification and push

**Files:**
- Modify: none expected unless verification reveals defects

**Step 1: Run formatting and lint**

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

**Step 3: Run ignored persistence/restart coverage**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_transaction_create_run_with_task_commits_both_records -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_preserves_original_recovery_started_at -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored
```

Expected:
- PASS

**Step 4: Push**

```bash
git add docs/plans/2026-03-13-controller-poll-capacity-atomicity-design.md docs/plans/2026-03-13-controller-poll-capacity-atomicity-plan.md crates/rapidbyte-controller/src/agent_service.rs
git commit -m "fix(controller): make poll capacity admission atomic"
git push
```
