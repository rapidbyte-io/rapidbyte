# Engine Cancellation Lifecycle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix controller assignment races, make cancellation cooperative at engine safe points, stop shutdown hangs during completion retries, and reclaim expired preview spool memory.

**Architecture:** Keep assignment fencing in the controller service, move cancellation decisions into the engine/orchestrator safe boundaries, let worker completion retries observe shutdown, and make preview spool eviction both eager and periodic.

**Tech Stack:** Rust, Tokio, tonic gRPC, Arrow/engine orchestration, in-memory controller state, existing unit-test suites.

---

### Task 1: Document The Cancellation Model

**Files:**
- Create: `docs/plans/2026-03-12-engine-cancellation-lifecycle-design.md`
- Create: `docs/plans/2026-03-12-engine-cancellation-lifecycle-plan.md`

**Step 1: Write the approved design**

Capture controller assignment fencing, engine safe-point cancellation, shutdown-aware completion retries, and spool reclamation.

**Step 2: Save the implementation plan**

Record the TDD task list in this file.

**Step 3: Commit**

```bash
git add docs/plans/2026-03-12-engine-cancellation-lifecycle-design.md docs/plans/2026-03-12-engine-cancellation-lifecycle-plan.md
git commit -m "docs: plan engine cancellation lifecycle fixes"
```

### Task 2: Fence Cancelled Assignments In The Controller

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`

**Step 1: Write the failing test**

Add a controller regression test showing that if the run is cancelled between `tasks.poll()` and the run-state transition, `poll_task` returns `NoTask` instead of a task assignment.

**Step 2: Run the targeted test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller agent_service::tests::poll_task_does_not_return_cancelled_assignment -- --exact
```

Expected: failure showing the controller still returns the claimed assignment.

**Step 3: Write the minimal implementation**

If `runs.transition(...Assigned)` fails after `tasks.poll()`, clear/cancel the claimed task in the scheduler and return `NoTask`.

**Step 4: Run the targeted test to verify it passes**

Run the same command and expect it to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-controller/src/scheduler.rs
git commit -m "fix(controller): reject cancelled poll assignments"
```

### Task 3: Add Engine Safe-Point Cancellation

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Modify: `crates/rapidbyte-agent/src/executor.rs`

**Step 1: Write the failing tests**

Add tests for:

- cancellation before destination write begins returns a safe-to-retry cancelled terminal error
- cancellation after destination work has started does not fabricate a clean cancellation

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent executor::tests::cancellation_before_destination_write_returns_cancelled_failure -- --exact
cargo test -p rapidbyte-agent executor::tests::cancellation_after_destination_start_preserves_real_outcome -- --exact
```

Expected: failures showing cancellation is still only observed before `run_pipeline`.

**Step 3: Write the minimal implementation**

- Thread a `CancellationToken` into `run_pipeline` and its internal helpers.
- Check cancellation at safe boundaries only.
- Return a structured cancellation error only before destination write starts.

**Step 4: Run the targeted tests to verify they pass**

Run the same commands and expect both tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/runner.rs crates/rapidbyte-agent/src/executor.rs
git commit -m "feat(engine): add cooperative cancellation safe points"
```

### Task 4: Make Worker Shutdown And Spool TTL Real

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`

**Step 1: Write the failing tests**

Add tests for:

- completion retries stop when shutdown is requested
- expired `PreviewSpool::get()` evicts the entry

**Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent worker::tests::completion_retries_stop_on_shutdown -- --exact
cargo test -p rapidbyte-agent spool::tests::expired_get_evicts_entry -- --exact
```

Expected: failure showing retries continue forever and expired gets leave the entry resident.

**Step 3: Write the minimal implementation**

- Add shutdown awareness to `report_completion_until_terminal`.
- Trigger spool cleanup from heartbeat or another persistent loop.
- Make `get()` evict expired entries eagerly.

**Step 4: Run the targeted tests to verify they pass**

Run the same commands and expect both tests to pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs crates/rapidbyte-agent/src/spool.rs
git commit -m "fix(agent): stop shutdown hangs and reclaim previews"
```

### Task 5: Full Verification

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Modify: `crates/rapidbyte-agent/src/executor.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`

**Step 1: Run package tests**

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-engine
cargo test -p rapidbyte-agent
cargo test -p rapidbyte-cli
```

**Step 2: Inspect git status**

```bash
git status --short
```

**Step 3: Final commit**

```bash
git add <touched files>
git commit -m "fix distributed cancellation and lifecycle handling"
```
