# Cancelled Outcome Mapping Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Preserve `CANCELLED` as a real cancelled outcome for safe pre-commit active-run cancellation.

**Architecture:** Detect the specific engine cancellation error shape in the agent executor and map it to `TaskOutcomeKind::Cancelled`, while preserving all other plugin errors as failures. Back it with agent and controller regressions.

**Tech Stack:** Rust, Tokio, rapidbyte-agent, rapidbyte-controller

---

### Task 1: Add the failing executor regression

**Files:**
- Modify: `crates/rapidbyte-agent/src/executor.rs`

**Step 1: Write the failing test**

Add a test showing a runner that returns `PluginError("CANCELLED").with_commit_state(BeforeCommit)` after execution starts is mapped to `TaskOutcomeKind::Cancelled`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-agent <new_test_name>`

Expected: FAIL because the current code maps it to `Failed`.

**Step 3: Write minimal implementation**

Add a narrow mapping helper for the safe pre-commit cancel shape and use it in executor error handling.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-agent <new_test_name>`

Expected: PASS

### Task 2: Add controller semantic coverage

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write the test**

Add a regression showing a run in `Cancelling` completed with `TaskOutcome::Cancelled` ends in controller state `Cancelled`.

**Step 2: Run test**

Run: `cargo test -p rapidbyte-controller <new_test_name>`

Expected: PASS after implementation

### Task 3: Verify affected packages

**Files:**
- Verify only

**Step 1: Run tests**

Run:
- `cargo test -p rapidbyte-agent`
- `cargo test -p rapidbyte-controller`

Expected: PASS

**Step 2: Run lint gate**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS
