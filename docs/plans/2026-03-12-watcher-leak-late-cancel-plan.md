# Watcher Leak and Late-Cancel Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the `WatchRun` leak on missing run IDs and replace the weak late-cancellation regression with a behavior-based finalization test.

**Architecture:** Preserve subscribe-first watch semantics, but explicitly clean up watcher state on `NotFound`. Replace the constant post-stream cancellation helper with a closure-based finalization boundary that preserves real outcomes and can be tested with real finalization logic.

**Tech Stack:** Rust, Tokio, rapidbyte-controller, rapidbyte-engine

---

### Task 1: Add the failing controller regression

**Files:**
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/watcher.rs`

**Step 1: Write the failing test**

Add a `PipelineService` test that calls `watch_run_after_subscribe()` with an unknown run ID, expects `NotFound`, and asserts the watcher map has no channel for that ID.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-controller <new_test_name>`

Expected: FAIL because the channel remains allocated.

**Step 3: Write minimal implementation**

Expose a small watcher count/introspection helper for tests and remove the watcher entry on the missing-run error path.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-controller <new_test_name>`

Expected: PASS

### Task 2: Add the failing engine regression

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`

**Step 1: Write the failing test**

Add a finalization test that cancels the token before the post-stream boundary helper runs, then executes real `finalize_successful_run_state(...)` logic through that helper and asserts the persisted run status is still `Completed`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine <new_test_name>`

Expected: FAIL because the current helper/test shape does not exercise real finalization.

**Step 3: Write minimal implementation**

Replace the constant helper with a closure-based post-stream boundary helper and route the real finalization path through it.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine <new_test_name>`

Expected: PASS

### Task 3: Verify the affected packages

**Files:**
- Verify only

**Step 1: Run package tests**

Run:
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-engine`
- `cargo test -p rapidbyte-agent`

Expected: PASS

**Step 2: Run workspace lint gate**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS
