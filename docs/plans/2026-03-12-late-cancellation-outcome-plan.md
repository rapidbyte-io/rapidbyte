# Late Cancellation Outcome Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Preserve the real pipeline outcome when cancellation arrives after stream execution completes.

**Architecture:** Keep cancellation checks at safe pre-execution boundaries, but remove the unsafe post-`execute_streams(...)` gate that can overwrite a real outcome during finalization. Protect the behavior with a focused engine regression test.

**Tech Stack:** Rust, Tokio, rapidbyte-engine test suite

---

### Task 1: Add the failing regression

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-engine/src/orchestrator.rs`

**Step 1: Write the failing test**

Add a targeted orchestrator test that models the post-stream boundary and asserts cancellation does not replace the real outcome once execution has completed.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine <new_test_name>`

Expected: FAIL because the current post-stream cancellation check still returns a synthetic cancellation.

**Step 3: Write minimal implementation**

Remove the post-`execute_streams(...)` cancellation gate in `execute_pipeline_once()` and leave the earlier safe cancellation checks in place.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine <new_test_name>`

Expected: PASS

### Task 2: Verify no regressions in existing cancellation behavior

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` if test naming/coverage cleanup is needed
- Test: `crates/rapidbyte-agent/src/executor.rs`

**Step 1: Run focused engine and agent cancellation tests**

Run:
- `cargo test -p rapidbyte-engine`
- `cargo test -p rapidbyte-agent`

Expected: PASS

**Step 2: Run quality gate**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS
