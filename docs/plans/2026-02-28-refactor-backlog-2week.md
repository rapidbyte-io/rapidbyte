# Rapidbyte 2-Week Refactor Backlog Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Execute a focused 2-week refactor cycle that reduces correctness and performance risk in P0/P1 crates using the CODING_STYLE rubric.

**Architecture:** Work in strict priority order (P0 then P1) with TDD slices and small commits. Each task starts with a failing test, applies the minimal change, and closes with crate-level plus cross-crate verification. Keep runtime/protocol behavior stable while improving reliability, observability, and hot-path efficiency.

**Tech Stack:** Rust, Tokio, Arrow, Wasmtime, rapidbyte-engine/runtime/state/sdk, source-postgres, dest-postgres, e2e/bench harness.

---

## Week 1 (P0)

### Task 1: Engine checkpoint + stage-correlation hardening

**Files:**
- Modify: `crates/rapidbyte-engine/src/checkpoint.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Test: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write failing test**
- Add test proving cursor/checkpoint does not advance on partial destination confirmation.

**Step 2: Run test to verify failure**
- Run: `cargo test -p rapidbyte-engine checkpoint::`
- Expected: FAIL on new test.

**Step 3: Implement minimal fix**
- Tighten checkpoint advancement guard to require full destination correlation.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p rapidbyte-engine checkpoint::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/rapidbyte-engine/src/checkpoint.rs crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/runner.rs crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "refactor(engine): harden checkpoint advancement invariants"
```

### Task 2: Runtime host boundary and frame-handling invariants

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`
- Modify: `crates/rapidbyte-runtime/src/frame.rs`
- Modify: `crates/rapidbyte-runtime/src/engine.rs`
- Test: `crates/rapidbyte-runtime/src/host_state.rs`

**Step 1: Write failing test**
- Add test that invalid host call ordering returns typed boundary errors (not panics).

**Step 2: Run test to verify failure**
- Run: `cargo test -p rapidbyte-runtime host_state::`
- Expected: FAIL on new test.

**Step 3: Implement minimal fix**
- Replace panic-prone paths with explicit error propagation where user-triggerable.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p rapidbyte-runtime host_state::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/rapidbyte-runtime/src/host_state.rs crates/rapidbyte-runtime/src/frame.rs crates/rapidbyte-runtime/src/engine.rs
git commit -m "refactor(runtime): enforce host boundary invariants with typed errors"
```

### Task 3: Destination hot-path allocation + policy clarity

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/decode.rs`
- Test: `connectors/dest-postgres/src/writer.rs`

**Step 1: Write failing test**
- Add test locking behavior for adaptive flush fallback and policy override precedence.

**Step 2: Run test to verify failure**
- Run: `cargo test --manifest-path connectors/dest-postgres/Cargo.toml writer::`
- Expected: FAIL on new policy test.

**Step 3: Implement minimal fix**
- Make policy decision path explicit and remove avoidable temporary allocations in write loop.

**Step 4: Run tests to verify pass**
- Run: `cargo test --manifest-path connectors/dest-postgres/Cargo.toml writer::`
- Expected: PASS (or `cargo check` if wasm test execution is blocked).

**Step 5: Commit**
```bash
git add connectors/dest-postgres/src/writer.rs connectors/dest-postgres/src/decode.rs
git commit -m "refactor(dest-postgres): clarify flush policy path and reduce hot-loop allocation churn"
```

## Week 2 (P1)

### Task 4: Source query/cursor boundary tightening

**Files:**
- Modify: `connectors/source-postgres/src/query.rs`
- Modify: `connectors/source-postgres/src/cursor.rs`
- Modify: `connectors/source-postgres/src/reader.rs`
- Test: `connectors/source-postgres/src/query.rs`

**Step 1: Write failing test**
- Add test for deterministic query generation under cursor and projection combinations.

**Step 2: Run test to verify failure**
- Run: `cargo test --manifest-path connectors/source-postgres/Cargo.toml query::`
- Expected: FAIL on new case.

**Step 3: Implement minimal fix**
- Normalize query assembly path and remove duplicate branch logic.

**Step 4: Run tests to verify pass**
- Run: `cargo test --manifest-path connectors/source-postgres/Cargo.toml query::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add connectors/source-postgres/src/query.rs connectors/source-postgres/src/cursor.rs connectors/source-postgres/src/reader.rs
git commit -m "refactor(source-postgres): normalize query/cursor path invariants"
```

### Task 5: State backend error and transaction boundary cleanup

**Files:**
- Modify: `crates/rapidbyte-state/src/lib.rs`
- Modify: `crates/rapidbyte-state/src/sqlite.rs`
- Modify: `crates/rapidbyte-state/src/postgres.rs`
- Test: `crates/rapidbyte-state/benches/state_backend.rs`

**Step 1: Write failing test**
- Add test for transaction error context propagation in state writes.

**Step 2: Run test to verify failure**
- Run: `cargo test -p rapidbyte-state`
- Expected: FAIL on new context assertion.

**Step 3: Implement minimal fix**
- Add consistent context-rich errors across sqlite/postgres backend operations.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p rapidbyte-state`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/rapidbyte-state/src/lib.rs crates/rapidbyte-state/src/sqlite.rs crates/rapidbyte-state/src/postgres.rs
git commit -m "refactor(state): unify transaction error context across backends"
```

### Task 6: SDK host/context surface simplification

**Files:**
- Modify: `crates/rapidbyte-sdk/src/context.rs`
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `crates/rapidbyte-sdk/src/connector.rs`
- Test: `crates/rapidbyte-sdk/src/context.rs`

**Step 1: Write failing test**
- Add test for stable error propagation when host FFI returns invalid payloads.

**Step 2: Run test to verify failure**
- Run: `cargo test -p rapidbyte-sdk context::`
- Expected: FAIL on new error-shape assertion.

**Step 3: Implement minimal fix**
- Consolidate repeated decoding/error mapping helper logic for host bridge calls.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p rapidbyte-sdk context::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/rapidbyte-sdk/src/context.rs crates/rapidbyte-sdk/src/host_ffi.rs crates/rapidbyte-sdk/src/connector.rs
git commit -m "refactor(sdk): simplify host bridge error mapping and context invariants"
```

## End-of-Week Verification Gates

### End of Week 1

- `just fmt`
- `just lint`
- `cargo test -p rapidbyte-engine`
- `cargo test -p rapidbyte-runtime`
- `cargo test --manifest-path connectors/dest-postgres/Cargo.toml` (or `cargo check` fallback)

### End of Week 2

- `just fmt`
- `just lint`
- `cargo test -p rapidbyte-state`
- `cargo test -p rapidbyte-sdk`
- `cargo test --manifest-path connectors/source-postgres/Cargo.toml`
- `cargo test --manifest-path tests/e2e/Cargo.toml --test transform`

## Completion Exit Criteria

- All six tasks merged with green verification evidence.
- No behavior drift in policy-critical e2e flows.
- Refactor notes documented in follow-up PR summaries.
- New work remains aligned with `CODING_STYLE.md` rule semantics and rubric.
