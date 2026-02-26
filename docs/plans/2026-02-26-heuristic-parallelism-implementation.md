# Heuristic Parallelism Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement static `resources.parallelism: auto` (default) with deterministic, correctness-preserving fanout sizing and measurable telemetry.

**Architecture:** Parse `parallelism` as `auto|manual`, resolve one `effective_parallelism` at run start in the orchestrator, preserve existing CDC/incremental/replace semantics, and expose mode/effective values in pipeline result + CLI bench JSON.

**Tech Stack:** Rust, serde/serde_yaml config parsing, rapidbyte-engine orchestrator, rapidbyte-engine result model, rapidbyte-cli run output, existing integration/perf harness.

---

## Task 1: Add `auto|manual` Parallelism Type in Config

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/types.rs`
- Test: `crates/rapidbyte-engine/src/config/types.rs` (existing unit test module)

**Step 1: Write the failing test**

Add tests that verify:
- `resources.parallelism: auto` parses successfully.
- omitted `resources.parallelism` defaults to `auto`.
- numeric `resources.parallelism: 4` still parses as manual.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine config::types::tests::test_parallelism -- --nocapture`
Expected: FAIL because `parallelism` is currently `u32` only.

**Step 3: Write minimal implementation**

In `types.rs`:
- Introduce `PipelineParallelism` enum with serde support for `auto` and integer.
- Replace `ResourceConfig.parallelism: u32` with `parallelism: PipelineParallelism`.
- Set `Default` for `ResourceConfig.parallelism` to `PipelineParallelism::Auto`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine config::types::tests::test_parallelism -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/config/types.rs
git commit -m "feat(config): add auto/manual parallelism type with auto default"
```

## Task 2: Add Validation for Manual Parallelism Bounds

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/validator.rs`
- Test: `crates/rapidbyte-engine/src/config/validator.rs` (unit tests)

**Step 1: Write the failing test**

Add tests that verify:
- `parallelism: 0` is rejected.
- `parallelism: auto` is accepted.
- `parallelism: 1` is accepted.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine config::validator::tests::test_parallelism -- --nocapture`
Expected: FAIL because validator has no mode-aware checks yet.

**Step 3: Write minimal implementation**

In `validator.rs`:
- Add mode-aware validation:
  - manual `0` => error
  - auto => valid
  - manual `>=1` => valid

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine config::validator::tests::test_parallelism -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/config/validator.rs
git commit -m "test(config): validate manual parallelism bounds and auto mode"
```

## Task 3: Implement Deterministic Heuristic Resolution in Orchestrator

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-engine/src/orchestrator.rs` (unit tests)

**Step 1: Write the failing test**

Add focused tests for heuristic resolver behavior:
- auto + no eligible streams => effective parallelism `1`.
- auto + eligible full-refresh streams => capped by `min(cores, 16)` and `max_shard_depth=4`.
- manual override keeps requested value (`>=1`).
- transform presence applies count-scaled factor `max(0.6, 1/(1+0.15*n))`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine orchestrator::tests::auto_parallelism -- --nocapture`
Expected: FAIL because resolver function does not exist.

**Step 3: Write minimal implementation**

In `orchestrator.rs`:
- Add `resolve_effective_parallelism(...)` helper implementing the heuristic from the design doc.
- Introduce explicit constants for readability/tuning (`MAX_AUTO_CAP=16`, `MAX_SHARD_DEPTH=4`, `TRANSFORM_PENALTY_SLOPE=0.15`, `MIN_TRANSFORM_FACTOR=0.6`).
- Use resolved value in:
  - stream context fanout/sharding decisions
  - worker semaphore sizing
  - result aggregation parallelism field
- Preserve existing gates:
  - only full-refresh source-postgres eligible for sharding
  - no new CDC/incremental sharding
  - replace mode unchanged

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine orchestrator::tests::auto_parallelism -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "feat(engine): resolve deterministic auto parallelism in orchestrator"
```

## Task 4: Expose Parallelism Mode Telemetry in Result + CLI JSON

**Files:**
- Modify: `crates/rapidbyte-engine/src/result.rs`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`
- Test: `crates/rapidbyte-cli/src/commands/run.rs` (bench JSON unit test)

**Step 1: Write the failing test**

Extend bench JSON test assertions for:
- `parallelism_mode`
- `parallelism_effective`
- `parallelism_cap`
- `parallelism_available_cores`
- `parallelism_eligible_streams`
- `parallelism_transform_count`

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-cli bench_json_includes_stream_shard_metrics -- --nocapture`
Expected: FAIL because new fields are missing.

**Step 3: Write minimal implementation**

In `result.rs`:
- Add new run-level telemetry fields for mode + heuristic context.

In `run.rs`:
- Emit these fields in `@@BENCH_JSON@@` output.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-cli bench_json_includes_stream_shard_metrics -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/result.rs crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(metrics): emit auto parallelism mode and heuristic telemetry"
```

## Task 5: Add Integration Coverage for Mode Semantics and Invariants

**Files:**
- Modify: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test**

Add integration tests that assert:
- auto default does not shard incremental/CDC streams.
- auto default does not shard replace mode streams.
- auto default does shard eligible full-refresh source-postgres streams.
- manual override still works.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration parallelism -- --nocapture`
Expected: FAIL because new expectations are not wired yet.

**Step 3: Write minimal implementation (if needed)**

Only add the smallest implementation adjustments required for tests to pass (likely already done in Task 3).

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration parallelism -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/tests/pipeline_integration.rs crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "test(engine): cover auto parallelism semantics and sharding invariants"
```

## Task 6: Full Verification + Bench Gate + Docs Update

**Files:**
- Modify: `PERF_ANALYSIS.md`
- Modify: `docs/benchmarks/*` (if baseline summaries change)

**Step 1: Run code quality and correctness gates**

Run:
- `cargo fmt`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace`

Expected: PASS.

**Step 2: Run parallel correctness scenario repeatedly**

Run:

```bash
for i in {1..5}; do tests/connectors/postgres/scenarios/10_parallelism.sh; done
```

Expected: all runs pass; no unsafe-retry regressions.

**Step 3: Run benchmark gate matrix**

Run:
- `just bench postgres --profile medium --iters 3`
- `just bench postgres --profile large --iters 3`

Expected:
- measurable uplift versus old omitted-parallelism behavior
- no correctness regressions
- telemetry fields populated in bench JSON

**Step 4: Update performance artifacts/documentation**

Document:
- before/after throughput
- CPU/core utilization deltas
- observed heuristic mode/effective values

**Step 5: Commit**

```bash
git add PERF_ANALYSIS.md docs/benchmarks
git commit -m "perf: validate static auto parallelism and publish benchmark evidence"
```
