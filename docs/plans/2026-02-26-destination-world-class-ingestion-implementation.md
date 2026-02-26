# Destination World-Class Ingestion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Re-architect destination execution into leader-prepared/worker-write-only flow that eliminates startup DDL races and delivers measurable throughput/core-utilization gains.

**Architecture:** Engine orchestrator performs strict leader-first setup sequencing per destination stream. `dest-postgres` produces an immutable `WriteContract` in leader setup and workers perform data-path-only writes with no DDL/setup access. Legacy fallback paths are removed once the new flow is validated.

**Tech Stack:** Rust, Tokio (`spawn_blocking`), Postgres (`tokio-postgres`), rapidbyte engine/runtime metrics, existing benchmark harness (`just bench`) and postgres e2e scenarios.

---

## Task 1: Add Destination Contract Types and Leader Setup API

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/lib.rs`
- Test: `connectors/dest-postgres/src/writer.rs` (unit tests module)

**Step 1: Write the failing test**

Add unit tests for:
- `prepare_stream_once` returns non-empty `qualified_table`/active columns for valid schema hints.
- `WriteContract` is cloneable/immutable and preserves configured write policy values.

**Step 2: Run test to verify it fails**

Run: `cargo test -p dest-postgres prepare_stream_once -- --nocapture`
Expected: FAIL because setup API/contract type does not exist.

**Step 3: Write minimal implementation**

In `writer.rs` add:
- `WriteContract` struct (table identity, active columns, mode details, flush/checkpoint settings, replace metadata, resume metadata).
- `prepare_stream_once(...) -> Result<WriteContract, String>` that executes setup and returns contract.

**Step 4: Run test to verify it passes**

Run: `cargo test -p dest-postgres prepare_stream_once -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs connectors/dest-postgres/src/lib.rs
git commit -m "feat(dest-postgres): add leader setup API and immutable write contract"
```

## Task 2: Enforce Worker Write-Only Execution Path

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/ddl/mod.rs`
- Modify: `connectors/dest-postgres/src/watermark.rs`
- Test: `connectors/dest-postgres/src/writer.rs` (unit tests)

**Step 1: Write the failing test**

Add tests proving worker entrypoint:
- accepts `WriteContract` and writes batches,
- cannot call schema/watermark ensure setup functions,
- errors if contract is missing required prepared fields.

**Step 2: Run test to verify it fails**

Run: `cargo test -p dest-postgres worker_write_only -- --nocapture`
Expected: FAIL because worker path still depends on setup behavior.

**Step 3: Write minimal implementation**

Refactor write path:
- `write_stream_worker(...)` uses contract only.
- Move/limit setup-only helpers behind leader API boundary.
- Remove worker DDL/setup calls.

**Step 4: Run test to verify it passes**

Run: `cargo test -p dest-postgres worker_write_only -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs connectors/dest-postgres/src/ddl/mod.rs connectors/dest-postgres/src/watermark.rs
git commit -m "refactor(dest-postgres): make worker path contract-only with no setup access"
```

## Task 3: Replace-Mode Single-Owner Lifecycle Guarantees

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/ddl/staging.rs`
- Test: `connectors/dest-postgres/src/writer.rs`

**Step 1: Write the failing test**

Add tests for replace mode:
- leader preps staging exactly once,
- worker writes never execute swap,
- final swap occurs once after successful worker completion.

**Step 2: Run test to verify it fails**

Run: `cargo test -p dest-postgres replace_mode_single_owner -- --nocapture`
Expected: FAIL due to shared/ambiguous ownership of replace lifecycle.

**Step 3: Write minimal implementation**

Implement explicit ownership in session state:
- leader owns prepare/swap lifecycle,
- workers produce write results only,
- finalization API executes swap once.

**Step 4: Run test to verify it passes**

Run: `cargo test -p dest-postgres replace_mode_single_owner -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs connectors/dest-postgres/src/ddl/staging.rs
git commit -m "fix(dest-postgres): enforce single-owner replace lifecycle"
```

## Task 4: Orchestrator Leader-First Sequencing

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Test: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test**

Add integration test for parallel destination startup:
- if destination setup fails, workers do not start,
- if setup succeeds, workers fan out and complete.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine pipeline_integration -- --nocapture`
Expected: FAIL because current sequencing allows concurrent startup in worker path.

**Step 3: Write minimal implementation**

Update orchestrator execution:
- perform leader setup before worker spawn,
- pass prepared contract into worker runs,
- fail fast on setup failure.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine pipeline_integration -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/runner.rs crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "feat(engine): enforce leader-first destination setup and worker fan-out sequencing"
```

## Task 5: Deterministic Failure and Cancellation Semantics

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/result.rs`
- Test: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test**

Add failure-injection integration tests:
- first worker failure cancels siblings,
- stream reports deterministic first error,
- commit-state classification remains correct.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine cancellation -- --nocapture`
Expected: FAIL due to non-deterministic or incomplete cancellation/reporting.

**Step 3: Write minimal implementation**

Implement deterministic cancellation flow and error collection ordering.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine cancellation -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/result.rs crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "fix(engine): deterministic destination worker cancellation and error semantics"
```

## Task 6: Add CPU/Worker Utilization Metrics

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`
- Modify: `crates/rapidbyte-engine/src/result.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test**

Add tests/assertions for JSON output fields:
- rapidbyte process CPU utilization summary,
- destination worker active/wait metrics,
- worker skew summary fields.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration -- --nocapture`
Expected: FAIL because fields are not emitted.

**Step 3: Write minimal implementation**

Add and plumb metric fields from orchestrator to CLI run output.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/run.rs crates/rapidbyte-engine/src/result.rs crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "feat(metrics): emit process cpu and destination worker utilization metrics"
```

## Task 7: Remove Legacy Destination Path (No Fallback)

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Test: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test**

Add tests ensuring only new architecture path is callable and old path symbols/branches are removed.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine architecture_path -- --nocapture`
Expected: FAIL because old branches still exist.

**Step 3: Write minimal implementation**

Delete legacy orchestration branches and dead code.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine architecture_path -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-engine/src/runner.rs crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "refactor: remove legacy destination path and enforce new architecture only"
```

## Task 8: End-to-End Validation and Performance Gate

**Files:**
- Modify: `PERF_ANALYSIS.md`
- Modify: `docs/benchmarks/*` (as needed for new baseline notes)
- Modify: `tests/connectors/postgres/scenarios/10_parallelism.sh` (only if assertions need metric checks)

**Step 1: Run code quality gates**

Run:
- `cargo fmt`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace`

Expected: PASS.

**Step 2: Run repeated race/correctness validation**

Run: `for i in {1..5}; do tests/connectors/postgres/scenarios/10_parallelism.sh; done`
Expected: all runs PASS with stable row counts and zero destination race retries.

**Step 3: Run benchmark gate matrix**

Run:
- `just bench postgres --profile medium --iters 3`
- `just bench postgres --profile large --iters 3`

Expected:
- `medium` throughput uplift `>= +20%` vs baseline
- `large` throughput uplift `>= +10%` vs baseline
- zero destination race retries
- utilization metrics show strong worker engagement in destination write windows

**Step 4: Update performance artifacts and documentation**

Document before/after evidence in `PERF_ANALYSIS.md` and benchmark notes.

**Step 5: Commit**

```bash
git add PERF_ANALYSIS.md docs/benchmarks tests/connectors/postgres/scenarios/10_parallelism.sh
git commit -m "perf: validate world-class destination ingestion architecture and publish evidence"
```

## 2026-02-26 Runtime Corrections (Post-Validation)

- Parallel startup uncovered a real PostgreSQL catalog race (`pg_type_typname_nsp_index`) during concurrent `CREATE TABLE IF NOT EXISTS` across workers.
- `dest-postgres` now wraps table creation in a savepoint and treats only that specific race as safe-to-continue while preserving failure semantics for all other DDL errors.
- Parallel full-refresh destination workers no longer share watermark resume/checkpoint state.
- Watermark resume remains enabled for non-partitioned stream execution; partitioned runs explicitly disable watermark state to avoid cross-worker row skipping.
- Repeated `e2e_parallel` validation now passes with `retry_count=0` and stable row counts.
