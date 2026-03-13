# Distributed Benchmark Summary Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `bench-summary` report correct totals for distributed benchmark artifacts and render a compact distributed runtime section.

**Architecture:** Keep the fix inside the benchmark summary path. Extend the summary model to extract distributed metadata and derive totals from existing artifact data when `stream_metrics` is empty, then render those values without changing the artifact schema.

**Tech Stack:** Rust, serde_json, benchmark summary unit tests

---

### Task 1: Add distributed summary model coverage

**Files:**
- Modify: `benchmarks/src/summary.rs`
- Test: `benchmarks/src/summary.rs`

**Step 1: Write the failing tests**

Add tests in `benchmarks/src/summary.rs` that construct:
- a distributed artifact with empty `stream_metrics`, distributed connector metadata, and non-zero totals elsewhere
- a local artifact with normal `stream_metrics`

Assert that:
- distributed summaries render `execution mode: distributed`
- distributed summaries render `agent count`, `controller url`, and `flight endpoint`
- distributed summaries render non-zero `records written` / `bytes written`
- local summaries still behave as before

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml summary::tests -- --nocapture`

Expected: FAIL because the current summary does not surface distributed metadata and resolves totals only from `stream_metrics`.

**Step 3: Write minimal implementation**

In `benchmarks/src/summary.rs`:
- extend `SummaryGroup` with:
  - `execution_mode: Option<String>`
  - `distributed_agent_count: Option<u64>`
  - `distributed_controller_url: Option<String>`
  - `distributed_flight_endpoint: Option<String>`
- add helper extraction functions for:
  - distributed connector metadata
  - totals fallback when `stream_metrics` is empty
- keep the new logic additive and local-only behavior unchanged

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml summary::tests -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "fix: surface distributed benchmark summary details"
```

### Task 2: Render distributed section in summary output

**Files:**
- Modify: `benchmarks/src/summary.rs`
- Test: `benchmarks/src/summary.rs`

**Step 1: Write the failing assertion**

Extend the rendering assertions so the summary output includes the distributed section in a stable location near the existing execution metadata.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml summary::tests::render_summary_report_* -- --nocapture`

Expected: FAIL until rendering is updated.

**Step 3: Write minimal implementation**

Update `render_summary_report` to print, when present:
- `execution mode: distributed`
- `agent count: ...`
- `controller url: ...`
- `flight endpoint: ...`

Place these lines after build/AOT metadata and before row/byte totals.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml summary::tests::render_summary_report_* -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "feat: render distributed benchmark summary section"
```

### Task 3: Verify against the real distributed artifact

**Files:**
- Modify: none unless defects are found
- Test: `target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl`

**Step 1: Run targeted tests**

Run: `cargo test --manifest-path benchmarks/Cargo.toml`

Expected: PASS

**Step 2: Run summary on the real artifact**

Run: `cargo run --manifest-path benchmarks/Cargo.toml -- summary target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl`

Expected:
- summary shows `execution mode: distributed`
- summary shows controller/agent endpoint metadata
- summary shows correct non-zero written rows/bytes

**Step 3: Run lint verification**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

**Step 4: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "test: verify distributed benchmark summary output"
```
