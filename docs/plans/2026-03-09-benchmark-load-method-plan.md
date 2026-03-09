# Benchmark Load Method Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore distinct benchmark coverage so `insert` mode explicitly uses PostgreSQL INSERT and `copy` mode explicitly uses PostgreSQL COPY.

**Architecture:** Keep the benchmark harness unchanged and fix the contract at the fixture layer. Add a small regression test around rendered benchmark YAML so future destination default changes cannot make both modes hit the same load path.

**Tech Stack:** Rust, Cargo test, serde_yaml benchmark fixture rendering, YAML pipeline fixtures

---

### Task 1: Add a failing regression test for insert benchmark semantics

**Files:**
- Modify: `tests/bench/src/main.rs`
- Test: `tests/bench/src/main.rs`

**Step 1: Write the failing test**

Add a unit test that:
- creates a minimal `BenchContext`
- calls `render_pipeline(&context, "insert")`
- parses the rendered YAML
- asserts `destination.config.load_method == "insert"`

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/bench/Cargo.toml insert_benchmark_fixture_explicitly_uses_insert`

Expected: FAIL because `bench_pg.yaml` currently inherits the destination default and does not include `load_method: insert`

**Step 3: Commit**

```bash
git add tests/bench/src/main.rs
git commit -m "test(bench): lock insert benchmark load method"
```

### Task 2: Fix the benchmark fixture

**Files:**
- Modify: `tests/bench/fixtures/pipelines/bench_pg.yaml`

**Step 1: Write the minimal implementation**

Set `destination.config.load_method: insert` in `tests/bench/fixtures/pipelines/bench_pg.yaml`.

**Step 2: Run targeted test to verify it passes**

Run: `cargo test --manifest-path tests/bench/Cargo.toml insert_benchmark_fixture_explicitly_uses_insert`

Expected: PASS

**Step 3: Commit**

```bash
git add tests/bench/fixtures/pipelines/bench_pg.yaml tests/bench/src/main.rs
git commit -m "fix(bench): separate insert and copy benchmark modes"
```

### Task 3: Verify benchmark harness health

**Files:**
- Modify: `tests/bench/src/main.rs` if formatting or tiny test cleanup is needed

**Step 1: Run the relevant benchmark crate test suite**

Run: `cargo test --manifest-path tests/bench/Cargo.toml`

Expected: PASS

**Step 2: Optionally inspect rendered fixtures manually if needed**

Run: `cargo test --manifest-path tests/bench/Cargo.toml insert_benchmark_fixture_explicitly_uses_insert -- --nocapture`

Expected: PASS with no benchmark harness regressions

**Step 3: Commit verification-ready state**

```bash
git add tests/bench/fixtures/pipelines/bench_pg.yaml tests/bench/src/main.rs
git commit -m "test(bench): verify distinct benchmark load paths"
```
