# Distributed `pg_dest_copy_release` Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a first-class distributed benchmark analogue of `pg_dest_copy_release` inside the existing benchmark runner, using benchmark-owned `postgres + controller + agent` infrastructure and preserving comparable benchmark artifacts.

**Architecture:** Extend the existing benchmark scenario/environment model with a distributed pipeline execution mode rather than building a separate harness. The runner should provision distributed benchmark infrastructure, submit a rendered pipeline via the controller, watch it to terminal completion, and emit the normal artifact shape plus additive distributed metrics.

**Tech Stack:** Rust, clap, serde/serde_yaml, benchmark runner in `benchmarks/`, Docker Compose benchmark environments, existing `rapidbyte` CLI/controller/agent binaries and gRPC clients.

---

### Task 1: Define distributed scenario metadata in the benchmark model

**Files:**
- Modify: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing tests**

Add tests that define and parse a distributed pipeline scenario manifest and assert:
- the manifest round-trips with an explicit distributed execution marker
- existing local pipeline scenarios remain valid without that marker
- invalid combinations fail validation

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml scenario`
Expected: FAIL because distributed scenario metadata is not modeled yet.

**Step 3: Write minimal implementation**

Implement the smallest scenario-model extension needed to distinguish:
- local pipeline benchmark execution
- distributed pipeline benchmark execution

Keep it additive and backward-compatible with existing local scenarios.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml scenario`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/scenario.rs
git commit -m "feat: model distributed benchmark scenarios"
```

### Task 2: Add a benchmark environment profile for single-node distributed runtime

**Files:**
- Create: `benchmarks/environments/local-bench-distributed-postgres.yaml`
- Modify: `benchmarks/docker-compose.yml`
- Modify: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing tests**

Add tests that assert the environment layer can resolve a benchmark-owned distributed profile with:
- postgres service binding
- controller endpoint details
- agent endpoint details

Also add tests that invalid distributed environment profiles are rejected clearly.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml environment`
Expected: FAIL because controller/agent distributed environment details are not represented yet.

**Step 3: Write minimal implementation**

Extend the benchmark environment model to support benchmark-owned distributed runtime details without breaking existing Postgres-only profiles. Add the new environment profile and update Compose to provision the required services.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml environment`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/environments/local-bench-distributed-postgres.yaml benchmarks/docker-compose.yml benchmarks/src/environment.rs
git commit -m "feat: add distributed benchmark environment profile"
```

### Task 3: Add the distributed analogue lab scenario

**Files:**
- Create: `benchmarks/scenarios/lab/pg_dest_copy_release_distributed.yaml`
- Modify: `benchmarks/scenarios/README.md`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing test**

Add a test that loads the new scenario manifest and asserts:
- it matches the same workload/assertion profile as `pg_dest_copy_release`
- it resolves as a distributed pipeline scenario
- it references the distributed benchmark environment profile

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml pg_dest_copy_release_distributed`
Expected: FAIL because the scenario file and/or distributed execution metadata does not exist yet.

**Step 3: Write minimal implementation**

Create the distributed lab scenario and update scenario documentation to describe how it differs from the local release benchmark.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml pg_dest_copy_release_distributed`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/scenarios/lab/pg_dest_copy_release_distributed.yaml benchmarks/scenarios/README.md benchmarks/src/scenario.rs
git commit -m "feat: add distributed pg copy release scenario"
```

### Task 4: Add distributed benchmark execution path to the runner

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/pipeline.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add runner tests that simulate a distributed pipeline benchmark and assert:
- the runner chooses the distributed execution path for the new scenario
- it measures `submit -> terminal completion`
- terminal failure aborts the benchmark
- existing local pipeline execution path remains unchanged

Use dependency seams or test doubles instead of requiring real services in unit tests.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml runner`
Expected: FAIL because the runner only knows the local pipeline execution path.

**Step 3: Write minimal implementation**

Extend the runner with a dedicated distributed pipeline execution path that:
- prepares fixtures
- renders the pipeline
- submits via controller
- watches to terminal completion
- translates terminal run metadata into benchmark results

Keep the local `rapidbyte run` path intact.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml runner`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/pipeline.rs
git commit -m "feat: execute distributed pipeline benchmarks"
```

### Task 5: Capture additive distributed metrics without breaking artifact comparability

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/artifact.rs`
- Test: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/artifact.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- distributed benchmark artifacts still populate canonical metrics exactly as expected
- distributed-specific metrics are additive under connector metrics or execution flags
- existing summary/compare behavior still accepts the artifact shape

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml artifact`
Expected: FAIL because distributed metrics are not yet emitted or are not shaped compatibly.

**Step 3: Write minimal implementation**

Emit the smallest distributed metrics section needed for useful analysis while keeping canonical metrics unchanged for local-versus-distributed comparisons.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml artifact`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/artifact.rs
git commit -m "feat: record distributed benchmark metrics"
```

### Task 6: Add readiness and failure-path coverage for benchmark-owned services

**Files:**
- Modify: `benchmarks/src/environment.rs`
- Modify: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:
- controller/agent readiness failures stop the benchmark early
- watch interruption fails the iteration
- non-terminal or hung runs do not get silently retried

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml distributed`
Expected: FAIL because readiness/watch failure behavior is not fully defined yet.

**Step 3: Write minimal implementation**

Add strict failure handling around distributed provisioning and run watching. Keep failure semantics explicit and non-retrying inside the benchmark harness.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml distributed`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs benchmarks/src/runner.rs
git commit -m "fix: harden distributed benchmark failures"
```

### Task 7: Add an integration-style benchmark smoke path

**Files:**
- Create: `benchmarks/tests/distributed_pg_copy.rs`
- Modify: `benchmarks/Cargo.toml`
- Test: `benchmarks/tests/distributed_pg_copy.rs`

**Step 1: Write the failing test**

Add a smoke/integration test that:
- provisions benchmark-owned distributed infrastructure
- runs the distributed analogue scenario once
- asserts terminal success and row-count correctness

Gate it appropriately if the suite already distinguishes unit and environment-backed tests.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path benchmarks/Cargo.toml --test distributed_pg_copy -- --nocapture`
Expected: FAIL because the distributed benchmark path is not fully wired yet.

**Step 3: Write minimal implementation**

Add the smallest integration harness needed to exercise the real distributed benchmark path end to end.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path benchmarks/Cargo.toml --test distributed_pg_copy -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/tests/distributed_pg_copy.rs benchmarks/Cargo.toml
git commit -m "test: add distributed benchmark smoke coverage"
```

### Task 8: Document usage and comparison workflow

**Files:**
- Modify: `benchmarks/scenarios/README.md`
- Modify: `benchmarks/analysis/README.md`
- Optional Modify: `justfile`

**Step 1: Write the failing doc task**

Document:
- how to run the local release benchmark
- how to run the distributed analogue
- how to compare the resulting artifacts

If the repo already has command-level smoke checks for docs or helper commands, add them first.

**Step 2: Run doc-related verification**

Run: `rg -n "pg_dest_copy_release_distributed|local-bench-distributed-postgres" benchmarks`
Expected: Missing references before implementation.

**Step 3: Write minimal documentation**

Add concise operator-facing docs and helper commands only if they reduce friction without duplicating existing instructions.

**Step 4: Run verification**

Run: `rg -n "pg_dest_copy_release_distributed|local-bench-distributed-postgres" benchmarks`
Expected: References present in benchmark docs.

**Step 5: Commit**

```bash
git add benchmarks/scenarios/README.md benchmarks/analysis/README.md justfile
git commit -m "docs: document distributed benchmark workflow"
```

### Task 9: Full verification and final integration commit

**Files:**
- Modify: any touched files from previous tasks

**Step 1: Run focused benchmark crate tests**

Run: `cargo test --manifest-path benchmarks/Cargo.toml`
Expected: PASS

**Step 2: Run formatting and lint verification**

Run: `cargo clippy --manifest-path benchmarks/Cargo.toml --all-targets -- -D warnings`
Expected: PASS

**Step 3: Run the real distributed scenario once**

Run: `cargo run --manifest-path benchmarks/Cargo.toml -- run --suite lab --scenario pg_dest_copy_release_distributed --env-profile local-bench-distributed-postgres --output target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl`
Expected: PASS and artifact emitted with successful correctness checks.

**Step 4: Review git diff**

Run: `git status --short`
Expected: Only intended benchmark files are modified.

**Step 5: Commit**

```bash
git add benchmarks docs/plans justfile
git commit -m "feat: add distributed pg copy release benchmark"
```
