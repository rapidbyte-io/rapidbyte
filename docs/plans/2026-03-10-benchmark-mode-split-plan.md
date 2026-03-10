# Benchmark Mode Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the native Postgres COPY benchmark into explicit regression and release modes, and make benchmark artifacts plus summaries report truthful execution metadata.

**Architecture:** Extend benchmark scenarios with an execution profile that declares build mode and AOT policy, update the benchmark runner to launch the CLI accordingly and record the actual execution metadata, fix the engine-reported parallelism so artifacts reflect real execution, and expand the benchmark summary to show the extra context needed to interpret throughput and bandwidth correctly.

**Tech Stack:** Rust, Serde YAML/JSON, Clap, Cargo, Just, Markdown docs.

---

### Task 1: Extend scenario manifests with benchmark execution mode

**Files:**
- Modify: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing test**

Add a parsing test that loads a scenario YAML containing benchmark execution
settings and asserts:

- `build_mode` parses as `debug` or `release`
- `aot` parses as `true` or `false`
- omitted values fall back to the intended default profile

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks scenario::tests::parses_benchmark_execution_profile
```

Expected:
- FAIL because `ScenarioManifest` does not yet model benchmark execution mode

**Step 3: Write minimal implementation**

Update `benchmarks/src/scenario.rs` to:

- add a small manifest-facing benchmark execution profile type
- attach it to `ScenarioManifest`
- provide explicit defaults for backward compatibility

Keep the shape minimal:
- `build_mode`
- `aot`

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks scenario::tests::parses_benchmark_execution_profile
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/scenario.rs
git commit -m "feat(bench): add benchmark execution profile to scenarios"
```

### Task 2: Add explicit regression and release COPY scenarios

**Files:**
- Create: `benchmarks/scenarios/lab/pg_dest_copy_regression.yaml`
- Create: `benchmarks/scenarios/lab/pg_dest_copy_release.yaml`
- Modify: `benchmarks/scenarios/README.md`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing test**

Add or update a scenario discovery test to assert:

- both new scenario manifests parse successfully
- both point at the committed `local-dev-postgres` environment profile
- regression declares debug plus AOT disabled
- release declares release plus AOT enabled

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks scenario::tests::native_lab_copy_modes_reference_committed_environment_profile
```

Expected:
- FAIL because the new manifests do not exist yet

**Step 3: Write minimal implementation**

Create the two manifests by cloning the current COPY scenario semantics and
changing only:

- scenario id and name
- benchmark execution profile
- sample budget if needed

Update `benchmarks/scenarios/README.md` to show both scenarios.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks scenario::tests::native_lab_copy_modes_reference_committed_environment_profile
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/scenarios/lab/pg_dest_copy_regression.yaml benchmarks/scenarios/lab/pg_dest_copy_release.yaml benchmarks/scenarios/README.md benchmarks/src/scenario.rs
git commit -m "feat(bench): add regression and release copy scenarios"
```

### Task 3: Make the benchmark runner honor build mode and AOT

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add targeted tests that assert:

- debug scenarios keep the existing `cargo run` style invocation
- release scenarios add `--release`
- AOT is forced through `RAPIDBYTE_WASMTIME_AOT=1` or `0`
- emitted artifacts record the scenario-selected build mode and AOT flag

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::release_scenarios_use_release_runner
cargo test -p rapidbyte-benchmarks runner::tests::artifacts_record_selected_build_mode_and_aot
```

Expected:
- FAIL because the runner currently hardcodes `debug` and `aot: false`

**Step 3: Write minimal implementation**

Update `benchmarks/src/runner.rs` to:

- replace the hardcoded artifact build mode
- replace the hardcoded AOT flag
- derive launch settings from the scenario execution profile
- pass `RAPIDBYTE_WASMTIME_AOT` explicitly to the CLI process
- add `--release` for release scenarios

Keep the current measurement formulas unchanged.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::release_scenarios_use_release_runner
cargo test -p rapidbyte-benchmarks runner::tests::artifacts_record_selected_build_mode_and_aot
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs
git commit -m "feat(bench): honor build mode and AOT in benchmark runner"
```

### Task 4: Fix effective parallelism reporting in benchmark JSON

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`
- Test: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-cli/src/commands/run.rs`

**Step 1: Write the failing test**

Add a focused test that exercises a partitioned full-refresh pipeline and
asserts the reported top-level `parallelism` matches the actual execution
parallelism used by the run, not a fallback value of `1`.

If a direct end-to-end test is too heavy, add:

- an engine-level test for the value placed on `PipelineRunResult.parallelism`
- a CLI-level JSON test that preserves the corrected field

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-engine orchestrator::tests::partitioned_runs_report_effective_parallelism
cargo test -p rapidbyte-cli commands::run::tests::bench_json_uses_effective_parallelism
```

Expected:
- FAIL because the engine currently emits `resolve_effective_parallelism(config, false)`

**Step 3: Write minimal implementation**

Update the engine result construction so the reported `parallelism` comes from
the real execution path, for example the already computed execution
parallelism, and ensure the CLI bench JSON preserves that corrected value.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-engine orchestrator::tests::partitioned_runs_report_effective_parallelism
cargo test -p rapidbyte-cli commands::run::tests::bench_json_uses_effective_parallelism
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-cli/src/commands/run.rs
git commit -m "fix(bench): report effective pipeline parallelism"
```

### Task 5: Expand benchmark summary context

**Files:**
- Modify: `benchmarks/src/summary.rs`
- Test: `benchmarks/src/summary.rs`

**Step 1: Write the failing tests**

Add summary tests that assert rendered output includes:

- build mode
- AOT
- effective parallelism
- total records written
- total bytes written
- bytes per row
- a note or label making it clear the rates are end-to-end wall-clock metrics

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::tests::render_summary_report_includes_execution_context
```

Expected:
- FAIL because the current summary prints only samples, throughput, bandwidth,
  latency, correctness, and connector metric keys

**Step 3: Write minimal implementation**

Update `benchmarks/src/summary.rs` to:

- aggregate total records and bytes from the artifacts
- derive bytes per row safely
- surface build mode and AOT from artifact identity
- surface effective parallelism from connector metrics
- keep existing throughput and latency summaries intact

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks summary::tests::render_summary_report_includes_execution_context
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/summary.rs
git commit -m "feat(bench): enrich summary execution context"
```

### Task 6: Refresh docs for the new benchmark modes

**Files:**
- Modify: `docs/BENCHMARKING.md`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the failing expectation**

Identify the docs gap:

- docs still present the old single COPY scenario
- docs do not explain regression vs release intent
- docs do not explain that rates are end-to-end wall-clock metrics

**Step 2: Re-read current files to confirm the gap**

Run:

```bash
rg -n "pg_dest_copy|bench-lab|records/sec|MB/sec" docs/BENCHMARKING.md README.md CONTRIBUTING.md
```

Expected:
- references only the old COPY scenario and incomplete interpretation guidance

**Step 3: Write minimal implementation**

Update docs to:

- list both COPY modes
- explain the intended use of each mode
- show example commands for both
- clarify how to interpret throughput and bandwidth

**Step 4: Re-read the docs**

Run:

```bash
rg -n "pg_dest_copy_regression|pg_dest_copy_release|end-to-end|wall-clock" docs/BENCHMARKING.md README.md CONTRIBUTING.md
```

Expected:
- docs reference both modes and explain the measurement contract

**Step 5: Commit**

```bash
git add docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "docs(bench): document regression and release benchmark modes"
```

### Task 7: Verify the feature end to end

**Files:**
- Verify in-place against:
  - `benchmarks/src/scenario.rs`
  - `benchmarks/src/runner.rs`
  - `benchmarks/src/summary.rs`
  - `benchmarks/scenarios/lab/pg_dest_copy_regression.yaml`
  - `benchmarks/scenarios/lab/pg_dest_copy_release.yaml`
  - `crates/rapidbyte-engine/src/orchestrator.rs`
  - `crates/rapidbyte-cli/src/commands/run.rs`
  - `docs/BENCHMARKING.md`
  - `README.md`
  - `CONTRIBUTING.md`

**Step 1: Check malformed diffs**

Run:

```bash
git diff --check
```

Expected:
- no output

**Step 2: Run targeted Rust tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks
cargo test -p rapidbyte-engine orchestrator::tests::partitioned_runs_report_effective_parallelism
cargo test -p rapidbyte-cli commands::run::tests::bench_json_uses_effective_parallelism
```

Expected:
- PASS

**Step 3: Run both benchmark modes locally**

Run:

```bash
just bench-lab pg_dest_copy_regression
just bench-lab pg_dest_copy_release
```

Expected:
- both write successful artifact sets
- regression artifact shows `build_mode: debug` and `aot: false`
- release artifact shows `build_mode: release` and `aot: true`

**Step 4: Review summary output for both artifacts**

Run:

```bash
just bench-summary target/benchmarks/lab/pg_dest_copy_regression.jsonl
just bench-summary target/benchmarks/lab/pg_dest_copy_release.jsonl
```

Expected:
- summary shows build mode, AOT, parallelism, total bytes, and bytes per row
- summary still shows throughput, bandwidth, latency, and correctness
- release and regression outputs are directly comparable

**Step 5: Commit**

```bash
git add benchmarks/src/scenario.rs benchmarks/src/runner.rs benchmarks/src/summary.rs benchmarks/scenarios/lab/pg_dest_copy_regression.yaml benchmarks/scenarios/lab/pg_dest_copy_release.yaml benchmarks/scenarios/README.md crates/rapidbyte-engine/src/orchestrator.rs crates/rapidbyte-cli/src/commands/run.rs docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "feat(bench): split copy benchmark into regression and release modes"
```
