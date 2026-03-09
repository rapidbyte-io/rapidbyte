# Connector-Agnostic Benchmark Platform Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the legacy benchmark harness with a connector-agnostic benchmark platform that supports both PR regression gating and broader lab benchmarking.

**Architecture:** Build a Rust execution layer around declarative benchmark scenarios and a Python/DuckDB analysis layer around versioned run artifacts. Migrate any reusable fixture logic out of `tests/bench`, then remove the legacy benchmark harness once the new platform provides equivalent entrypoints.

**Tech Stack:** Rust, Cargo, Rapidbyte runtime/plugin contracts, YAML scenario manifests, JSON artifacts, Python, DuckDB, `just`, GitHub Actions

---

### Task 1: Establish the new benchmark platform skeleton

**Files:**
- Create: `benchmarks/Cargo.toml`
- Create: `benchmarks/src/main.rs`
- Create: `benchmarks/src/cli.rs`
- Create: `benchmarks/src/scenario.rs`
- Create: `benchmarks/src/artifact.rs`
- Create: `benchmarks/scenarios/README.md`
- Modify: `Cargo.toml`
- Modify: `justfile`

**Step 1: Write the failing tests**

Add Rust tests that prove:
- a scenario file can be discovered and parsed
- invalid scenario manifests fail with useful errors
- artifact structs serialize with the required top-level fields

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks`

Expected: FAIL because the new benchmark crate and scenario model do not exist yet

**Step 3: Write the minimal implementation**

Create a new benchmark crate with:
- CLI entrypoint skeleton
- scenario schema types
- artifact schema types
- placeholder commands for `run` and `compare`

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks`

Expected: PASS

**Step 5: Commit**

```bash
git add Cargo.toml justfile benchmarks
git commit -m "feat(bench): add connector-agnostic benchmark skeleton"
```

### Task 2: Implement declarative scenarios and workload adapters

**Files:**
- Create: `benchmarks/src/workload.rs`
- Create: `benchmarks/src/adapters/mod.rs`
- Create: `benchmarks/src/adapters/source.rs`
- Create: `benchmarks/src/adapters/destination.rs`
- Create: `benchmarks/src/adapters/transform.rs`
- Create: `benchmarks/scenarios/pr/*.yaml`
- Create: `benchmarks/scenarios/lab/*.yaml`
- Modify: `benchmarks/src/scenario.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- logical workload families resolve into executable scenario plans
- connector capability requirements are validated before execution
- PR-tagged scenarios can be filtered independently from lab scenarios

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks scenario workload`

Expected: FAIL because workload adapter logic is not implemented

**Step 3: Write the minimal implementation**

Implement:
- workload family definitions
- connector-type adapters
- tag- and suite-based scenario filtering
- capability validation against scenario requirements

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks scenario workload`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks
git commit -m "feat(bench): add scenario workloads and adapters"
```

### Task 3: Build artifact emission and canonical metrics capture

**Files:**
- Create: `benchmarks/src/runner.rs`
- Create: `benchmarks/src/metrics.rs`
- Create: `benchmarks/src/output.rs`
- Modify: `benchmarks/src/artifact.rs`
- Modify: `benchmarks/src/main.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- canonical metrics are always present in emitted artifacts
- connector-specific metrics can be attached without breaking schema stability
- missing correctness assertions fail the run

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks artifact metrics`

Expected: FAIL because run orchestration and artifact emission are incomplete

**Step 3: Write the minimal implementation**

Implement:
- execution result model
- canonical metric capture
- connector metric attachment
- correctness assertion evaluation
- versioned JSON artifact output

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks artifact metrics`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks
git commit -m "feat(bench): emit canonical benchmark artifacts"
```

### Task 4: Add the analysis layer and rolling-baseline comparison

**Files:**
- Create: `benchmarks/analysis/requirements.txt`
- Create: `benchmarks/analysis/compare.py`
- Create: `benchmarks/analysis/schema.sql`
- Create: `benchmarks/analysis/README.md`
- Create: `benchmarks/analysis/tests/test_compare.py`
- Modify: `justfile`

**Step 1: Write the failing tests**

Add Python tests that prove:
- benchmark artifacts load into the analysis layer
- baseline matching uses scenario id, suite, hardware class, and execution flags
- regression thresholds are applied correctly
- low-sample runs produce insufficient-confidence output instead of false failures

**Step 2: Run tests to verify they fail**

Run: `python3 -m pytest benchmarks/analysis/tests/test_compare.py`

Expected: FAIL because the analysis layer does not exist yet

**Step 3: Write the minimal implementation**

Implement:
- artifact ingestion
- baseline selection
- threshold evaluation
- Markdown/CLI comparison output
- DuckDB schema for artifact history

**Step 4: Run tests to verify they pass**

Run: `python3 -m pytest benchmarks/analysis/tests/test_compare.py`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/analysis justfile
git commit -m "feat(bench): add rolling baseline comparison"
```

### Task 5: Create the PR suite and CI regression gate

**Files:**
- Create: `.github/workflows/bench-pr.yml`
- Create: `benchmarks/scenarios/pr/README.md`
- Modify: `justfile`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the failing tests or checks**

Define CI smoke expectations that prove:
- PR scenarios are discoverable
- benchmark artifacts are emitted
- comparison against a stored `main` artifact set runs

**Step 2: Run the checks to verify current behavior fails or is absent**

Run: `just bench-pr`

Expected: FAIL or command missing because the PR suite wiring does not exist yet

**Step 3: Write the minimal implementation**

Add:
- a `just bench-pr` entrypoint
- CI workflow wiring
- benchmark artifact upload/publish path
- contributor docs for the PR benchmark gate

**Step 4: Run the checks to verify they pass**

Run: `just bench-pr`

Expected: PASS locally for smoke coverage

**Step 5: Commit**

```bash
git add .github/workflows/bench-pr.yml justfile README.md CONTRIBUTING.md benchmarks/scenarios/pr
git commit -m "ci(bench): add PR regression gate"
```

### Task 6: Migrate reusable assets and remove the legacy benchmark harness

**Files:**
- Move or replace: `tests/bench/fixtures/**`
- Delete: `tests/bench/src/main.rs`
- Delete: `tests/bench/analyze.py`
- Delete: `tests/bench/lib/report.py`
- Modify: `justfile`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the failing tests or checks**

Add migration verification that proves:
- the new benchmark entrypoint covers the supported benchmark flows
- no commands still point at `tests/bench`
- fixture references resolve under the new `benchmarks/` tree

**Step 2: Run the checks to verify they fail**

Run: `rg -n "tests/bench|bench/src/main.rs|tests/bench/lib/report.py|tests/bench/analyze.py" .`

Expected: legacy references remain

**Step 3: Write the minimal implementation**

Migrate reusable fixtures into the new benchmark tree, update entrypoints, then delete the obsolete legacy benchmark code and scripts.

**Step 4: Run the checks to verify they pass**

Run: `rg -n "tests/bench|bench/src/main.rs|tests/bench/lib/report.py|tests/bench/analyze.py" .`

Expected: no active references outside commit history or explicit migration notes

**Step 5: Commit**

```bash
git add benchmarks justfile README.md CONTRIBUTING.md
git rm -r tests/bench
git commit -m "refactor(bench): retire legacy benchmark harness"
```

### Task 7: Run final verification across the new benchmark platform

**Files:**
- Modify: any touched benchmark files if final cleanup is required

**Step 1: Run Rust benchmark tests**

Run: `cargo test -p rapidbyte-benchmarks`

Expected: PASS

**Step 2: Run analysis-layer tests**

Run: `python3 -m pytest benchmarks/analysis/tests`

Expected: PASS

**Step 3: Run the PR benchmark smoke path**

Run: `just bench-pr`

Expected: PASS

**Step 4: Run repository CI baseline**

Run: `just ci`

Expected: PASS

**Step 5: Commit verification-ready state**

```bash
git add -A
git commit -m "test(bench): verify new benchmark platform"
```
