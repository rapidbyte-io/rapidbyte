# Native Postgres Benchmark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add native real-execution PostgreSQL benchmark support to the new framework and define explicit `insert` and `copy` lab scenarios.

**Architecture:** Extend scenario manifests with environment, connector override, and assertion sections; add a pipeline execution runner that renders temporary pipeline YAML from scenario data; wire two explicit PostgreSQL lab scenarios that differ only in destination `load_method`.

**Tech Stack:** Rust, Cargo, Rapidbyte CLI/runtime, YAML scenario manifests, temporary pipeline rendering, local Postgres, benchmark JSON artifacts

---

### Task 1: Extend the scenario schema for real pipeline execution

**Files:**
- Modify: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- scenario manifests parse `environment`
- scenario manifests parse `connector_options`
- scenario manifests parse `assertions`

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks scenario`

Expected: FAIL because the new fields do not exist

**Step 3: Write the minimal implementation**

Add strongly typed scenario structs for:
- Postgres benchmark environment
- source and destination connector option maps
- row-count assertions

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks scenario`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/scenario.rs
git commit -m "feat(bench): add real pipeline scenario fields"
```

### Task 2: Add native pipeline rendering from scenario data

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Create: `benchmarks/src/pipeline.rs`
- Test: `benchmarks/src/pipeline.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- a pipeline benchmark scenario renders a valid temporary pipeline YAML
- destination `load_method` override appears in the rendered pipeline
- source and destination connection data are injected from the environment block

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks pipeline`

Expected: FAIL because pipeline rendering does not exist

**Step 3: Write the minimal implementation**

Implement:
- scenario-to-pipeline rendering
- temporary pipeline file creation
- minimal support for Postgres source/destination pipeline generation

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks pipeline`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/pipeline.rs
git commit -m "feat(bench): render pipelines from scenarios"
```

### Task 3: Add native Postgres workload seeding

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/workload.rs`
- Test: `benchmarks/src/workload.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- `narrow_append` resolves to a deterministic seed plan
- row-count assertions derive expected counts from the seed plan

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks workload`

Expected: FAIL because no seed plan model exists

**Step 3: Write the minimal implementation**

Implement:
- seed plan generation for Postgres
- deterministic table name and schema defaults for the benchmark environment
- expected row-count tracking

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks workload`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/workload.rs
git commit -m "feat(bench): add postgres seed planning"
```

### Task 4: Execute real pipeline runs and enforce assertions

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/artifact.rs`
- Modify: `benchmarks/src/output.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- benchmark JSON from `rapidbyte run` maps into benchmark artifacts
- mismatched row-count assertions fail the scenario
- real execution path preserves scenario id and connector metrics

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks runner`

Expected: FAIL because the runner still emits synthetic stub artifacts

**Step 3: Write the minimal implementation**

Implement:
- real pipeline invocation
- bench JSON parsing
- assertion enforcement
- artifact materialization from actual run output

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks runner`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/artifact.rs benchmarks/src/output.rs
git commit -m "feat(bench): execute real pipeline benchmarks"
```

### Task 5: Add explicit PostgreSQL insert and copy lab scenarios

**Files:**
- Create: `benchmarks/scenarios/lab/pg_dest_insert.yaml`
- Create: `benchmarks/scenarios/lab/pg_dest_copy.yaml`
- Modify: `benchmarks/scenarios/README.md`

**Step 1: Write the scenario files**

Define two native scenarios with identical:
- environment
- workload
- execution
- assertions

and only one differing field:
- `connector_options.destination.load_method`

**Step 2: Run scenario parsing tests**

Run: `cargo test -p rapidbyte-benchmarks scenario`

Expected: PASS

**Step 3: Commit**

```bash
git add benchmarks/scenarios/lab benchmarks/scenarios/README.md
git commit -m "feat(bench): add postgres insert and copy lab scenarios"
```

### Task 6: Add manual benchmark entrypoint documentation

**Files:**
- Modify: `docs/BENCHMARKING.md`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Document how to run the new Postgres lab scenarios**

Document:
- how to invoke the lab suite
- how to target the explicit Postgres scenarios
- how to compare generated artifacts

**Step 2: Verify docs reference only the new framework**

Run: `rg -n "pg_dest_insert|pg_dest_copy|benchmarks/scenarios/lab" docs README.md CONTRIBUTING.md`

Expected: matching references exist in the updated docs

**Step 3: Commit**

```bash
git add docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "docs(bench): document native postgres benchmark scenarios"
```

### Task 7: Run final verification for the native Postgres benchmark slice

**Files:**
- Modify: any touched benchmark files if cleanup is needed

**Step 1: Run benchmark crate tests**

Run: `cargo test -p rapidbyte-benchmarks`

Expected: PASS

**Step 2: Run analysis-layer tests**

Run: `python3 -m unittest benchmarks.analysis.tests.test_compare`

Expected: PASS

**Step 3: Run the PR benchmark smoke path**

Run: `just bench-pr`

Expected: PASS

**Step 4: Run a manual native Postgres lab benchmark**

Run: `just bench --suite lab --output target/benchmarks/lab/results.jsonl`

Expected: PASS and emit artifacts for the explicit Postgres scenarios

**Step 5: Run repository CI baseline**

Run: `just ci`

Expected: PASS

**Step 6: Commit verification-ready state**

```bash
git add -A
git commit -m "test(bench): verify native postgres benchmark slice"
```
