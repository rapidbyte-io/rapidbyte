# Benchmark Environment Profiles Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace hardcoded benchmark connection details with environment profiles and add a local wrapper command that runs native lab benchmarks against the repo-supported dev environment.

**Architecture:** Add a dedicated environment profile model under `benchmarks/environments/`, update scenarios to reference logical environment ids, teach the runner to resolve scenario + profile + env-var overrides into concrete execution settings, and keep local provisioning in `Justfile` wrappers rather than the core runner.

**Tech Stack:** Rust, Cargo, Serde YAML, Just, Docker Compose, Rapidbyte benchmark runner

---

### Task 1: Add environment profile schema and parsing

**Files:**
- Create: `benchmarks/src/environment.rs`
- Modify: `benchmarks/src/main.rs`
- Modify: `benchmarks/Cargo.toml`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- a committed environment profile parses from YAML
- a profile exposes provider metadata, services, and source/destination bindings
- environment-variable overrides can replace PostgreSQL host/password/schema fields

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks environment`

Expected: FAIL because the environment module does not exist yet

**Step 3: Write the minimal implementation**

Implement:
- environment profile structs
- profile loading from `benchmarks/environments/`
- env-var override application for the initial PostgreSQL fields
- small helper APIs to resolve profile by id or path

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks environment`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs benchmarks/src/main.rs benchmarks/Cargo.toml Cargo.lock
git commit -m "feat(bench): add benchmark environment profiles"
```

### Task 2: Update scenario schema to use logical environment references

**Files:**
- Modify: `benchmarks/src/scenario.rs`
- Modify: `benchmarks/src/workload.rs`
- Test: `benchmarks/src/scenario.rs`
- Test: `benchmarks/src/workload.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- scenarios parse `environment.ref` and `environment.stream_name`
- real lab scenarios no longer require embedded source/destination connection blocks
- workload planning still derives deterministic seed expectations when a resolved environment is supplied later

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks scenario workload`

Expected: FAIL because the scenario schema still expects embedded Postgres connection details

**Step 3: Write the minimal implementation**

Implement:
- a logical environment reference field in scenario manifests
- removal or deprecation of direct connection details from scenarios
- any workload-plan adjustments needed so seed planning can consume resolved environment data instead of raw scenario YAML

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks scenario workload`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/scenario.rs benchmarks/src/workload.rs
git commit -m "refactor(bench): reference logical benchmark environments"
```

### Task 3: Teach the runner and pipeline renderer to resolve profiles

**Files:**
- Modify: `benchmarks/src/cli.rs`
- Modify: `benchmarks/src/runner.rs`
- Modify: `benchmarks/src/pipeline.rs`
- Modify: `benchmarks/src/main.rs`
- Test: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/pipeline.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- `--env-profile` is accepted by the CLI
- a real scenario plus environment profile resolves into concrete source/destination connection data
- a real scenario without a required profile fails early with a clear error
- rendered pipeline YAML uses resolved profile data, not hardcoded scenario credentials

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks runner pipeline`

Expected: FAIL because the runner does not yet accept or resolve environment profiles

**Step 3: Write the minimal implementation**

Implement:
- CLI flag `--env-profile`
- environment profile resolution in the runner
- scenario/profile merge before seed planning and pipeline rendering
- early validation for missing/unsupported profiles

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks runner pipeline`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/src/cli.rs benchmarks/src/main.rs benchmarks/src/runner.rs benchmarks/src/pipeline.rs
git commit -m "feat(bench): resolve benchmark environments at runtime"
```

### Task 4: Add committed local-dev profile and migrate native scenarios

**Files:**
- Create: `benchmarks/environments/local-dev-postgres.yaml`
- Modify: `benchmarks/scenarios/lab/pg_dest_insert.yaml`
- Modify: `benchmarks/scenarios/lab/pg_dest_copy.yaml`
- Modify: `benchmarks/scenarios/README.md`
- Test: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/scenario.rs`

**Step 1: Write the failing tests**

Add tests that prove:
- the committed local-dev profile parses correctly
- `pg_dest_insert` and `pg_dest_copy` reference the logical environment id instead of embedded credentials
- scenario discovery still finds and parses both lab scenarios

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks environment scenario`

Expected: FAIL because the new profile file and migrated scenario shape do not exist yet

**Step 3: Write the minimal implementation**

Implement:
- committed local-dev PostgreSQL environment profile
- migrated lab scenarios using `environment.ref`
- updated scenario catalog docs

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks environment scenario`

Expected: PASS

**Step 5: Commit**

```bash
git add benchmarks/environments/local-dev-postgres.yaml benchmarks/scenarios/lab/pg_dest_insert.yaml benchmarks/scenarios/lab/pg_dest_copy.yaml benchmarks/scenarios/README.md
git commit -m "feat(bench): add local benchmark environment profile"
```

### Task 5: Add connector-agnostic local wrapper command

**Files:**
- Modify: `Justfile`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing test**

Add or update a runner/CLI-focused test that proves selecting a lab scenario with an environment profile remains explicit and scenario-scoped.

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-benchmarks runner`

Expected: FAIL if the new invocation contract is not yet represented

**Step 3: Write the minimal implementation**

Add `just` wrapper commands such as:
- `bench-lab scenario env="local-dev-postgres"`

Behavior:
- `docker compose up -d --wait`
- `just build-all`
- run benchmark runner with `--suite lab --scenario {{scenario}} --env-profile {{env}}`

Keep the wrapper generic enough that other profiles and scenarios can use the same entrypoint later.

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-benchmarks runner`

Expected: PASS

**Step 5: Commit**

```bash
git add Justfile benchmarks/src/runner.rs
git commit -m "feat(bench): add local lab benchmark wrapper"
```

### Task 6: Update benchmark documentation and local usage guidance

**Files:**
- Modify: `docs/BENCHMARKING.md`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Update documentation**

Document:
- profile-based benchmark environments
- `--env-profile`
- `just bench-lab scenario=pg_dest_insert`
- env-var override behavior
- the fact that the core runner does not auto-provision infrastructure

**Step 2: Verify docs reference the new flow**

Run:

```bash
rg -n "env-profile|bench-lab|local-dev-postgres|RB_BENCH_PG_" docs README.md CONTRIBUTING.md
```

Expected: matches in the updated benchmark docs only

**Step 3: Commit**

```bash
git add docs/BENCHMARKING.md README.md CONTRIBUTING.md
git commit -m "docs: document benchmark environment profiles"
```

### Task 7: Final verification

**Files:**
- Verify the full changed surface

**Step 1: Run focused benchmark crate verification**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected: PASS

**Step 2: Run wrapper smoke verification**

Run:

```bash
just bench --suite pr --scenario pr_smoke_pipeline --output target/benchmarks/pr/manual.jsonl
```

Expected: PASS and write artifacts

**Step 3: Run local profile wrapper smoke verification**

Run:

```bash
just bench-lab scenario=pg_dest_insert env=local-dev-postgres
```

Expected: reaches the benchmark runner with the committed local profile; if Docker/plugins are available locally, benchmark executes successfully

**Step 4: Review worktree status**

Run:

```bash
git status --short
```

Expected: clean worktree

**Step 5: Commit any remaining verification-related fixes**

```bash
git add -A
git commit -m "test(bench): verify environment-profile benchmark flow"
```

