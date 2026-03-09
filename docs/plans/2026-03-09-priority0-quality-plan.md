# Priority 0 External Readiness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the repository externally testable by fixing the red lint gate, adding CI, replacing SDK conformance TODOs with executable checks, and making scaffold output intentionally safe.

**Architecture:** Execute in three slices. First restore a green baseline and encode it in CI. Next upgrade `rapidbyte-sdk::conformance` from placeholder helpers to executable contracts with fake-harness tests. Finally harden the scaffold generator so it emits compile-clean but explicitly unimplemented templates, backed by file-content tests.

**Tech Stack:** Rust workspace, GitHub Actions, `just`, Wasm plugin SDK, tempdir-based tests

**Design doc:** `docs/plans/2026-03-09-priority0-quality-design.md`

---

### Task 1: Fix the current clippy failures in `rapidbyte-state`

**Files:**
- Modify: `crates/rapidbyte-state/src/postgres.rs`
- Modify: `crates/rapidbyte-state/src/sqlite.rs`

**Step 1: Write the failing test or verification target**

No new behavior test is required here because this task is a lint-fix. The failing gate is the executable spec.

**Step 2: Run the failing check**

Run: `cargo clippy -p rapidbyte-state --all-targets -- -D warnings`
Expected: FAIL with `clippy::map_unwrap_or` in `postgres.rs` and `clippy::type_complexity` in `sqlite.rs`.

**Step 3: Write minimal implementation**

- Replace the `map(...).unwrap_or_else(...)` chain in `PostgresStateBackend::pool_size()` with a simpler form such as `map_or_else(...)`.
- Introduce a local type alias for the tuple returned by `SqliteStateBackend::get_run_row()` so the signature is no longer flagged as overly complex.

**Step 4: Run the crate check to verify it passes**

Run: `cargo clippy -p rapidbyte-state --all-targets -- -D warnings`
Expected: PASS

**Step 5: Run the affected tests**

Run: `cargo test -p rapidbyte-state --all-targets`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-state/src/postgres.rs crates/rapidbyte-state/src/sqlite.rs
git commit -m "fix(state): restore clean clippy baseline"
```

---

### Task 2: Add a canonical CI entrypoint in `Justfile`

**Files:**
- Modify: `Justfile`

**Step 1: Write the failing test or verification target**

The failing condition is the absence of a canonical CI recipe.

**Step 2: Confirm the command is missing**

Run: `just --list`
Expected: output does not include `ci`.

**Step 3: Write minimal implementation**

Add a `ci` recipe that runs, in order:

```just
ci:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --all-targets
    cargo test --manifest-path tests/e2e/Cargo.toml --no-run
```

**Step 4: Run the command to verify it passes**

Run: `just ci`
Expected: PASS

**Step 5: Commit**

```bash
git add Justfile
git commit -m "build: add canonical ci recipe"
```

---

### Task 3: Add GitHub Actions for the enforced baseline

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `README.md`
- Modify: `CONTRIBUTING.md`

**Step 1: Write the failing test or verification target**

The failing condition is structural: `.github/workflows/ci.yml` does not exist.

**Step 2: Confirm the workflow is absent**

Run: `find .github -maxdepth 3 -type f | sort`
Expected: only `.github/pull_request_template.md` appears.

**Step 3: Write minimal implementation**

Create a GitHub Actions workflow with separate jobs for:

- formatting
- clippy
- workspace tests
- e2e compile-only check

Use a current Ubuntu runner, install stable Rust plus `wasm32-wasip2`, cache Cargo artifacts, and invoke the same commands used by `just ci`.

Update `README.md` and `CONTRIBUTING.md` so they point contributors at `just ci` as the canonical pre-PR command.

**Step 4: Run local verification**

Run: `just ci`
Expected: PASS

Run: `sed -n '1,240p' .github/workflows/ci.yml`
Expected: workflow contains the four baseline jobs.

**Step 5: Commit**

```bash
git add .github/workflows/ci.yml README.md CONTRIBUTING.md
git commit -m "ci: enforce external readiness baseline"
```

---

### Task 4: Replace placeholder conformance helpers with executable partitioned-read tests

**Files:**
- Modify: `crates/rapidbyte-sdk/src/conformance.rs`

**Step 1: Write the failing test**

Add fake-harness tests to `conformance.rs` covering:

```rust
#[tokio::test]
async fn partitioned_read_rejects_duplicate_ids() { /* ... */ }

#[tokio::test]
async fn partitioned_read_requires_full_coverage() { /* ... */ }

#[tokio::test]
async fn partitioned_read_accepts_exact_coverage() { /* ... */ }
```

The fake harness should return seeded counts and a configurable set of observed IDs from a new partitioned-read execution method.

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidbyte-sdk --features conformance partitioned_read_`
Expected: FAIL because the trait and helper do not yet implement the asserted contract.

**Step 3: Write minimal implementation**

Refactor `ConformanceHarness` to add a method such as:

```rust
async fn run_partitioned_read_ids(
    &self,
    table: &str,
    shard_count: u32,
) -> Result<Vec<String>, Box<dyn std::error::Error>>;
```

Update `run_partitioned_read(...)` to:

- seed and count rows as before
- call the new harness method
- assert row-count match
- assert uniqueness by comparing observed IDs to a `HashSet`

Remove the old TODO block entirely.

**Step 4: Run the tests to verify they pass**

Run: `cargo test -p rapidbyte-sdk --features conformance partitioned_read_`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/conformance.rs
git commit -m "feat(sdk): execute partitioned read conformance checks"
```

---

### Task 5: Add executable CDC conformance helpers and tests

**Files:**
- Modify: `crates/rapidbyte-sdk/src/conformance.rs`

**Step 1: Write the failing test**

Add fake-harness tests to `conformance.rs` covering:

```rust
#[tokio::test]
async fn cdc_requires_all_mutations_to_be_observed() { /* ... */ }

#[tokio::test]
async fn cdc_accepts_expected_mutation_ids() { /* ... */ }
```

Use a fixed mutation list and a fake harness that returns observed CDC IDs from a new CDC execution method.

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidbyte-sdk --features conformance cdc_`
Expected: FAIL because the helper still does not assert real CDC behavior.

**Step 3: Write minimal implementation**

Refactor `ConformanceHarness` to add a method such as:

```rust
async fn run_cdc_capture_ids(
    &self,
    table: &str,
    mutations: &[Mutation],
) -> Result<Vec<String>, Box<dyn std::error::Error>>;
```

Update `run_cdc(...)` to:

- seed the initial rows
- define or accept a deterministic mutation list
- invoke the new harness method
- assert that each expected mutation ID is observed

Keep the API narrow; do not introduce speculative event-model abstractions.

**Step 4: Run the tests to verify they pass**

Run: `cargo test -p rapidbyte-sdk --features conformance cdc_`
Expected: PASS

**Step 5: Run the full crate tests**

Run: `cargo test -p rapidbyte-sdk --all-targets --features conformance`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-sdk/src/conformance.rs
git commit -m "feat(sdk): execute cdc conformance checks"
```

---

### Task 6: Add scaffold tests before changing generated output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing tests**

Add unit tests at the bottom of `scaffold.rs` that:

- create a temp directory
- call `run("source-example", Some(tempdir_path))`
- call `run("dest-example", Some(tempdir_path))`
- read the generated files and assert current bad placeholders are absent and new guardrails are present

Test names should cover:

```rust
#[test]
fn scaffold_source_generates_explicit_unimplemented_stubs() { /* ... */ }

#[test]
fn scaffold_destination_generates_explicit_unimplemented_stubs() { /* ... */ }
```

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p rapidbyte-cli scaffold_`
Expected: FAIL because the current templates still contain TODO-success placeholders.

**Step 3: Write minimal implementation**

Update the scaffold generators so they:

- stop embedding `3306 // TODO: Change to your plugin's default port`
- use a neutral `default_port()` value without misleading commentary
- return `PluginError::config(...)` or `PluginError::internal(...)` from validation/discover/read/write stubs with explicit `"UNIMPLEMENTED"` codes
- avoid fake success `ValidationStatus::Success` and zero-row summaries

**Step 4: Run the tests to verify they pass**

Run: `cargo test -p rapidbyte-cli scaffold_`
Expected: PASS

**Step 5: Run the crate tests**

Run: `cargo test -p rapidbyte-cli --all-targets`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "fix(cli): make scaffold output explicitly unimplemented"
```

---

### Task 7: Run the full Priority 0 verification sweep

**Files:**
- Verify only: workspace-wide

**Step 1: Run all required checks**

Run: `just ci`
Expected: PASS

Run: `cargo test -p rapidbyte-sdk --all-targets --features conformance`
Expected: PASS

Run: `cargo test -p rapidbyte-cli --all-targets`
Expected: PASS

**Step 2: Inspect git state**

Run: `git status --short`
Expected: only intended Priority 0 files are modified.

**Step 3: Commit final integration pass**

```bash
git add Justfile .github/workflows/ci.yml README.md CONTRIBUTING.md crates/rapidbyte-state/src/postgres.rs crates/rapidbyte-state/src/sqlite.rs crates/rapidbyte-sdk/src/conformance.rs crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "feat: complete priority 0 external readiness tranche"
```
