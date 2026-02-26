# Workspace Test Assurance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enforce comprehensive, explicit feature/knob test coverage across Rapidbyte with canonical test commands and a manifest validator.

**Architecture:** Add a manifest (`tests/coverage-matrix.yaml`) that maps each feature/knob to required test layers, then validate that mapping before any full test run. Standardize entrypoints in `Justfile` (`test-unit`, `test-integration`, `test-e2e`, `test`) and keep backward compatibility with `e2e`. Use small TDD slices to add validator logic, wire commands, then backfill and verify matrix coverage.

**Tech Stack:** `just`, Bash, `cargo test`, `ripgrep`, existing shell E2E harness, YAML manifest conventions.

---

### Task 1: Baseline Test Command Topology

**Files:**
- Modify: `Justfile`
- Test: command-line invocation only

**Step 1: Write the failing test (contract check)**

Create an explicit contract checklist in scratch notes (not committed): expected `just --list` output includes `test-unit`, `test-integration`, `test-e2e` and keeps `e2e`.

**Step 2: Run command to verify it fails now**

Run: `just --list | rg "test-unit|test-integration|test-e2e"`
Expected: no matches (or partial), proving commands are missing.

**Step 3: Write minimal implementation**

Edit `Justfile`:
- Add `test-unit` recipe (workspace + connectors unit-focused execution).
- Add `test-integration` recipe (workspace integration test targets).
- Add `test-e2e` recipe calling `./tests/e2e.sh`.
- Update `test` recipe to call validator then unit/integration/e2e in sequence.
- Keep `e2e` recipe as alias wrapper to `test-e2e`.

**Step 4: Run command to verify it passes**

Run: `just --list | rg "test-unit|test-integration|test-e2e|e2e"`
Expected: all commands listed.

**Step 5: Commit**

```bash
git add Justfile
git commit -m "build: add canonical test command tiers"
```

### Task 2: Add Coverage Matrix Skeleton

**Files:**
- Create: `tests/coverage-matrix.yaml`
- Modify: `docs/BUILD.md`
- Test: file/schema sanity checks

**Step 1: Write the failing test**

Define validation expectations in comments at top of new manifest format (required fields and valid layers).

**Step 2: Run check to verify failure pre-file**

Run: `test -f tests/coverage-matrix.yaml`
Expected: exit code 1 (file missing).

**Step 3: Write minimal implementation**

Create `tests/coverage-matrix.yaml` with:
- top-level `version`
- `entries` array
- representative initial entries for key knobs (compression, load mode, incremental, CDC, ACL, transform)
- layer mappings (`unit`, `integration`, `e2e`) and referenced tests/scenarios.

Update `docs/BUILD.md` command section with new test commands and intended use.

**Step 4: Run check to verify it passes**

Run: `test -f tests/coverage-matrix.yaml && rg -n "required_layers|entries|test-e2e" tests/coverage-matrix.yaml docs/BUILD.md`
Expected: file exists and schema markers are present.

**Step 5: Commit**

```bash
git add tests/coverage-matrix.yaml docs/BUILD.md
git commit -m "test: add coverage matrix manifest scaffold"
```

### Task 3: Implement Coverage Validator Script

**Files:**
- Create: `scripts/validate-test-coverage.sh`
- Modify: `tests/coverage-matrix.yaml` (small fixture entries for validator development)
- Test: validator command invocations

**Step 1: Write the failing test**

Create a temporary intentionally-invalid matrix entry (missing required layer mapping) in `tests/coverage-matrix.yaml`.

**Step 2: Run test to verify it fails**

Run: `bash scripts/validate-test-coverage.sh`
Expected: fails with actionable message (script may not exist yet, which is acceptable first failure).

**Step 3: Write minimal implementation**

Create `scripts/validate-test-coverage.sh` that:
- loads `tests/coverage-matrix.yaml`
- validates required fields and unique IDs
- validates `required_layers` values
- verifies every required layer has mapped tests
- verifies references exist:
  - rust tests discoverable via `rg`
  - e2e scenario paths exist under `tests/connectors/*/scenarios`
- prints concise failures and exits non-zero on any violation.

**Step 4: Run test to verify it passes**

Run:
- `bash scripts/validate-test-coverage.sh` (with valid matrix)
- `bash scripts/validate-test-coverage.sh; echo $?`
Expected: success output and exit code `0`.

**Step 5: Commit**

```bash
git add scripts/validate-test-coverage.sh tests/coverage-matrix.yaml
git commit -m "test: add coverage matrix validator"
```

### Task 4: Wire Validator into `just test`

**Files:**
- Modify: `Justfile`
- Test: `just test` command behavior

**Step 1: Write the failing test**

Temporarily break matrix (duplicate ID) and verify `just test` does not yet fail early due to validator.

**Step 2: Run test to verify it fails**

Run: `just test`
Expected before wiring: test suite starts without validator gate.

**Step 3: Write minimal implementation**

Modify `Justfile` `test` recipe to run `bash scripts/validate-test-coverage.sh` before any test tiers.

**Step 4: Run test to verify it passes**

Run:
- `just test` with valid matrix -> validator runs first then test tiers.
- optional negative check: invalid matrix -> immediate failure before tests.

Expected: validator gate is enforced.

**Step 5: Commit**

```bash
git add Justfile
git commit -m "test: gate full test command on coverage validation"
```

### Task 5: Backfill Coverage Matrix for Current Feature Surface

**Files:**
- Modify: `tests/coverage-matrix.yaml`
- Modify: `tests/connectors/postgres/scenarios/*.sh` (only if naming normalization needed)
- Test: validator + targeted suite checks

**Step 1: Write the failing test**

Run validator against expanded feature list checklist and confirm missing mappings are reported.

**Step 2: Run test to verify it fails**

Run: `bash scripts/validate-test-coverage.sh`
Expected: failures for uncovered feature IDs.

**Step 3: Write minimal implementation**

Expand matrix to cover all existing supported knobs/features:
- source modes: full-refresh, incremental, incremental timestamp, CDC
- destination modes: append/replace/upsert/copy and watermark behavior
- transform SQL paths
- compression knobs (`lz4`, `zstd`, none)
- ACL/permissions scenarios
- schema evolution / all-types / edge-case handling
- parallelism and DLQ behavior

Attach test references at required layers for each.

**Step 4: Run test to verify it passes**

Run:
- `bash scripts/validate-test-coverage.sh`
- `just test-unit`
- `just test-integration`
- `just test-e2e postgres` (or equivalent connector subset)

Expected: validator green and all tiers pass.

**Step 5: Commit**

```bash
git add tests/coverage-matrix.yaml tests/connectors/postgres/scenarios
git commit -m "test: map existing feature knobs to unit integration and e2e coverage"
```

### Task 6: Add/Adjust Missing Tests Revealed by Matrix

**Files:**
- Modify: `crates/rapidbyte-engine/tests/pipeline_integration.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Modify: `connectors/source-postgres/src/{query.rs,reader.rs,cdc/encode.rs}`
- Modify: `connectors/dest-postgres/src/{writer.rs,watermark.rs,copy.rs}`
- Modify: `connectors/transform-sql/src/transform.rs`
- Test: crate-local and tier commands

**Step 1: Write the failing test**

For each missing matrix mapping, add one focused failing test at the required layer.

**Step 2: Run test to verify it fails**

Run targeted command per new test (e.g. `cargo test -p rapidbyte-engine pipeline_integration::<name>`).
Expected: deterministic failure proving missing behavior coverage.

**Step 3: Write minimal implementation**

Add only test scaffolding/assertions needed to represent behavior (avoid production logic changes unless genuine bug found).

**Step 4: Run test to verify it passes**

Run targeted test commands, then full tier commands:
- `just test-unit`
- `just test-integration`
- `just test-e2e postgres`

Expected: all pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine tests connectors
git commit -m "test: close coverage gaps for uncovered feature knobs"
```

### Task 7: Final Verification and Developer UX Pass

**Files:**
- Modify: `docs/BUILD.md`
- Modify: `docs/plans/2026-02-26-workspace-test-assurance-design.md` (if status/update note needed)
- Test: end-to-end command verification

**Step 1: Write the failing test**

Check docs do not yet describe exact command order and validator behavior.

**Step 2: Run test to verify it fails**

Run: `rg -n "test-unit|test-integration|test-e2e|validate-test-coverage" docs/BUILD.md`
Expected: incomplete guidance.

**Step 3: Write minimal implementation**

Update docs with:
- command purpose and sequencing
- quick “local fast path” and “full assurance path” examples
- troubleshooting for validator failure messages.

**Step 4: Run test to verify it passes**

Run:
- `just test`
- `just test-unit`
- `just test-integration`
- `just test-e2e postgres`

Expected: all commands execute successfully with clear behavior.

**Step 5: Commit**

```bash
git add docs/BUILD.md docs/plans/2026-02-26-workspace-test-assurance-design.md
git commit -m "docs: document assurance test workflow and validator usage"
```

## Notes for Execution

- Keep commits small and scoped to one task.
- Use @superpowers:test-driven-development discipline for each behavior gap.
- Use @superpowers:verification-before-completion before reporting final success.
- Prefer deterministic tests over randomized/fuzzy checks unless fuzzing is explicitly added as a separate feature.
- If validator implementation complexity grows in Bash, switch to a small Rust or Python utility only if necessary and justified.
