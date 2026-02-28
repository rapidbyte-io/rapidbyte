# Transform Validate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a new `transform-validate` Wasm connector that enforces in-flight data contract rules (`assert_not_null`, `assert_regex`, `assert_range`, `assert_unique`) and applies `on_data_error` policy (`fail`, `skip`, `dlq`) with full per-row failure reasons.

**Architecture:** Create a dedicated connector crate following the existing `transform-sql` pattern: parse and validate connector config, normalize shorthand rules into a compiled rule list, then evaluate each incoming batch and split valid/invalid rows. Forward valid rows downstream, and route invalid rows according to stream policy using connector error returns or DLQ emission.

**Tech Stack:** Rust, `rapidbyte-sdk`, Arrow (`arrow-array`, `arrow-select`, `arrow-json`), `regex`, existing e2e harness.

---

### Task 1: Scaffold Connector Crate and Wire Build Metadata

**Files:**
- Create: `connectors/transform-validate/Cargo.toml`
- Create: `connectors/transform-validate/build.rs`
- Create: `connectors/transform-validate/src/main.rs`
- Create: `connectors/transform-validate/src/config.rs`
- Create: `connectors/transform-validate/src/transform.rs`
- Modify: `tests/e2e/src/harness/connectors.rs`

**Step 1: Write the failing wiring test (e2e harness build path)**

Add assertion path in harness connector preparation by adding `transform-validate` to `ensure_connector(...)` list, then run an e2e transform test that should fail because the connector crate does not exist yet.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-e2e --test transform -- --nocapture`
Expected: FAIL with missing connector path/build failure for `connectors/transform-validate`.

**Step 3: Add minimal connector crate files**

Create crate mirroring `transform-sql` structure with a placeholder transform implementation that compiles and returns passthrough summary.

**Step 4: Run target test to verify wiring passes build stage**

Run: `cargo test -p rapidbyte-e2e --test transform -- --nocapture`
Expected: progresses past connector build failure (may still fail on behavior not implemented yet).

**Step 5: Commit**

```bash
git add connectors/transform-validate tests/e2e/src/harness/connectors.rs
git commit -m "feat(transform): scaffold transform-validate connector crate"
```

### Task 2: Implement Config Parsing, Shorthand Normalization, and Validation

**Files:**
- Modify: `connectors/transform-validate/src/config.rs`
- Modify: `connectors/transform-validate/src/main.rs`
- Test: `connectors/transform-validate/src/config.rs` (unit tests module)

**Step 1: Write failing unit tests for config forms**

Add tests for:
- repeated `assert_not_null`
- list shorthand for `assert_not_null` and `assert_unique`
- map shorthand for `assert_regex`
- range config parsing and invalid bound validation
- empty rules rejected by `validate`

**Step 2: Run tests to verify failure**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml config::tests`
Expected: FAIL on missing config types/normalization behavior.

**Step 3: Implement minimal config model and normalization**

Implement serde-friendly raw config + normalized compiled rules output used by runtime.
Implement `Transform::validate` checks:
- non-empty rules
- non-empty fields
- valid regex compile
- range has at least one bound and `min <= max`

**Step 4: Run tests to verify pass**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml config::tests`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/config.rs connectors/transform-validate/src/main.rs
git commit -m "feat(transform-validate): add rule config parsing and validation"
```

### Task 3: Implement Batch Rule Engine and Policy Handling

**Files:**
- Modify: `connectors/transform-validate/src/transform.rs`
- Modify: `connectors/transform-validate/src/main.rs`
- Test: `connectors/transform-validate/src/transform.rs` (unit tests module)

**Step 1: Write failing unit tests for rule evaluation and policy behavior**

Add tests for:
- not-null failures
- regex failures
- range failures
- unique per-batch failures including duplicate null
- row failure aggregation includes multiple reasons
- invalid index partitioning leaves valid rows only

**Step 2: Run tests to verify failure**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml transform::tests`
Expected: FAIL on unimplemented evaluation/policy logic.

**Step 3: Implement runtime evaluation loop**

Implement:
- batch decoding loop with `ctx.next_batch`
- per-row `RuleFailure` collection
- valid/invalid row partitioning (Arrow row filtering)
- policy handling:
  - `fail` => `ConnectorError::data("VALIDATION_FAILED", ... )`
  - `skip` => drop invalid rows
  - `dlq` => `ctx.emit_dlq_record(...)` per invalid row with concatenated reasons
- accurate `TransformSummary` accounting

**Step 4: Run tests to verify pass**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml`
Expected: PASS for connector unit tests.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/transform.rs connectors/transform-validate/src/main.rs
git commit -m "feat(transform-validate): enforce rules with fail/skip/dlq policies"
```

### Task 4: Add E2E Coverage for Pipeline Integration

**Files:**
- Modify: `tests/e2e/src/harness/mod.rs`
- Modify: `tests/e2e/tests/transform.rs`
- Create: `tests/e2e/tests/snapshots/transform__validate_transform_skips_invalid_rows.snap`

**Step 1: Write failing e2e tests**

Add e2e scenarios:
- `on_data_error: fail` returns error
- `on_data_error: skip` writes only valid rows and snapshot matches
- `on_data_error: dlq` run succeeds and valid-row write count matches expected

Add helper YAML renderer for `transform-validate` in harness.

**Step 2: Run e2e tests to verify failure**

Run: `cargo test -p rapidbyte-e2e --test transform -- --nocapture`
Expected: FAIL on missing behavior/snapshots before implementation finalized.

**Step 3: Finalize integration details**

Ensure transform connector reference, config payload, and policy interactions are correct in generated pipeline YAML.

**Step 4: Run e2e tests to verify pass**

Run: `cargo test -p rapidbyte-e2e --test transform -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/mod.rs tests/e2e/tests/transform.rs tests/e2e/tests/snapshots
git commit -m "test(e2e): cover transform-validate fail skip dlq scenarios"
```

### Task 5: Full Verification and Documentation Updates

**Files:**
- Modify: `IDEA.md` (transform-validate status if needed)
- Modify: `docs/PROTOCOL.md` (if connector list/examples need mention)

**Step 1: Run format and lint**

Run: `just fmt && just lint`
Expected: PASS.

**Step 2: Run host workspace tests**

Run: `just test`
Expected: PASS.

**Step 3: Run connector-specific tests**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml`
Expected: PASS.

**Step 4: Update docs to reflect implementation status**

Move `transform-validate` from planned to implemented where appropriate.

**Step 5: Commit**

```bash
git add IDEA.md docs/PROTOCOL.md
git commit -m "docs: document transform-validate as implemented"
```
