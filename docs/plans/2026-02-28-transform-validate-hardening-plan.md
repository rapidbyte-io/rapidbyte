# Transform Validate Hardening Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `transform-validate` production-ready by hardening config schema validation, extending numeric range support to decimal Arrow types, and emitting labeled validation metrics.

**Architecture:** Keep the existing connector architecture and strengthen internals: tighten JSON schema shape constraints in `config.rs`, add decimal-aware range comparison and stable metric emissions in `transform.rs`, and verify behavior via focused unit tests plus e2e regressions. Preserve existing fail/skip/dlq behavior and all-failure-per-row reporting.

**Tech Stack:** Rust, Arrow (`arrow`), `rapidbyte-sdk`, `regex`, existing e2e harness.

---

### Task 1: Harden Config Schema and Validation Tests

**Files:**
- Modify: `connectors/transform-validate/src/config.rs`

**Step 1: Write failing tests for schema/shape constraints**

Add tests that should fail currently for:
- unknown properties in rule objects
- empty rules arrays
- malformed shorthand forms rejected

**Step 2: Run tests to verify failure**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml config::tests`
Expected: FAIL for new schema-shape expectations.

**Step 3: Implement strict Draft-07 schema**

Replace permissive schema string with richer schema including:
- `rules` minItems
- per-rule oneOf forms
- `additionalProperties: false` where applicable
- field/pattern requirements

**Step 4: Run tests to verify pass**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml config::tests`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/config.rs
git commit -m "feat(transform-validate): harden config schema and shape validation"
```

### Task 2: Add Decimal Range Support with Unit Tests

**Files:**
- Modify: `connectors/transform-validate/src/transform.rs`

**Step 1: Write failing decimal range tests**

Add tests for `assert_range` with:
- `Decimal128` in-range and out-of-range values
- `Decimal256` in-range and out-of-range values

**Step 2: Run tests to verify failure**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml transform::tests`
Expected: FAIL due to unsupported decimal numeric parsing.

**Step 3: Implement decimal-aware numeric extraction**

Add decimal handling in range evaluation, respecting precision/scale comparisons and preserving existing behavior for unsupported types.

**Step 4: Run tests to verify pass**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml transform::tests`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/transform.rs
git commit -m "feat(transform-validate): support decimal range assertions"
```

### Task 3: Emit Validation Metrics with Labels

**Files:**
- Modify: `connectors/transform-validate/src/transform.rs`

**Step 1: Write failing tests for metric shape generation**

Add unit tests for metric helper output ensuring:
- `validation_failures_total` emits `rule` + `field` labels
- aggregate valid/invalid row counters are produced

**Step 2: Run tests to verify failure**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml transform::tests`
Expected: FAIL because metrics aren’t emitted yet.

**Step 3: Implement metrics emission**

Emit metrics once per batch:
- `validation_failures_total` (counter) with `rule` and `field` labels
- `validation_rows_valid_total` and `validation_rows_invalid_total`

**Step 4: Run tests to verify pass**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/transform-validate/src/transform.rs
git commit -m "feat(transform-validate): emit per-rule labeled validation metrics"
```

### Task 4: Regression and E2E Verification

**Files:**
- Modify (if needed): `tests/e2e/tests/transform.rs`

**Step 1: Run targeted e2e transform suite**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml --test transform`
Expected: PASS.

**Step 2: Run connector test suite**

Run: `cargo test --manifest-path connectors/transform-validate/Cargo.toml`
Expected: PASS.

**Step 3: Run engine regression suite**

Run: `cargo test -p rapidbyte-engine`
Expected: PASS.

**Step 4: Format**

Run: `just fmt`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/transform-validate tests/e2e/tests/transform.rs
git commit -m "test: harden transform-validate regressions for production readiness"
```
