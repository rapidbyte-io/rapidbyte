# Runtime Autotune Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship default-on in-run autotuning for parallelism, postgres partition mode, and COPY flush bytes, while honoring manual pins and removing superseded legacy logic.

**Architecture:** Introduce a probe-then-steady `AutotuneController` in `rapidbyte-engine` that chooses runtime overrides per stream execution. Plumb overrides through `StreamContext` into source/destination connectors, with strict guardrails and deterministic fallback to baseline values on controller failure. Replace obsolete heuristic branches after parity verification.

**Tech Stack:** Rust workspace (`rapidbyte-engine`, `rapidbyte-types`, `connectors/source-postgres`, `connectors/dest-postgres`), existing benchmark harness (`tests/bench`), cargo test, e2e tests.

---

### Task 1: Add Config Surface For Autotune + Manual Pins

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/types.rs`
- Modify: `crates/rapidbyte-engine/src/config/validator.rs`
- Modify: `crates/rapidbyte-engine/src/config/parser.rs` (if needed for defaulting behavior)
- Test: `crates/rapidbyte-engine/src/config/types.rs` (existing tests section)
- Test: `crates/rapidbyte-engine/src/config/validator.rs` (existing tests section)

**Step 1: Write failing config tests**

Add tests for:
- `resources.autotune` defaulting to `true`
- manual pin parsing for autotune knobs
- invalid pin values rejected by validator

Example skeleton:
```rust
#[test]
fn test_autotune_defaults_to_true() {
    let config = parse_yaml(minimal_yaml()).unwrap();
    assert!(config.resources.autotune.enabled);
}

#[test]
fn test_autotune_manual_pins_parse() {
    let yaml = r#"... resources: { autotune: { enabled: true, pin_parallelism: 8 } } ..."#;
    let config = parse_yaml(yaml).unwrap();
    assert_eq!(config.resources.autotune.pin_parallelism, Some(8));
}
```

**Step 2: Run targeted tests and verify failure**

Run: `cargo test -p rapidbyte-engine config::types::tests::test_autotune_defaults_to_true`
Expected: FAIL (fields not found yet)

**Step 3: Implement config structs and defaults**

Add `AutotuneConfig` (enabled + optional pins) under resources and wire defaults.

**Step 4: Implement validator rules**

Validate pin ranges and incompatible combinations.

**Step 5: Run config tests**

Run: `cargo test -p rapidbyte-engine config::types::tests config::validator::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-engine/src/config/types.rs crates/rapidbyte-engine/src/config/validator.rs crates/rapidbyte-engine/src/config/parser.rs
git commit -m "engine/config: add default-on autotune config with manual pins"
```

### Task 2: Add Runtime Override Fields To Stream Context

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Test: `crates/rapidbyte-types/src/stream.rs` tests
- Test: `crates/rapidbyte-engine/src/orchestrator.rs` tests

**Step 1: Write failing type/serialization tests**

Add tests for new fields:
- partition strategy override enum
- copy flush bytes override
- effective parallelism metadata

**Step 2: Run tests to verify failure**

Run: `cargo test -p rapidbyte-types stream::tests`
Expected: FAIL

**Step 3: Implement StreamContext extensions**

Add explicit optional override fields (keep backward compatible defaults).

**Step 4: Update orchestrator stream context constructors**

Ensure all builders initialize new fields explicitly.

**Step 5: Run tests**

Run: `cargo test -p rapidbyte-types && cargo test -p rapidbyte-engine orchestrator::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/stream.rs crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "types/engine: add stream runtime override fields for autotune"
```

### Task 3: Implement Autotune Controller Core (Probe -> Steady)

**Files:**
- Create: `crates/rapidbyte-engine/src/autotune.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (module export)
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` (controller integration)
- Test: `crates/rapidbyte-engine/src/autotune.rs` (unit tests in-module)

**Step 1: Write failing unit tests for decision logic**

Cover:
- candidate generation/clamping
- skip logic for ineligible streams
- score ranking and tie-break
- manual pin precedence over tuner output

**Step 2: Run tests and verify failure**

Run: `cargo test -p rapidbyte-engine autotune::tests`
Expected: FAIL (module missing)

**Step 3: Implement controller data model**

Add:
- candidate structs
- window metrics input
- objective scoring
- final decision struct

**Step 4: Integrate with orchestrator execution flow**

Apply probe budget, run candidate windows, choose winner, switch to steady mode.

**Step 5: Emit structured decision logs/metrics**

Add `autotune-decision` and `autotune-skipped` events.

**Step 6: Run tests**

Run: `cargo test -p rapidbyte-engine autotune::tests orchestrator::tests`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/rapidbyte-engine/src/autotune.rs crates/rapidbyte-engine/src/lib.rs crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "engine: add probe/steady autotune controller"
```

### Task 4: Source Postgres Override Plumbing (Partition Mode)

**Files:**
- Modify: `connectors/source-postgres/src/reader.rs`
- Modify: `connectors/source-postgres/src/query.rs`
- Test: `connectors/source-postgres/src/query.rs` tests
- Test: `connectors/source-postgres/src/reader.rs` tests (add if missing)

**Step 1: Write failing tests for override precedence**

Test order:
- StreamContext override wins
- manual config pin wins over autotune
- env fallback only when no override

**Step 2: Run tests and verify failure**

Run: `cd connectors/source-postgres && cargo test query::tests::build_base_query_full_refresh_partitioned_by_id`
Expected: FAIL for new cases

**Step 3: Implement override-driven partition mode selection**

Replace env-only selection path with context override + fallback.

**Step 4: Run connector tests**

Run: `cd connectors/source-postgres && cargo test`
Expected: PASS

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/reader.rs connectors/source-postgres/src/query.rs
git commit -m "source-postgres: support stream override partition strategy for autotune"
```

### Task 5: Destination Postgres Override Plumbing (COPY Flush Bytes)

**Files:**
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/config.rs` (if pin plumbing needs explicit field docs)
- Test: `connectors/dest-postgres/src/writer.rs` tests

**Step 1: Write failing tests**

Cover:
- stream override flush bytes applied
- manual pin prevents autotune modification
- clamp behavior to hard guardrail bounds

**Step 2: Run tests and verify failure**

Run: `cd connectors/dest-postgres && cargo test writer::tests::adaptive_copy_flush_uses_large_for_large_rows`
Expected: FAIL for new override behavior

**Step 3: Implement override precedence in write path**

Use runtime override first, then pinned config, then adaptive fallback.

**Step 4: Run connector tests**

Run: `cd connectors/dest-postgres && cargo test`
Expected: PASS

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/writer.rs connectors/dest-postgres/src/config.rs
git commit -m "dest-postgres: apply runtime copy flush override with guardrails"
```

### Task 6: End-to-End Integration Tests For Autotune Behavior

**Files:**
- Modify: `tests/e2e/src/harness/mod.rs`
- Create or Modify: `tests/e2e/tests/autotune.rs`
- Modify: `tests/e2e/tests/mode_matrix.rs` (if needed for shared helpers)

**Step 1: Write failing e2e tests**

Cases:
- autotune enabled (default) improves or maintains baseline throughput envelope in benchmark-like run
- manual pin respected
- output equivalence with autotune on/off

**Step 2: Run failing tests**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml --test autotune -- --nocapture`
Expected: FAIL

**Step 3: Implement harness support for autotune/pins**

Add pipeline rendering helpers for autotune settings.

**Step 4: Run e2e tests**

Run: `just e2e`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/mod.rs tests/e2e/tests/autotune.rs tests/e2e/tests/mode_matrix.rs
git commit -m "e2e: add autotune behavior and manual pin precedence coverage"
```

### Task 7: Remove Superseded Legacy Paths (No Tech Debt)

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `connectors/source-postgres/src/reader.rs`
- Modify: `connectors/dest-postgres/src/writer.rs`
- Modify: any now-obsolete helper modules discovered during implementation

**Step 1: Identify obsolete branches**

List dead/duplicated decision paths introduced by migration.

**Step 2: Delete obsolete code and update tests**

Keep one canonical decision flow per tuned knob.

**Step 3: Run full test matrix**

Run:
- `cargo test --workspace`
- `just e2e`

Expected: PASS

**Step 4: Commit cleanup**

```bash
git add -A
git commit -m "refactor: remove superseded pre-autotune decision paths"
```

### Task 8: Benchmark Validation And Docs

**Files:**
- Modify: `tests/bench/src/main.rs` (if automation helpers for tuning sweeps are added)
- Modify: `docs/PROTOCOL.md` (if runtime override semantics are documented there)
- Modify: `CLAUDE.md` or project docs for operational knobs
- Create: `docs/plans/2026-02-28-autotune-performance-results.md`

**Step 1: Run benchmark validation matrix**

Run (minimum):
- `just bench postgres --profile small --iters 3`
- `just bench postgres --profile medium --iters 3`
- `just bench postgres --profile large --iters 3`

Capture baseline vs autotune-enabled and pinned-control runs.

**Step 2: Write results report**

Include:
- throughput deltas
- probe overhead
- selected decisions by profile
- regressions and mitigations

**Step 3: Final verification**

Run:
- `just fmt`
- `just lint`
- `cargo test --workspace`
- `just e2e`

Expected: PASS

**Step 4: Commit docs/report**

```bash
git add tests/bench/src/main.rs docs/PROTOCOL.md CLAUDE.md docs/plans/2026-02-28-autotune-performance-results.md
git commit -m "docs/bench: validate and document default-on autotune behavior"
```

## Final Completion Checklist

- [ ] Autotune default-on verified.
- [ ] Manual pins verified to override tuner decisions.
- [ ] Superseded logic removed (single canonical path).
- [ ] Bench and e2e validation completed.
- [ ] Docs updated for users/operators.
