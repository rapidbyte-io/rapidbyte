# PostgreSQL Connector Standard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor Rapidbyte PostgreSQL source and destination connectors into a clean-cut, flat, production-grade standard with Estuary-like operational safeguards and test coverage.

**Architecture:** Start with PostgreSQL operational behavior, not package cosmetics. Refactor `source-postgres` first to add explicit prerequisites, richer discovery, and stronger CDC diagnostics; then refactor `dest-postgres` to separate prerequisites, validate, apply, and write responsibilities, backed by stronger migration and compatibility tests. Only after the PostgreSQL behavior is correct should the resulting shape be treated as the standard for future connectors.

**Tech Stack:** Rust, Tokio, `tokio-postgres`, Rapidbyte SDK/WIT v7, Cargo tests, local PostgreSQL integration tests

---

### Task 1: Freeze The Current PostgreSQL Baseline

**Files:**
- Modify: `docs/plans/2026-03-21-postgres-connector-standard-design.md`
- Verify: `plugins/sources/postgres/Cargo.toml`
- Verify: `plugins/destinations/postgres/Cargo.toml`

**Step 1: Add a short “current baseline” appendix to the design doc**

Record the current source and destination operational behavior that will be intentionally replaced:
- source lacks meaningful `prerequisites()`
- source discovery is `public`-schema `information_schema` only
- source CDC uses `pg_logical_slot_get_binary_changes`
- destination validate/apply/runtime responsibilities are not sharply separated

**Step 2: Run existing PostgreSQL plugin tests**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Expected: current tests pass and provide a before-refactor baseline.

**Step 3: Commit the baseline note**

```bash
git add docs/plans/2026-03-21-postgres-connector-standard-design.md
git commit -m "docs: capture postgres connector baseline"
```

### Task 2: Add Source PostgreSQL Prerequisites Lifecycle

**Files:**
- Create: `plugins/sources/postgres/src/prerequisites.rs`
- Modify: `plugins/sources/postgres/src/main.rs`
- Modify: `plugins/sources/postgres/src/config.rs`
- Test: `plugins/sources/postgres/src/prerequisites.rs`

**Step 1: Write the failing tests**

Add unit tests for:
- rejecting unsupported version metadata
- rejecting non-`logical` WAL level
- rejecting missing replication privilege
- rejecting invalid publication and slot state
- producing a structured `PrerequisitesReport`

**Step 2: Run the source tests to verify the new tests fail**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml prerequisites
```

Expected: FAIL because the prerequisite module and lifecycle wiring do not exist yet.

**Step 3: Implement minimal prerequisite checks**

Implement:
- typed prerequisite helpers in `prerequisites.rs`
- source `prerequisites()` in `main.rs`
- any config validation additions needed for explicit prerequisite behavior

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml prerequisites
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/sources/postgres/src/prerequisites.rs plugins/sources/postgres/src/main.rs plugins/sources/postgres/src/config.rs
git commit -m "feat(source-postgres): add prerequisite checks"
```

### Task 3: Deepen Source Discovery To Production Grade

**Files:**
- Modify: `plugins/sources/postgres/src/discovery.rs`
- Modify: `plugins/sources/postgres/src/types.rs`
- Modify: `plugins/sources/postgres/src/config.rs`
- Test: `plugins/sources/postgres/src/discovery.rs`

**Step 1: Write the failing discovery tests**

Add tests for:
- primary key extraction
- generated-column omission or marking
- non-`public` schema selection
- publication-aware filtering behavior
- cursor-field suggestion behavior

**Step 2: Run the source discovery tests to verify failure**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml discovery
```

Expected: FAIL because the current discovery implementation does not satisfy the new behaviors.

**Step 3: Implement minimal discovery enhancements**

Implement:
- richer catalog queries
- schema selection config
- primary key handling
- generated-column handling
- metadata needed for operator-facing discovery and validation

**Step 4: Run discovery tests again**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml discovery
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/sources/postgres/src/discovery.rs plugins/sources/postgres/src/types.rs plugins/sources/postgres/src/config.rs
git commit -m "feat(source-postgres): harden discovery behavior"
```

### Task 4: Harden Source CDC Diagnostics And Recovery Semantics

**Files:**
- Modify: `plugins/sources/postgres/src/cdc/mod.rs`
- Create: `plugins/sources/postgres/src/diagnostics.rs`
- Modify: `plugins/sources/postgres/src/main.rs`
- Test: `plugins/sources/postgres/src/cdc/mod.rs`

**Step 1: Write the failing CDC tests**

Add tests for:
- slot mismatch diagnostics
- publication mismatch diagnostics
- destructive-read checkpoint failure messaging
- resume ambiguity warnings/errors

**Step 2: Run CDC-focused tests to verify failure**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml cdc
```

Expected: FAIL because current CDC handling does not emit the new diagnostics or stricter checks.

**Step 3: Implement minimal CDC hardening**

Implement:
- reusable diagnostics helpers
- stricter error shaping and guidance
- lifecycle wiring for clearer operator-facing failure messages

**Step 4: Re-run CDC tests**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml cdc
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/sources/postgres/src/cdc/mod.rs plugins/sources/postgres/src/diagnostics.rs plugins/sources/postgres/src/main.rs
git commit -m "feat(source-postgres): improve cdc diagnostics"
```

### Task 5: Separate Destination Prerequisites, Validate, And Apply

**Files:**
- Create: `plugins/destinations/postgres/src/prerequisites.rs`
- Create: `plugins/destinations/postgres/src/validate.rs`
- Create: `plugins/destinations/postgres/src/apply.rs`
- Modify: `plugins/destinations/postgres/src/main.rs`
- Modify: `plugins/destinations/postgres/src/contract.rs`
- Modify: `plugins/destinations/postgres/src/config.rs`
- Test: `plugins/destinations/postgres/src/main.rs`

**Step 1: Write the failing destination lifecycle tests**

Add tests for:
- prerequisite report behavior
- validate returning stronger compatibility output
- apply owning pre-run structural setup
- write no longer being responsible for avoidable structural checks

**Step 2: Run destination lifecycle tests to verify failure**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml validate
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml apply
```

Expected: FAIL because the lifecycle is not yet split this way.

**Step 3: Implement the lifecycle split**

Implement:
- `prerequisites.rs` for connectivity and capability checks
- `validate.rs` for compatibility logic
- `apply.rs` for explicit pre-run schema/table preparation
- `main.rs` wiring for `prerequisites()`, `validate()`, and `apply()`

**Step 4: Re-run destination lifecycle tests**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml validate
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml apply
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/destinations/postgres/src/prerequisites.rs plugins/destinations/postgres/src/validate.rs plugins/destinations/postgres/src/apply.rs plugins/destinations/postgres/src/main.rs plugins/destinations/postgres/src/contract.rs plugins/destinations/postgres/src/config.rs
git commit -m "feat(dest-postgres): split validate and apply lifecycle"
```

### Task 6: Expand Destination Migration And Drift Safety Tests

**Files:**
- Modify: `plugins/destinations/postgres/src/ddl/drift.rs`
- Modify: `plugins/destinations/postgres/src/writer.rs`
- Modify: `plugins/destinations/postgres/src/session.rs`
- Create: `plugins/destinations/postgres/src/diagnostics.rs`
- Test: `plugins/destinations/postgres/src/ddl/drift.rs`
- Test: `plugins/destinations/postgres/src/writer.rs`

**Step 1: Write the failing tests**

Add tests for:
- new column / removed column / type change / nullability policy outcomes
- replace/append/upsert setup behavior
- checkpoint and watermark safety messaging
- migration-safe behavior when schema drift is detected

**Step 2: Run destination drift tests to verify failure**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml drift
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml writer
```

Expected: FAIL because the stricter safety and diagnostics behavior is not fully implemented.

**Step 3: Implement minimal safety improvements**

Implement:
- explicit diagnostics helpers
- drift policy behavior tightened where needed
- write/session error shaping aligned with the new operational contract

**Step 4: Re-run the drift and writer tests**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml drift
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml writer
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/destinations/postgres/src/ddl/drift.rs plugins/destinations/postgres/src/writer.rs plugins/destinations/postgres/src/session.rs plugins/destinations/postgres/src/diagnostics.rs
git commit -m "feat(dest-postgres): harden migration and drift safety"
```

### Task 7: Add Connector-Level Operational Docs And Package Standard

**Files:**
- Create: `plugins/sources/postgres/README.md`
- Create: `plugins/destinations/postgres/README.md`
- Create: `plugins/sources/postgres/CHANGELOG.md`
- Create: `plugins/destinations/postgres/CHANGELOG.md`
- Optionally Create: `plugins/sources/postgres/docker-compose.yaml`
- Optionally Create: `plugins/destinations/postgres/docker-compose.yaml`

**Step 1: Write the missing package docs**

Document for each connector:
- purpose
- supported modes/features
- prerequisites
- example config
- local test commands
- operational caveats

**Step 2: Verify docs match actual lifecycle and config**

Run:

```bash
rg -n "prerequisites|apply|cdc|schema evolution|checkpoint|watermark" plugins/sources/postgres plugins/destinations/postgres
```

Expected: documentation claims line up with real code paths and terminology.

**Step 3: Commit**

```bash
git add plugins/sources/postgres/README.md plugins/destinations/postgres/README.md plugins/sources/postgres/CHANGELOG.md plugins/destinations/postgres/CHANGELOG.md
git commit -m "docs: add postgres connector package docs"
```

### Task 8: Full Verification And Standard Lock-In

**Files:**
- Verify: `plugins/sources/postgres`
- Verify: `plugins/destinations/postgres`
- Verify: `docs/plans/2026-03-21-postgres-connector-standard-design.md`

**Step 1: Run the full PostgreSQL plugin test suites**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Expected: PASS

**Step 2: Run workspace verification most likely to catch integration issues**

Run:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS

**Step 3: Re-read the design and check actual outcomes**

Confirm the implementation now provides:
- explicit PostgreSQL prerequisites
- richer discovery
- hardened CDC diagnostics
- explicit destination validate/apply split
- stronger migration safety tests

**Step 4: Commit the final standardization pass**

```bash
git add plugins/sources/postgres plugins/destinations/postgres docs/plans/2026-03-21-postgres-connector-standard-design.md docs/plans/2026-03-21-postgres-connector-standard.md
git commit -m "refactor(postgres): standardize connector operations"
```
