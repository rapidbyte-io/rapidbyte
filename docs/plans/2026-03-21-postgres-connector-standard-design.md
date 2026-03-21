# PostgreSQL Connector Standard Design

**Date:** 2026-03-21
**Status:** Approved
**Scope:** Clean-cut refactor of Rapidbyte PostgreSQL source and destination connectors toward Estuary-grade operational behavior, while remaining native to the Rapidbyte WASM/Rust platform.

## Goal

Define a PostgreSQL connector standard that makes `source-postgres` and
`dest-postgres` operationally rigorous, internally consistent, and suitable as
the template for future Rapidbyte connectors.

The first priority is PostgreSQL operational behavior, not package cosmetics.
Package layout and DX should support the operational model, not drive it.

## Design Principles

1. **PostgreSQL operational behavior first.**
   The design should prioritize prerequisites, discovery correctness, CDC and
   backfill safety, schema evolution, validate/apply discipline, and recovery
   behavior.

2. **Near-parity with Estuary safeguards.**
   Estuary PostgreSQL connectors are treated as the operational reference
   baseline. Rapidbyte should match their safeguards and correctness checks
   unless the WASM/Rust platform gives a strong reason to do something else.

3. **Rapidbyte-native implementation.**
   We should not clone Estuary's Go APIs or connector boilerplate directly.
   Rapidbyte should express the same operational discipline through the v7 WIT
   lifecycle, Rust SDK traits, typed manifests, typed stream metadata, and
   WASM runtime.

4. **Flat by default.**
   Connector packages should stay shallow and easy to scan, closer to Estuary's
   production connectors. Subdirectories are justified only for genuinely dense
   technical areas or support assets.

5. **Clean cut, not backward compatibility.**
   Existing PostgreSQL plugin behavior and config are not treated as contracts.
   The refactor is free to change config shape and lifecycle expectations if
   that produces a better operational connector.

## Reference Comparison

### Estuary Strengths

- Explicit prerequisite checks for version, replication settings, roles, slot
  state, publication state, and operational setup.
- Discovery logic with primary key handling, secondary-index fallback,
  generated-column awareness, and publication-aware filtering.
- Production-grade CDC and backfill semantics with stronger recovery and
  diagnostics.
- More disciplined validate/apply behavior in materialization connectors.
- Strong connector-local tests for operational behavior.
- Flat package layout with clear lifecycle-oriented top-level files.

### Current Rapidbyte Strengths

- Strong typed lifecycle via WIT v7 (`spec`, `prerequisites`, `discover`,
  `validate`, `apply`, `run`, `teardown`).
- Typed stream, schema, policy, checkpoint, and error contracts.
- Clear Rust SDK macros and manifest-driven feature declaration.
- Destination side already has useful schema drift and checkpoint machinery.

### Current Rapidbyte Gaps

#### Source PostgreSQL

- No meaningful `prerequisites()` implementation.
- Discovery is limited and does not model production operational concerns.
- CDC path is less hardened than Estuary's long-lived replication flow.
- Recovery and operator diagnostics are too thin.

#### Destination PostgreSQL

- `validate`, `apply`, and runtime write responsibilities are not sharply
  separated.
- Migration and compatibility behavior are not yet defined and tested as
  explicitly as Estuary's materialization stack.
- Connector-local validation and SQL-generation test coverage is narrower than
  desired for a production PostgreSQL destination.

## Standard Package Shape

The standard is flat and lifecycle-oriented.

### Source PostgreSQL

Expected top-level source files:

- `main.rs`
- `config.rs`
- `spec.rs`
- `prerequisites.rs`
- `discovery.rs`
- `validate.rs`
- `read.rs`
- `query.rs`
- `cdc.rs`
- `client.rs`
- `metrics.rs`
- `diagnostics.rs`
- `types.rs`

### Destination PostgreSQL

Expected top-level source files:

- `main.rs`
- `config.rs`
- `spec.rs`
- `prerequisites.rs`
- `validate.rs`
- `apply.rs`
- `write.rs`
- `session.rs`
- `contract.rs`
- `drift.rs`
- `client.rs`
- `metrics.rs`
- `diagnostics.rs`
- `types.rs`

Subdirectories should only remain where they clearly improve locality, such as
support assets or a genuinely multi-file subsystem.

## Operational Standard

### Source PostgreSQL Standard

The source connector should explicitly support:

- prerequisite validation for:
  - minimum PostgreSQL version
  - `wal_level=logical`
  - replication privileges or provider-equivalent role membership
  - publication existence and visibility
  - replication slot state and mismatch checks
  - per-table readability checks where applicable
- richer discovery:
  - schema selection
  - primary key detection
  - generated-column handling
  - publication-aware visibility
  - better cursor-field suggestions
  - stream metadata suitable for operator-facing tooling
- hardened incremental/backfill behavior:
  - explicit cursor semantics
  - predictable resume behavior
  - diagnostics around invalid resume state
- hardened CDC behavior:
  - replication/publication sanity checks
  - destructive-read safety diagnostics
  - clear operator guidance when checkpoint failure implies risk
  - more explicit mismatch handling for slot state and resume state

### Destination PostgreSQL Standard

The destination connector should explicitly support:

- prerequisite validation for:
  - connectivity
  - target schema accessibility
  - required write and DDL capabilities
- validation semantics:
  - explicit compatibility checks
  - consistent field requirement output
  - clear incompatibility reasons for check-time failures
- apply semantics:
  - pre-run structural setup belongs in `apply()`
  - runtime `write()` should focus on data movement, not schema discovery
  - apply should be idempotent and testable
- write semantics:
  - append, replace, and upsert behavior should be explicit
  - checkpoint and watermark behavior should be intentional and documented
- schema evolution semantics:
  - drift detection should be explicit
  - policy application should be test-driven
  - migration safety should be validated outside the hot data path

## Testing Standard

The PostgreSQL standard should require a stronger test pyramid.

### Source Tests

- unit tests for config, query building, cursor handling, and discovery mapping
- integration tests for prerequisites, discovery, incremental read, CDC, and
  recovery conditions
- snapshot-style tests where output shape or diagnostics matter
- conformance coverage for partitioned read and CDC features where feasible

### Destination Tests

- unit tests for config, contract mapping, schema drift, and write-policy logic
- integration tests for validate/apply behavior and migration safety
- SQL-generation and DDL behavior tests
- checkpoint and resume-related write tests where feasible

## Refactor Phasing

### Phase 1: Source Operational Parity

- add `prerequisites.rs`
- deepen `discovery.rs`
- harden CDC diagnostics and recovery handling

### Phase 2: Destination Operational Parity

- add `prerequisites.rs`
- separate `validate.rs` and `apply.rs`
- expand migration and compatibility test coverage

### Phase 3: Standardization and Reuse

- use the refactored PostgreSQL connectors as the canonical standard
- extract only the abstractions justified by the Postgres implementation
- then propagate the standard to other connectors

## Non-Goals

- preserving backward compatibility with current PostgreSQL plugin config or
  behavior
- generic connector boilerplate extraction before PostgreSQL proves the shape
- repo-wide mass refactor before the PostgreSQL standard is validated

## Result

The intended end state is:

- Estuary-grade PostgreSQL operational discipline
- a flat connector layout that is easy to understand
- a Rapidbyte-native implementation that aligns with the v7 plugin lifecycle
- PostgreSQL source and destination plugins that serve as the reference design
  for future Rapidbyte connectors
