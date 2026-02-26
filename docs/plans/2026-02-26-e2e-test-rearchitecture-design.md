# E2E Test Rearchitecture Design (Rust-Native, No Legacy Path)

Date: 2026-02-26
Owner: rapidbyte engineering
Status: Approved

## Goals

1. Replace slow Bash + `docker-compose` E2E orchestration with a Rust-native harness.
2. Eliminate legacy test paths entirely (no backward compatibility layer).
3. Make E2E tests fast enough to run routinely in local workflows and CI.
4. Improve confidence by expanding coverage via property tests and component-boundary tests.

## Non-Goals

1. Preserving old `tests/e2e.sh` scenario interfaces.
2. Maintaining shell-based scenario assets under `tests/connectors/postgres/`.
3. Adding adapter code to support both old and new E2E systems simultaneously.

## Current Problems

1. E2E tests are orchestrated by shell scripts and `docker-compose`, adding startup and process overhead.
2. Assertions rely on external CLI execution patterns rather than native DB clients.
3. Connector WASM builds happen in script flows and are not optimized for suite-level reuse.
4. Many critical integration tests are `#[ignore]` and depend on manual environment setup.

## Architecture Decision Summary

1. Canonical E2E location: `tests/e2e/`.
2. E2E runner implementation: Rust crate-like test harness (`tests/e2e/Cargo.toml`).
3. Container strategy: one shared Postgres container per test process.
4. Isolation strategy: unique schemas per test (`src_<test>_<nonce>`, `dst_<test>_<nonce>`).
5. DB setup and assertions: `tokio-postgres` only.
6. Legacy handling: remove old shell/docker-compose test system.

## New Testing Pyramid

### 1) Unit + Property Tests (ms)

Scope:
- Arrow encoding/decoding invariants.
- Config parsing and validation boundaries.
- Policy combinator behavior (schema evolution, error policy, limits).

Approach:
- Keep pure unit tests in owning crates.
- Add `proptest` generators for policy combinations and boundary values.
- Assert no panics and typed error behavior for invalid configurations.

### 2) Component Boundary Tests (sub-second)

Scope:
- Host/WASM boundary behavior.
- Frame lifecycle constraints.
- Yield/`WouldBlock`/partial write handling in host imports.

Approach:
- Add focused tests around runtime + SDK host boundary modules.
- Use stubs/mocks for transport and host callbacks.

### 3) Rust-Native E2E (seconds)

Scope:
- Real Postgres -> source connector -> engine/runtime -> destination connector.

Approach:
- `testcontainers` starts Postgres lazily once.
- Per-test schema creation, seed, pipeline execution, assertion, cleanup.
- `rstest` parameterizes mode matrices (sync/write/compression/policy).
- `insta` snapshot asserts complex transformed outputs and drift behavior.

## Detailed E2E Runtime Flow

1. Bootstrap shared Postgres container and admin client once.
2. Ensure connector WASM artifacts exist in `target/connectors`.
3. For each test:
   - Allocate source/destination schemas.
   - Seed source data via native SQL.
   - Materialize a test-specific pipeline config from templates.
   - Run `rapidbyte_engine::orchestrator::run_pipeline` directly.
   - Assert counts/outcomes and destination data with native queries.
   - Drop schemas.
4. Keep container alive for full process lifetime.

## Proposed File Layout

```text
tests/
  e2e/
    Cargo.toml
    fixtures/
      pipelines/
        full_refresh.yaml
        incremental.yaml
        cdc.yaml
        transform.yaml
    src/
      lib.rs
      harness/
        container.rs
        db.rs
        connectors.rs
        pipeline.rs
        fixtures.rs
    tests/
      full_refresh.rs
      incremental.rs
      cdc.rs
      transform.rs
      policies.rs
```

## One-Time Connector Build Strategy

1. Harness checks for:
   - `target/connectors/source_postgres.wasm`
   - `target/connectors/dest_postgres.wasm`
   - `target/connectors/transform_sql.wasm`
2. If missing, run controlled builds from Rust using `cargo build` in each connector directory.
3. Copy artifacts to `target/connectors` and set `RAPIDBYTE_CONNECTOR_DIR` in-process.
4. Cache completion in a process-global `OnceCell` to avoid repeated work.

## Migration and Deletion Plan

### Remove

1. `tests/e2e.sh`
2. `tests/connectors/postgres/` (all scripts and docker-compose scenarios)
3. Legacy fixture files only referenced by removed shell scenarios

### Add/Update

1. Add Rust E2E harness under `tests/e2e/`.
2. Repoint `just e2e` to `cargo test --manifest-path tests/e2e/Cargo.toml`.
3. Keep `just build-connectors` as optional prebuild path, but E2E harness remains self-sufficient.

## Coverage Matrix Mapping

Primary matrix dimensions for parameterized E2E tests:

1. Sync mode: `full_refresh`, `incremental`, `cdc`
2. Destination write mode: `append`, `upsert`, `replace`
3. Compression: `none`, `lz4`, `zstd`
4. Policy axis:
   - schema evolution policy variants
   - data error policy variants

Not every Cartesian product should run in default profile. Default profile covers risk-dense paths; full matrix can run in CI nightly profile.

## CI and Local Workflow

1. Local default: `just e2e` runs fast profile with shared container + representative matrix.
2. CI PR gate: same fast profile.
3. CI nightly: expanded matrix and higher property-test case counts.

## Risks and Mitigations

1. Risk: shared container cross-test contamination.
   - Mitigation: strict per-test schema namespace + guaranteed schema drop in teardown.
2. Risk: connector build race under parallel test start.
   - Mitigation: global build lock/`OnceCell` guard around build step.
3. Risk: CDC tests may require stricter isolation than shared container.
   - Mitigation: optional isolated module path for CDC stress tests if flakiness appears.

## Success Criteria

1. No shell/docker-compose code remains in test execution path.
2. E2E suite is runnable via one Rust command path.
3. Core pipeline modes are covered by native assertions and pass reliably in parallel.
4. Property-based and component-boundary tests exist for key policy/runtime invariants.

## Rollout Phases

1. Phase 1: create Rust harness skeleton and one green full-refresh E2E.
2. Phase 2: migrate incremental/upsert/replace and transform scenarios.
3. Phase 3: migrate CDC tests and policy matrix coverage.
4. Phase 4: delete legacy shell/docker-compose assets and update docs/just commands.
