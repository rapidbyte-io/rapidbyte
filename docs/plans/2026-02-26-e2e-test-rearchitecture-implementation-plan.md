# Rust-Native E2E Test Rearchitecture Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace shell/docker-compose E2E tests with a Rust-native harness under `tests/e2e/`, with one shared Postgres testcontainer, per-test schema isolation, and native SQL assertions.

**Architecture:** Build a dedicated E2E harness crate in `tests/e2e/` that owns container lifecycle, connector artifact resolution/build, fixture materialization, and assertions. Keep unit/property/component tests near owning crates and migrate integration coverage from ignored/manual flows to executable Rust tests. Remove all legacy shell scenario assets and repoint `just e2e` to the Rust harness.

**Tech Stack:** Rust (`tokio`, `testcontainers`, `tokio-postgres`, `rstest`, `serde_yaml`, `insta`, `proptest`), existing Rapidbyte engine/runtime crates.

---

### Task 1: Scaffold New Rust E2E Harness Crate

**Files:**
- Create: `tests/e2e/Cargo.toml`
- Create: `tests/e2e/src/lib.rs`
- Create: `tests/e2e/src/harness/mod.rs`
- Create: `tests/e2e/tests/smoke.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn harness_smoke_bootstrap() {
    let ctx = rapidbyte_e2e::harness::bootstrap().await.unwrap();
    assert!(ctx.postgres_port > 0);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml smoke::harness_smoke_bootstrap -q`
Expected: FAIL with unresolved module/crate symbol errors.

**Step 3: Write minimal implementation**

```rust
pub struct HarnessContext { pub postgres_port: u16 }
pub async fn bootstrap() -> anyhow::Result<HarnessContext> {
    Ok(HarnessContext { postgres_port: 1 })
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml smoke::harness_smoke_bootstrap -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e
git commit -m "test(e2e): scaffold rust-native harness crate"
```

### Task 2: Implement Shared Postgres Container Bootstrap

**Files:**
- Create: `tests/e2e/src/harness/container.rs`
- Modify: `tests/e2e/src/harness/mod.rs`
- Modify: `tests/e2e/tests/smoke.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn shared_container_is_reused() {
    let a = rapidbyte_e2e::harness::bootstrap().await.unwrap();
    let b = rapidbyte_e2e::harness::bootstrap().await.unwrap();
    assert_eq!(a.postgres_port, b.postgres_port);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml shared_container_is_reused -q`
Expected: FAIL because bootstrap does not initialize real shared state.

**Step 3: Write minimal implementation**

```rust
static CONTAINER: tokio::sync::OnceCell<ContainerState> = tokio::sync::OnceCell::const_new();
```

Use `testcontainers` Postgres image; store host port in a process-global state.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml shared_container_is_reused -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/container.rs tests/e2e/src/harness/mod.rs tests/e2e/tests/smoke.rs
git commit -m "test(e2e): add shared postgres testcontainer bootstrap"
```

### Task 3: Add Per-Test Schema Isolation Helpers

**Files:**
- Create: `tests/e2e/src/harness/db.rs`
- Modify: `tests/e2e/src/harness/mod.rs`
- Modify: `tests/e2e/tests/smoke.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn allocates_and_drops_unique_schemas() {
    let ctx = rapidbyte_e2e::harness::bootstrap().await.unwrap();
    let a = ctx.allocate_schema_pair("smoke").await.unwrap();
    let b = ctx.allocate_schema_pair("smoke").await.unwrap();
    assert_ne!(a.source_schema, b.source_schema);
    ctx.drop_schema_pair(&a).await.unwrap();
    ctx.drop_schema_pair(&b).await.unwrap();
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml allocates_and_drops_unique_schemas -q`
Expected: FAIL with missing API/schema functions.

**Step 3: Write minimal implementation**

Create helper methods backed by `tokio-postgres`:
- `allocate_schema_pair(test_name)`
- `drop_schema_pair(pair)`

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml allocates_and_drops_unique_schemas -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/db.rs tests/e2e/src/harness/mod.rs tests/e2e/tests/smoke.rs
git commit -m "test(e2e): add per-test schema isolation helpers"
```

### Task 4: Add Connector Artifact Resolver/Builder

**Files:**
- Create: `tests/e2e/src/harness/connectors.rs`
- Modify: `tests/e2e/src/harness/mod.rs`
- Modify: `tests/e2e/tests/smoke.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn connector_dir_is_prepared_once() {
    let a = rapidbyte_e2e::harness::prepare_connectors().await.unwrap();
    let b = rapidbyte_e2e::harness::prepare_connectors().await.unwrap();
    assert_eq!(a, b);
    assert!(a.join("source_postgres.wasm").exists());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml connector_dir_is_prepared_once -q`
Expected: FAIL with missing function/files.

**Step 3: Write minimal implementation**

Implement:
- artifact existence checks in `target/connectors`
- guarded `cargo build` calls for connector crates when missing
- process-global once guard

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml connector_dir_is_prepared_once -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/connectors.rs tests/e2e/src/harness/mod.rs tests/e2e/tests/smoke.rs
git commit -m "test(e2e): add one-time connector artifact preparation"
```

### Task 5: Add Pipeline Fixture Materialization

**Files:**
- Create: `tests/e2e/src/harness/pipeline.rs`
- Create: `tests/e2e/src/harness/fixtures.rs`
- Create: `tests/e2e/fixtures/pipelines/full_refresh.yaml`
- Modify: `tests/e2e/src/harness/mod.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn materialized_pipeline_contains_runtime_values() {
    let rendered = rapidbyte_e2e::harness::render_pipeline_fixture("full_refresh", &params()).unwrap();
    assert!(rendered.contains("source_schema"));
    assert!(rendered.contains("destination_schema"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml materialized_pipeline_contains_runtime_values -q`
Expected: FAIL due to missing fixture rendering APIs.

**Step 3: Write minimal implementation**

Implement placeholder substitution for runtime fields:
- Postgres host/port
- source/destination schema
- connector dir

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml materialized_pipeline_contains_runtime_values -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/src/harness/pipeline.rs tests/e2e/src/harness/fixtures.rs tests/e2e/fixtures/pipelines/full_refresh.yaml tests/e2e/src/harness/mod.rs
git commit -m "test(e2e): add pipeline fixture materialization"
```

### Task 6: Implement First Real E2E (Full Refresh)

**Files:**
- Create: `tests/e2e/tests/full_refresh.rs`
- Modify: `tests/e2e/src/harness/db.rs`
- Modify: `tests/e2e/src/harness/pipeline.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn full_refresh_copies_all_rows() {
    let h = rapidbyte_e2e::harness::bootstrap().await.unwrap();
    let run = h.run_case("full_refresh_users").await.unwrap();
    assert_eq!(run.records_written, 3);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml full_refresh_copies_all_rows -q`
Expected: FAIL until seed + run + assertion flow is implemented.

**Step 3: Write minimal implementation**

Implement helpers to:
- seed source schema
- run orchestrator with rendered config
- query destination row counts

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml full_refresh_copies_all_rows -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/tests/full_refresh.rs tests/e2e/src/harness/db.rs tests/e2e/src/harness/pipeline.rs
git commit -m "test(e2e): add full refresh postgres-to-postgres test"
```

### Task 7: Parameterize Core Mode Matrix with `rstest`

**Files:**
- Create: `tests/e2e/tests/mode_matrix.rs`
- Create: `tests/e2e/fixtures/pipelines/incremental.yaml`
- Modify: `tests/e2e/src/harness/pipeline.rs`

**Step 1: Write the failing test**

```rust
#[rstest]
#[case("full_refresh", "append")]
#[case("incremental", "upsert")]
#[case("full_refresh", "replace")]
#[tokio::test]
async fn mode_matrix_runs(#[case] sync_mode: &str, #[case] write_mode: &str) {
    let result = run_mode_case(sync_mode, write_mode).await.unwrap();
    assert!(result.records_written > 0);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml mode_matrix_runs -q`
Expected: FAIL before matrix fixture mapping exists.

**Step 3: Write minimal implementation**

Implement mode/write mapping and test fixture parameters.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml mode_matrix_runs -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/tests/mode_matrix.rs tests/e2e/fixtures/pipelines/incremental.yaml tests/e2e/src/harness/pipeline.rs
git commit -m "test(e2e): add rstest mode matrix coverage"
```

### Task 8: Add Transform + Snapshot Assertions

**Files:**
- Create: `tests/e2e/tests/transform.rs`
- Create: `tests/e2e/fixtures/pipelines/transform.yaml`
- Create: `tests/e2e/tests/snapshots/transform__*.snap`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn transform_output_matches_snapshot() {
    let rows = run_transform_case().await.unwrap();
    insta::assert_snapshot!(normalize_rows(rows));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml transform_output_matches_snapshot -q`
Expected: FAIL with missing snapshot.

**Step 3: Write minimal implementation**

Implement transform fixture and normalized output serializer.

**Step 4: Run test to verify it passes**

Run: `INSTA_UPDATE=always cargo test --manifest-path tests/e2e/Cargo.toml transform_output_matches_snapshot -q`
Expected: PASS and snapshot generated.

**Step 5: Commit**

```bash
git add tests/e2e/tests/transform.rs tests/e2e/fixtures/pipelines/transform.yaml tests/e2e/tests/snapshots
git commit -m "test(e2e): add transform snapshot assertions"
```

### Task 9: Add CDC E2E Coverage

**Files:**
- Create: `tests/e2e/tests/cdc.rs`
- Create: `tests/e2e/fixtures/pipelines/cdc.yaml`
- Modify: `tests/e2e/src/harness/db.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn cdc_applies_insert_update_delete_sequence() {
    let outcome = run_cdc_case().await.unwrap();
    assert_eq!(outcome.final_count, 2);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml cdc_applies_insert_update_delete_sequence -q`
Expected: FAIL before CDC seed/mutation helpers exist.

**Step 3: Write minimal implementation**

Implement CDC setup helpers and final-state assertions.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path tests/e2e/Cargo.toml cdc_applies_insert_update_delete_sequence -q`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/e2e/tests/cdc.rs tests/e2e/fixtures/pipelines/cdc.yaml tests/e2e/src/harness/db.rs
git commit -m "test(e2e): add cdc integration coverage"
```

### Task 10: Migrate Existing Engine Integration Tests to New Harness or Unit Scope

**Files:**
- Modify: `crates/rapidbyte-engine/tests/pipeline_integration.rs`

**Step 1: Write the failing test update**

Convert ignored manual E2E tests to either:
- harness-driven tests in `tests/e2e/`, or
- non-E2E unit/integration checks local to engine crate.

**Step 2: Run test to verify expected failures**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration -q`
Expected: FAIL until references to removed manual setup are eliminated.

**Step 3: Write minimal implementation**

Remove obsolete ignored/manual assumptions and retain only relevant crate-level integration tests.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine --test pipeline_integration -q`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/tests/pipeline_integration.rs
git commit -m "test(engine): remove legacy manual e2e assumptions"
```

### Task 11: Remove Legacy Shell E2E System

**Files:**
- Delete: `tests/e2e.sh`
- Delete: `tests/connectors/postgres/config.sh`
- Delete: `tests/connectors/postgres/docker-compose.yml`
- Delete: `tests/connectors/postgres/lib.sh`
- Delete: `tests/connectors/postgres/setup.sh`
- Delete: `tests/connectors/postgres/teardown.sh`
- Delete: `tests/connectors/postgres/scenarios/*`

**Step 1: Write failing checks**

Run command(s) that still reference removed scripts to confirm breakpoints before rewiring.

**Step 2: Run check to verify failures**

Run: `just e2e`
Expected: FAIL (still points to removed shell script path).

**Step 3: Write minimal implementation**

Delete legacy files and adjust command wiring in next task.

**Step 4: Run test to verify no stale references remain**

Run: `rg -n "tests/e2e.sh|tests/connectors/postgres" justfile docs crates tests`
Expected: no runtime references (except historical docs if intentionally kept).

**Step 5: Commit**

```bash
git add -A tests
git commit -m "test(e2e): remove legacy shell and docker-compose e2e harness"
```

### Task 12: Rewire Commands and Docs to New E2E Entry Point

**Files:**
- Modify: `justfile`
- Modify: `docs/BUILD.md`
- Modify: `README.md` (if E2E commands documented)

**Step 1: Write failing command expectation**

```text
Current `just e2e` should fail after legacy deletion.
```

**Step 2: Run command to verify failure**

Run: `just e2e`
Expected: FAIL before recipe update.

**Step 3: Write minimal implementation**

Update:
- `just e2e` => `cargo test --manifest-path tests/e2e/Cargo.toml`
- optional `just e2e <test_name>` filter passthrough

**Step 4: Run command to verify pass**

Run: `just e2e`
Expected: PASS (or known test failures if not all scenarios implemented yet).

**Step 5: Commit**

```bash
git add justfile docs/BUILD.md README.md
git commit -m "build: point e2e command at rust-native harness"
```

### Task 13: Add Property-Based Coverage for Policy Invariants

**Files:**
- Create: `crates/rapidbyte-engine/tests/policy_proptest.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml`

**Step 1: Write the failing test**

```rust
proptest! {
    #[test]
    fn schema_policy_never_panics(policy in arb_schema_policy()) {
        let result = apply_policy_to_mock_data(policy);
        prop_assert!(result.is_ok() || result.is_err());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine --test policy_proptest -q`
Expected: FAIL before generators/helpers are wired.

**Step 3: Write minimal implementation**

Implement strategy generators and stable non-panic assertion semantics.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine --test policy_proptest -q`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/tests/policy_proptest.rs crates/rapidbyte-engine/Cargo.toml
git commit -m "test(engine): add proptest policy invariant coverage"
```

### Task 14: Full Verification Pass

**Files:**
- No new files expected

**Step 1: Run formatting**

Run: `just fmt`
Expected: PASS

**Step 2: Run lint**

Run: `just lint`
Expected: PASS

**Step 3: Run workspace tests**

Run: `just test`
Expected: PASS

**Step 4: Run new E2E suite**

Run: `just e2e`
Expected: PASS

**Step 5: Commit final stabilization changes**

```bash
git add -A
git commit -m "test: finalize rust-native e2e rearchitecture"
```
