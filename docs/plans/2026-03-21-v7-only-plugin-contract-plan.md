# V7-Only Plugin Contract Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `rapidbyte:plugin@7.0.0` the only plugin contract and schema source of truth by removing legacy discovery/catalog bridges, fixing validation semantics, and deleting remaining catalog-era helpers.

**Architecture:** Refactor the engine, CLI, dev tooling, scaffold, tests, and remaining helpers so discovery flows entirely through typed `DiscoveredStream` and schemas flow entirely through `StreamSchema`. Treat schema-aware validation as authoritative, move actionable config errors into `ValidationReport`, replace Arrow schema helpers that still depend on `ColumnSchema`, then delete the dead legacy catalog types and compatibility paths.

**Tech Stack:** Rust, WIT component bindings, Wasmtime host/runtime glue, Tokio tests, CLI snapshot-style assertions, plugin unit tests.

---

### Task 1: Add failing engine tests for full v7 discovery preservation

**Files:**
- Modify: `crates/rapidbyte-engine/tests/wasm_runner_integration.rs`
- Modify: `crates/rapidbyte-engine/src/application/discover.rs`
- Modify: `crates/rapidbyte-engine/src/application/testing.rs`
- Test: `crates/rapidbyte-engine/tests/wasm_runner_integration.rs`
- Test: `crates/rapidbyte-engine/src/application/discover.rs`

**Step 1: Write the failing tests**

Replace tests that assert `catalog_json` with tests that assert the engine
returns the real v7 fields from discovery:

- `schema` is present and includes expected fields
- `supported_sync_modes` survive the adapter
- `default_cursor_field`, `estimated_row_count`, and `metadata_json` survive
- application tests enqueue and assert typed discovery values instead of
  `catalog_json`

Example test shape:

```rust
assert_eq!(streams[0].name, "test-stream");
assert_eq!(streams[0].schema.fields.len(), 2);
assert_eq!(streams[0].supported_sync_modes, vec![SyncMode::FullRefresh]);
assert_eq!(streams[0].default_cursor_field, None);
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-engine test_discover_source_streams
cargo test -p rapidbyte-engine discover_resolves_and_discovers
```

Expected:
- FAIL because the adapter still fabricates `catalog_json` and drops schema and
  metadata

**Step 3: Write minimal implementation**

Refactor engine discovery types to use the shared typed discovery model:

- update the runner port to return typed v7 `DiscoveredStream`
- update fake runners and discover use cases to match
- remove `catalog_json` from the engine-facing discovery struct
- update the WASM adapter to map every WIT discovered-stream field directly

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-engine test_discover_source_streams
cargo test -p rapidbyte-engine discover_resolves_and_discovers
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/tests/wasm_runner_integration.rs crates/rapidbyte-engine/src/application/discover.rs crates/rapidbyte-engine/src/application/testing.rs crates/rapidbyte-engine/src/domain/ports/runner.rs crates/rapidbyte-engine/src/adapter/wasm_runner.rs
git commit -m "refactor(engine): preserve v7 discovery types end to end"
```

### Task 2: Add failing check tests for schema-aware validation authority

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/check.rs`
- Test: `crates/rapidbyte-engine/src/application/check.rs`

**Step 1: Write the failing tests**

Add tests that prove:

- a transform that succeeds without schema but fails with real upstream schema
  causes `check_pipeline` to fail that transform
- destination reconciliation still runs with the final `StreamSchema`
- discovery absence keeps `check` best-effort without inventing schema results

Example test shape:

```rust
assert_eq!(result.transform_validations[0].status, ValidationStatus::Failed);
assert!(result.transform_validations[0].message.contains("missing field"));
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-engine check_schema_negotiation_with_discover
cargo test -p rapidbyte-engine check_schema_negotiation_surfaces_transform_failure
```

Expected:
- FAIL because schema-aware transform failures are currently dropped

**Step 3: Write minimal implementation**

Refactor `check_pipeline` so:

- schema-aware validation updates the authoritative transform result
- failed schema-aware validations are not discarded
- destination reconciliation uses the same final schema path
- no duplicate contradictory result is kept from the schema-less pass

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-engine check_schema_negotiation_with_discover
cargo test -p rapidbyte-engine check_schema_negotiation_surfaces_transform_failure
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/application/check.rs
git commit -m "fix(engine): make schema-aware validation authoritative"
```

### Task 3: Add failing plugin tests for validation-vs-open semantics

**Files:**
- Modify: `plugins/transforms/sql/src/main.rs`
- Modify: `plugins/transforms/validate/src/main.rs`
- Test: `plugins/transforms/sql/src/main.rs`
- Test: `plugins/transforms/validate/src/main.rs`

**Step 1: Write the failing tests**

Add tests that assert:

- invalid SQL config does not need `init` to fail; `validate` returns
  `ValidationReport::Failed`
- invalid validate-transform config produces `ValidationReport::Failed`
- valid config can still be initialized and executed normally

Example test shape:

```rust
assert_eq!(validation.status, ValidationStatus::Failed);
assert!(validation.message.contains("failed to parse SQL query"));
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_reports_parse_error_for_invalid_sql
cargo test --manifest-path plugins/transforms/validate/Cargo.toml invalid_config_reports_validation_failure
```

Expected:
- FAIL because config errors still surface from `init`/`open`

**Step 3: Write minimal implementation**

Refactor both transforms so:

- `init` only constructs runnable state from already-acceptable config
- `validate` performs parse/compile checks and returns `ValidationReport`
- runtime still rejects impossible execution states if necessary, but malformed
  config is represented as validation failure

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml validate_reports_parse_error_for_invalid_sql
cargo test --manifest-path plugins/transforms/validate/Cargo.toml invalid_config_reports_validation_failure
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/transforms/sql/src/main.rs plugins/transforms/validate/src/main.rs
git commit -m "fix(plugins): report transform config issues as validation failures"
```

### Task 4: Add failing CLI and dev-tool tests for typed discovery output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/discover.rs`
- Modify: `crates/rapidbyte-dev/src/repl.rs`
- Test: relevant CLI command tests
- Test: relevant REPL tests

**Step 1: Write the failing tests**

Add tests that assert:

- CLI discover serializes typed `DiscoveredStream` JSON directly
- human-readable output is derived from typed fields
- dev REPL stores and renders discovered streams without converting through
  `Catalog`

If existing tests are sparse, add focused unit tests around the formatting and
conversion seams rather than broad end-to-end coverage first.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli discover
cargo test -p rapidbyte-dev repl
```

Expected:
- FAIL because CLI and REPL still depend on legacy catalog conversion

**Step 3: Write minimal implementation**

Refactor CLI and dev tooling so:

- discover output uses `Vec<rapidbyte_types::discovery::DiscoveredStream>`
- REPL state uses typed discovery structures
- formatting code reads fields from `StreamSchema` and `supported_sync_modes`
  directly
- all remaining `catalog_json` reparsing is removed

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-cli discover
cargo test -p rapidbyte-dev repl
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/discover.rs crates/rapidbyte-dev/src/repl.rs
git commit -m "refactor(cli): consume typed v7 discovery output"
```

### Task 5: Add failing scaffold tests for v7-only template output

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`
- Test: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing tests**

Add assertions that generated source, destination, and transform templates:

- do not mention `PluginInfo`
- do not mention `ValidationResult`
- do not mention `Catalog`
- use `&self`, not `&mut self`
- use `ValidationReport`
- use `Vec<DiscoveredStream>` for source discovery

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli scaffold
```

Expected:
- FAIL because the templates still emit pre-v7 signatures and helper types

**Step 3: Write minimal implementation**

Rewrite the scaffold templates and helper stubs to the current v7 SDK API.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-cli scaffold
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "refactor(scaffold): generate v7-only plugin templates"
```

### Task 6: Add failing tests for StreamSchema-based Arrow helpers

**Files:**
- Modify: `crates/rapidbyte-sdk/src/arrow/schema.rs`
- Modify: `plugins/sources/postgres/src/types.rs`
- Test: `crates/rapidbyte-sdk/src/arrow/schema.rs`
- Test: `plugins/sources/postgres/src/types.rs`

**Step 1: Write the failing tests**

Add tests that build Arrow schemas from `StreamSchema`/`SchemaField` without
using `ColumnSchema`.

Also add a focused plugin-side test if needed to prove Postgres column metadata
can map directly to `SchemaField` and into an Arrow `Schema`.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk arrow::schema
cargo test --manifest-path plugins/sources/postgres/Cargo.toml types
```

Expected:
- FAIL because the helper API still requires legacy `ColumnSchema`

**Step 3: Write minimal implementation**

Refactor the SDK Arrow helper API and remaining plugin callers so:

- Arrow schema construction accepts `StreamSchema` or `&[SchemaField]`
- plugins no longer convert through `rapidbyte_sdk::catalog::ColumnSchema`
- comments and exports no longer describe `ColumnSchema` as canonical

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk arrow::schema
cargo test --manifest-path plugins/sources/postgres/Cargo.toml types
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/arrow/schema.rs plugins/sources/postgres/src/types.rs
git commit -m "refactor(schema): build Arrow schemas from StreamSchema"
```

### Task 7: Remove dead legacy catalog code and exports

**Files:**
- Modify: `crates/rapidbyte-types/src/lib.rs`
- Delete: `crates/rapidbyte-types/src/catalog.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: any remaining callers found by `rg`
- Test: repo-wide compile and targeted test suites

**Step 1: Write the failing test or compile expectation**

Use compile failures as the proof point for dead-call-site cleanup after the
previous tasks have removed live dependencies.

Record the call sites before deletion using:

```bash
rg -n "\bcatalog\b|ColumnSchema|Catalog" crates plugins tests
```

**Step 2: Run compile or search to verify legacy usages still exist**

Run:

```bash
rg -n "\bcatalog\b|ColumnSchema|Catalog" crates plugins tests
```

Expected:
- finds the last references that must be removed or rewritten

**Step 3: Write minimal implementation**

Delete the catalog module and remove its exports and remaining call sites.
Keep only v7 discovery and schema types.

**Step 4: Run tests to verify the repo is green**

Run:

```bash
cargo test -p rapidbyte-types
cargo test -p rapidbyte-sdk
cargo test -p rapidbyte-engine
cargo test -p rapidbyte-cli
cargo test -p rapidbyte-dev
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/lib.rs crates/rapidbyte-sdk/src/lib.rs crates/rapidbyte-types/src/catalog.rs
git commit -m "refactor(types): remove legacy catalog schema model"
```

### Task 8: Final verification sweep

**Files:**
- No intentional source changes
- Test: repo-wide verification targets

**Step 1: Run targeted verification**

Run:

```bash
cargo test -p rapidbyte-types
cargo test -p rapidbyte-sdk
cargo test -p rapidbyte-engine
cargo test -p rapidbyte-cli
cargo test -p rapidbyte-dev
cargo test --manifest-path plugins/transforms/sql/Cargo.toml
cargo test --manifest-path plugins/transforms/validate/Cargo.toml
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
```

**Step 2: Run final legacy-usage search**

Run:

```bash
rg -n "catalog_json|ValidationResult|PluginInfo|ColumnSchema|Catalog" crates plugins tests docs/plans
```

Expected:
- no product code references remain outside historical plan/design documents or
  intentionally unrelated concepts

**Step 3: Commit**

```bash
git add -A
git commit -m "refactor: complete v7-only plugin contract migration"
```
