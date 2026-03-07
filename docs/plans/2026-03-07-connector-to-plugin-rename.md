# Connector → Plugin Rename Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename all "connector" terminology to "plugin" across the entire Rapidbyte codebase — types, functions, files, directories, WIT, YAML, CLI, env vars, docs, tests. No backward compatibility.

**Architecture:** Full atomic rename following the crate dependency graph bottom-up: types → runtime → sdk → engine → cli → directories → fixtures → docs. Each task is one logical unit that can be verified independently.

**Tech Stack:** Rust workspace, WIT (WASI component model), YAML pipeline configs, proc-macro SDK, Wasmtime bindings.

**Design doc:** `docs/plans/2026-03-07-connector-to-plugin-rename-design.md`

---

### Task 1: Rename types in `rapidbyte-types` (leaf crate)

**Files:**
- Modify: `crates/rapidbyte-types/src/wire.rs`
- Modify: `crates/rapidbyte-types/src/manifest.rs`
- Modify: `crates/rapidbyte-types/src/error.rs`

**Step 1: Update `wire.rs`**

Rename `ConnectorRole` → `PluginKind`, `ConnectorInfo` → `PluginInfo`. Update all doc comments from "connector" to "plugin". Update `ProtocolVersion::V4` → `ProtocolVersion::V5` with `#[serde(rename = "5")]`. Update all tests.

Specific changes in `wire.rs`:
- Line 47-57: `ConnectorRole` → `PluginKind`, doc "Role a connector fulfills" → "Kind of plugin in a pipeline"
- Line 59-61: doc "Capability flag declared by a connector" → "Capability flag declared by a plugin"
- Line 78-88: `ConnectorInfo` → `PluginInfo`, doc "Connector self-description" → "Plugin self-description"
- Line 12-18: `ProtocolVersion::V4` → `ProtocolVersion::V5`, `#[serde(rename = "4")]` → `#[serde(rename = "5")]`
- All test functions: update type references and test names

**Step 2: Update `manifest.rs`**

Rename `ConnectorManifest` → `PluginManifest`. Update doc comments. Update `supports_role` → `supports_kind` taking `PluginKind`. Update all tests.

Specific changes in `manifest.rs`:
- Line 1-5: doc comments "Connector manifest" → "Plugin manifest"
- Line 7: import `ConnectorRole` → `PluginKind`
- Line 66-76: doc "Capabilities declared when a connector supports the Source role" → "Capabilities declared for a source plugin"
- Lines 103-150: `ConnectorManifest` → `PluginManifest`, `supports_role` → `supports_kind`, param `ConnectorRole` → `PluginKind`, match arms updated
- Lines 152-220: update all test function names and type references

**Step 3: Update `error.rs`**

Rename `ConnectorError` → `PluginError`. Update all doc comments. Update all factory methods and tests.

Specific changes in `error.rs`:
- Line 1-4: doc comments "connector operations" → "plugin operations"
- Line 9-16: doc "Broad classification of a connector error" → "Broad classification of a plugin error"
- Line 180: `pub struct ConnectorError` → `pub struct PluginError`
- Line 196: `impl ConnectorError` → `impl PluginError`
- All test functions: update type references

**Step 4: Verify types crate compiles**

Run: `cargo check -p rapidbyte-types`
Expected: compiles with no errors (downstream crates will fail, that's expected)

**Step 5: Commit**

```
git add crates/rapidbyte-types/
git commit -m "refactor(types): rename connector types to plugin"
```

---

### Task 2: Rename WIT interface

**Files:**
- Rename: `wit/rapidbyte-connector.wit` → `wit/rapidbyte-plugin.wit`

**Step 1: Rename file and update contents**

Rename the WIT file. Then update all "connector" references inside:

- Line 1: `package rapidbyte:connector@4.0.0` → `package rapidbyte:plugin@5.0.0`
- Line 6: `connector-role` → `plugin-kind`
- Line 58: `connector-instance` → `plugin-instance`
- Line 67: `record connector-error` → `record plugin-error`
- Line 128: `role: connector-role` → `kind: plugin-kind`
- Lines 146-174 (host interface): all `connector-error` → `plugin-error`
- Lines 176-220 (source/dest/transform interfaces): all `connector-error` → `plugin-error`

World names stay: `rapidbyte-source`, `rapidbyte-destination`, `rapidbyte-transform`, `rapidbyte-host`.

**Step 2: Commit**

```
git add wit/
git commit -m "refactor(wit): rename to rapidbyte:plugin@5.0.0"
```

---

### Task 3: Update runtime crate bindings and connector resolution

**Files:**
- Rename: `crates/rapidbyte-runtime/src/connector.rs` → `crates/rapidbyte-runtime/src/plugin.rs`
- Modify: `crates/rapidbyte-runtime/src/lib.rs`
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`

**Step 1: Rename `connector.rs` → `plugin.rs` and update contents**

In the renamed `plugin.rs`:
- Module doc: "Connector reference parsing" → "Plugin reference parsing"
- Line 6: import `ConnectorManifest` → `PluginManifest`
- Lines 19-44: `extract_manifest_from_wasm` return type `Option<ConnectorManifest>` → `Option<PluginManifest>`, local var names
- Lines 53-65: `parse_connector_ref` → `parse_plugin_ref`, doc comments, param names `connector_ref` → `plugin_ref`
- Lines 67-82: `connector_search_dirs` → `plugin_search_dirs`, change `RAPIDBYTE_CONNECTOR_DIR` → `RAPIDBYTE_PLUGIN_DIR`

**IMPORTANT — Resolution logic change:** The function signature changes:

```rust
// Before
pub fn resolve_connector_path(connector_ref: &str) -> Result<PathBuf>

// After
pub fn resolve_plugin_path(plugin_ref: &str, kind: PluginKind) -> Result<PathBuf>
```

New implementation:
- Map `PluginKind` to subdirectory: `Source` → `"sources"`, `Destination` → `"destinations"`, `Transform` → `"transforms"`
- Filename: `plugin_ref` with hyphens → underscores, append `.wasm`
- Search: for each dir in `plugin_search_dirs()`, check `dir/<kind_subdir>/<filename>`
- Error: `"Plugin '{plugin_ref}' ({kind:?}) not found. Searched for '{filename}' in RAPIDBYTE_PLUGIN_DIR/<kind>/ and ~/.rapidbyte/plugins/<kind>/"`

Update `load_connector_manifest` → `load_plugin_manifest`, `extract_manifest_from_wasm` stays same name. Update all tests.

**Step 2: Update `lib.rs`**

- Line 26: `pub mod connector;` → `pub mod plugin;`
- Lines 41-43: update re-exports:
  ```rust
  pub use plugin::{
      plugin_search_dirs, load_plugin_manifest, parse_plugin_ref, resolve_plugin_path,
  };
  ```
- Line 1: doc comment "Wasmtime component runtime for Rapidbyte connectors" → "Wasmtime component runtime for Rapidbyte plugins"
- Lines 4, 14, 16: update doc comments referencing connector → plugin

**Step 3: Update `bindings.rs`**

All `rapidbyte::connector::` paths become `rapidbyte::plugin::` due to WIT package rename. Specifically:

- Line 2: doc "connector worlds" → "plugin worlds"
- Line 32: import `ConnectorError` → `PluginError`
- Line 66: `$module::rapidbyte::connector::types::ConnectorError` → `$module::rapidbyte::plugin::types::PluginError`
- Lines 67-71: type aliases `ConnectorError as CConnectorError` → `PluginError as CPluginError`
- Line 111: `$module::rapidbyte::connector::types::ConnectorError` → `$module::rapidbyte::plugin::types::PluginError`
- All `$module::rapidbyte::connector::types::*` → `$module::rapidbyte::plugin::types::*` throughout the file (error converters, host trait impl, validation converter)
- Line 188: `$module::rapidbyte::connector::host::Host` → `$module::rapidbyte::plugin::host::Host`
- Line 379: `$module::rapidbyte::connector::types::ValidationReport` → `$module::rapidbyte::plugin::types::ValidationReport`
- Line 403: `$module::rapidbyte::connector::types::Host` → `$module::rapidbyte::plugin::types::Host`
- Line 415: test name `v4_world_bindings_exist` → `v5_world_bindings_exist`
- Lines 420-421: `source_bindings::rapidbyte::connector::types::*` → `source_bindings::rapidbyte::plugin::types::*`

**Step 4: Verify runtime crate compiles**

Run: `cargo check -p rapidbyte-runtime`

**Step 5: Commit**

```
git add crates/rapidbyte-runtime/
git commit -m "refactor(runtime): rename connector to plugin, update bindings"
```

---

### Task 4: Update SDK crate

**Files:**
- Rename: `crates/rapidbyte-sdk/src/connector.rs` → `crates/rapidbyte-sdk/src/plugin.rs`
- Rename: `crates/rapidbyte-sdk/macros/src/connector.rs` → `crates/rapidbyte-sdk/macros/src/plugin.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Modify: `crates/rapidbyte-sdk/macros/src/lib.rs`
- Modify: any files in `crates/rapidbyte-sdk/src/` that reference `ConnectorError` or `ConnectorInfo`

**Step 1: Rename and update SDK `connector.rs` → `plugin.rs`**

- Line 1: doc "Async-first connector traits" → "Async-first plugin traits"
- Line 7: import `ConnectorError` → `PluginError`
- Line 10: import `ConnectorInfo` → `PluginInfo`
- Lines 13-21: `default_validation` return type `Result<ValidationResult, ConnectorError>` → `Result<ValidationResult, PluginError>`
- Lines 24-26: `default_close` return type `Result<(), ConnectorError>` → `Result<(), PluginError>`
- Line 28: doc "Source connector lifecycle" → "Source plugin lifecycle"
- Lines 30-53: `Source` trait — all `ConnectorError` → `PluginError`, `ConnectorInfo` → `PluginInfo`
- Lines 55-78: `Destination` trait — same changes
- Lines 80-103: `Transform` trait — same changes
- All tests: update type references

**Step 2: Update `lib.rs`**

- Line 4: doc "WASI-based data pipeline connectors" → "WASI-based data pipeline plugins"
- Line 11: `pub mod connector;` → `pub mod plugin;`
- Line 50-52: `pub use rapidbyte_sdk_macros::connector;` → `pub use rapidbyte_sdk_macros::plugin;`

**Step 3: Update `prelude.rs`**

- Line 1: doc "Convenience re-exports for connector authors" → "Convenience re-exports for plugin authors"
- Line 7-8: `crate::connector::` → `crate::plugin::`
- Line 14: `ConnectorError` → `PluginError`
- Line 17: `ConnectorInfo` → `PluginInfo`

**Step 4: Update SDK macro crate**

Rename `macros/src/connector.rs` → `macros/src/plugin.rs`.

In `macros/src/plugin.rs`:
- Line 1: doc "`#[connector(...)]`" → "`#[plugin(...)]`"
- Line 12: `ConnectorRole` → `PluginKind`
- Lines 18-34: update Parse impl for `PluginKind`
- Line 37: `expand(role: ConnectorRole, ...)` → `expand(kind: PluginKind, ...)`
- Lines 41-54: match on `PluginKind::Source` etc., change trait paths from `::rapidbyte_sdk::connector::Source` → `::rapidbyte_sdk::plugin::Source` (same for Destination, Transform)
- Line 115: `static CONNECTOR` → `static PLUGIN`
- Lines 117-119: `get_state` references `CONNECTOR` → `PLUGIN`
- Lines 122-168: `to_component_error` — `ConnectorError` → `PluginError`, all `rapidbyte::connector::types` → `rapidbyte::plugin::types`
- Lines 170-183: `parse_stream_context` — same type path changes
- Lines 184-199: `parse_config`, `parse_saved_config` — same type path changes
- Lines 201-218: `to_component_validation` — same type path changes
- Lines 230-243: guest trait paths `exports::rapidbyte::connector::source::Guest` → `exports::rapidbyte::plugin::source::Guest` (same for dest, transform)
- Lines 258-304: lifecycle methods — all type paths updated
- Lines 308-440: role-specific methods — all `rapidbyte::connector::types` → `rapidbyte::plugin::types`, `ConnectorRole` → `PluginKind`

In `macros/src/lib.rs`:
- Line 44-52: rename function `connector` → `plugin`, update call to `connector::ConnectorRole` → `plugin::PluginKind`, `connector::expand` → `plugin::expand`
- Doc comments: `#[connector(source)]` → `#[plugin(source)]`

**Step 5: Update other SDK files that reference `ConnectorError`/`ConnectorInfo`**

Grep all `ConnectorError` and `ConnectorInfo` in `crates/rapidbyte-sdk/src/` and update to `PluginError`/`PluginInfo`. Files likely affected: `host_tcp.rs`, `host_ffi.rs`, `build.rs`, `arrow/ipc.rs`, `arrow/mod.rs`, `context.rs`.

**Step 6: Verify SDK crate compiles**

Run: `cargo check -p rapidbyte-sdk && cargo check -p rapidbyte-sdk-macros`

**Step 7: Commit**

```
git add crates/rapidbyte-sdk/
git commit -m "refactor(sdk): rename connector to plugin, update macro"
```

---

### Task 5: Update engine crate

**Files:**
- Modify: `crates/rapidbyte-engine/src/resolve.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs`
- Modify: `crates/rapidbyte-engine/src/config/validator.rs`
- Modify: `crates/rapidbyte-engine/src/config/types.rs`

**Step 1: Update `resolve.rs`**

- Line 9: import `ConnectorManifest` → `PluginManifest`
- Line 10: import `ConnectorRole` → `PluginKind`
- Lines 15-22: `ResolvedConnectors` → `ResolvedPlugins`, field types `ConnectorManifest` → `PluginManifest`
- Lines 24-59: `resolve_connectors` → `resolve_plugins`, update calls to `resolve_connector_path` → `resolve_plugin_path` (now passing `PluginKind`), `ConnectorRole::Source` → `PluginKind::Source`, etc.
- Lines 61-95: param `connector_ref` → `plugin_ref`, `ConnectorRole` → `PluginKind`, `ConnectorManifest` → `PluginManifest`, `load_connector_manifest` → `load_plugin_manifest`
- Lines 97-125: `validate_config_against_schema` — param `connector_ref` → `plugin_ref`, `ConnectorManifest` → `PluginManifest`, all error messages/tracing "connector" → "plugin"
- Update tracing messages: "Loaded connector manifest" → "Loaded plugin manifest"

**Step 2: Update `runner.rs`**

- All `connector_id` params → `plugin_id`, `connector_version` → `plugin_version`
- All `rapidbyte_connector_source()` → `rapidbyte_plugin_source()`, `rapidbyte_connector_destination()` → `rapidbyte_plugin_destination()`, `rapidbyte_connector_transform()` → `rapidbyte_plugin_transform()`
- All `source_bindings::rapidbyte::connector::types::*` → `source_bindings::rapidbyte::plugin::types::*` (same for dest, transform)
- All tracing: "connector" → "plugin" in structured fields and messages
- `validate_connector` → `validate_plugin`
- `ConnectorRole` → `PluginKind`
- Doc comments: "connector" → "plugin"

**Step 3: Update `orchestrator.rs`**

- Line 23: import `ConnectorRole` → `PluginKind`
- Lines 62-70: `LoadedTransformModule` — `connector_id` → `plugin_id`, `connector_version` → `plugin_version`
- Lines 90-98: `StreamParams` — `source_connector_id` → `source_plugin_id`, `dest_connector_id` → `dest_plugin_id`, same for versions and permissions
- All calls to `resolve_connectors` → `resolve_plugins`, `ResolvedConnectors` → `ResolvedPlugins`
- `validate_connector` → `validate_plugin`
- `discover_connector` → `discover_plugin`
- Line 1: doc "resolves connectors" → "resolves plugins"

**Step 4: Update `lib.rs`**

- Line 3: doc "connector runners" → "plugin runners"
- Line 20: doc "connector runners" → "plugin runners"
- Line 43: `discover_connector` → `discover_plugin` re-export

**Step 5: Update `config/validator.rs`**

- Line 33-34: doc and function `validate_connector_overrides` → `validate_plugin_overrides`
- Lines 80, 100: error messages "Source connector reference" → "Source plugin reference", "Destination connector reference" → "Destination plugin reference"
- Lines 131-151: calls to `validate_connector_overrides` → `validate_plugin_overrides`

**Step 6: Update `config/types.rs`**

- Line 61: doc "Combined pipeline-level permission overrides for a connector" → "...for a plugin"
- No struct renames needed — `SourceConfig`, `DestinationConfig`, `TransformConfig` don't have "connector" in their names.
- Update YAML test fixtures in the tests to use `use: postgres` instead of `use: source-postgres`, `use: dest-postgres`.

**Step 7: Verify engine crate compiles**

Run: `cargo check -p rapidbyte-engine`

**Step 8: Commit**

```
git add crates/rapidbyte-engine/
git commit -m "refactor(engine): rename connector to plugin throughout"
```

---

### Task 6: Update CLI crate

**Files:**
- Rename: `crates/rapidbyte-cli/src/commands/connectors.rs` → `crates/rapidbyte-cli/src/commands/plugins.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Rename and update `connectors.rs` → `plugins.rs`**

- Line 1: doc "Connector listing subcommand" → "Plugin listing subcommand"
- Lines 6-10: `ConnectorEntry` → `PluginEntry`
- Line 12: doc "Execute the `connectors` command" → "Execute the `plugins` command"
- Line 22: `connector_search_dirs` → `plugin_search_dirs`
- Lines 25-26: error message "No connectors found" → "No plugins found", `RAPIDBYTE_CONNECTOR_DIR` → `RAPIDBYTE_PLUGIN_DIR`
- Line 43: `load_connector_manifest` → `load_plugin_manifest`
- Lines 54-65: `ConnectorEntry` → `PluginEntry`
- Lines 71-73: same error message updates

**IMPORTANT — Resolution change for `plugins` command:** The listing command now needs to scan subdirectories (`sources/`, `destinations/`, `transforms/`) instead of a flat directory. Update the scanning logic to iterate through kind subdirectories.

**Step 2: Update `commands/mod.rs`**

- Line 11: `pub mod connectors;` → `pub mod plugins;`

**Step 3: Update `main.rs`**

- Line 77: doc "Discover available streams from a source connector" → "Discover available streams from a source plugin"
- Line 82-83: `/// List available connectors` → `/// List available plugins`, `Connectors` → `Plugins`
- Lines 84-91: `/// Scaffold a new connector project` → `/// Scaffold a new plugin project`, doc "Connector name" → "Plugin name", default output change
- Line 112: `Commands::Connectors => commands::connectors::execute(verbosity)` → `Commands::Plugins => commands::plugins::execute(verbosity)`

**Step 4: Update `scaffold.rs`**

- Line 1: doc "Connector project scaffolding" → "Plugin project scaffolding"
- Line 8: doc "Connector role derived from the name prefix" → "Plugin kind derived from the name prefix"
- Line 38: error message "Connector name must start with" → "Plugin name must start with"
- Line 44: default path `"connectors"` → output based on kind: `"plugins/sources"` for source, `"plugins/destinations"` for dest
- Line 147: "Scaffolded {} connector" → "Scaffolded {} plugin"
- Lines 253-281: `gen_build_rs` — "Source connector for" → "Source plugin for", "Destination connector for" → "Destination plugin for"
- Lines 284-331: `gen_source_main` — `#[rapidbyte_sdk::connector(source)]` → `#[rapidbyte_sdk::plugin(source)]`, `ConnectorInfo` → `PluginInfo`, `ConnectorError` → `PluginError`, `ProtocolVersion::V4` → `ProtocolVersion::V5`
- Lines 333-375: `gen_dest_main` — same changes as source
- Lines 415-428: `gen_client_rs` — `ConnectorError` → `PluginError`
- Lines 430-446: `gen_reader_rs` — `ConnectorError` → `PluginError`
- Lines 449-458: `gen_schema_rs` — `ConnectorError` → `PluginError`
- Lines 461-478: `gen_writer_rs` — `ConnectorError` → `PluginError`

**Step 5: Verify CLI crate compiles**

Run: `cargo check -p rapidbyte-cli`

**Step 6: Commit**

```
git add crates/rapidbyte-cli/
git commit -m "refactor(cli): rename connectors to plugins"
```

---

### Task 7: Rename plugin directories and update build system

**Files:**
- Move: `connectors/source-postgres/` → `plugins/sources/postgres/`
- Move: `connectors/dest-postgres/` → `plugins/destinations/postgres/`
- Move: `connectors/transform-sql/` → `plugins/transforms/sql/`
- Move: `connectors/transform-validate/` → `plugins/transforms/validate/`
- Modify: `Justfile`
- Modify: Cargo.toml paths in each plugin's deps (relative paths to crates/)
- Modify: `.cargo/config.toml` in each plugin (rustc-wrapper path)
- Modify: `wit` path reference in each plugin (build.rs or WIT path)

**Step 1: Create new directory structure and move files**

```bash
mkdir -p plugins/sources plugins/destinations plugins/transforms
git mv connectors/source-postgres plugins/sources/postgres
git mv connectors/dest-postgres plugins/destinations/postgres
git mv connectors/transform-sql plugins/transforms/sql
git mv connectors/transform-validate plugins/transforms/validate
```

**Step 2: Update relative paths in each plugin**

The depth changed from `connectors/<name>/` (2 levels) to `plugins/<kind>/<name>/` (3 levels). All relative paths need one more `../`:

In each plugin's `Cargo.toml`:
- `path = "../../crates/rapidbyte-sdk"` → `path = "../../../crates/rapidbyte-sdk"`

In each plugin's `.cargo/config.toml`:
- `rustc-wrapper = "../../scripts/rustc-wrapper.sh"` → `rustc-wrapper = "../../../scripts/rustc-wrapper.sh"`

In each plugin's macro-generated WIT path (in `macros/src/plugin.rs`):
- `path: "../../wit"` → this is set in the macro, check if it needs updating. The macro hardcodes `path: "../../wit"` — this must change to `"../../../wit"` or be made configurable. **Since the macro generates code that runs inside the plugin crate**, the path is relative to the plugin's Cargo.toml. Update the macro to use `path: "../../../wit"`.

Wait — the WIT path in the macro (`gen_wit_bindings`) uses `"../../wit"`. With the new 3-level nesting, this must be `"../../../wit"`. However, the runtime bindings in `bindings.rs` also reference `"../../wit"` which is relative to the runtime crate (correct). So only the SDK macro needs updating.

**Step 3: Update plugin `#[connector(...)]` → `#[plugin(...)]` in each plugin's main.rs**

- `plugins/sources/postgres/src/main.rs`: `#[rapidbyte_sdk::connector(source)]` → `#[rapidbyte_sdk::plugin(source)]`
- `plugins/destinations/postgres/src/main.rs`: `#[rapidbyte_sdk::connector(destination)]` → `#[rapidbyte_sdk::plugin(destination)]`
- `plugins/transforms/sql/src/main.rs`: `#[rapidbyte_sdk::connector(transform)]` → `#[rapidbyte_sdk::plugin(transform)]`
- `plugins/transforms/validate/src/main.rs`: `#[rapidbyte_sdk::connector(transform)]` → `#[rapidbyte_sdk::plugin(transform)]`

Also update any `ConnectorError` → `PluginError`, `ConnectorInfo` → `PluginInfo`, `ProtocolVersion::V4` → `ProtocolVersion::V5` in plugin source files.

**Step 4: Update Justfile**

```just
# Recipe renames
_build-connectors → _build-plugins
build-all: _build-host _build-plugins

# Env var
RAPIDBYTE_PLUGIN_DIR=target/plugins (replaces RAPIDBYTE_CONNECTOR_DIR)

# Build paths
cd plugins/sources/postgres && cargo build ...
cd plugins/destinations/postgres && cargo build ...
cd plugins/transforms/sql && cargo build ...
cd plugins/transforms/validate && cargo build ...

# Output with subdirectories
mkdir -p target/plugins/sources target/plugins/destinations target/plugins/transforms
cp plugins/sources/postgres/target/wasm32-wasip2/{{MODE}}/source_postgres.wasm target/plugins/sources/postgres.wasm
cp plugins/destinations/postgres/target/wasm32-wasip2/{{MODE}}/dest_postgres.wasm target/plugins/destinations/postgres.wasm
cp plugins/transforms/sql/target/wasm32-wasip2/{{MODE}}/transform_sql.wasm target/plugins/transforms/sql.wasm
cp plugins/transforms/validate/target/wasm32-wasip2/{{MODE}}/transform_validate.wasm target/plugins/transforms/validate.wasm

# Strip commands updated similarly

# Scaffold default
scaffold name kind output=("plugins/" + kind + "s/" + name)

# Comments
"Build host + connectors" → "Build host + plugins"
```

**Step 5: Verify plugins build**

Run: `just _build-plugins` (or manually build one plugin to verify paths)

**Step 6: Commit**

```
git add plugins/ Justfile
git rm -r connectors/
git commit -m "refactor: move connectors/ to plugins/ with kind subdirectories"
```

---

### Task 8: Update test fixtures and E2E harness

**Files:**
- Rename: `tests/e2e/src/harness/connectors.rs` → `tests/e2e/src/harness/plugins.rs`
- Modify: `tests/e2e/src/harness/mod.rs`
- Modify: All pipeline YAML fixtures in `tests/fixtures/pipelines/`
- Modify: All pipeline YAML fixtures in `tests/bench/fixtures/pipelines/`
- Modify: Any other test files referencing `RAPIDBYTE_CONNECTOR_DIR` or connector names

**Step 1: Rename and update E2E harness**

Rename `connectors.rs` → `plugins.rs`. Update:
- `CONNECTOR_DIR` → `PLUGIN_DIR`
- `prepare_connector_dir` → `prepare_plugin_dir`
- `ensure_connector` → `ensure_plugin`
- Output directory: `target/connectors` → `target/plugins` with subdirectories
- Build paths: `connectors/source-postgres` → `plugins/sources/postgres`, etc.
- Copy output: same file rename logic as Justfile (cargo name → kind subdir name)

**Step 2: Update `harness/mod.rs`**

- `mod connectors;` → `mod plugins;`
- `connector_dir` field → `plugin_dir`
- Calls to `connectors::prepare_connector_dir` → `plugins::prepare_plugin_dir`
- All YAML template strings: `use: source-postgres` → `use: postgres`, `use: dest-postgres` → `use: postgres`, `use: transform-sql` → `use: sql`, `use: transform-validate` → `use: validate`
- `RAPIDBYTE_CONNECTOR_DIR` env var → `RAPIDBYTE_PLUGIN_DIR`

**Step 3: Update test fixture YAMLs**

For each YAML file in `tests/fixtures/pipelines/`:
- `use: source-postgres` → `use: postgres`
- `use: dest-postgres` → `use: postgres`
- `use: transform-sql` → `use: sql`
- `use: transform-validate` → `use: validate`

Same for `tests/bench/fixtures/pipelines/`.

**Step 4: Update bench harness**

Search for `RAPIDBYTE_CONNECTOR_DIR` in `tests/bench/` and update to `RAPIDBYTE_PLUGIN_DIR`. Update any connector name references.

**Step 5: Verify E2E compiles**

Run: `cargo check --manifest-path tests/e2e/Cargo.toml`

**Step 6: Commit**

```
git add tests/
git commit -m "refactor(tests): update fixtures and harness for plugin rename"
```

---

### Task 9: Update documentation

**Files:**
- Rename: `docs/CONNECTOR_DEV.md` → `docs/PLUGIN_DEV.md`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `docs/PROTOCOL.md`

**Step 1: Rename and update `CONNECTOR_DEV.md` → `PLUGIN_DEV.md`**

Global find-replace throughout:
- "connector" → "plugin" (case-sensitive for lowercase)
- "Connector" → "Plugin" (case-sensitive for uppercase)
- `#[connector(` → `#[plugin(`
- `RAPIDBYTE_CONNECTOR_DIR` → `RAPIDBYTE_PLUGIN_DIR`
- `ConnectorError` → `PluginError`
- `ConnectorInfo` → `PluginInfo`
- `ConnectorRole` → `PluginKind`
- `ProtocolVersion::V4` → `ProtocolVersion::V5`
- Directory paths: `connectors/` → `plugins/`
- YAML examples: `use: source-postgres` → `use: postgres`, etc.

**Step 2: Update `README.md`**

- "Connectors" section heading → "Plugins"
- CLI: `rapidbyte connectors` → `rapidbyte plugins`
- Plugin list: `source-postgres` description stays but context changes
- Architecture section: "connectors" → "plugins"

**Step 3: Update `CLAUDE.md`**

- Project structure: `connectors/` → `plugins/` with new subdirectory layout
- Commands: `build-connectors` → `build-plugins`, `RAPIDBYTE_CONNECTOR_DIR` → `RAPIDBYTE_PLUGIN_DIR`
- Architecture notes: "connectors" → "plugins"
- Notes section: `#[connector(source)]` → `#[plugin(source)]`

**Step 4: Update `docs/PROTOCOL.md`**

- WIT package: `rapidbyte:connector@4.0.0` → `rapidbyte:plugin@5.0.0`
- Type references: `connector-error` → `plugin-error`, `connector-role` → `plugin-kind`
- "connector" → "plugin" throughout

**Step 5: Commit**

```
git add docs/ README.md CLAUDE.md
git commit -m "docs: update all documentation for plugin rename"
```

---

### Task 10: Full workspace build and test verification

**Step 1: Build the full workspace**

Run: `cargo build`
Expected: clean build, no errors

**Step 2: Run workspace tests**

Run: `cargo test`
Expected: all tests pass

**Step 3: Build plugins**

Run: `just _build-plugins` (or build one plugin manually)
Expected: WASM binaries produced in `target/plugins/sources/`, `target/plugins/destinations/`, `target/plugins/transforms/`

**Step 4: Verify CLI**

Run: `RAPIDBYTE_PLUGIN_DIR=target/plugins ./target/release/rapidbyte plugins`
Expected: lists available plugins with correct names and roles

**Step 5: Run a smoke test with a fixture**

Run: `just run tests/fixtures/pipelines/simple_pg_to_pg.yaml` (if docker is available)
Or just verify the YAML parses: dry-run check

**Step 6: Final grep for any remaining "connector" references**

Run: `grep -ri "connector" --include="*.rs" --include="*.wit" --include="*.yaml" --include="*.toml" --include="*.md" --include="*.just" -l`
Expected: no results (or only in git history / third-party deps)

**Step 7: Commit any fixes**

If grep finds stragglers, fix them and commit.

**Step 8: Final commit and cleanup**

```
git commit -m "refactor: complete connector→plugin rename"
```
