# V7-Only Plugin Contract Design

## Summary

Complete the `rapidbyte:plugin@7.0.0` migration and make the v7 contract the
only source of truth across SDK, runtime, engine, CLI, dev tooling, plugins,
tests, and scaffolding. Remove the legacy discovery/catalog bridge, stop
round-tripping discovery data through ad hoc JSON, and delete catalog-era schema
types once their remaining helper uses have been replaced by `StreamSchema`.

## Goals

- Preserve the full v7 `discovered-stream` payload end-to-end.
- Make `rapidbyte_types::discovery::DiscoveredStream` the only discovery model.
- Make `rapidbyte_types::schema::StreamSchema` the only schema model used for
  runtime flow, validation, discovery, and Arrow schema construction.
- Ensure `validate` is the canonical place for config and schema validation
  failures surfaced by `rapidbyte check`.
- Remove pre-v7 scaffold output and dead compatibility code.
- Update tests so the v7 model is what the codebase asserts, not just what the
  WIT file claims.

## Non-Goals

- Preserving backward compatibility for legacy `Catalog`, `catalog_json`, or
  `ColumnSchema`-based discovery flows.
- Keeping dual representations at the edge for a migration period.
- Introducing new plugin protocol features beyond completing the existing v7
  contract.

## Current Problems

- The v7 WIT contract defines a rich `discovered-stream`, but the engine adapter
  drops almost all of it and reconstructs a fake `catalog_json`.
- The engine runner port still exposes discovery as a legacy hybrid struct
  rather than the typed v7 model.
- CLI and dev tooling convert or reparse discovery data through legacy catalog
  types instead of rendering directly from v7 discovery types.
- `check` runs schema-aware validation but does not consistently treat those
  results as authoritative.
- Some transforms now validate configuration in `init`, which surfaces malformed
  config as fatal open errors instead of standard validation failures.
- `rapidbyte scaffold` still generates pre-v7 plugin trait signatures and helper
  types.
- Remaining SDK and plugin helper code still depends on `ColumnSchema` for Arrow
  schema construction.

## Alternatives Considered

### 1. Recommended: complete the migration in one typed pass

Make `DiscoveredStream` and `StreamSchema` canonical everywhere, refactor
consumers to match, and delete all compatibility shims in the same slice.

Pros:
- Single source of truth.
- Eliminates the root cause of the current split-brain failures.
- Avoids carrying temporary debt into later refactors.

Cons:
- Touches many layers in one pass.
- Requires broad test updates.

### 2. Stage the engine migration and delete helpers later

Fix engine, CLI, and validation semantics now, then remove remaining catalog and
schema helpers in a follow-up.

Pros:
- Lower immediate code churn.

Cons:
- Leaves legacy types alive on purpose.
- Conflicts with the requested “no legacy code” end state.

### 3. Keep legacy presentation adapters at the edge

Use v7 internally but preserve `Catalog` and `ColumnSchema` as CLI/dev-facing
adapters.

Pros:
- Smaller surface change.

Cons:
- Retains two schemas of truth.
- Preserves the migration ambiguity that caused the current regressions.

## Recommended Design

### Canonical Discovery Model

`rapidbyte_types::discovery::DiscoveredStream` becomes the only discovery type
used by runtime, engine, CLI, dev tooling, and tests.

The engine runner port should return the typed v7 struct directly. The WASM
adapter should preserve all fields emitted by the plugin:

- `name`
- `schema`
- `supported_sync_modes`
- `default_cursor_field`
- `estimated_row_count`
- `metadata_json`

`catalog_json` should be removed entirely. CLI and developer tooling should
render or serialize directly from `Vec<DiscoveredStream>`, not by reparsing
fabricated JSON into a legacy catalog model.

### Canonical Schema Model

`rapidbyte_types::schema::StreamSchema` and `SchemaField` become the only schema
model.

That implies:

- removing `ColumnSchema` and `Catalog::Stream::schema` from normal product flow
- rewriting SDK Arrow helpers to build Arrow `Schema` from `StreamSchema` or
  `SchemaField`
- updating plugin internals that still convert through legacy `ColumnSchema`
- deleting the shared `catalog` module once no live code depends on it

### Validation and Lifecycle Semantics

`validate` should be the canonical place where config and schema incompatibility
is turned into `ValidationReport`.

`open`/`init` should only fail for parsing/deserialization or unavoidable
instance-construction failures that cannot be represented as validation
feedback. Config checks that can produce actionable validation output should
live in `validate`.

In practice:

- move SQL query validation that currently fails in `init` into `validate`
- move validate-transform rule compilation failures that currently fail in
  `init` into `validate`
- preserve runtime-ready state caching where useful, but not at the cost of
  turning validation failures into fatal `open` errors

### Check Pipeline Semantics

`check_pipeline` should no longer treat schema-less validation and schema-aware
validation as independent truths.

Rules:

- the schema-aware pass is authoritative when source discovery provides schemas
- transform validation failures with real upstream schema must surface in the
  final `transform_validations`
- destination field-requirement reconciliation must build on the same typed
  `StreamSchema` flow
- discovery failure remains non-fatal, but only because schema evidence is
  missing, not because failures are ignored

### CLI and Dev Tooling

The CLI discover command should emit typed v7 discovery JSON directly. Human
readable output should be formatted from `DiscoveredStream` fields rather than a
legacy `Catalog`.

The dev REPL should likewise store and render typed discovered streams instead
of converting to `Catalog`.

### Scaffold Output

`rapidbyte scaffold` must emit only v7 plugin APIs:

- `init(config) -> Result<Self, PluginError>`
- `&self` methods, not `&mut self`
- `discover() -> Result<Vec<DiscoveredStream>, PluginError>`
- `validate(&self, ctx, upstream) -> Result<ValidationReport, PluginError>`
- no `PluginInfo`
- no `ValidationResult`
- no `Catalog`

Scaffold helper files should use the same typed discovery and validation models
as the hand-written plugins.

## Testing Strategy

Add and update coverage for:

- engine discover preserving full v7 stream metadata
- CLI discover emitting and rendering typed v7 discovery output
- dev tooling consuming typed discovery output directly
- check surfacing schema-aware transform failures
- transform plugins reporting malformed config as validation failures rather
  than fatal open errors
- scaffold output generating only v7 trait signatures and helper types
- Arrow schema helpers working from `StreamSchema`/`SchemaField`

Tests that currently assert `catalog_json` or legacy catalog round-trips should
be rewritten or deleted.

## Change Scope

### Core files

- `wit/rapidbyte-plugin.wit`
- `crates/rapidbyte-sdk/macros/src/plugin.rs`
- `crates/rapidbyte-runtime/src/bindings.rs`
- `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- `crates/rapidbyte-engine/src/domain/ports/runner.rs`
- `crates/rapidbyte-engine/src/application/check.rs`
- `crates/rapidbyte-engine/src/application/discover.rs`
- `crates/rapidbyte-cli/src/commands/discover.rs`
- `crates/rapidbyte-dev/src/repl.rs`
- `crates/rapidbyte-cli/src/commands/scaffold.rs`

### Shared types and helpers

- `crates/rapidbyte-types/src/discovery.rs`
- `crates/rapidbyte-types/src/schema.rs`
- `crates/rapidbyte-types/src/catalog.rs`
- `crates/rapidbyte-types/src/lib.rs`
- `crates/rapidbyte-sdk/src/arrow/schema.rs`
- `crates/rapidbyte-sdk/src/prelude.rs`

### Plugins and tests

- `plugins/transforms/sql/src/main.rs`
- `plugins/transforms/validate/src/main.rs`
- `plugins/sources/postgres/src/types.rs`
- `crates/rapidbyte-engine/tests/wasm_runner_integration.rs`
- engine application tests
- scaffold tests

## Outcome

After this change, the codebase will have one plugin contract and one schema
model. Discovery, validation, and runtime flow will all reflect the v7 WIT
contract directly, and the remaining catalog-era compatibility code will be
deleted instead of preserved behind adapters.
