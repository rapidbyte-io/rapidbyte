# Eliminate `clippy::too_many_arguments` Suppressions

**Date:** 2026-03-15
**Branch:** refactoring1
**Scope:** Engine runner, controller store, E2E harness

## Problem

16 functions suppress `clippy::too_many_arguments` across the codebase. The worst offenders are in the engine runner (14-15 params each) where 10 params are repeated across `run_source_stream`, `run_destination_stream`, and `run_transform_stream`. This makes the API harder to understand and refactors riskier.

## Decision: What to Fix, What to Leave

| Area | Action | Reason |
|------|--------|--------|
| Engine runner (3 fns) | **Fix** | 10 shared params repeated 3x, called in a loop |
| Controller store (4 sites) | **Fix** | Clear payload grouping, trait + impls + caller |
| E2E harness (5 fns) | **Fix** | Collapse entire wrapper chain into one function |
| CLI commands (5 fns) | **Skip** | 1:1 clap flag pass-through, single call site each |
| Orchestrator internals (3 fns) | **Skip** | Private lifecycle functions, params are stage-specific |
| Plugin ddl/mod.rs (1 fn) | **Skip** | Plugin-side code, separate target |

## Design

### 1. `StreamRunContext` (engine runner)

**File:** `crates/rapidbyte-engine/src/runner.rs`

```rust
pub struct StreamRunContext<'a> {
    pub module: &'a LoadedComponent,
    pub state_backend: Arc<dyn StateBackend>,
    pub pipeline_name: &'a str,
    pub metric_run_label: &'a str,
    pub plugin_id: &'a str,
    pub plugin_version: &'a str,
    pub stream_ctx: &'a StreamContext,
    pub permissions: Option<&'a Permissions>,
    pub compression: Option<CompressionCodec>,
    pub overrides: Option<&'a SandboxOverrides>,
}
```

10 fields — all shared across the three runner functions. `stats: Arc<Mutex<RunStats>>` is NOT included because `run_transform_stream` does not take it.

**Before/After:**

| Function | Before | After |
|----------|--------|-------|
| `run_source_stream` | 14 params | `ctx` + 4 (`sender`, `source_config`, `stats`, `on_emit`) |
| `run_destination_stream` | 14 params | `ctx` + 4 (`receiver`, `dlq_records`, `dest_config`, `stats`) |
| `run_transform_stream` | 15 params | `ctx` + 5 (`receiver`, `sender`, `dlq_records`, `transform_index`, `transform_config`) |

The orchestrator builds one `StreamRunContext` per stream in the `execute_streams` loop and passes it to each runner. All three `#[allow(clippy::too_many_arguments)]` suppressions removed.

### 2. `PreviewData` (controller store)

**File:** `crates/rapidbyte-controller/src/store/mod.rs`

```rust
pub struct PreviewData<'a> {
    pub flight_endpoint: &'a str,
    pub ticket: &'a [u8],
    pub streams: &'a [PreviewStreamEntry],
    pub created_at: SystemTime,
    pub ttl: Duration,
}
```

**Before:** `upsert_preview(&self, run_id, task_id, flight_endpoint, ticket, streams, created_at, ttl)` — 8 params
**After:** `upsert_preview(&self, run_id: &str, task_id: &str, preview: &PreviewData<'_>)` — 4 params

`run_id` and `task_id` remain separate as the primary key.

**Affected sites (4):**
1. `DurableMetadataStore` trait definition (line ~155)
2. `MetadataStore` inherent method (line ~397)
3. `impl DurableMetadataStore for MetadataStore` (line ~541)
4. `FailingMetadataStore` test impl (line ~1714)

**Affected caller (1):**
- `persist_preview_record` in `crates/rapidbyte-controller/src/state.rs` (line ~220)

### 3. `PipelinePolicies` (E2E harness)

**File:** `tests/e2e/src/harness/mod.rs`

```rust
pub struct PipelinePolicies<'a> {
    pub sync_mode: &'a str,
    pub write_mode: &'a str,
    pub compression: Option<&'a str>,
    pub on_data_error: Option<&'a str>,
    pub schema_evolution_block: Option<&'a str>,
    pub autotune: Option<&'a AutotuneOptions>,
}
```

**Collapse the entire wrapper chain** — 5 functions become 1:

| Remove | Callers redirect to |
|--------|-------------------|
| `run_pipeline(schemas, sync_mode, write_mode, state_db_path)` | `run_pipeline(schemas, state_db_path, policies)` with defaults |
| `run_pipeline_with_compression(...)` | same, with `compression` set |
| `run_pipeline_with_autotune(...)` | same, with `autotune` set |
| `run_pipeline_with_policies(...)` | same |
| `run_pipeline_with_policies_and_autotune(...)` | becomes the sole `run_pipeline` |

**After:** Single function `run_pipeline(&self, schemas: &SchemaPair, state_db_path: &Path, policies: &PipelinePolicies<'_>)` — 4 params.

All callers in test files (`mode_matrix.rs`, `autotune.rs`, etc.) construct `PipelinePolicies` directly with the fields they need and `..Default::default()` for the rest.

Both `#[allow(clippy::too_many_arguments)]` suppressions removed.

## What Gets Removed

- 7 `#[allow(clippy::too_many_arguments)]` suppressions (3 runner + 2 store + 2 harness)
- 4 wrapper functions in E2E harness (`run_pipeline`, `run_pipeline_with_compression`, `run_pipeline_with_autotune`, `run_pipeline_with_policies`)

## What Stays

- CLI command `execute`/`build_config` functions (5 suppressions) — trivial dispatchers, single call site
- Orchestrator `execute_pipeline_once`/`execute_streams`/`finalize_run` (3 suppressions) — private lifecycle functions with stage-specific params
- Plugin `ddl/mod.rs` (1 suppression) — separate crate, out of scope

## Testing

No new tests needed. This is a signature refactor — all existing tests continue to exercise the same code paths. `cargo test --workspace --all-targets` must pass with zero `too_many_arguments` suppressions on the targeted functions.
