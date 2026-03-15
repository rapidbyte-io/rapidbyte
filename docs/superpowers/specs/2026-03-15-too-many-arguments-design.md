# Eliminate `clippy::too_many_arguments` Suppressions

**Date:** 2026-03-15
**Branch:** refactoring1
**Scope:** Engine runner, controller store, E2E harness

## Problem

14 functions suppress `clippy::too_many_arguments`. The worst offenders are in the engine runner (11-13 params each) where the same 8 params are repeated across `run_source_stream`, `run_destination_stream`, and `run_transform_stream`. This makes the API harder to understand and refactors riskier.

## Decision: What to Fix, What to Leave

| Area | Action | Reason |
|------|--------|--------|
| Engine runner (3 fns) | **Fix** | 8 shared params repeated 3x, called in a loop |
| Controller store (2 fns) | **Fix** | Clear payload grouping, trait + impl |
| E2E harness (2 fns) | **Fix** | Wrapper elimination, clean test API |
| CLI commands (5 fns) | **Skip** | 1:1 clap flag pass-through, single call site each |
| Orchestrator internals (3 fns) | **Skip** | Private lifecycle functions, params are stage-specific |

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
    pub stats: Arc<Mutex<RunStats>>,
}
```

**Before/After:**

| Function | Before | After |
|----------|--------|-------|
| `run_source_stream` | 11 params | `ctx` + 3 (sender, source_config, permissions) |
| `run_destination_stream` | 11 params | `ctx` + 3 (receiver, dlq_records, dest_config) |
| `run_transform_stream` | 13 params | `ctx` + 5 (receiver, sender, dlq_records, transform_index, transform_config) |

The orchestrator builds one `StreamRunContext` per stream in the `execute_streams` loop and passes it to each runner.

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

`run_id` and `task_id` remain separate as the primary key. Applies to both the `MetadataStore` trait and the `InMemoryMetadataStore` impl.

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

**Before:** Two functions — `run_pipeline_with_policies` (8 params, wrapper) and `run_pipeline_with_policies_and_autotune` (9 params, impl).
**After:** Single function `run_pipeline(&self, schemas, state_db_path, policies)` — 4 params. The wrapper is eliminated; callers set `autotune: None` directly.

All callers of `run_pipeline_with_policies` updated to use `run_pipeline` with `autotune: None`.

## What Gets Removed

- All `#[allow(clippy::too_many_arguments)]` on the 7 functions above
- `run_pipeline_with_policies` wrapper function in E2E harness
- Associated `#[allow(clippy::too_many_arguments)]` annotations

## What Stays

- CLI command `execute`/`build_config` functions (5 suppressions) — trivial dispatchers, single call site
- Orchestrator `execute_pipeline_once`/`execute_streams`/`finalize_run` (3 suppressions) — private lifecycle functions with stage-specific params

## Testing

No new tests needed. This is a signature refactor — all existing tests continue to exercise the same code paths. `cargo test --workspace` must pass with zero `too_many_arguments` suppressions on the targeted functions.
