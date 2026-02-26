# Engine Crate Refactor Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Decompose the 1289-line `orchestrator.rs` god module, fix naming inconsistencies, and simplify internal data flow — without changing any public behavior or benchmark output.

**Architecture:** Extract connector resolution/manifest/state-backend logic into `resolve.rs`, move public result types into `result.rs`, rename `errors.rs` → `error.rs` for naming consistency, and reduce clone sprawl in stream execution with a shared `Arc<StreamParams>`.

**Tech Stack:** Rust, Cargo workspace, clippy::pedantic

---

## First-Principles Analysis

### What's wrong

| Problem | Where | Impact |
|---------|-------|--------|
| God module | `orchestrator.rs` (1289 lines) | Hard to navigate, 7 private structs, 3 `#[allow(too_many_lines)]` |
| Wrong module for result types | `PipelineResult`/`CheckResult` defined in `runner.rs` | Consumers (CLI) import from `runner` which is a misnomer |
| Naming inconsistency | `errors.rs` (plural) vs every sibling crate's `error.rs` (singular) | Breaks project convention |
| Useless wrapper type | `StreamError` wraps `PipelineError` adding zero value | Noise, extra conversions |
| Clone sprawl | `execute_streams` clones ~15 variables per stream task | Verbose, error-prone |
| Mixed concerns | `orchestrator.rs` does resolution, loading, execution, finalization, check, discover, state creation | Single Responsibility violation |

### What's fine (don't touch)

- `arrow.rs` — clean, focused, well-tested (203 lines)
- `checkpoint.rs` — clean, focused, well-tested (220 lines)
- `dlq.rs` — clean, minimal (38 lines)
- `config/` — clean 3-file module, good separation (types/parser/validator)
- Backoff logic in errors — clean, well-tested
- Runner functions — the Wasm lifecycle pattern differs enough per role that DRY extraction would add complexity without benefit

### Target module structure

```
src/
  lib.rs             Module declarations + re-exports (updated)
  config/            Unchanged — types, parser, validator
  error.rs           Renamed from errors.rs — PipelineError + backoff
  result.rs          NEW — PipelineResult, CheckResult, timing structs
  resolve.rs         NEW — connector resolution, manifest, schema validation, state backend
  orchestrator.rs    SLIMMED — pipeline run/check/discover, stream execution
  runner.rs          SLIMMED — Wasm connector runners only
  arrow.rs           Unchanged
  checkpoint.rs      Unchanged
  dlq.rs             Unchanged
```

Line count targets: orchestrator.rs drops from 1289 → ~900, runner.rs drops from 763 → ~680.

---

### Task 1: Rename `errors.rs` → `error.rs`

**Files:**
- Rename: `src/errors.rs` → `src/error.rs`
- Modify: `src/lib.rs`
- Modify: `src/orchestrator.rs`
- Modify: `src/runner.rs` (if it imports from errors)

**Step 1: Rename the file**

```bash
cd crates/rapidbyte-engine
git mv src/errors.rs src/error.rs
```

**Step 2: Update `lib.rs` module declaration and re-export**

Change:
```rust
pub mod errors;
```
to:
```rust
pub mod error;
```

Change re-export:
```rust
pub use errors::PipelineError;
```
to:
```rust
pub use error::PipelineError;
```

**Step 3: Update internal imports**

In `orchestrator.rs`, change:
```rust
use crate::errors::{compute_backoff, PipelineError};
```
to:
```rust
use crate::error::{compute_backoff, PipelineError};
```

In `runner.rs`, change:
```rust
use crate::errors::PipelineError;
```
to:
```rust
use crate::error::PipelineError;
```

**Step 4: Update the module doc comment in `error.rs`**

No change needed — the doc says "Pipeline error model" which is correct.

**Step 5: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor(engine): rename errors.rs to error.rs for naming consistency"
```

---

### Task 2: Create `result.rs` — extract public pipeline result types

**Files:**
- Create: `src/result.rs`
- Modify: `src/runner.rs` (remove moved types)
- Modify: `src/lib.rs` (add module, update re-exports)
- Modify: `src/orchestrator.rs` (update imports)
- Modify: `crates/rapidbyte-cli/src/commands/run.rs` (update imports)
- Modify: `crates/rapidbyte-cli/src/commands/check.rs` (update imports)
- Modify: `tests/pipeline_integration.rs` (if it imports these types)

**Step 1: Create `src/result.rs`**

Move these types from `runner.rs` to `result.rs`:
- `PipelineCounts`
- `SourceTiming`
- `DestTiming`
- `PipelineResult`
- `CheckResult`

The file should look like:

```rust
//! Pipeline execution result types and timing breakdowns.

use rapidbyte_types::error::ValidationResult;

/// Aggregate record/byte counts for a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Source connector timing breakdown.
#[derive(Debug, Clone, Default)]
pub struct SourceTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
    pub arrow_encode_secs: f64,
    pub emit_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
}

/// Destination connector timing breakdown.
#[derive(Debug, Clone, Default)]
pub struct DestTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
    pub vm_setup_secs: f64,
    pub recv_secs: f64,
    pub recv_nanos: u64,
    pub decompress_nanos: u64,
    pub recv_count: u64,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source: SourceTiming,
    pub dest: DestTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub duration_secs: f64,
    pub wasm_overhead_secs: f64,
    pub retry_count: u32,
}

/// Result of a pipeline check.
#[derive(Debug)]
pub struct CheckResult {
    pub source_validation: ValidationResult,
    pub destination_validation: ValidationResult,
    pub transform_validations: Vec<ValidationResult>,
    pub state_ok: bool,
}
```

**Step 2: Remove moved types from `runner.rs`**

Delete the `PipelineCounts`, `SourceTiming`, `DestTiming`, `PipelineResult`, `CheckResult` definitions from `runner.rs`. Add `use crate::result::{...}` where needed. Keep `SourceRunResult`, `DestRunResult`, `TransformRunResult` in `runner.rs` (they're `pub(crate)` internal types).

**Step 3: Update `lib.rs`**

Add module declaration:
```rust
pub mod result;
```

Update re-exports:
```rust
pub use result::{CheckResult, PipelineResult};
```

Remove the old re-exports from runner:
```rust
// Remove: pub use runner::{CheckResult, PipelineResult};
```

**Step 4: Update `orchestrator.rs` imports**

Change:
```rust
use crate::runner::{
    ..., CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming,
};
```
to:
```rust
use crate::result::{CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming};
use crate::runner::{...};
```

**Step 5: Verify CLI imports still work**

The CLI uses `rapidbyte_engine::runner::{CheckResult, PipelineResult}` or the top-level re-export. Since we update the re-export, the CLI should still compile. Check and update if needed.

**Step 6: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 7: Commit**

```bash
git add -A
git commit -m "refactor(engine): extract pipeline result types into result.rs"
```

---

### Task 3: Create `resolve.rs` — extract connector resolution and state backend

**Files:**
- Create: `src/resolve.rs`
- Modify: `src/orchestrator.rs` (remove extracted functions)
- Modify: `src/lib.rs` (add module)

**Step 1: Create `src/resolve.rs`**

Move from `orchestrator.rs`:
- `ResolvedConnectors` struct
- `resolve_connectors()` fn
- `load_and_validate_manifest()` fn
- `validate_config_against_schema()` fn
- `create_state_backend()` fn
- `check_state_backend()` fn
- `build_sandbox_overrides()` fn

All functions stay `pub(crate)` visibility except `create_state_backend` which can be `pub(crate)`.

The file header:
```rust
//! Connector resolution, manifest validation, and state backend creation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rapidbyte_types::manifest::{ConnectorManifest, Permissions, ResourceLimits};
use rapidbyte_types::wire::{ConnectorRole, ProtocolVersion};

use crate::config::types::{
    parse_byte_size, PipelineConfig, PipelineLimits, PipelinePermissions, StateBackendKind,
};
use crate::error::PipelineError;
use rapidbyte_runtime::{resolve_min_limit, SandboxOverrides};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
```

Move the `test_create_state_backend_custom_path` test from orchestrator to resolve.

**Step 2: Remove extracted items from `orchestrator.rs`**

Delete the moved structs and functions. Update orchestrator to import from `crate::resolve`:

```rust
use crate::resolve::{
    build_sandbox_overrides, check_state_backend, create_state_backend,
    load_and_validate_manifest, resolve_connectors, validate_config_against_schema,
    ResolvedConnectors,
};
```

**Step 3: Update `lib.rs`**

Add:
```rust
pub(crate) mod resolve;
```

(`pub(crate)` since these are internal functions, not public API.)

**Step 4: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor(engine): extract resolve.rs for connector resolution and state backend"
```

---

### Task 4: Remove `StreamError` wrapper from `orchestrator.rs`

**Files:**
- Modify: `src/orchestrator.rs`

**Step 1: Remove `StreamError` struct and `From` impl**

Delete:
```rust
struct StreamError {
    error: PipelineError,
}

impl From<PipelineError> for StreamError {
    fn from(error: PipelineError) -> Self {
        Self { error }
    }
}
```

**Step 2: Update `execute_streams` return types and error handling**

Change the stream join handle type from:
```rust
Vec<tokio::task::JoinHandle<Result<StreamResult, StreamError>>>
```
to:
```rust
Vec<tokio::task::JoinHandle<Result<StreamResult, PipelineError>>>
```

Replace all `StreamError { error: ... }` with just the `PipelineError` value directly. For example:
```rust
// Before:
Err(StreamError {
    error: PipelineError::Infrastructure(anyhow::anyhow!("...")),
})
// After:
Err(PipelineError::Infrastructure(anyhow::anyhow!("...")))
```

And where `.map_err(|e| StreamError { error: e })` is used:
```rust
// Before:
.map_err(|e| StreamError { error: e })?;
// After (just remove the wrapping):
?;
```

Update the result destructuring:
```rust
// Before:
Err(StreamError { error }) => {
    if first_error.is_none() {
        first_error = Some(error);
    }
}
// After:
Err(error) => {
    if first_error.is_none() {
        first_error = Some(error);
    }
}
```

**Step 3: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor(engine): remove StreamError wrapper, use PipelineError directly"
```

---

### Task 5: Introduce `StreamParams` to reduce clone sprawl

**Files:**
- Modify: `src/orchestrator.rs`

**Step 1: Define `StreamParams` struct**

Add at the top of orchestrator (near other private structs):

```rust
/// Shared parameters for per-stream pipeline execution.
/// Wrapped in `Arc` so each spawned stream task gets a cheap clone.
struct StreamParams {
    pipeline_name: String,
    source_config: serde_json::Value,
    dest_config: serde_json::Value,
    source_connector_id: String,
    source_connector_version: String,
    dest_connector_id: String,
    dest_connector_version: String,
    source_permissions: Option<Permissions>,
    dest_permissions: Option<Permissions>,
    source_overrides: Option<SandboxOverrides>,
    dest_overrides: Option<SandboxOverrides>,
    transform_overrides: Vec<Option<SandboxOverrides>>,
    compression: Option<CompressionCodec>,
    channel_capacity: usize,
}
```

**Step 2: Build `Arc<StreamParams>` in `execute_streams` before the stream loop**

Replace the 15+ individual variable declarations with a single struct construction:

```rust
let params = Arc::new(StreamParams {
    pipeline_name: config.pipeline.clone(),
    source_config: config.source.config.clone(),
    dest_config: config.destination.config.clone(),
    source_connector_id,
    source_connector_version,
    dest_connector_id,
    dest_connector_version,
    source_permissions: connectors.source_permissions.clone(),
    dest_permissions: connectors.dest_permissions.clone(),
    source_overrides,
    dest_overrides,
    transform_overrides,
    compression: stream_build.compression,
    channel_capacity,
});
```

**Step 3: Update the per-stream loop to clone `Arc<StreamParams>` instead of individual fields**

In the `for stream_ctx in &stream_build.stream_ctxs` loop, replace:
```rust
let pipeline_name = pipeline_name.clone();
let source_config = source_config.clone();
let dest_config = dest_config.clone();
// ... 12 more clones ...
```

With:
```rust
let p = params.clone();
```

Then inside the spawned task, access fields as `p.pipeline_name`, `p.source_config`, etc.

**Step 4: Update the runner calls to use `StreamParams` fields**

In `run_source_stream` call, change `&pipeline_name` → `&p.pipeline_name`, etc.
In `run_destination_stream` call, same pattern.
In `run_transform_stream` calls, same pattern.

**Step 5: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 6: Commit**

```bash
git add -A
git commit -m "refactor(engine): introduce StreamParams to consolidate per-stream cloning"
```

---

### Task 6: Update `lib.rs` re-exports and crate doc table

**Files:**
- Modify: `src/lib.rs`

**Step 1: Update the module doc table**

```rust
//! Pipeline orchestration engine for Rapidbyte.
//!
//! Wires together config parsing, validation, connector runners,
//! and the state backend to execute data pipelines.
//!
//! # Crate structure
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `config`       | Pipeline YAML config types, parsing, validation |
//! | `orchestrator` | Pipeline execution, retry, stream dispatch |
//! | `runner`       | Individual connector runners (source, dest, transform) |
//! | `resolve`      | Connector resolution, manifest validation, state backend |
//! | `result`       | Pipeline result types and timing breakdowns |
//! | `error`        | Pipeline error types and retry policy |
//! | `checkpoint`   | Cursor correlation and persistence |
//! | `arrow`        | Arrow IPC encode/decode utilities |
```

**Step 2: Verify all re-exports are correct**

Ensure:
```rust
pub mod arrow;
pub mod checkpoint;
pub mod config;
pub(crate) mod dlq;
pub mod error;
pub mod orchestrator;
pub(crate) mod resolve;
pub mod result;
pub mod runner;

pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use config::validator::validate_pipeline;
pub use error::PipelineError;
pub use orchestrator::{check_pipeline, discover_connector, run_pipeline};
pub use result::{CheckResult, PipelineResult};
```

**Step 3: Run tests**

```bash
cargo test --workspace
```

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor(engine): update lib.rs re-exports and crate doc table"
```

---

### Task 7: Clippy pedantic pass and dead code audit

**Files:**
- Potentially any file in `crates/rapidbyte-engine/src/`

**Step 1: Run clippy pedantic**

```bash
cargo clippy --workspace -- -W clippy::pedantic 2>&1 | grep "rapidbyte-engine"
```

**Step 2: Fix any new warnings**

Common issues after refactoring:
- Missing `#[must_use]` on new public functions
- Unused imports from moved code
- Inline format args
- `map_or` vs `map_or_else`

**Step 3: Check for unused items**

```bash
cargo test --workspace 2>&1 | grep "warning.*unused"
```

Remove any dead code left behind from the extraction.

**Step 4: Run tests**

```bash
cargo test --workspace
```

Expected: All 249 tests pass, 0 clippy warnings.

**Step 5: Commit**

```bash
git add -A
git commit -m "fix(engine): resolve clippy pedantic warnings after refactor"
```

---

### Task 8: Final verification and CLAUDE.md update

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Full test suite**

```bash
cargo test --workspace
```

Expected: All 249 tests pass.

**Step 2: Full clippy + doc check**

```bash
cargo clippy --workspace -- -W clippy::pedantic
cargo doc --workspace --no-deps 2>&1 | grep warning
```

Expected: 0 warnings on both.

**Step 3: Verify line counts improved**

```bash
wc -l crates/rapidbyte-engine/src/*.rs crates/rapidbyte-engine/src/config/*.rs
```

Expected: `orchestrator.rs` < 950 lines, `runner.rs` < 700 lines.

**Step 4: Update CLAUDE.md if module structure description needs updating**

The "Key Architecture" section references `orchestrator.rs` — update to mention the new module split.

**Step 5: Commit**

```bash
git add -A
git commit -m "docs: update CLAUDE.md with engine module structure"
```
