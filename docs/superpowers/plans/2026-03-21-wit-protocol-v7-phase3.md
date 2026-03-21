# WIT Protocol v7 — Phase 3: Engine Orchestration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add v7 lifecycle orchestration to the engine — spec retrieval, prerequisites, schema negotiation, apply, teardown, and cancellation propagation.

**Architecture:** Extend the `PluginRunner` trait with v7 methods (spec, prerequisites, apply, teardown), implement them in `WasmPluginRunner`, wire them into the `check_pipeline` and `run_pipeline` flows. Add schema negotiation to `check_pipeline` (source → transforms → destination constraint reconciliation). Propagate the `cancel_flag` to `ComponentHostState` during runs.

**Tech Stack:** Rust, async/await, tokio, wasmtime

**Spec:** `docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

**Depends on:** Phase 2 complete (SDK + runtime compile clean, 875 tests pass)

---

## Refactoring scope (integrated into tasks)

- Remove any dead validation paths from `check.rs` that don't use the new schema negotiation
- Clean up `check_pipeline` return type — `CheckResult` currently has separate manifest/config/validation fields; simplify to a single status + validation reports per stage
- Remove old `PluginInfo` handling from `run_pipeline` if any remains
- Wire cancellation token to `ComponentHostState.cancel_flag` (currently cancel only kills the outer loop — v7 makes it cooperative inside WASM)

---

### Task 1: Add v7 lifecycle methods to PluginRunner trait

**Files:**
- Modify: `crates/rapidbyte-engine/src/domain/ports/runner.rs`

- [ ] **Step 1: Read runner.rs to understand current PluginRunner trait and param types**

- [ ] **Step 2: Add new param types for v7 lifecycle**

```rust
/// Parameters for plugin spec retrieval.
pub struct SpecParams {
    pub wasm_path: PathBuf,
    pub plugin_id: String,
    pub plugin_version: String,
}

/// Parameters for prerequisites check.
pub struct PrerequisitesParams {
    pub wasm_path: PathBuf,
    pub plugin_id: String,
    pub plugin_version: String,
    pub config: serde_json::Value,
    pub permissions: Option<Permissions>,
    pub sandbox_overrides: Option<SandboxOverrides>,
}

/// Parameters for apply (resource provisioning).
pub struct ApplyParams {
    pub wasm_path: PathBuf,
    pub plugin_id: String,
    pub plugin_version: String,
    pub config: serde_json::Value,
    pub streams: Vec<StreamContext>,
    pub dry_run: bool,
    pub permissions: Option<Permissions>,
    pub sandbox_overrides: Option<SandboxOverrides>,
}

/// Parameters for teardown (resource cleanup).
pub struct TeardownParams {
    pub wasm_path: PathBuf,
    pub plugin_id: String,
    pub plugin_version: String,
    pub config: serde_json::Value,
    pub streams: Vec<String>,
    pub reason: String,
    pub permissions: Option<Permissions>,
    pub sandbox_overrides: Option<SandboxOverrides>,
}
```

- [ ] **Step 3: Extend PluginRunner trait**

Add methods with default implementations that return "not supported" for backward compat during transition:

```rust
async fn spec(&self, params: &SpecParams) -> Result<PluginSpec, PipelineError> {
    Err(PipelineError::unsupported("spec not implemented"))
}

async fn prerequisites(&self, params: &PrerequisitesParams) -> Result<PrerequisitesReport, PipelineError> {
    Ok(PrerequisitesReport::passed())
}

async fn apply(&self, params: &ApplyParams) -> Result<ApplyReport, PipelineError> {
    Ok(ApplyReport::noop())
}

async fn teardown(&self, params: &TeardownParams) -> Result<TeardownReport, PipelineError> {
    Ok(TeardownReport::noop())
}
```

Also update `ValidateParams` to include optional upstream schema:
```rust
pub struct ValidateParams {
    // ... existing fields ...
    pub upstream_schema: Option<StreamSchema>,  // NEW: for schema negotiation
}
```

- [ ] **Step 4: Verify compiles**

Run: `cargo check -p rapidbyte-engine`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(engine): add v7 lifecycle methods to PluginRunner trait (spec, prerequisites, apply, teardown)"
```

---

### Task 2: Implement v7 lifecycle methods in WasmPluginRunner

**Files:**
- Modify: `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`

- [ ] **Step 1: Read wasm_runner.rs to understand the pattern for calling WIT exports**

The existing methods (validate, discover, run_source) follow a pattern:
1. Load WASM component
2. Create store + linker
3. Instantiate the component
4. Call lifecycle methods (open → work → close)
5. Convert results

- [ ] **Step 2: Implement `spec()`**

Follow the same pattern but simpler — `spec()` is sessionless (no open/close needed):
1. Load WASM component
2. Instantiate
3. Call `spec()` export
4. Convert WIT `PluginSpec` to SDK `PluginSpec`
5. Return

- [ ] **Step 3: Implement `prerequisites()`**

1. Load WASM, instantiate
2. Call `open(config_json)` → session
3. Call `prerequisites(session)` → PrerequisitesReport
4. Call `close(session)`
5. Convert and return

- [ ] **Step 4: Implement `apply()`**

1. Load WASM, instantiate
2. Call `open(config_json)` → session
3. Build WIT `ApplyRequest` from params (convert StreamContexts)
4. Call `apply(session, request)` → ApplyReport
5. Call `close(session)`
6. Convert and return

- [ ] **Step 5: Implement `teardown()`**

1. Load WASM, instantiate
2. Call `open(config_json)` → session
3. Build WIT `TeardownRequest` from params
4. Call `teardown(session, request)` → TeardownReport
5. Call `close(session)`
6. Convert and return

- [ ] **Step 6: Update `validate_plugin()` to pass upstream schema**

The existing validate method needs to pass `ValidateParams.upstream_schema` through to the WIT `validate(session, option<stream-schema>)` call. Convert the SDK `StreamSchema` to the WIT-generated `StreamSchema` type.

- [ ] **Step 7: Verify compiles and tests pass**

Run: `cargo check -p rapidbyte-engine && cargo test -p rapidbyte-engine`

- [ ] **Step 8: Commit**

```bash
git commit -m "feat(engine): implement v7 lifecycle in WasmPluginRunner (spec, prerequisites, apply, teardown)"
```

---

### Task 3: Add schema negotiation to check_pipeline

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/check.rs`

This is the key v7 orchestration feature — the host validates the entire pipeline schema chain at check time.

- [ ] **Step 1: Read check.rs to understand the current flow**

- [ ] **Step 2: Add prerequisites phase to check flow**

After manifest and config validation, before stream-level validation:

```rust
// Prerequisites check (source)
let source_prerequisites = ctx.runner.prerequisites(&PrerequisitesParams {
    wasm_path: source_resolved.wasm_path.clone(),
    plugin_id: source_resolved.manifest.id.clone(),
    // ...
}).await?;

if !source_prerequisites.passed {
    // Report failed checks with fix hints
}

// Prerequisites check (destination)
let dest_prerequisites = ctx.runner.prerequisites(&PrerequisitesParams { ... }).await?;
```

- [ ] **Step 3: Add schema negotiation chain**

After discovery, for each stream in the pipeline:

```rust
// 1. Get source schema from discovery
let source_schema = discovered_streams.get(stream_name)
    .map(|ds| ds.schema.clone());

// 2. Thread schema through transforms
let mut current_schema = source_schema;
for transform in &pipeline.transforms {
    let report = ctx.runner.validate_plugin(&ValidateParams {
        upstream_schema: current_schema.clone(),
        // ...
    }).await?;

    // Transform's output_schema becomes next stage's input
    if let Some(output) = report.output_schema {
        current_schema = Some(output);
    }
}

// 3. Validate destination with final schema
let dest_report = ctx.runner.validate_plugin(&ValidateParams {
    upstream_schema: current_schema.clone(),
    // ...
}).await?;

// 4. Reconcile field requirements
if let Some(requirements) = &dest_report.field_requirements {
    if let Some(schema) = &current_schema {
        let reconciliation = reconcile_schema(schema, requirements);
        if !reconciliation.passed {
            // Report mismatches
        }
    }
}
```

- [ ] **Step 4: Implement `reconcile_schema()` helper**

Create a helper function (can live in check.rs or a new `schema_negotiation.rs`):

```rust
struct ReconciliationResult {
    passed: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
}

fn reconcile_schema(
    schema: &StreamSchema,
    requirements: &[FieldRequirement],
) -> ReconciliationResult {
    let mut errors = vec![];
    let mut warnings = vec![];

    for req in requirements {
        let field_exists = schema.fields.iter().any(|f| f.name == req.field_name);
        match req.constraint {
            FieldConstraint::FieldRequired if !field_exists => {
                errors.push(format!("Required field '{}' missing: {}", req.field_name, req.reason));
            }
            FieldConstraint::FieldForbidden if field_exists => {
                errors.push(format!("Forbidden field '{}' present: {}", req.field_name, req.reason));
            }
            FieldConstraint::TypeIncompatible => {
                errors.push(format!("Type incompatible for '{}': {}", req.field_name, req.reason));
            }
            FieldConstraint::FieldRecommended if !field_exists => {
                warnings.push(format!("Recommended field '{}' missing: {}", req.field_name, req.reason));
            }
            _ => {} // FieldOptional or field exists — fine
        }
    }

    ReconciliationResult {
        passed: errors.is_empty(),
        errors,
        warnings,
    }
}
```

- [ ] **Step 5: Write tests for schema reconciliation**

Test the `reconcile_schema` function with various scenarios:
- All fields satisfied → passed
- Required field missing → error
- Forbidden field present → error
- Recommended field missing → warning only
- Type incompatible → error

- [ ] **Step 6: Verify compiles and tests pass**

Run: `cargo test -p rapidbyte-engine`

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(engine): add schema negotiation to check_pipeline

- Prerequisites check for source and destination
- Schema chain: source → transforms → destination
- Field constraint reconciliation with clear error messages
- Tests for reconciliation logic"
```

---

### Task 4: Wire cancellation to ComponentHostState

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/run.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- Modify: `crates/rapidbyte-engine/src/domain/ports/runner.rs`

Currently, cancellation only kills the outer retry loop via `CancellationToken`. For v7, the `cancel_flag: Arc<AtomicBool>` on `ComponentHostState` must be wired to the pipeline's cancellation token so plugins can poll `is_cancelled()`.

- [ ] **Step 1: Add `cancel_flag` to run param types**

Add `pub cancel_flag: Arc<AtomicBool>` to `SourceRunParams`, `TransformRunParams`, `DestinationRunParams`.

- [ ] **Step 2: Create cancel flag in run_pipeline and pass to params**

In `run_pipeline`, create a shared `Arc<AtomicBool>` and set up a task that watches the `CancellationToken` and sets the flag:

```rust
let cancel_flag = Arc::new(AtomicBool::new(false));
let flag_clone = cancel_flag.clone();
let cancel_clone = cancel.clone();
tokio::spawn(async move {
    cancel_clone.cancelled().await;
    flag_clone.store(true, Ordering::Relaxed);
});
```

Pass `cancel_flag.clone()` to each run params struct.

- [ ] **Step 3: Wire cancel_flag into ComponentHostState in WasmPluginRunner**

In the wasm_runner helpers that create `ComponentHostState`, set the `cancel_flag` from params:

```rust
host_state.set_cancel_flag(params.cancel_flag.clone());
```

(The `set_cancel_flag` method was added in Phase 2's host_state.rs changes.)

- [ ] **Step 4: Verify compiles and tests pass**

Run: `cargo check -p rapidbyte-engine && cargo test -p rapidbyte-engine`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(engine): wire cancellation token to ComponentHostState cancel_flag

Plugins can now poll is_cancelled() during execution for cooperative shutdown."
```

---

### Task 5: Add apply phase to run_pipeline

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/run.rs`

- [ ] **Step 1: Read run_pipeline to find the right insertion point**

Apply should run AFTER plugin resolution but BEFORE stream execution. It's a one-time setup step.

- [ ] **Step 2: Add apply phase**

After plugin resolution (source, destination, transforms resolved), before the stream loop:

```rust
// Apply phase — provision resources if needed
if let Some(dest_resolved) = &dest_resolved {
    let apply_report = ctx.runner.apply(&ApplyParams {
        wasm_path: dest_resolved.wasm_path.clone(),
        plugin_id: dest_resolved.manifest.id.clone(),
        plugin_version: dest_resolved.manifest.version.clone(),
        config: pipeline.destination.config.clone(),
        streams: stream_contexts.clone(), // All streams
        dry_run: false,
        permissions: dest_resolved.manifest.permissions.clone().into(),
        sandbox_overrides: None,
    }).await?;

    for action in &apply_report.actions {
        ctx.progress.report_info(&format!("Apply: {}", action.description)).await;
    }
}
```

Do the same for source if needed (e.g., creating replication slots).

- [ ] **Step 3: Verify compiles**

Run: `cargo check -p rapidbyte-engine`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(engine): add apply phase to run_pipeline for resource provisioning"
```

---

### Task 6: Add `check --apply` support to CLI

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/check.rs`
- Modify: `crates/rapidbyte-engine/src/application/check.rs`

- [ ] **Step 1: Add `--apply` and `--dry-run` flags to check CLI**

The check command should accept `--apply` to run the apply phase after validation passes. `--dry-run` shows what would be done without executing.

- [ ] **Step 2: Extend check_pipeline to optionally run apply**

Add an `apply: bool` and `dry_run: bool` parameters to `check_pipeline` (or a `CheckOptions` struct). If `apply` is true and all validations pass, call `runner.apply()` for source and destination.

- [ ] **Step 3: Display apply results in CLI output**

Print each `ApplyAction` with its description and DDL (if present).

- [ ] **Step 4: Verify compiles**

Run: `cargo check --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(cli): add --apply and --dry-run flags to check command

rapidbyte check --apply pipeline.yaml provisions destination tables
rapidbyte check --apply --dry-run pipeline.yaml shows what would be done"
```

---

### Task 7: Add teardown support

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/mod.rs` (or create `teardown.rs`)
- Modify: `crates/rapidbyte-cli/src/main.rs` (add teardown subcommand)

- [ ] **Step 1: Create teardown use case**

```rust
pub async fn teardown_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
    reason: &str,
) -> Result<TeardownReport, PipelineError> {
    let source_resolved = ctx.resolver.resolve(&pipeline.source.plugin).await?;
    let dest_resolved = ctx.resolver.resolve(&pipeline.destination.plugin).await?;

    let stream_names: Vec<String> = pipeline.source.streams.iter()
        .map(|s| s.name.clone())
        .collect();

    let mut all_actions = vec![];

    // Source teardown
    let source_report = ctx.runner.teardown(&TeardownParams {
        wasm_path: source_resolved.wasm_path,
        config: pipeline.source.config.clone(),
        streams: stream_names.clone(),
        reason: reason.to_string(),
        // ...
    }).await?;
    all_actions.extend(source_report.actions);

    // Destination teardown
    let dest_report = ctx.runner.teardown(&TeardownParams {
        wasm_path: dest_resolved.wasm_path,
        config: pipeline.destination.config.clone(),
        streams: stream_names,
        reason: reason.to_string(),
        // ...
    }).await?;
    all_actions.extend(dest_report.actions);

    Ok(TeardownReport { actions: all_actions })
}
```

- [ ] **Step 2: Add `rapidbyte teardown` CLI command**

Add a new subcommand:
```
rapidbyte teardown pipeline.yaml [--reason pipeline_deleted|reconfigure|stream_removed]
```

- [ ] **Step 3: Verify compiles**

Run: `cargo check --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: add teardown command for resource cleanup

rapidbyte teardown pipeline.yaml — cleans up replication slots, staging tables
Supports --reason flag: pipeline_deleted, reconfigure, stream_removed"
```

---

### Task 8: Clean up dead orchestration code and verify

**Files:**
- Various engine files

- [ ] **Step 1: Search for remaining v6 patterns**

Grep for:
- `PluginInfo` references in engine/CLI (should be gone or replaced by PluginSpec)
- `SchemaHint` references
- `Catalog` (old wrapper) references
- Old checkpoint handling that doesn't use transactional model
- `state_cas` references

- [ ] **Step 2: Remove/update any found references**

- [ ] **Step 3: Run full workspace check and tests**

```bash
cargo check --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
cargo clippy --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform -- -D warnings
```

All must pass clean.

- [ ] **Step 4: Commit any fixes**

```bash
git commit -m "refactor(engine): remove dead v6 orchestration code"
```

Phase 3 complete. The engine now orchestrates the full v7 lifecycle:
- `spec()` for plugin introspection
- `prerequisites()` for environment readiness
- Schema negotiation chain (source → transforms → destination)
- `apply()` for resource provisioning (via `check --apply` and at run start)
- `run()` with cooperative cancellation propagated to plugins
- `teardown()` for resource cleanup (via `rapidbyte teardown` command)
