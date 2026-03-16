# RAPIDBYTE-ENGINE — Comprehensive Refactoring Guide

**Breaking Down God Objects · Eliminating Code Smells · SOLID & DRY**
**Naming Conventions · Module Restructuring · Blueprint Standards**

Version 1.0 — March 2026
*No backward compatibility assumed. Forward-looking blueprint.*

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current Architecture Assessment](#2-current-architecture-assessment)
3. [Target Module Structure](#3-target-module-structure)
4. [Breaking Down God Objects](#4-breaking-down-god-objects)
5. [Eliminating Code Smells & Technical Debt](#5-eliminating-code-smells--technical-debt)
6. [Applying SOLID Principles](#6-applying-solid-principles)
7. [DRY Violations & Remediation](#7-dry-violations--remediation)
8. [Naming Convention Standards](#8-naming-convention-standards)
9. [Refactoring Execution Plan](#9-refactoring-execution-plan)
10. [Verification & Acceptance Criteria](#10-verification--acceptance-criteria)

---

## 1. Executive Summary

This document is a comprehensive refactoring guide for the `rapidbyte-engine` crate, the core pipeline orchestration layer of the RapidByte data ingestion platform. The engine is responsible for resolving plugins, loading WASM modules, scheduling parallel stream execution, managing state/checkpoints, and producing timing diagnostics.

The current codebase is functional and well-tested but has accumulated structural debt that will impede velocity as the product scales. The primary issues are concentrated in two files: `orchestrator.rs` (approx. 1,600 lines of production code plus 700 lines of tests) and `runner.rs` (approx. 550 lines), both of which violate the Single Responsibility Principle and contain duplicated patterns.

Since RapidByte is pre-release with no backward compatibility obligation, this refactoring should be executed aggressively. The refactored crate will serve as the architectural blueprint for all other RapidByte crates.

### Guiding Principles

- **No backward compatibility:** rename, restructure, and delete freely.
- **SOLID over convenience:** each struct/function has one reason to change.
- **DRY over copy-paste:** extract shared patterns into typed abstractions.
- **Naming is documentation:** every identifier must self-describe without ambiguity.
- **Blueprint quality:** this crate's conventions define the standard for all RapidByte crates.

---

## 2. Current Architecture Assessment

### 2.1 Module Inventory

The engine currently consists of 11 source modules in a flat structure:

| Module | Lines (approx.) | Responsibility | Assessment |
|--------|-----------------|----------------|------------|
| `orchestrator.rs` | 2,300+ | Plugin resolution, module loading, stream scheduling, execution, finalization, dry-run, check, discover | **God object — needs decomposition** |
| `runner.rs` | 550+ | Source/dest/transform plugin lifecycle (open, run, close), validation, discovery | **Too many roles — extract per-kind runners** |
| `config/types.rs` | 500+ | Pipeline YAML config structs and serde | Healthy — minor naming fixes |
| `config/parser.rs` | 300+ | YAML parsing with secret interpolation | Healthy |
| `config/validator.rs` | 400+ | Semantic config validation | Healthy |
| `result.rs` | 100+ | Pipeline result types and timing structs | Healthy — move timing builders here |
| `execution.rs` | 50+ | ExecutionOptions, DryRunResult, PipelineOutcome | Healthy |
| `resolve.rs` | 200+ | Plugin resolution, manifest validation, sandbox overrides | Healthy |
| `error.rs` | 120+ | PipelineError enum and backoff computation | Healthy |
| `checkpoint.rs` | 120+ | Cursor correlation and persistence | Healthy |
| `autotune.rs` | 80+ | Parallelism auto-tuning decisions | Healthy |

### 2.2 Critical Problems Identified

**Problem 1: `orchestrator.rs` is a God Module.** It currently owns plugin resolution, WASM module loading, stream context construction, parallelism computation, semaphore management, channel wiring, task spawning, dry-run frame collection, destination preflight, stream result aggregation, finalization/checkpoint persistence, DLQ persistence, metrics snapshotting, and timing assembly. This is **14+ distinct responsibilities** in a single file with 8 private structs and 25+ functions.

**Problem 2: `runner.rs` has triple code duplication.** The functions `run_source_stream`, `run_destination_stream`, and `run_transform_stream` each independently build a `ComponentHostState`, create a store, instantiate a linker, call open/run/close, and handle errors. The structural pattern is identical across all three, with only the binding types and summary extraction differing.

**Problem 3: Data clumps in `StreamParams` and `StreamRunContext`.** `StreamParams` carries 15 fields that represent 4 distinct concerns (plugin identity, config, sandbox overrides, channel settings). `StreamRunContext` has 10 fields, mixing module references, identity, and execution context. These flat structs force every consumer to accept all fields even when they only need a subset.

**Problem 4: Inconsistent execution terminology.** The codebase mixes `Run`, `Execute`, `Stream`, and `Pipeline` as prefixes interchangeably (`run_pipeline` calls `execute_pipeline_once` which calls `execute_streams`; `SourceRunResult` vs. `PipelineResult` vs. `StreamResult`). This makes it hard to reason about which layer is which.

---

## 3. Target Module Structure

The refactored crate should decompose the flat layout into a clear hierarchy where each module has a single, well-defined responsibility:

```
crates/rapidbyte-engine/src/
├── lib.rs                    # Public API surface & re-exports
├── config/
│   ├── mod.rs
│   ├── types.rs              # Pipeline YAML config structs
│   ├── parser.rs             # YAML parsing + secret interpolation
│   └── validator.rs          # Semantic config validation
├── plugin/                   # NEW: Plugin lifecycle management
│   ├── mod.rs
│   ├── loader.rs             # WASM module loading (from orchestrator)
│   ├── resolver.rs           # Plugin resolution + manifest (from resolve.rs)
│   └── sandbox.rs            # SandboxOverrides builder (from resolve.rs)
├── pipeline/                 # NEW: Pipeline execution core
│   ├── mod.rs
│   ├── planner.rs            # Stream context construction + autotune
│   ├── scheduler.rs          # Concurrency (semaphores, JoinSet, permits)
│   ├── executor.rs           # Single-stream pipeline execution
│   └── preflight.rs          # Destination DDL preflight logic
├── runner/                   # NEW: Per-kind plugin runners
│   ├── mod.rs                # PluginRunner trait + shared lifecycle
│   ├── source.rs             # Source-specific run logic
│   ├── destination.rs        # Destination-specific run logic
│   ├── transform.rs          # Transform-specific run logic
│   └── validator.rs          # Plugin validation (check command)
├── state/                    # NEW: State management
│   ├── mod.rs
│   ├── checkpoint.rs         # Cursor correlation (from checkpoint.rs)
│   ├── dlq.rs                # DLQ persistence (from dlq.rs)
│   └── finalizer.rs          # Run finalization (from orchestrator)
├── orchestrator.rs           # SLIM: Top-level run_pipeline, check, discover
├── result.rs                 # Pipeline result types + timing builders
├── execution.rs              # ExecutionOptions, PipelineOutcome
├── error.rs                  # PipelineError + backoff
├── progress.rs               # Progress events
└── arrow.rs                  # Arrow IPC utilities
```

This decomposition reduces `orchestrator.rs` from **2,300+ lines to approximately 200 lines** of top-level coordination, with each extracted module owning exactly one responsibility.

---

## 4. Breaking Down God Objects

### 4.1 Decomposing `orchestrator.rs`

The orchestrator currently owns the entire pipeline lifecycle. After refactoring, it should be a thin coordinator that delegates to specialized components:

| Current Location | Responsibility | Target Module | Target Struct/Function |
|-----------------|----------------|---------------|----------------------|
| `execute_pipeline_once` | Pipeline attempt lifecycle | `orchestrator.rs` | `execute_attempt()` (slimmed) |
| `load_modules` | WASM module compilation | `plugin/loader.rs` | `PluginLoader::load_all()` |
| `resolve_plugins` | Plugin path + manifest | `plugin/resolver.rs` | `PluginResolver::resolve()` |
| `build_stream_contexts` | Stream context + autotune | `pipeline/planner.rs` | `PipelinePlanner::plan()` |
| `execute_streams` | Parallel stream dispatch | `pipeline/scheduler.rs` | `StreamScheduler::execute()` |
| stream spawn closures | Single stream pipeline | `pipeline/executor.rs` | `StreamExecutor::run()` |
| destination preflight | DDL preflight loop | `pipeline/preflight.rs` | `run_destination_preflight()` |
| `finalize_run` | Checkpoint + DLQ + stats | `state/finalizer.rs` | `RunFinalizer::finalize()` |
| `collect_dry_run_frames` | Dry-run frame collector | `pipeline/executor.rs` | `DryRunCollector::collect()` |
| `build_source_timing` etc. | Timing assembly | `result.rs` | `SourceTiming::from_snapshot()` |

### 4.2 Decomposing `StreamParams` (Data Clump)

The current `StreamParams` struct is a 15-field data clump. It should be decomposed into cohesive sub-structs:

```rust
// BEFORE: 15 flat fields in StreamParams
struct StreamParams {
    pipeline_name, metric_run_label,
    source_config, dest_config,
    source_plugin_id, source_plugin_version,
    dest_plugin_id, dest_plugin_version,
    source_permissions, dest_permissions,
    source_overrides, dest_overrides,
    transform_overrides,
    compression, channel_capacity,
}

// AFTER: Decomposed into cohesive structs
struct PipelineIdentity {
    name: String,
    metric_run_label: String,
}

struct PluginIdentity {
    id: String,
    version: String,
    config: serde_json::Value,
    permissions: Option<Permissions>,
    overrides: Option<SandboxOverrides>,
}

struct StreamExecutionParams {
    pipeline: PipelineIdentity,
    source: PluginIdentity,
    destination: PluginIdentity,
    transforms: Vec<TransformIdentity>,
    compression: Option<CompressionCodec>,
    channel_capacity: usize,
}
```

### 4.3 Decomposing `AggregatedStreamResults`

`AggregatedStreamResults` has 14 fields mixing summaries, checkpoints, timings, errors, diagnostics, and dry-run data. It should be split into:

- **`ReadWriteTotals`:** `total_read_summary`, `total_write_summary` (aggregated record/byte counters).
- **`TimingMaxima`:** `max_source_duration`, `max_dest_duration`, `max_vm_setup_secs`, `max_recv_secs`, `transform_durations`.
- **`ExecutionDiagnostics`:** `stream_metrics`, `execution_parallelism`, `dry_run_streams`.
- **`StateOutcome`:** `source_checkpoints`, `dest_checkpoints`, `dlq_records`, `final_stats`, `first_error`.

Each sub-struct is consumed by a different downstream function, making ownership boundaries clean.

---

## 5. Eliminating Code Smells & Technical Debt

### 5.1 Unnecessary Wrapper: `run_blocking_infrastructure_task`

This function wraps `tokio::task::spawn_blocking` with a static string context. It adds no meaningful abstraction over standard Tokio and obscures the call site:

```rust
// CURRENT: Unnecessary wrapper
async fn run_blocking_infrastructure_task<T, F>(
    task: F, context: &'static str,
) -> Result<T, PipelineError> { ... }

// REFACTORED: Use spawn_blocking directly with .context()
tokio::task::spawn_blocking(move || create_state_backend(&config))
    .await
    .map_err(|e| PipelineError::infra(e))??
```

**Action:** Delete `run_blocking_infrastructure_task`. Replace all 3 call sites with direct `spawn_blocking` calls using `anyhow::Context` for error messages. The test that validates "runtime-sensitive init/drop" can test `spawn_blocking` directly.

### 5.2 Nested Execution Wrappers

The current call chain is: `run_pipeline` → `execute_pipeline_once` → `execute_streams`. The middle layer (`execute_pipeline_once`) exists solely to wrap the retry loop body, but it takes 8 parameters and is allowed 2 `clippy::too_many_*` suppressions.

**Action:** Inline the retry logic into `run_pipeline` and have it directly call a `PipelineAttempt` struct that owns the attempt lifecycle. This removes one level of nesting and eliminates the 8-parameter function signature.

```rust
// REFACTORED
pub async fn run_pipeline(config, options, ...) -> Result<PipelineOutcome> {
    let mut attempt = PipelineAttempt::new(config, options, ...);
    loop {
        match attempt.execute().await {
            Ok(outcome) => return Ok(outcome),
            Err(e) if e.is_retryable() && attempt.can_retry() => {
                attempt.backoff().await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 5.3 Duplicated Plugin Loading Pattern

In `load_modules`, the source and destination module loading logic is copy-pasted with only the variable names changed:

```rust
// CURRENT: Duplicated for source and destination
let source_wasm_for_load = plugins.source_wasm.clone();
let runtime_for_source = runtime.clone();
let source_load_task = tokio::task::spawn_blocking(move || {
    let load_start = Instant::now();
    let module = runtime_for_source.load_module(&source_wasm_for_load)?;
    let load_ms = load_start.elapsed().as_millis() as u64;
    Ok((module, load_ms))
});
// ... exact same pattern repeated for dest ...
```

**Action:** Extract into a generic function in `plugin/loader.rs`:

```rust
struct LoadedModule {
    component: LoadedComponent,
    load_ms: u64,
}

fn spawn_module_load(
    runtime: Arc<WasmRuntime>,
    wasm_path: PathBuf,
) -> JoinHandle<Result<LoadedModule, PipelineError>> {
    tokio::task::spawn_blocking(move || {
        let start = Instant::now();
        let component = runtime.load_module(&wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        Ok(LoadedModule {
            component,
            load_ms: start.elapsed().as_millis() as u64,
        })
    })
}
```

### 5.4 Duplicated Plugin Runner Pattern

The three runner functions (`run_source_stream`, `run_destination_stream`, `run_transform_stream`) share an identical structural pattern with approximately **70% code overlap**:

| Step | Source | Destination | Transform |
|------|--------|-------------|-----------|
| Build `ComponentHostState` | Identical | Identical | Identical |
| Create store + linker | `source_bindings` | `dest_bindings` | `transform_bindings` |
| Instantiate + call open | Identical pattern | Identical pattern | Identical pattern |
| Serialize config to JSON | Identical | Identical | Identical |
| Build `RunRequest` | `RunPhase::Read` | `RunPhase::Write` | `RunPhase::Transform` |
| Call run + extract summary | `ReadSummary` | `WriteSummary` | `TransformSummary` |
| Send `EndStream` | Yes | No | Yes |
| Call close + handle result | Identical pattern | Identical pattern | Identical pattern |
| Collect checkpoints | Identical | Identical | Identical |

**Action:** Create a `PluginRunner` trait in `runner/mod.rs` that provides the shared lifecycle and lets each kind override only what differs. See Section 6.3 for the trait design.

### 5.5 No-Op Passthrough: `preserve_real_outcome_after_stream_execution`

This function currently does nothing — it accepts a future and `await`s it:

```rust
async fn preserve_real_outcome_after_stream_execution<T, Fut>(
    _cancel_token: &CancellationToken,
    finalize: Fut,
) -> Result<T, PipelineError>
where
    Fut: Future<Output = Result<T, PipelineError>>,
{
    finalize.await
}
```

**Action:** Delete entirely. Inline the future at the call site.

### 5.6 Suppress Annotations as Debt Markers

The codebase has **8 instances** of `#[allow(clippy::too_many_lines)]` and **6 instances** of `#[allow(clippy::too_many_arguments)]`. These are explicit admissions of structural debt. After this refactoring, no function should need these suppressions. If a function triggers these lints after refactoring, it indicates the decomposition was insufficient.

**Target:** Zero clippy suppressions for `too_many_lines` and `too_many_arguments` in the refactored crate.

---

## 6. Applying SOLID Principles

### 6.1 Single Responsibility Principle

Each extracted module should have exactly one reason to change:

| Module | Single Responsibility | Changes When... |
|--------|----------------------|-----------------|
| `plugin/loader.rs` | Compile WASM modules | Wasmtime API changes |
| `plugin/resolver.rs` | Resolve plugin paths + validate manifests | Registry protocol changes |
| `pipeline/planner.rs` | Build stream contexts from config | Partitioning/autotune logic changes |
| `pipeline/scheduler.rs` | Manage concurrency + task dispatch | Parallelism strategy changes |
| `pipeline/executor.rs` | Execute a single stream pipeline | Channel wiring or stage ordering changes |
| `state/finalizer.rs` | Persist run outcomes | State backend interface changes |
| `runner/source.rs` | Run source plugin lifecycle | Source protocol changes |

### 6.2 Open/Closed Principle

The pipeline should be extensible without modifying the core scheduler. Currently, adding a new pipeline stage (e.g., a pre-transform validator) would require modifying the massive `execute_streams` closure. After refactoring, new stages can be added by implementing the `PluginRunner` trait and registering the stage in the executor.

### 6.3 Dependency Inversion — `PluginRunner` Trait

Currently, the orchestrator directly calls `run_source_stream`, `run_destination_stream`, and `run_transform_stream`. This creates a hard dependency on the concrete implementations. Introducing a trait enables testing and extensibility:

```rust
// runner/mod.rs
pub trait PluginRunner: Send + Sync {
    type Summary;

    fn setup_host_state(
        &self,
        ctx: &RunContext,
    ) -> Result<ComponentHostState, PipelineError>;

    fn execute(
        &self,
        ctx: &RunContext,
        host_state: ComponentHostState,
    ) -> Result<RunOutcome<Self::Summary>, PipelineError>;
}

pub struct RunOutcome<S> {
    pub duration_secs: f64,
    pub summary: S,
    pub checkpoints: Vec<Checkpoint>,
}
```

Each concrete runner (`SourceRunner`, `DestinationRunner`, `TransformRunner`) implements the trait, and the `StreamExecutor` operates on `dyn PluginRunner` or generics. This also enables injecting mock runners in integration tests without WASM.

### 6.4 Interface Segregation

`StreamRunContext` currently bundles 10 fields needed by source, destination, and transform runners. Not all fields are relevant to all roles (e.g., `metric_run_label` is unused by validators; `compression` is unused by the discover flow). After decomposition, the `RunContext` struct should carry only what is common, with role-specific fields in the concrete runner.

---

## 7. DRY Violations & Remediation

### 7.1 Inventory of DRY Violations

| Location | Pattern | Occurrences | Remedy |
|----------|---------|-------------|--------|
| `load_modules()` | `spawn_blocking` + time + `load_module` | 2 (source, dest) | Extract `spawn_module_load()` |
| `runner.rs` | build host state + store + linker + open/run/close | 3 (source, dest, transform) | `PluginRunner` trait |
| `runner.rs` | `handle_close_result` pattern | 3 (source, dest, transform) | Shared `close_plugin()` helper |
| `orchestrator.rs` | `PipelineError::Infrastructure(anyhow!())` | 30+ sites | Add `PipelineError::infra()` constructor |
| `orchestrator.rs` | `stats.lock().map_err` poisoned | 4 sites | Extract `lock_or_infra()` helper |
| `orchestrator.rs` | `ensure_not_cancelled` + `tokio::select` | 5 sites | Extract `acquire_permit_cancellable()` |
| `finalize_run()` | `spawn_blocking` for state ops | 4 sites | `state/finalizer.rs` with `BlockingStateOps` helper |
| `validate_plugin()` | Match on `PluginKind` with 80% identical arms | 3 arms | Macro or generic over binding type |

### 7.2 `PipelineError` Convenience Constructors

The pattern `PipelineError::Infrastructure(anyhow::anyhow!("..."))` appears 30+ times. This should be replaced with convenience constructors:

```rust
impl PipelineError {
    /// Shorthand for Infrastructure variant from a string.
    pub fn infra(msg: impl Into<String>) -> Self {
        Self::Infrastructure(anyhow::anyhow!(msg.into()))
    }

    /// Shorthand for Infrastructure from an anyhow::Error.
    pub fn from_infra(err: anyhow::Error) -> Self {
        Self::Infrastructure(err)
    }

    /// Wrap a JoinError from tokio::task::spawn_blocking.
    pub fn task_panicked(context: &str, err: tokio::task::JoinError) -> Self {
        Self::infra(format!("{context} task panicked: {err}"))
    }

    /// Cancelled pipeline error with safe-to-retry metadata.
    pub fn cancelled(message: &str) -> Self {
        let mut error = PluginError::internal("CANCELLED", message);
        error.safe_to_retry = true;
        error.commit_state = Some(CommitState::BeforeCommit);
        Self::Plugin(error)
    }
}
```

### 7.3 Cancellation-Aware Semaphore Acquisition

The pattern of `ensure_not_cancelled` + `tokio::select` on cancellation + semaphore acquisition appears 5 times. Extract it:

```rust
async fn acquire_permit(
    semaphore: &Arc<Semaphore>,
    cancel: &CancellationToken,
    context: &str,
) -> Result<OwnedSemaphorePermit, PipelineError> {
    tokio::select! {
        _ = cancel.cancelled() => {
            Err(PipelineError::cancelled(context))
        }
        permit = semaphore.clone().acquire_owned() => {
            permit.map_err(|e| PipelineError::infra(
                format!("Semaphore closed: {e}")))
        }
    }
}
```

### 7.4 Mutex Lock Helper

The pattern `stats.lock().map_err(|_| PipelineError::Infrastructure(anyhow!("...mutex poisoned")))` appears 4 times:

```rust
fn lock_or_infra<T>(mutex: &Mutex<T>, name: &str) -> Result<MutexGuard<'_, T>, PipelineError> {
    mutex.lock().map_err(|_| PipelineError::infra(format!("{name} mutex poisoned")))
}
```

---

## 8. Naming Convention Standards

These naming rules apply to `rapidbyte-engine` and all future RapidByte crates.

### 8.1 General Rules

| Rule | Convention | Example |
|------|-----------|---------|
| Functions | `snake_case`, verb-first, no `_impl` suffix | `load_module`, `resolve_plugins` |
| Structs | `PascalCase`, noun-phrase, role-specific | `PluginLoader`, `StreamScheduler` |
| Enums | `PascalCase`, variants are adjectives or nouns | `PipelineOutcome::Completed` |
| Constants | `SCREAMING_SNAKE`, semantic name | `MAX_COPY_FLUSH_BYTES` |
| Type aliases | `PascalCase`, describes the contained type | `ProgressSender` |
| Modules | `snake_case`, singular nouns or verb-nouns | `plugin`, `runner`, `checkpoint` |
| Traits | `PascalCase`, `-able` or `-er` suffix | `PluginRunner`, `Retryable` |

### 8.2 Specific Renames

| Current Name | Problem | New Name | Rationale |
|-------------|---------|----------|-----------|
| `run_pipeline` | Ambiguous (run vs execute) | `run_pipeline` (keep) | Top-level public API; 'run' is the user-facing verb |
| `execute_pipeline_once` | `_once` is a flow detail, not a role | `PipelineAttempt::execute` | Single-attempt logic becomes a struct method |
| `execute_streams` | Verb is too generic | `StreamScheduler::dispatch` | 'dispatch' implies concurrency management |
| `run_blocking_infrastructure_task` | Wrapper adding no value | *(deleted)* | Use `spawn_blocking` directly |
| `SourceRunResult` | Inconsistent with `PipelineResult` | `SourceOutcome` | Consistent 'Outcome' suffix across all stages |
| `DestRunResult` | Inconsistent with `PipelineResult` | `DestinationOutcome` | Full word, consistent suffix |
| `TransformRunResult` | Inconsistent with `PipelineResult` | `TransformOutcome` | Consistent suffix |
| `StreamResult` | Too generic | `StreamShardOutcome` | Clarifies it includes partition info |
| `StreamParams` | Data clump | `StreamExecutionParams` | Decomposed sub-structs (see 4.2) |
| `StreamBuild` | Unclear noun | `ExecutionPlan` | Describes what it represents |
| `LoadedTransformModule` | Verbose, leaks 'loaded' state | `TransformModule` | All modules in scope are loaded |
| `LoadedModules` | Verbose | `PluginModules` | Clean container name |
| `AggregatedStreamResults` | Verbose, unclear role | `StreamAggregation` | Concise, clear |
| `StreamTaskCollection` | Unclear | `StreamTaskOutcomes` | Describes content |
| `CollectedTransforms` | Unclear | `TransformStageOutcomes` | Describes content |
| `MetricsRuntime` | Not a runtime | `MetricsSnapshot` (or inline) | It only does snapshots |
| `ProgressTx` | Abbreviation | `ProgressSender` | Full word for clarity |
| `send_progress` | Global helper | `ProgressSender::emit` (method) | Move to a newtype wrapper |
| `collect_dry_run_frames` | Verb-first but it's a blocking fn | `DryRunCollector::collect` | Struct method on `DryRunCollector` |
| `preserve_real_outcome_after_stream_execution` | Extremely verbose | *(inline or remove)* | Currently a no-op passthrough |

### 8.3 Naming Anti-Patterns to Eliminate

**No `_impl` suffixes:** If a function is the implementation, it should have the real name. The 'public' version should be the one with the clear name, not a wrapper calling `_impl`.

**No `_with_something` functions:** Functions like `resolve_workload_plan_with_environment` should take an optional parameter or an `EnvironmentContext` struct and be named `resolve_workload_plan`.

**No `_for_*` suffixes on helpers:** Functions like `finalization_failed_stats` should be `RunStats::for_failure()` as a constructor method.

**No mixing Run/Execute/Stream:** Pick one: 'Execution' is the domain noun (an `ExecutionPlan`, an `ExecutionOutcome`). 'Run' is the user-facing verb (`run_pipeline`). 'Stream' is the data model noun. Never use them interchangeably.

**Strict 'Context' usage:** Only use the word `Context` for structs that are passed down the call stack to provide environmental/ambient data (e.g., `StreamContext` from the protocol layer is correct). Do not use it for one-off parameter bundles (`StreamRunContext` should be renamed to `RunContext` or broken into smaller pieces).

---

## 9. Refactoring Execution Plan

Execute in strict order. Each phase is independently testable and merge-ready.

### Phase 1: Foundation (No Behavior Change)

**Goal:** establish the new module hierarchy and move code without changing any logic.

1. Create the new directory structure: `plugin/`, `pipeline/`, `runner/`, `state/`.
2. Move `resolve_plugins`, `load_and_validate_manifest`, `validate_config_against_schema` to `plugin/resolver.rs`.
3. Move `build_sandbox_overrides` to `plugin/sandbox.rs`.
4. Move `load_modules` to `plugin/loader.rs`. Deduplicate source/dest loading immediately.
5. Move `checkpoint.rs` to `state/checkpoint.rs`. Move `dlq.rs` to `state/dlq.rs`.
6. Move `finalize_run` and `finalize_successful_run_state` to `state/finalizer.rs`.
7. Update `lib.rs` re-exports. All existing tests must pass unchanged.

**Verification:** `cargo test` passes. `cargo clippy` is clean. No public API changes.

### Phase 2: Decompose `orchestrator.rs`

**Goal:** break the god module into the `pipeline/` sub-modules.

1. Extract `build_stream_contexts` + autotune calls into `pipeline/planner.rs` as `PipelinePlanner`.
2. Extract the stream spawn closure from `execute_streams` into `pipeline/executor.rs` as `StreamExecutor`.
3. Extract semaphore + `JoinSet` management into `pipeline/scheduler.rs` as `StreamScheduler`.
4. Extract destination preflight into `pipeline/preflight.rs`.
5. Delete `run_blocking_infrastructure_task`. Replace with direct `spawn_blocking`.
6. Delete `preserve_real_outcome_after_stream_execution` (currently a no-op).
7. Inline `MetricsRuntime` (only wraps two references; add `snapshot_for_run` as a free function or method).

**Verification:** `orchestrator.rs` is under 250 lines. No `clippy::too_many_*` suppressions remain.

### Phase 3: Unify Plugin Runners

**Goal:** eliminate the triple duplication in `runner.rs`.

1. Define the `PluginRunner` trait in `runner/mod.rs`.
2. Extract the shared lifecycle (build host state, create store, instantiate linker, call open, call close) into a `run_plugin_lifecycle` generic function.
3. Implement `SourceRunner` in `runner/source.rs`, `DestinationRunner` in `runner/destination.rs`, `TransformRunner` in `runner/transform.rs`.
4. Extract `validate_plugin` match arms into a generic or macro in `runner/validator.rs`.

**Verification:** No duplicated open/run/close pattern remains. Each runner file is under 150 lines.

### Phase 4: Naming & Polish

**Goal:** apply all renames from Section 8.2 and add convenience constructors.

1. Rename all result structs: `SourceRunResult` → `SourceOutcome`, etc.
2. Decompose `StreamParams` into `PipelineIdentity` + `PluginIdentity` + `StreamExecutionParams`.
3. Rename `StreamBuild` → `ExecutionPlan`, `StreamResult` → `StreamShardOutcome`, etc.
4. Add `PipelineError::infra()`, `PipelineError::task_panicked()`, `PipelineError::cancelled()` constructors.
5. Replace `ProgressTx` type alias with a `ProgressSender` newtype with an `emit()` method.
6. Move `build_source_timing`, `build_dest_timing`, `build_dry_run_result` into `result.rs` as constructors.
7. Move `finalization_failed_stats` to `RunStats::for_failure()` constructor.
8. Final `cargo clippy --all-targets -- -D warnings` pass with zero suppressions for structural lints.

**Verification:** Full test suite green. `cargo doc` generates clean, navigable documentation. All struct/function names are self-descriptive.

---

## 10. Verification & Acceptance Criteria

### 10.1 Quantitative Targets

| Metric | Before | Target |
|--------|--------|--------|
| `orchestrator.rs` production lines | ~1,600 | < 250 |
| `runner.rs` production lines | ~550 | < 100 (`mod.rs`) + 3 × 150 (per-kind) |
| `clippy::too_many_lines` suppressions | 8 | 0 |
| `clippy::too_many_arguments` suppressions | 6 | 0 |
| Max function parameters | 10 (`execute_pipeline_once`) | ≤ 5 |
| Max struct fields | 15 (`StreamParams`) | ≤ 6 |
| Duplicated code blocks (>10 lines) | 5+ | 0 |
| Module count in `src/` | 11 flat files | ~20 files in 5 directories |

### 10.2 Qualitative Criteria

- Every module can be described in a single sentence without using the word 'and'.
- Every public function name reads as a self-documenting verb-noun phrase.
- No struct requires more than 6 fields (decompose into sub-structs if exceeded).
- No function requires more than 5 parameters (use a context/params struct if exceeded).
- Adding a new pipeline stage (e.g., audit logger) requires zero changes to existing files.
- The crate can be used as a reference implementation for naming and structure in all other RapidByte crates.

### 10.3 Test Strategy

- All existing tests must pass after each phase without modification to test logic (only import paths change).
- New unit tests for each extracted module (`PluginLoader`, `PipelinePlanner`, `StreamScheduler`, `RunFinalizer`).
- Integration test in `tests/pipeline_integration.rs` continues to pass as the primary regression gate.
- Property tests in `tests/policy_proptest.rs` continue to pass.

---

## 11. Execution Learnings — Transferable Principles

*Added post-execution (March 2026). These lessons emerged from implementing the refactoring and apply to all RapidByte crates.*

### 11.1 Crate Responsibility Boundaries

**Each crate must own construction of its own types.** If crate A defines `BackendKind::Sqlite | Postgres`, crate A should provide the `open_backend(kind, connection)` factory. Consumer crates should never import concrete implementation types (`SqliteStateBackend`, `PostgresStateBackend`) — they use the factory and get `Arc<dyn Trait>` back. This was missed initially: the engine imported concrete state backend types and duplicated construction logic. Fixed by adding `open_backend()` to `rapidbyte-state`.

**Test for responsibility leaks:** If you need to import a type from crate B just to construct a type from crate B, the factory belongs in crate B.

### 11.2 Naming Hygiene

**Remove jargon prefixes.** Config fields like `pin_parallelism` confused users — "pin" is an internal concept. Use `parallelism` when context is clear (inside an `autotune` config section). Always ask: would a new team member understand this field name without reading the implementation?

**Spell out abbreviations in struct fields.** `vm_setup_secs` → `wasm_instantiation_secs`, `recv_secs` → `frame_receive_secs`. Abbreviations save keystrokes but cost comprehension. Local variables can abbreviate; struct fields and JSON keys should not.

**Include units in field names.** `_secs`, `_ms`, `_nanos`, `_bytes`. Prevents the most common timing bug: passing milliseconds where seconds were expected.

**Remove redundant prefixes in nested contexts.** `PipelineNetworkPermissions` → `NetworkPermissions` when nested inside `PipelinePermissions`. The parent provides the context.

### 11.3 Consolidation vs. Extraction

**Single-use helpers don't need their own module.** `autotune.rs` (139 lines, consumed by one function in `planner.rs`) was eliminated by inlining into planner. The test: if only one caller exists and the helper is small, inline it.

**Consolidate related types into one file.** `execution.rs` (input options) and `result.rs` (output types) were separate files for no good reason. Merged into `outcome.rs` — all pipeline operation types in one place.

**Don't consolidate different concerns.** `progress.rs` (real-time events + `ProgressSender` with logic) stays separate from `outcome.rs` (pure data types). Different rates of change = different files.

### 11.4 Error Handling Patterns

**Provide convenience constructors for repeated error patterns.** `PipelineError::infra("message")` replaced 30+ occurrences of `PipelineError::Infrastructure(anyhow::anyhow!("message"))`. Also: `task_panicked(context, err)`, `cancelled(message)`, `lock_or_infra(mutex, name)`.

**Distinguish fire-and-forget from propagated error paths.** When consolidating functions that handle both success and failure paths, don't accidentally make the success path fire-and-forget. The original code had `complete_run_status(...).await?` on success and `let _ = complete_run_status(...)` on failure — the consolidation incorrectly unified them as fire-and-forget. Fixed by splitting into `mark_run_complete() -> Result` (success path) and `mark_run_failed()` (failure path, logs and suppresses).

**Typed errors over string matching.** From the secrets hardening: replace `err.to_string().contains("timeout")` with `SecretResolutionError::Transient(...)`. Retry classification should be driven by structured signals, not substring searches.

### 11.5 Config Schema Evolution

**Use `deny_unknown_fields` when renaming config keys.** Without it, old YAML keys parse successfully but settings are silently dropped. With `#[serde(deny_unknown_fields)]`, old keys produce a clear parse error telling users to update their config.

**Never add aliases for backward compatibility in pre-release.** Aliases accumulate as tech debt. In pre-release: rename aggressively, fail loudly on old names, update all consumers in the same PR.

### 11.6 Cross-Crate Refactoring Discipline

**Update ALL consumers in the same change.** When renaming struct fields that appear in JSON output (e.g., benchmark metrics), the producer and consumer must update together. The engine refactoring renamed timing fields but initially missed the benchmark parser, causing silent metric loss. Always grep the entire workspace.

**Pre-push hooks catch what tests don't.** Workspace-wide `cargo clippy -- -D warnings` caught dead code, visibility issues, and doc-lint failures that crate-level tests passed. The hook is the last line of defense before CI.

**Worktrees isolate safely.** All refactoring was done in a git worktree (`.worktrees/engine-refactoring`). The main repo stayed untouched throughout — rust-analyzer diagnostics from the main repo were noise, not real errors. Verify work in the worktree, not the IDE.

### 11.7 Trust Boundary Clarity

From the secrets hardening: **don't let resolution functions mix trust levels.** The controller dispatch path (high trust) was using the same `substitute_variables()` function as local execution (low trust), causing env vars from the submitter's shell to leak into the controller trust boundary. Fix: separate entry points for separate trust levels (`substitute_secret_refs_only` vs `substitute_env_vars_only`).

**Make rollback durability mandatory at trust boundaries.** If a secret dispatch fails, the rollback state must be persisted durably before returning. Logging the error and continuing creates invisible inconsistency between in-memory and persisted state.

---

*— End of Document —*
