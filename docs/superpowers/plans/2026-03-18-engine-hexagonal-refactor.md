# Engine Hexagonal Refactor Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `rapidbyte-engine` using hexagonal architecture, extract misplaced concerns to proper crates, eliminate `rapidbyte-state`.

**Architecture:** Three-layer hexagonal (domain/application/adapter) matching the controller pattern. Config types move to `rapidbyte-pipeline-config`, Arrow IPC to `rapidbyte-types`, `StateBackend` trait to `rapidbyte-types`. Postgres-only with SQLx migrations. All ports are trait objects in `EngineContext` DI container.

**Tech Stack:** Rust, async_trait, sqlx (Postgres), tokio, wasmtime, arrow, thiserror, anyhow

**Spec:** `docs/superpowers/specs/2026-03-18-engine-hexagonal-refactor-design.md`

---

## Phase 1: Crate Extractions

These tasks move code out of `rapidbyte-engine` to where it belongs. Each task must leave the workspace compiling.

### Task 1: Move Arrow IPC to rapidbyte-types

**Files:**
- Copy: `crates/rapidbyte-engine/src/arrow.rs` → `crates/rapidbyte-types/src/arrow.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs` (add `pub mod arrow`)
- Modify: `crates/rapidbyte-types/Cargo.toml` (add `arrow` dependency)
- Modify: `crates/rapidbyte-engine/src/arrow.rs` (re-export from types)
- Modify: `crates/rapidbyte-engine/src/lib.rs` (keep re-export for now)

**Why re-export temporarily:** Engine consumers still import `rapidbyte_engine::arrow::*`. We add the canonical location in `types` first, then update consumers in Task 4.

- [ ] **Step 1:** Read `crates/rapidbyte-engine/src/arrow.rs` to understand all public functions and their dependencies
- [ ] **Step 2:** Add `arrow` dependency to `crates/rapidbyte-types/Cargo.toml` — match the version used in engine's `Cargo.toml`
- [ ] **Step 3:** Copy `crates/rapidbyte-engine/src/arrow.rs` to `crates/rapidbyte-types/src/arrow.rs`. Adjust any `use` paths that reference engine-internal types (there should be none — these are standalone codec functions)
- [ ] **Step 4:** Add `pub mod arrow;` to `crates/rapidbyte-types/src/lib.rs`
- [ ] **Step 5:** Replace `crates/rapidbyte-engine/src/arrow.rs` contents with re-exports:
  ```rust
  //! Re-exported from `rapidbyte_types::arrow`. Use that crate directly.
  pub use rapidbyte_types::arrow::*;
  ```
- [ ] **Step 6:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 7:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 8:** Commit: `refactor: move Arrow IPC codec to rapidbyte-types`

### Task 2: Move StateBackend trait to rapidbyte-types

**Files:**
- Copy: `crates/rapidbyte-state/src/backend.rs` → `crates/rapidbyte-types/src/state_backend.rs`
- Copy: `crates/rapidbyte-state/src/error.rs` → `crates/rapidbyte-types/src/state_error.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs` (add modules + re-exports)
- Modify: `crates/rapidbyte-state/src/backend.rs` (re-export from types)
- Modify: `crates/rapidbyte-state/src/lib.rs` (re-export)
- Modify: `crates/rapidbyte-runtime/src/host_state.rs` (update import)

- [ ] **Step 1:** Read `crates/rapidbyte-state/src/backend.rs` and `crates/rapidbyte-state/src/error.rs` to understand the trait and error types
- [ ] **Step 2:** Copy `backend.rs` → `crates/rapidbyte-types/src/state_backend.rs`. Update `use` paths: the trait already references `rapidbyte_types::*` types, so imports simplify to `crate::*`
- [ ] **Step 3:** Copy the `StateError` type from `error.rs` → `crates/rapidbyte-types/src/state_error.rs`. Add only what's needed for the trait's return type
- [ ] **Step 4:** Add modules and re-exports to `crates/rapidbyte-types/src/lib.rs`
- [ ] **Step 5:** Update `crates/rapidbyte-state/src/backend.rs` to re-export: `pub use rapidbyte_types::state_backend::*;`
- [ ] **Step 6:** Update `crates/rapidbyte-state/src/error.rs` to re-export the error type from types
- [ ] **Step 7:** Update `crates/rapidbyte-runtime/src/host_state.rs` import: `use rapidbyte_types::state_backend::StateBackend;`
- [ ] **Step 8:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 9:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 10:** Commit: `refactor: move StateBackend trait to rapidbyte-types`

### Task 3: Move config types, parser, and validator to rapidbyte-pipeline-config

**Files:**
- Copy: `crates/rapidbyte-engine/src/config/types.rs` → `crates/rapidbyte-pipeline-config/src/types.rs`
- Copy: `crates/rapidbyte-engine/src/config/parser.rs` → `crates/rapidbyte-pipeline-config/src/parser.rs`
- Copy: `crates/rapidbyte-engine/src/config/validator.rs` → `crates/rapidbyte-pipeline-config/src/validator.rs`
- Modify: `crates/rapidbyte-pipeline-config/src/lib.rs` (add modules + re-exports)
- Modify: `crates/rapidbyte-pipeline-config/Cargo.toml` (add dependencies: serde, serde_yaml, jsonschema, regex)
- Modify: `crates/rapidbyte-engine/src/config/mod.rs` (re-export from pipeline-config)
- Modify: config types to simplify `StateConfig` (remove backend field, connection required)

**Note:** The config types reference types from `rapidbyte-types` (e.g., `SyncMode`, `WriteMode`). Verify all referenced types exist in `rapidbyte-types` before moving.

- [ ] **Step 1:** Read `crates/rapidbyte-engine/src/config/types.rs`, `parser.rs`, `validator.rs` — catalog all external dependencies
- [ ] **Step 2:** Add needed dependencies to `crates/rapidbyte-pipeline-config/Cargo.toml` (serde, serde_yaml, jsonschema, regex, rapidbyte-types, etc.)
- [ ] **Step 3:** Copy `types.rs` → `crates/rapidbyte-pipeline-config/src/types.rs`. Simplify `StateConfig`: remove `backend: StateBackendKind` field, make `connection: String` required. Remove `StateBackendKind` enum. **Critical:** Remove the `pub use rapidbyte_state::BackendKind as StateBackendKind` import — do NOT add `rapidbyte-state` as a dependency of `pipeline-config`. Update all `use` paths
- [ ] **Step 4:** Copy `parser.rs` → `crates/rapidbyte-pipeline-config/src/parser.rs`. The existing substitution functions already live in this crate, so consolidate the parsing pipeline. Update `use` paths
- [ ] **Step 5:** Copy `validator.rs` → `crates/rapidbyte-pipeline-config/src/validator.rs`. Update `use` paths. **Add validation:** reject `state.backend: sqlite` configurations with a clear error message. Enforce `state.connection` as a required field
- [ ] **Step 6:** Add modules and public re-exports to `crates/rapidbyte-pipeline-config/src/lib.rs`:
  ```rust
  pub mod types;
  pub mod parser;
  pub mod validator;
  pub use types::PipelineConfig;
  pub use parser::parse_pipeline;
  pub use validator::validate_pipeline;
  ```
- [ ] **Step 7:** Update `crates/rapidbyte-engine/src/config/mod.rs` to re-export:
  ```rust
  pub use rapidbyte_pipeline_config::types;
  pub use rapidbyte_pipeline_config::parser;
  pub use rapidbyte_pipeline_config::validator;
  pub use rapidbyte_pipeline_config::*;
  ```
- [ ] **Step 8:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 9:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 10:** Commit: `refactor: move config types, parser, validator to rapidbyte-pipeline-config`

### Task 4: Update all consumer imports to canonical locations

**Files:**
- Modify: `crates/rapidbyte-cli/src/**/*.rs` — update config + arrow imports
- Modify: `crates/rapidbyte-agent/src/**/*.rs` — update config imports
- Modify: `crates/rapidbyte-dev/src/repl.rs` — update arrow import
- Modify: `benchmarks/src/**/*.rs` — update config + arrow imports
- Modify: `tests/e2e/src/**/*.rs` — update config imports
- Modify: consumer `Cargo.toml` files — add `rapidbyte-pipeline-config` dependency where needed

**Important:** Do NOT delete the engine's `config/` re-export shim or `arrow.rs` in this task. The engine's internal modules (orchestrator.rs, pipeline/, runner/) still reference `crate::config::*` and `crate::arrow::*`. The re-export shims stay until Task 25 (cleanup) when the old modules are also removed.

- [ ] **Step 1:** Search for all `use rapidbyte_engine::config` imports in **external** consumer crates (CLI, agent, benchmarks, e2e). Update each to `use rapidbyte_pipeline_config::*`
- [ ] **Step 2:** Search for all `use rapidbyte_engine::arrow` imports in **external** consumer crates. Update each to `use rapidbyte_types::arrow::*`
- [ ] **Step 3:** Add `rapidbyte-pipeline-config` as dependency in CLI, agent, benchmarks, e2e `Cargo.toml` files where not already present. Note: engine already has this dependency
- [ ] **Step 4:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 5:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 6:** Commit: `refactor: update external consumer imports to canonical crate locations`

---

## Phase 2: Engine Domain Layer

Build the new domain layer alongside the existing engine code. Nothing references it yet — it just needs to compile.

### Task 5: Create domain port traits

**Files:**
- Create: `crates/rapidbyte-engine/src/domain/mod.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/mod.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/runner.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/resolver.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/cursor.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/run_record.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/dlq.rs`
- Create: `crates/rapidbyte-engine/src/domain/ports/metrics.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (add `pub mod domain`)
- Modify: `crates/rapidbyte-engine/Cargo.toml` (promote `async-trait` from dev-dependency to regular dependency)

**Reference:** Controller ports at `crates/rapidbyte-controller/src/domain/ports/`

**Note:** `async-trait` is currently only a dev-dependency in the engine's `Cargo.toml`. Port traits use `#[async_trait]` in production code, so it must be a regular dependency. Also note that `BackoffClass` already exists in `rapidbyte_types::error` — the domain layer should re-export or reference it from there, not redefine it.

- [ ] **Step 1:** Promote `async-trait` from `[dev-dependencies]` to `[dependencies]` in `crates/rapidbyte-engine/Cargo.toml`
- [ ] **Step 2:** Create directory structure: `src/domain/` and `src/domain/ports/`
- [ ] **Step 2:** Create `src/domain/ports/runner.rs` with `PluginRunner` trait. Define param structs: `SourceRunParams`, `TransformRunParams`, `DestinationRunParams`, `ValidateParams`, `DiscoverParams`. Define outcome structs: `SourceOutcome`, `TransformOutcome`, `DestinationOutcome`. Reference the spec (lines 85-93) for the trait definition. Use `#[async_trait]`
- [ ] **Step 3:** Create `src/domain/ports/resolver.rs` with `PluginResolver` trait. Define `ResolvedPlugin` struct (wasm path, manifest, permissions). Reference spec (lines 100-109)
- [ ] **Step 4:** Create `src/domain/ports/cursor.rs` with `CursorRepository` trait. Reference spec (lines 119-127). Import `PipelineId`, `StreamName`, `CursorState` from `rapidbyte_types::state`
- [ ] **Step 5:** Create `src/domain/ports/run_record.rs` with `RunRecordRepository` trait. Reference spec (lines 129-133). Import `RunStatus`, `RunStats` from `rapidbyte_types::state`
- [ ] **Step 6:** Create `src/domain/ports/dlq.rs` with `DlqRepository` trait. Reference spec (lines 135-138). Import `DlqRecord` from `rapidbyte_types::envelope`
- [ ] **Step 7:** Create `src/domain/ports/metrics.rs` with `MetricsSnapshot` trait. Reference spec (lines 155-158). Define `RunMetricsSnapshot` struct
- [ ] **Step 8:** Create `src/domain/ports/mod.rs` — re-export all traits and types
- [ ] **Step 9:** Create `src/domain/mod.rs` — `pub mod ports;`
- [ ] **Step 10:** Add `pub mod domain;` to `src/lib.rs`
- [ ] **Step 11:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 12:** Commit: `feat(engine): add domain port traits`

### Task 6: Create domain error types

**Files:**
- Create: `crates/rapidbyte-engine/src/domain/error.rs`
- Modify: `crates/rapidbyte-engine/src/domain/mod.rs` (add `pub mod error`)

**Reference:** Controller errors at `crates/rapidbyte-controller/src/domain/error.rs` and `crates/rapidbyte-controller/src/application/error.rs`

- [ ] **Step 1:** Write tests for `PipelineError` in `error.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      #[test]
      fn infra_error_is_not_retryable() {
          let err = PipelineError::infra("db down");
          assert!(err.retry_params().is_none());
      }
      #[test]
      fn plugin_error_extracts_retry_params() {
          let plugin_err = PluginError { retryable: true, safe_to_retry: true, /* ... */ };
          let err = PipelineError::Plugin(plugin_err);
          let params = err.retry_params().unwrap();
          assert!(params.retryable);
      }
      #[test]
      fn cancelled_is_not_retryable() {
          let err = PipelineError::Cancelled;
          assert!(err.retry_params().is_none());
      }
  }
  ```
- [ ] **Step 2:** Run test to verify it fails: `cargo test -p rapidbyte-engine domain::error`
- [ ] **Step 3:** Implement `PipelineError`, `RepositoryError`, `RetryParams` per spec (lines 165-202). Use `thiserror` for `Display`/`Error` impls. Map `PluginError` fields to `RetryParams` in `retry_params()`. Derive `BackoffClass` from `PluginError::category`
- [ ] **Step 4:** Run tests: `cargo test -p rapidbyte-engine domain::error` — expect PASS
- [ ] **Step 5:** Commit: `feat(engine): add domain error types`

### Task 7: Create retry policy

**Files:**
- Create: `crates/rapidbyte-engine/src/domain/retry.rs`
- Modify: `crates/rapidbyte-engine/src/domain/mod.rs` (add `pub mod retry`)

**Reference:** Existing backoff logic at `crates/rapidbyte-engine/src/error.rs` (functions `compute_backoff`, `is_retryable`)

- [ ] **Step 1:** Write comprehensive tests for `RetryPolicy`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      #[test]
      fn gives_up_when_not_retryable() { ... }
      #[test]
      fn gives_up_when_not_safe_to_retry() { ... }
      #[test]
      fn retries_with_fast_backoff() { ... }
      #[test]
      fn retries_with_normal_backoff() { ... }
      #[test]
      fn retries_with_slow_backoff() { ... }
      #[test]
      fn exponential_backoff_doubles_each_attempt() { ... }
      #[test]
      fn backoff_capped_at_60_seconds() { ... }
      #[test]
      fn respects_retry_after_hint() { ... }
      #[test]
      fn gives_up_after_max_attempts() { ... }
  }
  ```
- [ ] **Step 2:** Run tests to verify they fail: `cargo test -p rapidbyte-engine domain::retry`
- [ ] **Step 3:** Implement `BackoffClass`, `RetryDecision`, `RetryPolicy` per spec (lines 209-227). Port the backoff computation from `crates/rapidbyte-engine/src/error.rs` — base values: Fast=100ms, Normal=1s, Slow=5s. Formula: `base_ms * 2^(attempt-1)`, capped at 60s
- [ ] **Step 4:** Run tests: `cargo test -p rapidbyte-engine domain::retry` — expect PASS
- [ ] **Step 5:** Commit: `feat(engine): add retry policy domain logic`

### Task 8: Create outcome types

**Files:**
- Create: `crates/rapidbyte-engine/src/domain/outcome.rs`
- Modify: `crates/rapidbyte-engine/src/domain/mod.rs` (add `pub mod outcome`)

**Reference:** Existing outcomes at `crates/rapidbyte-engine/src/outcome.rs`

- [ ] **Step 1:** Create `src/domain/outcome.rs` with all outcome types per spec (lines 233-248): `PipelineOutcome`, `PipelineResult`, `PipelineCounts`, `SourceTiming`, `DestTiming`, `StreamResult`, `DryRunResult`, `DryRunStreamResult`, `CheckResult`, `CheckComponentStatus`, `CheckStatus`, `ExecutionOptions`. Port field definitions from existing `crates/rapidbyte-engine/src/outcome.rs`
- [ ] **Step 2:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 3:** Commit: `feat(engine): add domain outcome types`

### Task 9: Create progress types and ProgressReporter trait

**Files:**
- Create: `crates/rapidbyte-engine/src/domain/progress.rs`
- Modify: `crates/rapidbyte-engine/src/domain/mod.rs` (add `pub mod progress`)

- [ ] **Step 1:** Create `src/domain/progress.rs` with `Phase`, `ProgressEvent` enums and `ProgressReporter` trait per spec (lines 252-261, 145-148)
- [ ] **Step 2:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 3:** Commit: `feat(engine): add progress types and ProgressReporter trait`

---

## Phase 3: Engine Application Layer

### Task 10: Create EngineContext and EngineConfig

**Files:**
- Create: `crates/rapidbyte-engine/src/application/mod.rs`
- Create: `crates/rapidbyte-engine/src/application/context.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (add `pub mod application`)
- Modify: `crates/rapidbyte-engine/Cargo.toml` (add `tokio-util` for `CancellationToken` if not present)

- [ ] **Step 1:** Create `src/application/context.rs` with `EngineConfig` and `EngineContext` per spec (lines 268-284). All fields are `Arc<dyn Trait>` for port traits
- [ ] **Step 2:** Create `src/application/mod.rs`: `pub mod context;`
- [ ] **Step 3:** Add `pub mod application;` to `src/lib.rs`
- [ ] **Step 4:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 5:** Commit: `feat(engine): add EngineContext DI container`

### Task 11: Create testing infrastructure

**Files:**
- Create: `crates/rapidbyte-engine/src/application/testing.rs`
- Modify: `crates/rapidbyte-engine/src/application/mod.rs` (add cfg-gated module)
- Modify: `crates/rapidbyte-engine/Cargo.toml` (add `test-support` feature)

**Reference:** Controller testing at `crates/rapidbyte-controller/src/application/testing.rs`

- [ ] **Step 1:** Add to `Cargo.toml`:
  ```toml
  [features]
  test-support = []
  ```
- [ ] **Step 2:** Add to `src/application/mod.rs`:
  ```rust
  #[cfg(any(test, feature = "test-support"))]
  pub mod testing;
  ```
- [ ] **Step 3:** Create `src/application/testing.rs` with all fakes:
  - `FakePluginRunner` — `VecDeque` for enqueued results per method
  - `FakePluginResolver` — `HashMap<String, ResolvedPlugin>`
  - `FakeCursorRepository` — `HashMap<(PipelineId, StreamName), CursorState>`
  - `FakeRunRecordRepository` — `HashMap` + counter
  - `FakeDlqRepository` — `Vec<DlqRecord>`
  - `FakeProgressReporter` — `Vec<ProgressEvent>` for assertions
  - `FakeMetricsSnapshot` — returns empty/fixed snapshots
  - `TestContext` struct per spec (lines 343-354)
  - `fake_context()` factory function
- [ ] **Step 4:** Implement all `#[async_trait] impl Trait for Fake*` blocks. Follow the controller pattern: `Mutex<HashMap>` for storage, clone values on read
- [ ] **Step 5:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 6:** Write a test that verifies `fake_context()` produces a usable `TestContext`:
  ```rust
  #[tokio::test]
  async fn fake_context_is_wired_correctly() {
      let tc = fake_context();
      // Verify all fields are accessible
      tc.runner.enqueue_source(Ok(SourceOutcome::default()));
      let result = tc.ctx.runner.run_source(&SourceRunParams::default()).await;
      assert!(result.is_ok());
  }
  ```
- [ ] **Step 7:** Run `cargo test -p rapidbyte-engine application::testing` — expect PASS
- [ ] **Step 8:** Commit: `feat(engine): add testing infrastructure with fakes`

### Task 12: Create discover_plugin use case

**Files:**
- Create: `crates/rapidbyte-engine/src/application/discover.rs`
- Modify: `crates/rapidbyte-engine/src/application/mod.rs`

- [ ] **Step 1:** Write test:
  ```rust
  #[tokio::test]
  async fn discover_resolves_and_discovers() {
      let tc = fake_context();
      tc.resolver.register("source-pg", resolved_source());
      tc.runner.enqueue_discover(Ok(vec![discovered_stream("users")]));
      let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
      assert_eq!(streams.len(), 1);
      assert_eq!(streams[0].name, "users");
  }
  ```
- [ ] **Step 2:** Run test to verify it fails
- [ ] **Step 3:** Implement `discover_plugin()` per spec (lines 329-333). It's simple: resolve → discover
- [ ] **Step 4:** Run test — expect PASS
- [ ] **Step 5:** Commit: `feat(engine): add discover_plugin use case`

### Task 13: Create check_pipeline use case

**Files:**
- Create: `crates/rapidbyte-engine/src/application/check.rs`
- Modify: `crates/rapidbyte-engine/src/application/mod.rs`

- [ ] **Step 1:** Write tests:
  ```rust
  #[tokio::test]
  async fn check_validates_all_components() { ... }
  #[tokio::test]
  async fn check_returns_error_on_resolution_failure() { ... }
  #[tokio::test]
  async fn check_reports_validation_status_per_component() { ... }
  ```
- [ ] **Step 2:** Run tests to verify they fail
- [ ] **Step 3:** Implement `check_pipeline()` per spec (lines 318-324). Resolve source + dest + transforms, call `runner.validate_plugin()` for each, collect `CheckComponentStatus` results into `CheckResult`
- [ ] **Step 4:** Run tests — expect PASS
- [ ] **Step 5:** Commit: `feat(engine): add check_pipeline use case`

### Task 14: Create run_pipeline use case

**Files:**
- Create: `crates/rapidbyte-engine/src/application/run.rs`
- Modify: `crates/rapidbyte-engine/src/application/mod.rs`

**This is the largest task.** The orchestration logic from `crates/rapidbyte-engine/src/orchestrator.rs`, `pipeline/executor.rs`, `pipeline/planner.rs`, `pipeline/scheduler.rs`, `pipeline/preflight.rs`, and `finalizers/run.rs` is rewritten here as private helpers.

- [ ] **Step 1:** Write test for the happy path (single stream, no retries):
  ```rust
  #[tokio::test]
  async fn run_pipeline_single_stream_happy_path() {
      let tc = fake_context();
      // Setup: resolver returns resolved plugins, runner returns outcomes
      tc.resolver.register("src", resolved_source());
      tc.resolver.register("dst", resolved_dest());
      tc.runner.enqueue_source(Ok(source_outcome(100)));
      tc.runner.enqueue_destination(Ok(dest_outcome(100)));
      let pipeline = test_pipeline("src", "dst", &["users"]);
      let cancel = CancellationToken::new();
      let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await.unwrap();
      match result {
          PipelineOutcome::Run(r) => assert_eq!(r.counts.rows_read, 100),
          _ => panic!("expected Run outcome"),
      }
  }
  ```
- [ ] **Step 2:** Run test to verify it fails
- [ ] **Step 3:** Implement minimal `run_pipeline()` — resolve, plan, execute single stream, finalize. No retry loop yet, no parallelism
- [ ] **Step 4:** Run test — expect PASS
- [ ] **Step 5:** Commit: `feat(engine): add run_pipeline basic orchestration`

- [ ] **Step 6:** Write test for retry logic:
  ```rust
  #[tokio::test]
  async fn run_pipeline_retries_on_retryable_error() {
      let tc = fake_context();
      tc.resolver.register("src", resolved_source());
      tc.resolver.register("dst", resolved_dest());
      // First attempt: retryable error
      tc.runner.enqueue_source(Err(retryable_plugin_error()));
      // Second attempt: success
      tc.runner.enqueue_source(Ok(source_outcome(50)));
      tc.runner.enqueue_destination(Ok(dest_outcome(50)));
      let cancel = CancellationToken::new();
      let result = run_pipeline(&tc.ctx, &pipeline, &opts, cancel).await.unwrap();
      assert_eq!(result.retry_count, 1);
  }
  ```
- [ ] **Step 7:** Run test to verify it fails
- [ ] **Step 8:** Add retry loop to `run_pipeline()` using `RetryPolicy::should_retry()`. On `RetryDecision::Retry`, sleep for delay and re-resolve/re-execute
- [ ] **Step 9:** Run test — expect PASS
- [ ] **Step 10:** Commit: `feat(engine): add retry logic to run_pipeline`

- [ ] **Step 11:** Write test for cancellation:
  ```rust
  #[tokio::test]
  async fn run_pipeline_respects_cancellation() {
      let tc = fake_context();
      let cancel = CancellationToken::new();
      cancel.cancel(); // Pre-cancel
      let result = run_pipeline(&tc.ctx, &pipeline, &opts, cancel).await;
      assert!(matches!(result, Err(PipelineError::Cancelled)));
  }
  ```
- [ ] **Step 12:** Run test to verify it fails
- [ ] **Step 13:** Add cancellation checks at phase boundaries in `run_pipeline()`
- [ ] **Step 14:** Run test — expect PASS
- [ ] **Step 15:** Commit: `feat(engine): add cancellation support to run_pipeline`

- [ ] **Step 16:** Write test for multi-stream parallelism:
  ```rust
  #[tokio::test]
  async fn run_pipeline_executes_streams_in_parallel() { ... }
  ```
- [ ] **Step 17:** Run test to verify it fails
- [ ] **Step 18:** Add `execute_streams()` with semaphore-based parallelism. Port the parallelism computation logic from `crates/rapidbyte-engine/src/pipeline/planner.rs`
- [ ] **Step 19:** Run test — expect PASS
- [ ] **Step 20:** Commit: `feat(engine): add parallel stream execution`

- [ ] **Step 21:** Write test for dry-run mode:
  ```rust
  #[tokio::test]
  async fn run_pipeline_dry_run_collects_batches() { ... }
  ```
- [ ] **Step 22:** Implement dry-run path — collect batches in memory instead of running destination
- [ ] **Step 23:** Run test — expect PASS
- [ ] **Step 24:** Commit: `feat(engine): add dry-run support`

- [ ] **Step 25:** Write test for progress events:
  ```rust
  #[tokio::test]
  async fn run_pipeline_emits_progress_events() {
      let tc = fake_context();
      // ... setup ...
      run_pipeline(&tc.ctx, &pipeline, &opts, cancel).await.unwrap();
      let events = tc.progress.events();
      assert!(events.iter().any(|e| matches!(e, ProgressEvent::PhaseChanged { phase: Phase::Resolving })));
      assert!(events.iter().any(|e| matches!(e, ProgressEvent::PhaseChanged { phase: Phase::Running })));
      assert!(events.iter().any(|e| matches!(e, ProgressEvent::StreamCompleted { .. })));
  }
  ```
- [ ] **Step 26:** Add `ctx.progress.report(...)` calls at phase boundaries and stream events
- [ ] **Step 27:** Run test — expect PASS
- [ ] **Step 28:** Commit: `feat(engine): add progress reporting to run_pipeline`

- [ ] **Step 29:** Write test for finalization (checkpoint correlation, run record, DLQ):
  ```rust
  #[tokio::test]
  async fn run_pipeline_finalizes_checkpoints_and_records() { ... }
  ```
- [ ] **Step 30:** Implement finalization logic — port from `crates/rapidbyte-engine/src/finalizers/run.rs` and `checkpoint.rs`. Save checkpoints via `ctx.cursors`, record run via `ctx.runs`, insert DLQ via `ctx.dlq`
- [ ] **Step 31:** Run test — expect PASS
- [ ] **Step 32:** Run all engine tests: `cargo test -p rapidbyte-engine` — expect PASS
- [ ] **Step 33:** Commit: `feat(engine): add finalization to run_pipeline`

---

## Phase 4: Engine Adapter Layer

### Task 15: Create RegistryPluginResolver adapter

**Files:**
- Create: `crates/rapidbyte-engine/src/adapter/mod.rs`
- Create: `crates/rapidbyte-engine/src/adapter/registry_resolver.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` (add `pub mod adapter`)

**Reference:** Existing logic at `crates/rapidbyte-engine/src/plugin/resolver.rs` and `loader.rs`

**Note:** Adapter unit tests are intentionally skipped for Tasks 15-17. These adapters wrap external systems (Wasmtime, OCI registry, Postgres) that require real infrastructure. Integration coverage comes from e2e tests (Task 24). This matches the controller pattern where Postgres adapters have no unit tests.

- [ ] **Step 1:** Create `src/adapter/mod.rs` and `src/adapter/registry_resolver.rs`
- [ ] **Step 2:** Implement `RegistryPluginResolver` — port plugin resolution from `plugin/resolver.rs`. Resolve OCI ref via registry, load manifest, validate protocol version + kind + config schema. Return `ResolvedPlugin`
- [ ] **Step 3:** Implement `PluginResolver` trait for `RegistryPluginResolver`
- [ ] **Step 4:** Add `pub mod adapter;` to `src/lib.rs`
- [ ] **Step 5:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 6:** Commit: `feat(engine): add RegistryPluginResolver adapter`

### Task 16: Create WasmPluginRunner adapter

**Files:**
- Create: `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/mod.rs`

**Reference:** Existing logic at `crates/rapidbyte-engine/src/runner/source.rs`, `destination.rs`, `transform.rs`, `validator.rs`

- [ ] **Step 1:** Create `src/adapter/wasm_runner.rs` with `WasmPluginRunner` struct holding `WasmRuntime` and `Arc<dyn StateBackend>`
- [ ] **Step 2:** Implement `PluginRunner::run_source()` — port from `runner/source.rs`. Build host state, create Wasmtime store, instantiate source component, call open/run/close, extract `SourceOutcome`
- [ ] **Step 3:** Implement `PluginRunner::run_transform()` — port from `runner/transform.rs`
- [ ] **Step 4:** Implement `PluginRunner::run_destination()` — port from `runner/destination.rs`
- [ ] **Step 5:** Implement `PluginRunner::validate_plugin()` — port from `runner/validator.rs`
- [ ] **Step 6:** Implement `PluginRunner::discover()` — port from `runner/validator.rs`
- [ ] **Step 7:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 8:** Commit: `feat(engine): add WasmPluginRunner adapter`

### Task 17: Create PgBackend adapter and migrations

**Files:**
- Create: `crates/rapidbyte-engine/src/adapter/postgres/mod.rs`
- Create: `crates/rapidbyte-engine/src/adapter/postgres/cursor.rs`
- Create: `crates/rapidbyte-engine/src/adapter/postgres/run_record.rs`
- Create: `crates/rapidbyte-engine/src/adapter/postgres/dlq.rs`
- Create: `crates/rapidbyte-engine/src/adapter/postgres/state_backend.rs`
- Create: `crates/rapidbyte-engine/migrations/0001_engine_initial.sql`
- Modify: `crates/rapidbyte-engine/src/adapter/mod.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml` (add `sqlx` with postgres+migrate features, `postgres` for sync client)

**Reference:** Controller Postgres adapter at `crates/rapidbyte-controller/src/adapter/postgres/`. Existing state SQL at `crates/rapidbyte-state/src/postgres.rs`.

- [ ] **Step 1:** Add `sqlx` to `[workspace.dependencies]` in the root `Cargo.toml` if not already present. Then add dependencies to the engine's `Cargo.toml`:
  ```toml
  sqlx = { workspace = true, features = ["runtime-tokio-rustls", "postgres", "chrono", "migrate"] }
  postgres = { version = "0.19", features = ["with-chrono-0_4"] }
  ```
- [ ] **Step 2:** Create `migrations/0001_engine_initial.sql` with the DDL from spec (lines 482-519). Port from `crates/rapidbyte-state/src/postgres.rs` with `TIMESTAMPTZ` upgrades
- [ ] **Step 3:** Create `src/adapter/postgres/mod.rs` with `PgBackend` struct. Implement `connect()` (creates `PgPool` + sync `ClientPool`), `migrate()` (runs `sqlx::migrate!()`), `as_state_backend()`
- [ ] **Step 4:** Create `src/adapter/postgres/cursor.rs` — implement `CursorRepository` for `PgBackend`. Port SQL queries from `crates/rapidbyte-state/src/postgres.rs` (get_cursor, set_cursor, compare_and_set), convert to sqlx
- [ ] **Step 5:** Create `src/adapter/postgres/run_record.rs` — implement `RunRecordRepository`. Port start_run, complete_run queries
- [ ] **Step 6:** Create `src/adapter/postgres/dlq.rs` — implement `DlqRepository`. Port insert_dlq_records query
- [ ] **Step 7:** Create `src/adapter/postgres/state_backend.rs` — implement `StateBackend` for `PgBackend`. This uses the sync `ClientPool` (same pattern as existing `PostgresStateBackend`). Port all methods from `crates/rapidbyte-state/src/postgres.rs`
- [ ] **Step 8:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 9:** Commit: `feat(engine): add PgBackend adapter with SQLx migrations`

### Task 18: Create ChannelProgressReporter and OtelMetricsSnapshot adapters

**Files:**
- Create: `crates/rapidbyte-engine/src/adapter/progress.rs`
- Create: `crates/rapidbyte-engine/src/adapter/metrics.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/mod.rs`

- [ ] **Step 1:** Create `src/adapter/progress.rs` with `ChannelProgressReporter` per spec (lines 394-406). Holds `mpsc::UnboundedSender<ProgressEvent>`, best-effort send
- [ ] **Step 2:** Create `src/adapter/metrics.rs` with `OtelMetricsSnapshot` per spec (lines 414-418). Wraps `rapidbyte_metrics::snapshot::SnapshotReader`
- [ ] **Step 3:** Update `src/adapter/mod.rs` to export both
- [ ] **Step 4:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 5:** Commit: `feat(engine): add ChannelProgressReporter and OtelMetricsSnapshot adapters`

---

## Phase 5: Update lib.rs and Public API

### Task 19: Update engine lib.rs with new public API

**Files:**
- Modify: `crates/rapidbyte-engine/src/lib.rs`

- [ ] **Step 1:** Update `src/lib.rs` to match the spec's public API surface (lines 631-646). Add all top-level re-exports. Keep `pub mod domain`, `pub mod application`, `pub mod adapter`
- [ ] **Step 2:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 3:** Commit: `feat(engine): update public API surface`

---

## Phase 6: Consumer Migration

Update all consumers to use the new hexagonal engine API. Each task covers one consumer crate.

### Task 20: Update CLI to use new engine API

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs` — construct `EngineContext`, call `run_pipeline()`
- Modify: `crates/rapidbyte-cli/src/commands/check.rs` — call `check_pipeline()`
- Modify: `crates/rapidbyte-cli/src/commands/discover.rs` — call `discover_plugin()`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs` — wiring (create PgBackend, WasmPluginRunner, etc.)
- Modify: `crates/rapidbyte-cli/Cargo.toml` — add sqlx, postgres dependencies

**Reference:** Current CLI wiring at `crates/rapidbyte-cli/src/commands/`

- [ ] **Step 1:** Read all CLI command files to understand current wiring
- [ ] **Step 2:** Create a shared wiring function (or struct) that builds `EngineContext` from CLI args (connection string, registry config, etc.)
- [ ] **Step 3:** Update `run.rs` — replace direct `orchestrator::run_pipeline()` call with new `run_pipeline(&ctx, &pipeline, &opts, cancel)`
- [ ] **Step 4:** Update `check.rs` — replace direct `orchestrator::check_pipeline()` with new API
- [ ] **Step 5:** Update `discover.rs` — replace direct `orchestrator::discover_plugin()` with new API
- [ ] **Step 6:** Update all import paths
- [ ] **Step 7:** Run `cargo build -p rapidbyte-cli` — verify it compiles
- [ ] **Step 8:** Commit: `refactor(cli): migrate to hexagonal engine API`

### Task 21: Update agent to use new engine API

**Files:**
- Modify: `crates/rapidbyte-agent/src/executor.rs` — construct `EngineContext`, call `run_pipeline()`
- Modify: `crates/rapidbyte-agent/src/progress.rs` — update `ProgressEvent` imports
- Modify: `crates/rapidbyte-agent/src/flight.rs` — update outcome imports
- Modify: `crates/rapidbyte-agent/Cargo.toml`

- [ ] **Step 1:** Read agent executor and flight files to understand current wiring
- [ ] **Step 2:** Update executor to construct `EngineContext` with `PgBackend`, `WasmPluginRunner`, etc.
- [ ] **Step 3:** Update all import paths for `PipelineError`, `ProgressEvent`, outcome types
- [ ] **Step 4:** Run `cargo build -p rapidbyte-agent` — verify it compiles
- [ ] **Step 5:** Commit: `refactor(agent): migrate to hexagonal engine API`

### Task 22: Update dev REPL

**Files:**
- Modify: `crates/rapidbyte-dev/src/repl.rs` — already updated arrow import in Task 4. Verify no other engine imports remain

- [ ] **Step 1:** Search for any remaining `rapidbyte_engine::` imports in dev crate
- [ ] **Step 2:** Update if needed
- [ ] **Step 3:** Run `cargo build -p rapidbyte-dev` — verify it compiles
- [ ] **Step 4:** Commit (if changes): `refactor(dev): update remaining engine imports`

### Task 23: Update benchmarks to use run_pipeline

**Files:**
- Modify: `benchmarks/src/pipeline.rs` — replace `run_source_stream()`/`run_destination_stream()` with `run_pipeline()`
- Modify: `benchmarks/src/isolation.rs` — update arrow imports
- Modify: `benchmarks/Cargo.toml` — add sqlx, engine test-support feature

**Note:** `benchmarks/src/isolation.rs` uses internal runner APIs (`run_source_stream`, `run_destination_stream`, `run_transform_stream`, `StreamRunContext`) that become private after the refactor. This file needs a full rewrite or removal — it cannot call the old runner internals.

- [ ] **Step 1:** Read benchmark files to understand current low-level runner usage
- [ ] **Step 2:** Rewrite `pipeline.rs` to construct `EngineContext` with real adapters and call `run_pipeline()`. Use `PipelineResult.source_timing`/`dest_timing` for per-stage metrics
- [ ] **Step 3:** Rewrite or remove `isolation.rs` — the old runner APIs are no longer public. If isolated stage benchmarks are still needed, they must go through `run_pipeline()` with single-stage pipeline configs, or be deferred to a future task
- [ ] **Step 4:** Update arrow imports to `rapidbyte_types::arrow`
- [ ] **Step 5:** Run `cargo build -p rapidbyte-benchmarks` — verify it compiles
- [ ] **Step 6:** Commit: `refactor(bench): migrate to run_pipeline orchestrator API`

### Task 24: Update e2e tests

**Files:**
- Modify: `tests/e2e/src/harness/mod.rs` — update imports, construct `EngineContext`

- [ ] **Step 1:** Read e2e harness to understand current wiring
- [ ] **Step 2:** Update to construct `EngineContext` with `PgBackend`, `WasmPluginRunner`, call `run_pipeline()`
- [ ] **Step 3:** Run `cargo build` for e2e — verify it compiles
- [ ] **Step 4:** Commit: `refactor(e2e): migrate to hexagonal engine API`

---

## Phase 7: Cleanup

### Task 25: Remove old engine modules

**Files:**
- Delete: `crates/rapidbyte-engine/src/orchestrator.rs`
- Delete: `crates/rapidbyte-engine/src/error.rs` (old one)
- Delete: `crates/rapidbyte-engine/src/outcome.rs` (old one)
- Delete: `crates/rapidbyte-engine/src/progress.rs` (old one)
- Delete: `crates/rapidbyte-engine/src/arrow.rs` (re-export shim from Task 1)
- Delete: `crates/rapidbyte-engine/src/config/` (re-export shim from Task 3)
- Delete: `crates/rapidbyte-engine/src/runner/` (entire directory)
- Delete: `crates/rapidbyte-engine/src/plugin/` (entire directory)
- Delete: `crates/rapidbyte-engine/src/pipeline/` (entire directory)
- Delete: `crates/rapidbyte-engine/src/finalizers/` (entire directory)
- Update or delete: `crates/rapidbyte-engine/tests/pipeline_integration.rs`, `policy_proptest.rs`, `resolve_visibility.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs` — remove old module declarations

- [ ] **Step 1:** Remove all old module declarations from `src/lib.rs` — only keep `domain`, `application`, `adapter` and the public re-exports. Remove `pub mod config`, `pub mod arrow`, `pub mod orchestrator`, `pub mod error`, `pub mod outcome`, `pub mod progress`, `pub mod runner`, `pub mod plugin`, `pub mod pipeline`, `pub mod finalizers`
- [ ] **Step 2:** Delete old files: `orchestrator.rs`, `error.rs`, `outcome.rs`, `progress.rs`, `arrow.rs`
- [ ] **Step 3:** Delete re-export shim directory: `config/`
- [ ] **Step 4:** Delete old directories: `runner/`, `plugin/`, `pipeline/`, `finalizers/`
- [ ] **Step 5:** Update existing integration tests at `crates/rapidbyte-engine/tests/`. Either rewrite them to use the new hexagonal API (`EngineContext`, `fake_context()`) or delete them if their coverage is now provided by the new unit tests in `application/` modules. The `policy_proptest.rs` (permission/limit validation) should move to `rapidbyte-pipeline-config` tests since validation now lives there. The `resolve_visibility.rs` tests should be updated or replaced to verify the new public API surface
- [ ] **Step 6:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 7:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 8:** Run `cargo clippy --workspace -- -D warnings` — verify no clippy warnings. There should be zero `allow(dead_code)` anywhere
- [ ] **Step 9:** Commit: `refactor(engine): remove old pre-hexagonal modules`

### Task 26: Remove rapidbyte-state crate

**Files:**
- Delete: `crates/rapidbyte-state/` (entire directory)
- Modify: workspace `Cargo.toml` — remove `rapidbyte-state` from members
- Modify: `crates/rapidbyte-runtime/Cargo.toml` — remove `rapidbyte-state` dependency, add `rapidbyte-types` for `StateBackend` trait if not present
- Modify: `crates/rapidbyte-runtime/src/host_state.rs` — update import to `rapidbyte_types::state_backend::StateBackend`

- [ ] **Step 1:** Remove `rapidbyte-state` from workspace `Cargo.toml` members list
- [ ] **Step 2:** Remove `rapidbyte-state` dependency from `crates/rapidbyte-runtime/Cargo.toml`
- [ ] **Step 3:** Update `crates/rapidbyte-runtime/src/host_state.rs` import (should already point to `rapidbyte_types` from Task 2, verify)
- [ ] **Step 4:** Search for any remaining `rapidbyte-state` or `rapidbyte_state` references across workspace
- [ ] **Step 5:** Delete `crates/rapidbyte-state/` directory
- [ ] **Step 6:** Run `cargo build --workspace` — verify it compiles
- [ ] **Step 7:** Run `cargo test --workspace` — verify tests pass
- [ ] **Step 8:** Commit: `refactor: remove rapidbyte-state crate`

### Task 27: Final verification

- [ ] **Step 1:** Run `cargo build --workspace` — PASS
- [ ] **Step 2:** Run `cargo test --workspace` — PASS
- [ ] **Step 3:** Run `cargo clippy --workspace -- -D warnings` — PASS, zero suppressions
- [ ] **Step 4:** Run `just lint` — PASS
- [ ] **Step 5:** Run `just e2e` — PASS (requires Docker PG)
- [ ] **Step 6:** Verify no `allow(dead_code)`, `allow(unused)`, or similar clippy suppressions exist in engine crate
- [ ] **Step 7:** Verify engine's `lib.rs` has the correct module doc table
- [ ] **Step 8:** Update `CLAUDE.md` — update the crate dependency graph, module table, and project structure to reflect the new hexagonal engine architecture, removed `rapidbyte-state` crate, moved config/arrow modules, and Postgres-only state backend
- [ ] **Step 9:** Commit any final fixes
- [ ] **Step 10:** Final commit: `refactor(engine): complete hexagonal architecture refactor`
