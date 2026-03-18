# Engine Test Suite Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 66 new tests (43 unit + 23 integration) to the engine crate, bringing total from 49 to 115, covering all happy paths, error paths, and edge cases.

**Architecture:** Unit tests use `fake_context()` fakes (no I/O, fast). Integration tests use testcontainers for Postgres and minimal WASM test plugins. Integration tests gated behind `#[cfg(feature = "integration")]`.

**Tech Stack:** Rust, tokio, async-trait, testcontainers, testcontainers-modules (postgres), rapidbyte-sdk (test plugins, wasm32-wasip2)

**Spec:** `docs/superpowers/specs/2026-03-18-engine-test-suite-design.md`

---

## Task 1: Add dev-dependencies and integration feature

**Files:**
- Modify: `crates/rapidbyte-engine/Cargo.toml`

- [ ] **Step 1:** Add testcontainers dev-dependencies and integration feature to `crates/rapidbyte-engine/Cargo.toml`:
  ```toml
  [dev-dependencies]
  testcontainers = "0.23"
  testcontainers-modules = { version = "0.11", features = ["postgres"] }

  [features]
  integration = []
  test-support = []  # already exists, verify
  ```
  Note: `integration` feature may already exist — check first. Only add what's missing.

- [ ] **Step 2:** Run `cargo build -p rapidbyte-engine` — verify it compiles
- [ ] **Step 3:** Commit: `chore(engine): add testcontainers dev-dependencies`

---

## Task 2: Domain layer unit tests

**Files:**
- Modify: `crates/rapidbyte-engine/src/domain/error.rs` (add tests to existing `#[cfg(test)]` module)
- Modify: `crates/rapidbyte-engine/src/domain/retry.rs` (add tests)
- Modify: `crates/rapidbyte-engine/src/domain/ports/mod.rs` (add `#[cfg(test)]` module)

### error.rs — 2 new tests

- [ ] **Step 1:** Add tests to the existing `#[cfg(test)] mod tests` in `error.rs`:
  ```rust
  #[test]
  fn infra_error_displays_message() {
      let err = PipelineError::infra("db down");
      assert!(err.to_string().contains("db down"));
  }

  #[test]
  fn cancelled_displays_message() {
      let err = PipelineError::Cancelled;
      assert_eq!(err.to_string(), "pipeline cancelled");
  }
  ```
- [ ] **Step 2:** Run `cargo test -p rapidbyte-engine domain::error` — verify 8 tests pass
- [ ] **Step 3:** Commit: `test(engine): add Display tests for PipelineError`

### retry.rs — 3 new tests

- [ ] **Step 4:** Add tests to existing `#[cfg(test)] mod tests` in `retry.rs`:
  ```rust
  #[test]
  fn retry_attempt_zero_uses_base_delay() {
      let policy = RetryPolicy::new(3);
      match policy.should_retry(0, true, true, &BackoffClass::Fast, None) {
          RetryDecision::Retry { delay } => {
              // 2^(0-1) with saturating_sub = 2^0 = 1, so 100ms * 1
              assert_eq!(delay, Duration::from_millis(100));
          }
          RetryDecision::GiveUp { .. } => panic!("expected Retry"),
      }
  }

  #[test]
  fn max_attempts_one_means_no_retries() {
      let policy = RetryPolicy::new(1);
      match policy.should_retry(1, true, true, &BackoffClass::Normal, None) {
          RetryDecision::GiveUp { .. } => {} // expected
          RetryDecision::Retry { .. } => panic!("expected GiveUp"),
      }
  }

  #[test]
  fn retry_after_hint_overrides_even_slow_class() {
      let policy = RetryPolicy::new(5);
      let hint = Some(Duration::from_millis(200));
      match policy.should_retry(1, true, true, &BackoffClass::Slow, hint) {
          RetryDecision::Retry { delay } => {
              assert_eq!(delay, Duration::from_millis(200));
          }
          RetryDecision::GiveUp { .. } => panic!("expected Retry"),
      }
  }
  ```
- [ ] **Step 5:** Run `cargo test -p rapidbyte-engine domain::retry` — verify 10 tests pass
- [ ] **Step 6:** Commit: `test(engine): add retry policy edge case tests`

### ports/mod.rs — 3 new tests

- [ ] **Step 7:** Add `#[cfg(test)]` module to `domain/ports/mod.rs`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[test]
      fn is_conflict_returns_true_for_conflict() {
          let err = RepositoryError::Conflict("race condition".into());
          assert!(err.is_conflict());
      }

      #[test]
      fn is_conflict_returns_false_for_other() {
          let err = RepositoryError::other(std::io::Error::other("boom"));
          assert!(!err.is_conflict());
      }

      #[test]
      fn other_wraps_error() {
          let inner = std::io::Error::other("db failed");
          let err = RepositoryError::other(inner);
          assert!(err.to_string().contains("db failed"));
      }
  }
  ```
- [ ] **Step 8:** Run `cargo test -p rapidbyte-engine domain::ports` — verify 3 tests pass
- [ ] **Step 9:** Commit: `test(engine): add RepositoryError unit tests`

---

## Task 3: Application run.rs tests (13 new)

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/run.rs` (add tests to existing `#[cfg(test)]` module)

This is the largest test addition. All tests use `fake_context()`.

**Important:** Retry tests MUST call `tokio::time::pause()` at the start to avoid real sleeps.

- [ ] **Step 1:** Read the existing test module in `run.rs` to understand helpers (`test_pipeline_config`, `make_source_outcome`, etc.) and how fakes are set up.

- [ ] **Step 2:** Add retry tests. You'll need a helper to create retryable plugin errors:
  ```rust
  fn retryable_plugin_error() -> PipelineError {
      // Create a PipelineError::Plugin with a PluginError that has
      // retryable: true, safe_to_retry: true
      // Look at domain/error.rs tests for how to construct PluginError
  }

  fn non_retryable_plugin_error() -> PipelineError {
      // Same but retryable: false
  }
  ```

  Then write the three retry tests:
  ```rust
  #[tokio::test]
  async fn retry_on_retryable_plugin_error() {
      tokio::time::pause();
      let tc = fake_context();
      // Setup: resolver returns plugins
      tc.resolver.register("src", test_resolved_plugin());
      tc.resolver.register("dst", test_resolved_plugin());
      // First attempt: retryable source error
      tc.runner.enqueue_source(Err(retryable_plugin_error()));
      // Second attempt: success
      tc.runner.enqueue_source(Ok(make_source_outcome(50)));
      tc.runner.enqueue_destination(Ok(make_dest_outcome(50)));

      let pipeline = test_pipeline_config("src", "dst", &["users"]);
      let cancel = CancellationToken::new();
      let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await.unwrap();

      match result {
          PipelineOutcome::Run(r) => {
              assert_eq!(r.retry_count, 1);
              assert_eq!(r.counts.records_read, 50);
          }
          _ => panic!("expected Run outcome"),
      }
      // Verify RetryScheduled event was emitted
      let events = tc.progress.events();
      assert!(events.iter().any(|e| matches!(e, ProgressEvent::RetryScheduled { .. })));
  }

  #[tokio::test]
  async fn retry_gives_up_after_max_attempts() {
      tokio::time::pause();
      let tc = fake_context();
      tc.resolver.register("src", test_resolved_plugin());
      tc.resolver.register("dst", test_resolved_plugin());
      // All attempts fail
      for _ in 0..=tc.ctx.config.max_retries {
          tc.runner.enqueue_source(Err(retryable_plugin_error()));
      }
      let pipeline = test_pipeline_config("src", "dst", &["users"]);
      let cancel = CancellationToken::new();
      let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await;
      assert!(result.is_err());
  }

  #[tokio::test]
  async fn non_retryable_plugin_error_not_retried() {
      let tc = fake_context();
      tc.resolver.register("src", test_resolved_plugin());
      tc.resolver.register("dst", test_resolved_plugin());
      tc.runner.enqueue_source(Err(non_retryable_plugin_error()));
      // Don't enqueue a second source — if retry happens, it'll panic on empty queue
      let pipeline = test_pipeline_config("src", "dst", &["users"]);
      let cancel = CancellationToken::new();
      let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await;
      assert!(result.is_err());
  }
  ```

- [ ] **Step 3:** Run `cargo test -p rapidbyte-engine application::run::tests` — verify new tests pass
- [ ] **Step 4:** Commit: `test(engine): add retry behavior tests for run_pipeline`

- [ ] **Step 5:** Add cursor persistence tests:
  ```rust
  #[tokio::test]
  async fn cursor_loaded_for_incremental_stream() {
      // Create pipeline with cursor_field set on stream
      // Pre-set a cursor in tc.cursors
      // Run pipeline
      // Verify source params received the cursor value
  }

  #[tokio::test]
  async fn cursor_saved_after_successful_run() {
      // Run pipeline with source that emits checkpoints
      // Verify tc.cursors has the checkpoint value saved
  }

  #[tokio::test]
  async fn no_cursor_load_for_full_refresh() {
      // Pipeline with no cursor_field
      // Verify cursors.get() was NOT called (or at least that it works without one)
  }
  ```
  Note: You may need to check the actual `run_pipeline` implementation to understand how cursors are loaded and what the stream config looks like for incremental vs full-refresh. Read `application/run.rs` lines 155-180.

- [ ] **Step 6:** Run tests — verify pass
- [ ] **Step 7:** Commit: `test(engine): add cursor persistence tests`

- [ ] **Step 8:** Add multi-transform tests:
  ```rust
  #[tokio::test]
  async fn transforms_execute_in_order() {
      let tc = fake_context();
      // Setup pipeline with 2 transforms
      // Enqueue source, 2 transform results, destination
      // Verify both transforms were called
  }

  #[tokio::test]
  async fn transform_error_fails_pipeline() {
      let tc = fake_context();
      // Source succeeds, transform fails
      // Verify error returned, destination NOT called
  }
  ```

- [ ] **Step 9:** Run tests — verify pass
- [ ] **Step 10:** Commit: `test(engine): add multi-transform pipeline tests`

- [ ] **Step 11:** Add remaining tests (per-stream stats, cursor error, edge cases):
  ```rust
  #[tokio::test]
  async fn per_stream_stats_recorded_independently() {
      // 2 streams: stream A reads 100 rows, stream B reads 200 rows
      // Verify run records have different counts
  }

  #[tokio::test]
  async fn cursor_save_failure_does_not_fail_pipeline() {
      // Inject error on cursor save (make fake return Err)
      // Verify pipeline still succeeds (fire-and-forget semantics)
  }

  #[tokio::test]
  async fn empty_streams_list_returns_empty_result() {
      // Pipeline with zero streams
      // Verify clean return with zero counts
  }

  #[tokio::test]
  async fn cancellation_between_streams() {
      // Cancel token triggered after first stream completes
      // Verify second stream not executed, Cancelled returned
  }

  #[tokio::test]
  async fn destination_error_after_source_success() {
      // Source OK, destination fails
      // Verify error propagated
  }
  ```

- [ ] **Step 12:** Run `cargo test -p rapidbyte-engine application::run::tests` — verify all 21 pass
- [ ] **Step 13:** Commit: `test(engine): add edge case tests for run_pipeline`

---

## Task 4: Application check/discover/testing tests

**Files:**
- Modify: `crates/rapidbyte-engine/src/application/check.rs`
- Modify: `crates/rapidbyte-engine/src/application/discover.rs`
- Modify: `crates/rapidbyte-engine/src/application/testing.rs`

### check.rs — 4 new tests

- [ ] **Step 1:** Add tests to existing `#[cfg(test)]` module:
  ```rust
  #[tokio::test]
  async fn check_with_no_transforms() {
      // Pipeline with empty transforms vec
      // Verify only source + dest validated (result has 2 components)
  }

  #[tokio::test]
  async fn check_transform_resolution_failure() {
      // Source + dest resolve, transform doesn't
      // Verify error propagated
  }

  #[tokio::test]
  async fn check_multiple_transforms_all_validated() {
      // Pipeline with 3 transforms
      // Verify transform_validations.len() == 3
  }

  #[tokio::test]
  async fn check_mixed_validation_results() {
      // Source validation OK, dest validation fails
      // Verify both captured, no early return on dest failure
  }
  ```
- [ ] **Step 2:** Run `cargo test -p rapidbyte-engine application::check` — verify 10 pass
- [ ] **Step 3:** Commit: `test(engine): add check_pipeline edge case tests`

### discover.rs — 2 new tests

- [ ] **Step 4:** Add tests:
  ```rust
  #[tokio::test]
  async fn discover_returns_multiple_streams() {
      let tc = fake_context();
      tc.resolver.register("src", test_resolved_plugin());
      tc.runner.enqueue_discover(Ok(vec![
          DiscoveredStream { name: "users".into(), catalog_json: "{}".into() },
          DiscoveredStream { name: "orders".into(), catalog_json: "{}".into() },
          DiscoveredStream { name: "products".into(), catalog_json: "{}".into() },
          DiscoveredStream { name: "events".into(), catalog_json: "{}".into() },
          DiscoveredStream { name: "logs".into(), catalog_json: "{}".into() },
      ]));
      let streams = discover_plugin(&tc.ctx, "src", None).await.unwrap();
      assert_eq!(streams.len(), 5);
  }

  #[tokio::test]
  async fn discover_with_no_config() {
      let tc = fake_context();
      tc.resolver.register("src", test_resolved_plugin());
      tc.runner.enqueue_discover(Ok(vec![
          DiscoveredStream { name: "test".into(), catalog_json: "{}".into() },
      ]));
      let streams = discover_plugin(&tc.ctx, "src", None).await.unwrap();
      assert_eq!(streams.len(), 1);
  }
  ```
- [ ] **Step 5:** Run `cargo test -p rapidbyte-engine application::discover` — verify 8 pass
- [ ] **Step 6:** Commit: `test(engine): add discover edge case tests`

### testing.rs — 3 new tests

- [ ] **Step 7:** Add tests to existing `#[cfg(test)]` module:
  ```rust
  #[tokio::test]
  async fn fake_dlq_repository_records_insertions() {
      let tc = fake_context();
      let pipeline = PipelineId::new("test");
      let records = vec![/* create a DlqRecord */];
      let count = tc.ctx.dlq.insert(&pipeline, 1, &records).await.unwrap();
      assert_eq!(count, records.len() as u64);
      assert_eq!(tc.dlq.inserted_records().len(), records.len());
  }

  #[tokio::test]
  async fn fake_run_record_tracks_completions() {
      let tc = fake_context();
      let pipeline = PipelineId::new("test");
      let stream = StreamName::new("users");
      let id = tc.ctx.runs.start(&pipeline, &stream).await.unwrap();
      assert!(id > 0);
      assert_eq!(tc.runs.started_count(), 1);
  }

  #[tokio::test]
  async fn fake_resolver_returns_error_for_unknown() {
      let tc = fake_context();
      let result = tc.ctx.resolver.resolve("nonexistent", PluginKind::Source, None).await;
      assert!(result.is_err());
  }
  ```
  Note: Adapt these to the actual types and methods available on the fakes. Read `testing.rs` to understand `inserted_records()`, `started_count()` etc.

- [ ] **Step 8:** Run `cargo test -p rapidbyte-engine application::testing` — verify 7 pass
- [ ] **Step 9:** Commit: `test(engine): add fake infrastructure validation tests`

---

## Task 5: Adapter layer unit tests

**Files:**
- Modify: `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/engine_factory.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/progress.rs`
- Modify: `crates/rapidbyte-engine/src/adapter/registry_resolver.rs`

### wasm_runner.rs — 6 new tests

- [ ] **Step 1:** Add tests to existing `#[cfg(test)]` module. Read the file first to find `parse_compression` and `plugin_instance_key` functions:
  ```rust
  #[test]
  fn parse_compression_lz4() {
      let result = parse_compression(Some("lz4")).unwrap();
      assert!(matches!(result, Some(CompressionCodec::Lz4)));
  }

  #[test]
  fn parse_compression_zstd() {
      let result = parse_compression(Some("zstd")).unwrap();
      assert!(matches!(result, Some(CompressionCodec::Zstd)));
  }

  #[test]
  fn parse_compression_none() {
      let result = parse_compression(None).unwrap();
      assert!(result.is_none());
  }

  #[test]
  fn parse_compression_empty_string_returns_none() {
      let result = parse_compression(Some("")).unwrap();
      assert!(result.is_none());
  }

  #[test]
  fn parse_compression_invalid() {
      let result = parse_compression(Some("brotli"));
      assert!(result.is_err());
  }

  #[test]
  fn plugin_instance_key_without_ordinal() {
      // Call plugin_instance_key with known args, verify format
      // Read the function to understand its signature and expected output pattern
  }
  ```
- [ ] **Step 2:** Run `cargo test -p rapidbyte-engine adapter::wasm_runner` — verify 7 pass
- [ ] **Step 3:** Commit: `test(engine): add wasm_runner helper tests`

### engine_factory.rs — 5 new tests

- [ ] **Step 4:** Add `#[cfg(test)]` module. Read the file first to understand `run_on_backend` and `StateBackendRepositoryAdapter`:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;

      #[tokio::test]
      async fn run_on_backend_propagates_result() {
          let backend: Arc<dyn StateBackend> = Arc::new(NoopStateBackend);
          let result = run_on_backend(&backend, "test", |b| {
              b.get_cursor(&PipelineId::new("p"), &StreamName::new("s"))
          }).await;
          assert!(result.is_ok());
      }

      #[tokio::test]
      async fn run_on_backend_converts_state_error() {
          let backend: Arc<dyn StateBackend> = Arc::new(NoopStateBackend);
          // Create a backend that returns StateError
          // Or use a closure that returns Err directly
          // Verify it becomes RepositoryError::Other
      }

      #[tokio::test]
      async fn run_on_backend_handles_panic() {
          let backend: Arc<dyn StateBackend> = Arc::new(NoopStateBackend);
          let result: Result<(), _> = run_on_backend(&backend, "panic-test", |_| {
              panic!("intentional test panic");
          }).await;
          assert!(result.is_err());
          assert!(result.unwrap_err().to_string().contains("panic"));
      }

      #[tokio::test]
      async fn cas_adapter_stub_returns_true() {
          // Create StateBackendRepositoryAdapter wrapping NoopStateBackend
          // Call compare_and_set
          // Verify returns Ok(true)
      }

      #[tokio::test]
      async fn dlq_adapter_stub_returns_zero() {
          // Create StateBackendRepositoryAdapter wrapping NoopStateBackend
          // Call insert with empty slice
          // Verify returns Ok(0)
      }
  }
  ```
- [ ] **Step 5:** Run `cargo test -p rapidbyte-engine adapter::engine_factory` — verify 5 pass
- [ ] **Step 6:** Commit: `test(engine): add engine_factory adapter tests`

### progress.rs — 2 new tests

- [ ] **Step 7:** Add `#[cfg(test)]` module:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::domain::progress::{Phase, ProgressEvent};

      #[test]
      fn channel_reporter_sends_events() {
          let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
          let reporter = ChannelProgressReporter::new(tx);
          reporter.report(ProgressEvent::PhaseChanged { phase: Phase::Running });
          let event = rx.try_recv().unwrap();
          assert!(matches!(event, ProgressEvent::PhaseChanged { phase: Phase::Running }));
      }

      #[test]
      fn channel_reporter_does_not_panic_when_receiver_dropped() {
          let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
          let reporter = ChannelProgressReporter::new(tx);
          drop(rx);
          // Should not panic
          reporter.report(ProgressEvent::PhaseChanged { phase: Phase::Finalizing });
      }
  }
  ```
- [ ] **Step 8:** Run `cargo test -p rapidbyte-engine adapter::progress` — verify 2 pass
- [ ] **Step 9:** Commit: `test(engine): add ChannelProgressReporter tests`

### registry_resolver.rs — 2 new tests

- [ ] **Step 10:** Read the existing test module and `validate_config_against_schema` function. Add tests:
  ```rust
  #[test]
  fn config_matches_schema_passes() {
      // Create a simple JSON Schema and a config that matches
      // Call validate_config_against_schema
      // Verify Ok(())
  }

  #[test]
  fn config_violates_schema_returns_error_with_details() {
      // Create a schema requiring "host" field
      // Pass config without "host"
      // Verify error message mentions the missing field
  }
  ```
- [ ] **Step 11:** Run `cargo test -p rapidbyte-engine adapter::registry_resolver` — verify 4 pass
- [ ] **Step 12:** Commit: `test(engine): add config schema validation tests`

---

## Task 6: Create test WASM plugins

**Files:**
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-source/Cargo.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-source/.cargo/config.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-source/src/lib.rs`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-destination/Cargo.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-destination/.cargo/config.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-destination/src/lib.rs`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-transform/Cargo.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-transform/.cargo/config.toml`
- Create: `crates/rapidbyte-engine/tests/fixtures/plugins/test-transform/src/lib.rs`
- Modify: `justfile` (add `build-test-plugins` target)

**Reference:** Look at existing plugins for the pattern:
- `plugins/sources/postgres/Cargo.toml` — for Cargo.toml structure
- `plugins/sources/postgres/.cargo/config.toml` — for wasm32-wasip2 target config
- `plugins/sources/postgres/src/lib.rs` — for `#[plugin(source)]` macro usage

- [ ] **Step 1:** Read an existing plugin to understand the SDK macro pattern and WIT interface
- [ ] **Step 2:** Create `test-source` plugin:
  - `Cargo.toml`: depends on `rapidbyte-sdk`
  - `.cargo/config.toml`: `[build] target = "wasm32-wasip2"`
  - `src/lib.rs`: ~60 lines implementing source interface
    - `open()` — parse `row_count` and `should_fail` from config
    - `run()` — emit `row_count` rows (single `id: Int64` column), emit checkpoint
    - `close()` — no-op
    - `discover()` — return one stream `"test-stream"`
    - `validate()` — return OK
- [ ] **Step 3:** Create `test-destination` plugin:
  - `src/lib.rs`: ~40 lines
    - `open()` — parse `should_fail` from config
    - `run()` — consume frames, count rows
    - `close()` — no-op
    - `validate()` — return OK
- [ ] **Step 4:** Create `test-transform` plugin:
  - `src/lib.rs`: ~30 lines
    - `open()` — no-op
    - `run()` — pass-through (read frame, write unchanged)
    - `close()` — no-op
    - `validate()` — return OK
- [ ] **Step 5:** Add `build-test-plugins` target to `justfile`:
  ```
  build-test-plugins:
      cd crates/rapidbyte-engine/tests/fixtures/plugins/test-source && cargo build
      cd crates/rapidbyte-engine/tests/fixtures/plugins/test-destination && cargo build
      cd crates/rapidbyte-engine/tests/fixtures/plugins/test-transform && cargo build
  ```
- [ ] **Step 6:** Run `just build-test-plugins` — verify all 3 compile to `.wasm`
- [ ] **Step 7:** Commit: `feat(engine): add test WASM plugins for integration tests`

---

## Task 7: Postgres integration tests

**Files:**
- Create: `crates/rapidbyte-engine/tests/postgres_integration.rs`

All tests gated behind `#[cfg(feature = "integration")]`.

- [ ] **Step 1:** Create `tests/postgres_integration.rs` with setup helper:
  ```rust
  #![cfg(feature = "integration")]

  use rapidbyte_engine::adapter::postgres::PgBackend;
  use rapidbyte_engine::{CursorRepository, RunRecordRepository, DlqRepository};
  use rapidbyte_types::state::*;
  use rapidbyte_types::state_backend::StateBackend;
  use rapidbyte_types::envelope::DlqRecord;
  use testcontainers::runners::AsyncRunner;
  use testcontainers_modules::postgres::Postgres;

  async fn setup_pg() -> (PgBackend, testcontainers::ContainerAsync<Postgres>) {
      let container = Postgres::default().start().await.unwrap();
      let port = container.get_host_port_ipv4(5432).await.unwrap();
      let connstr = format!("postgres://postgres:postgres@localhost:{port}/postgres");
      let backend = PgBackend::connect(&connstr).await.unwrap();
      backend.migrate().await.unwrap();
      (backend, container)
  }
  ```

- [ ] **Step 2:** Add cursor tests (6 tests):
  ```rust
  #[tokio::test]
  async fn pg_cursor_get_returns_none_for_missing() { ... }
  #[tokio::test]
  async fn pg_cursor_set_and_get_roundtrip() { ... }
  #[tokio::test]
  async fn pg_cursor_set_updates_existing() { ... }
  #[tokio::test]
  async fn pg_cursor_compare_and_set_succeeds() { ... }
  #[tokio::test]
  async fn pg_cursor_compare_and_set_fails_on_mismatch() { ... }
  #[tokio::test]
  async fn pg_cursor_compare_and_set_insert_if_absent() { ... }
  ```

- [ ] **Step 3:** Add run record tests (3 tests):
  ```rust
  #[tokio::test]
  async fn pg_run_record_start_returns_id() { ... }
  #[tokio::test]
  async fn pg_run_record_complete_sets_status() { ... }
  #[tokio::test]
  async fn pg_run_record_complete_nonexistent_fails() { ... }
  ```

- [ ] **Step 4:** Add DLQ tests (2 tests):
  ```rust
  #[tokio::test]
  async fn pg_dlq_insert_records() { ... }
  #[tokio::test]
  async fn pg_dlq_insert_empty_batch() { ... }
  ```

- [ ] **Step 5:** Add StateBackend sync tests (3 tests):
  ```rust
  #[tokio::test]
  async fn pg_state_backend_cursor_roundtrip() { ... }
  #[tokio::test]
  async fn pg_state_backend_run_lifecycle() { ... }
  #[tokio::test]
  async fn pg_state_backend_dlq_insert() { ... }
  ```

- [ ] **Step 6:** Add migration test:
  ```rust
  #[tokio::test]
  async fn pg_migrate_is_idempotent() {
      let (backend, _container) = setup_pg().await;
      // migrate() already called in setup_pg
      // Call it again
      backend.migrate().await.unwrap(); // should not error
  }
  ```

- [ ] **Step 7:** Run `cargo test -p rapidbyte-engine --features integration -- postgres` — verify 15 pass
- [ ] **Step 8:** Commit: `test(engine): add Postgres integration tests with testcontainers`

---

## Task 8: WASM runner integration tests

**Files:**
- Create: `crates/rapidbyte-engine/tests/wasm_runner_integration.rs`

Requires test plugins from Task 6. All tests gated behind `#[cfg(feature = "integration")]`.

- [ ] **Step 1:** Create `tests/wasm_runner_integration.rs` with plugin path helper:
  ```rust
  #![cfg(feature = "integration")]

  use std::path::PathBuf;
  use std::sync::{mpsc, Arc, Mutex};
  use rapidbyte_engine::adapter::wasm_runner::WasmPluginRunner;
  use rapidbyte_engine::domain::ports::runner::*;
  use rapidbyte_types::state_backend::{NoopStateBackend, noop_state_backend};
  use rapidbyte_runtime::engine::WasmRuntime;

  fn test_plugin_path(name: &str) -> PathBuf {
      let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
          .join("tests/fixtures/plugins")
          .join(name)
          .join("target/wasm32-wasip2/debug")
          .join(format!("{name}.wasm"));
      if !path.exists() {
          eprintln!("Test plugin not found: {path:?}. Run `just build-test-plugins` first.");
          std::process::exit(0); // Skip gracefully
      }
      path
  }

  fn make_runner() -> WasmPluginRunner {
      let runtime = WasmRuntime::new().unwrap();
      WasmPluginRunner::new(runtime, noop_state_backend())
  }
  ```

- [ ] **Step 2:** Add source tests:
  ```rust
  #[tokio::test]
  async fn wasm_source_emits_frames() { ... }
  #[tokio::test]
  async fn wasm_source_outcome_has_correct_counts() { ... }
  #[tokio::test]
  async fn wasm_source_error_propagates() { ... }
  ```

- [ ] **Step 3:** Add destination + transform tests:
  ```rust
  #[tokio::test]
  async fn wasm_destination_consumes_frames() { ... }
  #[tokio::test]
  async fn wasm_transform_passes_through() { ... }
  ```

- [ ] **Step 4:** Add validate + discover tests:
  ```rust
  #[tokio::test]
  async fn wasm_validate_returns_ok() { ... }
  #[tokio::test]
  async fn wasm_discover_returns_streams() { ... }
  ```

- [ ] **Step 5:** Add end-to-end test:
  ```rust
  #[tokio::test]
  async fn wasm_full_pipeline_source_to_dest() {
      // Create EngineContext with real WasmPluginRunner
      // Use test source + test destination
      // Call run_pipeline
      // Verify PipelineOutcome::Run with correct counts
  }
  ```

- [ ] **Step 6:** Run `just build-test-plugins && cargo test -p rapidbyte-engine --features integration -- wasm` — verify 8 pass
- [ ] **Step 7:** Commit: `test(engine): add WASM runner integration tests`

---

## Task 9: Final verification

- [ ] **Step 1:** Run all unit tests: `cargo test -p rapidbyte-engine` — verify ~83 pass
- [ ] **Step 2:** Run all tests including integration: `cargo test -p rapidbyte-engine --features integration` — verify ~115 pass
- [ ] **Step 3:** Run clippy: `cargo clippy -p rapidbyte-engine -- -D warnings` — verify clean
- [ ] **Step 4:** Verify no `allow(dead_code)` or `allow(unused)` in test code (except legitimate clippy pragmas)
- [ ] **Step 5:** Run full workspace: `cargo test --workspace` — verify no regressions
- [ ] **Step 6:** Commit: `test(engine): complete test suite — 115 tests across all layers`
