# Engine Test Suite Design

**Date:** 2026-03-18
**Status:** Approved
**Scope:** Comprehensive unit and integration test suite for the refactored `rapidbyte-engine` hexagonal crate.

## Goals

- Achieve test density comparable to `rapidbyte-controller` (~95 tests, up from 43)
- Cover all happy paths, error paths, and edge cases across domain/application/adapter layers
- Unit tests use fakes (fast, no I/O); integration tests use real infrastructure (testcontainers + test WASM plugins)
- Integration tests gated behind `#[cfg(feature = "integration")]` so `cargo test` stays fast
- No `allow(dead_code)` or test-only code leaking into production

## Architecture

```
Unit tests (fakes, fast)          Integration tests (real infra, slow)
─────────────────────────         ────────────────────────────────────
domain/error.rs (8)               postgres_integration.rs (15)
domain/retry.rs (10)                └── testcontainers Postgres
domain/ports/mod.rs (3)           wasm_runner_integration.rs (8)
application/run.rs (20)             └── test WASM plugins
application/check.rs (10)        pipeline_integration.rs (6, existing)
application/discover.rs (8)       policy_proptest.rs (2, existing)
application/testing.rs (7)        resolve_visibility.rs (1, existing)
adapter/wasm_runner.rs (5)
adapter/engine_factory.rs (3)
adapter/progress.rs (1)
adapter/registry_resolver.rs (2)
```

`cargo test` runs 63 unit tests (~1 second).
`cargo test --features integration` runs all 95 tests (~30 seconds with container startup).

## Domain Layer Tests

### error.rs — 8 tests (6 existing + 2 new)

Existing tests cover PipelineError classification and RetryParams extraction.

New tests:

| Test | Verifies |
|------|----------|
| `infra_error_displays_message` | `PipelineError::infra("db down").to_string()` contains "db down" |
| `cancelled_displays_message` | `PipelineError::Cancelled.to_string()` is "pipeline cancelled" |

### retry.rs — 10 tests (7 existing + 3 new)

Existing tests cover core backoff computation, cap, hint override, max attempts.

New edge case tests:

| Test | Verifies |
|------|----------|
| `retry_attempt_zero_uses_base_delay` | Attempt 0 doesn't underflow in `2^(attempt-1)` (saturating_sub) |
| `max_attempts_one_means_no_retries` | `RetryPolicy::new(1)` gives up on first error |
| `retry_after_hint_overrides_even_slow_class` | 200ms hint overrides 5s Slow base |

### ports/mod.rs — 3 new tests

| Test | Verifies |
|------|----------|
| `is_conflict_returns_true_for_conflict` | `RepositoryError::Conflict("x".into()).is_conflict()` is true |
| `is_conflict_returns_false_for_other` | `RepositoryError::other(io_error).is_conflict()` is false |
| `other_wraps_error` | `RepositoryError::other(err)` preserves source error via Display |

### outcome.rs, progress.rs — no direct tests

Pure data types — tested indirectly via application layer consumers. Adding tests would just verify struct construction, which Rust's type system already guarantees.

## Application Layer Tests

All use `fake_context()` from `testing.rs`. Fast, deterministic, no I/O.

### run.rs — 20 tests (8 existing + 12 new)

Existing tests cover: happy path, cancellation, progress events, run recording, dry-run, multiple streams, infra error, resolution failure.

New tests:

**Retry behavior (biggest gap):**

| Test | Verifies |
|------|----------|
| `retry_on_retryable_plugin_error` | First call fails (retryable), second succeeds. `retry_count == 1`, `RetryScheduled` event emitted |
| `retry_gives_up_after_max_attempts` | All attempts fail with retryable errors. Error returned after `max_retries` exhausted |
| `non_retryable_plugin_error_not_retried` | Plugin error with `retryable: false` — immediate failure, no retry |

**Cursor persistence:**

| Test | Verifies |
|------|----------|
| `cursor_loaded_for_incremental_stream` | Stream with `cursor_field` — `ctx.cursors.get()` called, cursor value passed to source |
| `cursor_saved_after_successful_run` | Source emits checkpoint — `ctx.cursors.set()` called with checkpoint value |
| `no_cursor_load_for_full_refresh` | Stream without `cursor_field` — no cursor lookup |

**Multi-transform pipeline:**

| Test | Verifies |
|------|----------|
| `transforms_execute_in_order` | 2 transforms — both called with correct `transform_index` (0, 1) |
| `transform_error_fails_pipeline` | Transform error — pipeline fails, destination not called |

**Per-stream stats:**

| Test | Verifies |
|------|----------|
| `per_stream_stats_recorded_independently` | 2 streams with different row counts — each run record gets its own counts |

**Edge cases:**

| Test | Verifies |
|------|----------|
| `empty_streams_list_returns_empty_result` | Zero streams — clean return with zero counts |
| `cancellation_between_streams` | Cancel after first stream — second not executed, `Cancelled` returned |
| `destination_error_after_source_success` | Source OK, destination fails — error propagated |

### check.rs — 10 tests (6 existing + 4 new)

| Test | Verifies |
|------|----------|
| `check_with_no_transforms` | Empty transforms list — only source + dest validated |
| `check_transform_resolution_failure` | Transform doesn't resolve — error propagated |
| `check_multiple_transforms_all_validated` | 3 transforms — all appear in CheckResult.components |
| `check_mixed_validation_results` | Source OK, dest fails — both statuses captured, no early return |

### discover.rs — 8 tests (6 existing + 2 new)

| Test | Verifies |
|------|----------|
| `discover_returns_multiple_streams` | Source reports 5 streams — all returned |
| `discover_with_no_config` | `config_json: None` — works, None passed through |

### testing.rs — 7 tests (4 existing + 3 new)

| Test | Verifies |
|------|----------|
| `fake_dlq_repository_records_insertions` | Insert records, verify `inserted_records()` returns them |
| `fake_run_record_tracks_completions` | Start + complete, verify `started_count()` increments |
| `fake_resolver_returns_error_for_unknown` | Unregistered plugin ref returns error |

## Adapter Layer Unit Tests

### wasm_runner.rs — 5 tests (1 existing + 4 new)

| Test | Verifies |
|------|----------|
| `parse_compression_lz4` | `parse_compression(Some("lz4"))` returns `Ok(Some(Lz4))` |
| `parse_compression_none` | `parse_compression(None)` returns `Ok(None)` |
| `parse_compression_invalid` | `parse_compression(Some("brotli"))` returns error |
| `plugin_instance_key_format` | Key follows expected pattern |

### engine_factory.rs — 3 new tests

| Test | Verifies |
|------|----------|
| `run_on_backend_propagates_result` | Successful backend call returns Ok through spawn_blocking |
| `run_on_backend_converts_state_error` | `StateError` maps to `RepositoryError::Other` |
| `run_on_backend_handles_panic` | Panicking closure maps to `RepositoryError::Other` |

### progress.rs — 1 new test

| Test | Verifies |
|------|----------|
| `channel_reporter_sends_events` | Report events, verify receiver gets them |

## Integration Tests

### postgres_integration.rs — 15 new tests (testcontainers)

Gated behind `#[cfg(feature = "integration")]`. Uses `testcontainers-modules` to spin up Postgres.

**Setup helper:**
```rust
async fn setup_pg() -> (PgBackend, ContainerAsync<Postgres>) {
    let container = Postgres::default().start().await.unwrap();
    let connstr = format!(
        "postgres://postgres:postgres@localhost:{}/postgres",
        container.get_host_port_ipv4(5432).await.unwrap()
    );
    let backend = PgBackend::connect(&connstr).await.unwrap();
    backend.migrate().await.unwrap();
    (backend, container)
}
```

**Cursor tests (6):**

| Test | Verifies |
|------|----------|
| `pg_cursor_get_returns_none_for_missing` | No cursor set — get returns None |
| `pg_cursor_set_and_get_roundtrip` | Set then get — values match |
| `pg_cursor_set_updates_existing` | Set twice — second value overwrites first |
| `pg_cursor_compare_and_set_succeeds` | CAS with matching expected — returns true, value updated |
| `pg_cursor_compare_and_set_fails_on_mismatch` | CAS with wrong expected — returns false, value unchanged |
| `pg_cursor_compare_and_set_insert_if_absent` | CAS with `expected: None` — inserts new key |

**Run record tests (3):**

| Test | Verifies |
|------|----------|
| `pg_run_record_start_returns_id` | Start run returns positive i64 |
| `pg_run_record_complete_sets_status` | Start + complete — no error |
| `pg_run_record_complete_nonexistent_fails` | Complete bogus ID — returns error |

**DLQ tests (2):**

| Test | Verifies |
|------|----------|
| `pg_dlq_insert_records` | Insert 3 records — returns count 3 |
| `pg_dlq_insert_empty_batch` | Insert empty slice — returns 0 |

**StateBackend sync tests (3):**

| Test | Verifies |
|------|----------|
| `pg_state_backend_cursor_roundtrip` | Sync set_cursor + get_cursor match |
| `pg_state_backend_run_lifecycle` | Sync start_run + complete_run |
| `pg_state_backend_dlq_insert` | Sync insert_dlq_records returns correct count |

**Migration test (1):**

| Test | Verifies |
|------|----------|
| `pg_migrate_is_idempotent` | `migrate()` called twice — second is no-op |

### wasm_runner_integration.rs — 8 new tests (test WASM plugins)

Gated behind `#[cfg(feature = "integration")]`. Uses minimal test plugins.

| Test | Verifies |
|------|----------|
| `wasm_source_emits_frames` | Test source emits expected frames through channel |
| `wasm_source_outcome_has_correct_counts` | SourceOutcome.summary.records_read matches emitted rows |
| `wasm_destination_consumes_frames` | Feed frames to test destination, verify records_written |
| `wasm_transform_passes_through` | Source → transform → destination, row counts preserved |
| `wasm_validate_returns_ok` | validate_plugin on test source returns success |
| `wasm_discover_returns_streams` | discover on test source returns expected stream list |
| `wasm_source_error_propagates` | Source configured to fail — PipelineError::Plugin returned |
| `wasm_full_pipeline_source_to_dest` | End-to-end via run_pipeline with EngineContext + WasmPluginRunner |

## Test Plugins

Three minimal WASM plugins in `crates/rapidbyte-engine/tests/fixtures/plugins/`:

### test-source (~60 lines)

- `open()` — reads `row_count` (default 10) and `should_fail` from config JSON
- `run()` — emits `row_count` rows as Arrow IPC (single `id: Int64` column). Emits checkpoint with cursor = last ID. If `should_fail`, errors after half.
- `close()` — no-op
- `discover()` — returns one stream `"test-stream"`
- `validate()` — returns OK

### test-destination (~40 lines)

- `open()` — reads `should_fail` from config
- `run()` — consumes frames, counts rows. If `should_fail`, errors after first batch.
- `close()` — no-op
- `validate()` — returns OK

### test-transform (~30 lines)

- `open()` — no-op
- `run()` — pass-through: reads frames, writes unchanged
- `close()` — no-op
- `validate()` — returns OK

Each is a standalone Cargo project targeting `wasm32-wasip2` with `rapidbyte-sdk`. Built via `just build-test-plugins`.

Integration tests skip with clear message if WASM binaries not found.

## Dependencies

```toml
# crates/rapidbyte-engine/Cargo.toml
[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }

[features]
integration = []
test-support = []  # existing
```

## Error Path Coverage

| Error Path | Test |
|------------|------|
| Plugin error (retryable) | `run::retry_on_retryable_plugin_error` |
| Plugin error (non-retryable) | `run::non_retryable_plugin_error_not_retried` |
| Infrastructure error | `run::run_pipeline_infra_error_not_retried` |
| Cancellation | `run::run_pipeline_respects_cancellation`, `cancellation_between_streams` |
| Resolution failure | `run::run_pipeline_resolution_failure`, `check::check_returns_error_on_*` |
| Transform error | `run::transform_error_fails_pipeline` |
| Destination error | `run::destination_error_after_source_success` |
| Max retries exhausted | `run::retry_gives_up_after_max_attempts` |
| Cursor CAS failure | `postgres::pg_cursor_compare_and_set_fails_on_mismatch` |
| WASM plugin error | `wasm::wasm_source_error_propagates` |
| spawn_blocking panic | `engine_factory::run_on_backend_handles_panic` |
| Missing plugin | `discover::discover_returns_error_on_resolution_failure` |
| Invalid compression | `wasm_runner::parse_compression_invalid` |

## Test Counts

| Layer | Unit | Integration | Total |
|-------|------|-------------|-------|
| Domain | 21 | — | 21 |
| Application | 45 | — | 45 |
| Adapter | 11 | — | 11 |
| Integration | — | 32 | 32 |
| **Total** | **77** | **32** | **95** |*

*Includes 9 pre-existing integration tests (pipeline_integration, policy_proptest, resolve_visibility).
