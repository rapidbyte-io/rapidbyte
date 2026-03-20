# Known Issues

Tracked issues for future work. Each entry includes context, impact, and recommended approach.

## Security

### Plugin signature verification disabled in distributed mode

**Severity:** High
**Context:** The controller hexagonal refactor (PR #refactoring5) removed `trust_policy` and `trusted_key_pems` from the `RegisterResponse` proto. Agents now default to `trust_policy = "skip"` unless explicitly configured via `--trust-policy` CLI flag.

**Impact:** In distributed deployments, agents can run with plugin signature verification skipped unless operators explicitly set `--trust-policy warn|verify` and `--trusted-key-path` on each agent. There is no centralized enforcement path.

**Current state:**
- Agent CLI already accepts `--trust-policy` and `--trusted-key-path` flags
- Agent default is `"skip"` (`worker.rs:91`)
- Controller config had `TrustConfig` but it was removed as dead code (server never consumed it)

**Recommended approach:**
1. Change agent default from `"skip"` to `"warn"` so unsigned plugins produce visible warnings
2. Log a startup warning if `trust_policy = "skip"` in distributed mode (agent connected to controller)
3. Consider re-adding trust fields to `RegisterResponse` for centralized enforcement (requires proto change, controller config, agent override logic)

## Engine — Hexagonal Refactor

### Partitioned/parallel stream execution not implemented

**Severity:** High
**Context:** The engine hexagonal refactor removed the old partition planner/scheduler (~500 lines). `run_pipeline` now executes streams sequentially and always sets `partition_count`, `partition_index`, and `partition_strategy` to `None` in StreamContext.

**Impact:** `resources.parallelism` and autotune partitioning are non-functional for partitioned reads. Sources that support partitioned execution (e.g., Postgres range-based partitioning) will only run unpartitioned, causing throughput loss for large tables.

**Current state:**
- `effective_parallelism` is set from `ResourceConfig.parallelism` but only as metadata
- `partition_count`/`partition_index` are always `None` (`run.rs:319-324`)
- TODO comment documents the gap

**Recommended approach:**
1. Reimplement a stream planner that splits streams into partition tasks based on `resources.parallelism`
2. Spawn partition tasks concurrently (already have concurrent stage execution infrastructure)
3. Aggregate per-partition outcomes into per-stream results
4. Port the autotune logic for adaptive partition count

### Cancellation cannot interrupt in-flight WASM plugin execution

**Severity:** Medium
**Context:** Cancellation is checked between streams and during retry sleep, but once source/transform/destination tasks are spawned via `tokio::spawn`, the code awaits them without racing against the cancel token. WASM execution is synchronous blocking inside `spawn_blocking`.

**Impact:** Ctrl-C / lease cancellation may not stop a long-running plugin until it naturally completes. This is a fundamental limitation of synchronous WASM execution — you cannot interrupt a running WASM module without killing the thread.

**Current state:**
- Cancel checked at: loop start (`run.rs:191`), between streams (`run.rs:273`), during retry sleep (`run.rs:650` via `tokio::select!`)
- Not checked during: source/transform/destination stage execution

**Recommended approach:**
1. Wasmtime epoch interruption — configure epoch deadlines on the Wasmtime store and periodically increment the epoch when cancellation is requested. This is the only way to interrupt WASM mid-execution without thread killing.
2. Alternatively, use `tokio::select!` to race stage handles against the cancel token, then abort the JoinHandle on cancellation (stage continues in background but result is discarded).

### Check does not validate state backend connectivity

**Severity:** Low
**Context:** `check_pipeline` uses a noop state backend via `build_lightweight_context`. State connectivity is validated at runtime by `build_run_context`. This is by design — check validates plugin compatibility, not infrastructure.

**Impact:** `rapidbyte check` can report success for a pipeline with a broken `state.connection`. The failure only surfaces at `rapidbyte run` time.

**Current state:**
- Documented in `check.rs:224-248`
- Validator does not require `state.connection` (would break discover/check flows)

**Recommended approach:**
- Consider adding a `--validate-infra` flag to `rapidbyte check` that optionally tests state connectivity
- Or add a warning (not error) in check output when `state.connection` is absent

### Tie-breaker cursor type not persisted in database schema

**Severity:** Medium
**Context:** The `sync_cursors` table only stores `cursor_value TEXT`. The cursor type (Utf8, Lsn, Json, Number, etc.) is not persisted. On load, tie-breaker cursors are detected by attempting JSON parse, with fallback to Utf8.

**Impact:** Cursor type fidelity is lost across runs. The JSON detection heuristic works for composite tie-breaker cursors but cannot distinguish between a plain string that happens to be valid JSON and an actual composite cursor.

**Current state:**
- Write path: `cursor_value_to_string()` serializes all types to string
- Read path: tries JSON parse for tie-breaker streams, falls back to Utf8 (`run.rs:266-275`)
- Tests cover both JSON and scalar fallback paths

**Recommended approach:**
1. Add `cursor_type TEXT` column to `sync_cursors` table (new migration)
2. Persist cursor type alongside value
3. On load, use persisted type to reconstruct the correct `CursorValue` variant

### Dry-run protocol field still present in WIT interface

**Severity:** Low
**Context:** The CLI `--dry-run` / `--limit` flags and engine-level dry-run logic were removed. However, the WIT `RunRequest` struct still has `dry_run: bool` and `max_records: Option<u64>` fields, always set to `false` / `None`.

**Impact:** Dead protocol fields. No runtime impact since they're always false/None.

**Recommended approach:**
- Remove `dry_run` and `max_records` from the WIT `RunRequest` in a protocol version bump
- This is a breaking plugin interface change requiring a new protocol version

## Efficiency

### Heartbeat N+1 query pattern

**Severity:** Medium
**Context:** The heartbeat use case processes tasks sequentially with per-task DB lookups: `find_by_id` for each task + `find_by_id` for each run (cancellation check). For an agent with N concurrent tasks, this generates 2N DB round-trips per heartbeat.

**Impact:** Latency scales linearly with concurrent tasks per agent. Acceptable at low concurrency but problematic at scale.

**Recommended approach:** Batch task/run lookups into a single query with `WHERE id IN (...)` or a JOIN-based approach. Requires new repository methods.

### SELECT * in hot paths

**Severity:** Low
**Context:** All Postgres repository queries use `SELECT *`, including `list_runs` which fetches full `pipeline_yaml` (potentially several KB per row) for summary views.

**Impact:** Unnecessary network/deserialization overhead, especially for list operations.

**Recommended approach:** Add column-specific queries for list/summary endpoints. Requires either a separate `RunSummary` row mapper or a lightweight query method on the repository.

### DLQ batch inserts are per-record

**Severity:** Low
**Context:** Both the async `DlqRepository` (`dlq.rs`) and the channel bridge worker (`mod.rs`) insert DLQ records one at a time in a loop within a transaction.

**Impact:** For large DLQ batches (100+ records), this generates N SQL round-trips instead of 1 multi-row INSERT.

**Recommended approach:** Use `sqlx::QueryBuilder` to construct a single multi-row INSERT statement, or use PostgreSQL `COPY` for bulk inserts.

## Testing

### No restart/recovery integration tests

**Severity:** Medium
**Context:** The prior reconciliation integration tests were removed with the old code. The new system self-heals via lease expiry, but there are no integration tests verifying controller restart while tasks are in-flight.

**Recommended approach:** Add an integration test (testcontainers) that submits a pipeline, polls a task, restarts the controller (new PgPool against same DB), and verifies the lease sweep picks up the orphaned task.

### No CDC cursor resume E2E test

**Severity:** Medium
**Context:** CDC cursor loading logic was changed (now uses `CursorType::Lsn`, defaults `cursor_field` to `"lsn"`). The CDC E2E test only verifies a single run's output count, not restart/resume semantics across two runs with persisted state.

**Recommended approach:** Add a CDC E2E test that:
1. Runs a CDC pipeline, verifies output
2. Inserts new rows in source
3. Runs again, verifies only new rows are processed (cursor resume worked)

### No timeout enforcement integration test

**Severity:** Low
**Context:** Sandbox `timeout_seconds` is now passed to the Wasmtime store for all paths (run, check, discover). But no test exercises actual timeout behavior — all test plugins complete quickly.

**Recommended approach:** Create a test plugin that deliberately sleeps/loops and verify the Wasmtime epoch interruption fires within the configured timeout.

### WASM runner integration tests pass sandbox_overrides: None

**Severity:** Low
**Context:** All WASM runner integration tests set `sandbox_overrides: None` for validate/discover params. The timeout enforcement path added in the refactor is not exercised.

**Recommended approach:** Add integration tests that pass `sandbox_overrides: Some(SandboxOverrides { timeout_seconds: Some(1), .. })` against the test plugins.
