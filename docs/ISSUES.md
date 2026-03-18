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

## Testing

### No restart/recovery integration tests

**Severity:** Medium
**Context:** The prior reconciliation integration tests were removed with the old code. The new system self-heals via lease expiry, but there are no integration tests verifying controller restart while tasks are in-flight.

**Recommended approach:** Add an integration test (testcontainers) that submits a pipeline, polls a task, restarts the controller (new PgPool against same DB), and verifies the lease sweep picks up the orphaned task.
