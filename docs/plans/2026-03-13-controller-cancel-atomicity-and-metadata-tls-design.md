# Controller Cancel Atomicity And Metadata TLS Design

**Date:** 2026-03-13

**Problem**

The persistent-controller branch still has two gaps:

1. `cancel_queued_run` can durably persist a terminal run while leaving its task durably pending if the task write fails after the run write. That breaks restart recovery because the DB snapshot can resurrect a zombie pending task.
2. The metadata Postgres connection is hardcoded to `NoTls`, even though metadata Postgres is now mandatory for controller startup in distributed mode.

**Goal**

Eliminate partial durable cancellation for queued runs and allow secure metadata Postgres transport without inventing controller-specific database TLS semantics.

## Decision Summary

Use snapshot-based rollback for queued cancellation and keep metadata DB TLS configuration in the Postgres connection string. Cancellation should only be considered accepted if both the run and latest queued task are durably updated; metadata DB TLS should follow standard Postgres URL parameters such as `sslmode`.

## Architecture

### Queued Cancel Atomicity

Treat queued cancel the same way submit and assignment are now treated: capture the pre-mutation run/task snapshots, mutate in memory, persist the changed task and run, and roll both in-memory records back if either durable write fails.

For queued cancel specifically:

- snapshot the run before transition
- snapshot the latest queued task before marking it cancelled
- transition the run to `Cancelled`
- cancel the latest queued task in memory
- persist the cancelled task first
- persist the cancelled run second
- publish the terminal event only after both writes succeed

If task persistence fails, restore both snapshots and return `INTERNAL`.

If task persistence succeeds but run persistence fails, best-effort durably restore the previous task snapshot, restore both in-memory snapshots, and return `INTERNAL`.

This keeps durable state and in-memory state aligned and prevents restart from rehydrating a live task behind a terminal run.

### Poll Rejection Repair

The review also called out the `poll_task` rejection branch. That branch is not the primary root cause for the queued-cancel zombie, but it should still preserve persistence symmetry when a claimed assignment is rejected because the run cannot transition. The fix is to persist the rejected task state before returning `None`, so a process crash cannot leave durable `Assigned` state after in-memory rejection.

### Metadata DB TLS

Do not add controller-specific flags like `--metadata-db-tls-cert`. Instead, pass the metadata Postgres URL through the standard tokio-postgres config parser and let URL parameters drive transport mode.

Expected behavior:

- existing local URLs continue working with default `prefer`/disabled TLS semantics as supported by the client stack
- production deployments can use `sslmode=require`, `sslmode=verify-ca`, or `sslmode=verify-full`
- invalid or unsupported TLS requirements fail closed during controller startup

This keeps the controller aligned with normal Postgres operational practice and avoids maintaining a parallel TLS DSL in the CLI.

## Integration Plan

### Controller State And Services

Add direct record-level persistence helpers where needed so cancel flows can persist exact snapshots rather than reading whatever happens to be in memory later.

Update `PipelineServiceImpl::cancel_queued_run` to use the same rollback structure already used for submit and assignment. Avoid publishing cancellation before durable writes succeed.

Update the agent-service rejection path only as far as needed to durably persist the rejected assignment state. Do not broaden that change into a larger scheduler refactor.

### Metadata Store Bootstrap

Replace `tokio_postgres::connect(..., NoTls)` with parsed `tokio_postgres::Config` and a TLS connector path that supports both non-TLS and TLS URLs. Startup should continue to fail closed if the URL is invalid or the requested TLS mode cannot be satisfied.

CLI validation stays simple: require a non-empty metadata DB URL, but do not inspect or reinterpret `sslmode` beyond deferring to Postgres parsing and connection behavior.

## Testing Strategy

- Add a failing controller test that injects task persistence failure during queued cancel and proves the run and task both roll back to pending state.
- Add a failing controller test that simulates run persistence failure after durable task cancellation and proves the task is durably restored and in-memory state is rolled back.
- Add or extend an agent-service test for the rejected-assignment branch if durable persistence changes there.
- Add metadata store tests proving TLS-capable config parsing accepts URL parameters and that non-TLS local URLs still work.
- Re-run the controller and CLI suites plus the ignored Postgres-backed persistence/restart tests.

## Rollout

Ship both fixes in this branch before merge. The queued-cancel atomicity issue is correctness-critical for restart recovery, and metadata DB TLS is required to make mandatory distributed metadata storage viable outside local trusted networks.
