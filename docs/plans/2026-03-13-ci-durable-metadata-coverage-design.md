# CI Durable Metadata Coverage Design

**Date:** 2026-03-13

**Problem**

The persistent-controller branch adds its most important behavior behind ignored Postgres-backed tests:

- controller metadata store roundtrip and transaction tests
- repaired-snapshot recovery tests
- distributed restart reconciliation and timeout/resume tests

Current GitHub Actions CI only runs `cargo test --workspace --all-targets`, so those ignored tests never execute in default PR validation. That leaves the branch's core durable-state guarantees unverified in CI.

## Decision Summary

Add one dedicated CI job that provisions Postgres, exports `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL`, and runs the ignored durable-metadata and restart-recovery test commands explicitly.

Keep the existing `test` job unchanged. The new job is additive: it validates the durable-store contract without slowing or destabilizing the normal unit/integration suite.

## Design

### Dedicated Workflow Job

Extend `.github/workflows/ci.yml` with a new job, for example `durable-metadata`.

Responsibilities:

1. start a Postgres service container
2. install the same Rust toolchain and `wasm32-wasip2` target as the existing test jobs
3. export `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL`
4. run the ignored controller-store tests
5. run the ignored distributed restart/integration tests

Recommended connection string:

`host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres`

The job should be independent of the regular `test` job so failures in durable-state coverage are surfaced directly.

### Postgres Service

Use a standard GitHub Actions service container:

- image: `postgres:16-alpine`
- env:
  - `POSTGRES_USER=postgres`
  - `POSTGRES_PASSWORD=postgres`
  - `POSTGRES_DB=postgres`
- ports:
  - `5432:5432`
- health check:
  - `pg_isready -U postgres`

This is enough for the existing ignored tests, which create isolated schemas inside the shared database rather than relying on pre-seeded state.

### Commands To Run

The job should execute the same commands already used for manual verification:

- `cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_transaction_create_run_with_task_commits_both_records -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_preserves_original_recovery_started_at -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_prunes_expired_previews -- --ignored`
- `cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

Do not convert these tests to non-ignored in this patch. Their current ignored status still makes sense for local development without Postgres.

### Scope Boundary

Do not rework the test suite structure or add new scripts unless the workflow becomes unreadable.

The minimal correct change is:

- one workflow job
- one service definition
- one shared env var for the metadata DB URL
- explicit ignored-test commands

## Testing Strategy

Validate locally by:

- reviewing the generated workflow diff
- confirming the commands exactly match the manually verified commands
- optionally running the existing ignored commands locally, which already passed on this branch

The workflow itself will be validated by GitHub Actions on the next push.

## Rollout

Ship this in the branch before merge. Without it, the branch's most important controller durability behavior is not part of standard PR validation.
