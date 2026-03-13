# CI Durable Metadata Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make PR CI execute the ignored Postgres-backed durable-metadata and restart-recovery tests added by the persistent controller state work.

**Architecture:** Add a dedicated GitHub Actions job with a Postgres service container and the existing metadata DB env var, then run the ignored controller-store and distributed restart tests explicitly. Keep the existing fast test job unchanged.

**Tech Stack:** GitHub Actions, Postgres 16 Alpine service container, Rust stable toolchain, cargo test, existing ignored controller-store and CLI distributed tests.

---

### Task 1: Add the CI durable-metadata job

**Files:**
- Modify: `.github/workflows/ci.yml`

**Step 1: Write the failing coverage change**

Add a new `durable-metadata` job that:

- runs on `ubuntu-latest`
- starts a Postgres service container
- sets `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL`
- installs Rust stable with `wasm32-wasip2`
- runs the ignored controller-store tests
- runs the ignored distributed restart tests

Recommended YAML shape:

```yaml
  durable-metadata:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: postgres
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    env:
      RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL: host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres
```

**Step 2: Review the exact commands**

Ensure the job runs:

- `cargo test -p rapidbyte-controller metadata_store_roundtrips_runs_and_tasks -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_transaction_create_run_with_task_commits_both_records -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_preserves_original_recovery_started_at -- --ignored`
- `cargo test -p rapidbyte-controller metadata_store_repaired_snapshot_prunes_expired_previews -- --ignored`
- `cargo test -p rapidbyte-cli distributed_submit_and_complete -- --ignored`
- `cargo test -p rapidbyte-cli distributed_restart_reconciles_then_times_out -- --ignored`
- `cargo test -p rapidbyte-cli distributed_restart_reconciles_and_resumes_execution -- --ignored`

**Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run durable metadata coverage in PRs"
```

### Task 2: Verify the workflow diff and branch state

**Files:**
- Modify: none expected unless review reveals a mistake

**Step 1: Inspect the workflow**

Run:

```bash
sed -n '1,260p' .github/workflows/ci.yml
git diff -- .github/workflows/ci.yml
```

Expected:
- one additive `durable-metadata` job
- no changes to existing `fmt`, `clippy`, `test`, or `e2e-compile` semantics beyond job ordering if any

**Step 2: Reconfirm local durable commands are still the source of truth**

Run:

```bash
git status --short
```

Expected:
- only the workflow and plan docs changed

**Step 3: Push**

```bash
git add .github/workflows/ci.yml docs/plans/2026-03-13-ci-durable-metadata-coverage-design.md docs/plans/2026-03-13-ci-durable-metadata-coverage-plan.md
git commit -m "ci: run durable metadata coverage in PRs"
git push
```
