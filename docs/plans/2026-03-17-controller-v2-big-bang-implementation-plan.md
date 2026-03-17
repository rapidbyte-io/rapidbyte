# Controller V2 Big-Bang Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current controller control plane with the V2 hexagonal architecture and V2 API, deleting legacy V1 code paths so the crate has no compatibility shims or structural tech debt.

**Architecture:** Build a domain-first controller core (`domain` + `app` + `ports`) and re-implement transport/storage/background behavior as adapters. Migrate protocol to `rapidbyte.v2` with session-based agent communication, then remove V1 services/modules once parity tests pass. Treat all migration work as replacement, not layering.

**Tech Stack:** Rust, Tokio, tonic/prost, tokio-postgres, rapidbyte-metrics, rapidbyte-secrets, workspace integration tests, controller unit/integration tests.

---

## Execution Rules

1. Use @superpowers/test-driven-development for each task.
2. Use @superpowers/systematic-debugging for every unexpected test failure.
3. Use @superpowers/verification-before-completion before declaring each milestone complete.
4. Do not add compatibility wrappers unless explicitly required by a test in this plan.
5. If functionality is confusing, redundant, or low-value, remove it and document removal in `docs/REFACTORING_CONTROLLER.md` follow-up notes.

---

### Task 1: Add Architecture Guardrails (Failing First)

**Files:**
- Create: `crates/rapidbyte-controller/tests/v2/architecture_boundaries.rs`
- Modify: `crates/rapidbyte-controller/Cargo.toml`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

**Step 1: Write the failing test**

Add an architecture test that fails when domain modules import transport/persistence crates:

```rust
#[test]
fn domain_must_not_depend_on_tonic_or_tokio_postgres() {
    let domain = std::fs::read_to_string("crates/rapidbyte-controller/src/domain/mod.rs")
        .unwrap_or_default();
    assert!(!domain.contains("tonic::"));
    assert!(!domain.contains("tokio_postgres::"));
}
```

**Step 2: Run the test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller architecture_boundaries -- --nocapture
```

Expected:
- FAIL (domain module does not exist yet)

**Step 3: Add minimal skeleton modules**

Create `src/domain/mod.rs`, `src/app/mod.rs`, `src/ports/mod.rs`, `src/adapters/mod.rs`, `src/bootstrap/mod.rs`, and export them from `lib.rs`.

**Step 4: Run the test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-controller architecture_boundaries -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/lib.rs crates/rapidbyte-controller/src/{domain,app,ports,adapters,bootstrap}/mod.rs crates/rapidbyte-controller/tests/v2/architecture_boundaries.rs crates/rapidbyte-controller/Cargo.toml
git commit -m "test(controller): add v2 architecture boundaries scaffold"
```

### Task 2: Build Domain State Machines (Run + TaskAttempt + Lease)

**Files:**
- Create: `crates/rapidbyte-controller/src/domain/run.rs`
- Create: `crates/rapidbyte-controller/src/domain/task_attempt.rs`
- Create: `crates/rapidbyte-controller/src/domain/lease.rs`
- Create: `crates/rapidbyte-controller/src/domain/error.rs`
- Modify: `crates/rapidbyte-controller/src/domain/mod.rs`
- Test: `crates/rapidbyte-controller/tests/v2/domain_state_transitions.rs`

**Step 1: Write failing state transition tests**

```rust
#[test]
fn run_cannot_transition_from_succeeded_to_executing() {
    let mut run = Run::new(RunId::new("r1"));
    run.force_state_for_test(RunState::Succeeded);
    assert!(run.transition(RunState::Executing).is_err());
}

#[test]
fn stale_lease_is_rejected() {
    let lease = Lease::new(10, "agent-a", std::time::SystemTime::now());
    assert!(!lease.matches("agent-a", 9));
}
```

**Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller domain_state_transitions -- --nocapture
```

Expected:
- FAIL (types not implemented)

**Step 3: Implement minimal domain model**

Implement:
- `RunState` and transition matrix
- `TaskAttemptState` and transition matrix
- `Lease::matches(agent_id, epoch)`
- typed domain errors (`StateViolation`, `LeaseStale`)

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-controller domain_state_transitions -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/domain/{mod.rs,run.rs,task_attempt.rs,lease.rs,error.rs} crates/rapidbyte-controller/tests/v2/domain_state_transitions.rs
git commit -m "feat(controller): add v2 domain run/task/lease state machines"
```

### Task 3: Define Ports + Transaction Boundary

**Files:**
- Create: `crates/rapidbyte-controller/src/ports/repositories.rs`
- Create: `crates/rapidbyte-controller/src/ports/unit_of_work.rs`
- Create: `crates/rapidbyte-controller/src/ports/event_bus.rs`
- Create: `crates/rapidbyte-controller/src/ports/telemetry.rs`
- Create: `crates/rapidbyte-controller/src/ports/secrets.rs`
- Create: `crates/rapidbyte-controller/src/ports/clock.rs`
- Create: `crates/rapidbyte-controller/src/ports/id_generator.rs`
- Modify: `crates/rapidbyte-controller/src/ports/mod.rs`
- Test: `crates/rapidbyte-controller/tests/v2/port_contracts_compile.rs`

**Step 1: Write failing compile contract test**

Add a test that instantiates in-memory fake implementations for all required ports and compiles a `SubmitRunUseCase` constructor.

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller port_contracts_compile -- --nocapture
```

Expected:
- FAIL (traits missing)

**Step 3: Implement minimal ports**

Include a transaction interface with explicit closure boundary:

```rust
#[async_trait::async_trait]
pub trait UnitOfWork: Send + Sync {
    async fn in_transaction<T, F, Fut>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        T: Send;
}
```

**Step 4: Re-run focused test**

Run:

```bash
cargo test -p rapidbyte-controller port_contracts_compile -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/ports/*.rs crates/rapidbyte-controller/tests/v2/port_contracts_compile.rs
git commit -m "feat(controller): define v2 ports and transaction boundary"
```

### Task 4: Implement Core Use-Cases (Submit/Get/List/Cancel/Retry)

**Files:**
- Create: `crates/rapidbyte-controller/src/app/submit_run.rs`
- Create: `crates/rapidbyte-controller/src/app/get_run.rs`
- Create: `crates/rapidbyte-controller/src/app/list_runs.rs`
- Create: `crates/rapidbyte-controller/src/app/cancel_run.rs`
- Create: `crates/rapidbyte-controller/src/app/retry_run.rs`
- Modify: `crates/rapidbyte-controller/src/app/mod.rs`
- Test: `crates/rapidbyte-controller/tests/v2/use_case_run_lifecycle.rs`

**Step 1: Write failing lifecycle tests**

```rust
#[tokio::test]
async fn submit_is_idempotent_inside_single_transaction_boundary() {
    let first = use_case.execute(cmd("key-1")).await.unwrap();
    let second = use_case.execute(cmd("key-1")).await.unwrap();
    assert_eq!(first.run_id, second.run_id);
}

#[tokio::test]
async fn retry_requires_terminal_retryable_failure() {
    assert!(retry_use_case.execute(retry_cmd("run-1")).await.is_err());
}
```

**Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller use_case_run_lifecycle -- --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal use-cases**

Implement orchestration only in app layer:
- `SubmitRunUseCase`
- `GetRunUseCase`
- `ListRunsUseCase`
- `CancelRunUseCase`
- `RetryRunUseCase`

Ensure every mutating use-case runs through `UnitOfWork::in_transaction`.

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-controller use_case_run_lifecycle -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/app/*.rs crates/rapidbyte-controller/tests/v2/use_case_run_lifecycle.rs
git commit -m "feat(controller): add v2 core run use-cases"
```

### Task 5: Add In-Memory Adapters For Fast Tests

**Files:**
- Create: `crates/rapidbyte-controller/src/adapters/in_memory/mod.rs`
- Create: `crates/rapidbyte-controller/src/adapters/in_memory/repos.rs`
- Create: `crates/rapidbyte-controller/src/adapters/in_memory/unit_of_work.rs`
- Create: `crates/rapidbyte-controller/src/adapters/in_memory/event_bus.rs`
- Modify: `crates/rapidbyte-controller/src/adapters/mod.rs`
- Test: `crates/rapidbyte-controller/tests/v2/in_memory_adapter_behavior.rs`

**Step 1: Write failing in-memory adapter tests**

Test atomic rollback semantics by forcing a fake error inside transaction closure and asserting no repository mutations escaped.

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller in_memory_adapter_behavior -- --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal adapters**

Implement deterministic in-memory adapters with rollback-on-error semantics for tests.

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-controller in_memory_adapter_behavior -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/adapters/in_memory/*.rs crates/rapidbyte-controller/src/adapters/mod.rs crates/rapidbyte-controller/tests/v2/in_memory_adapter_behavior.rs
git commit -m "feat(controller): add v2 in-memory adapters for deterministic tests"
```

### Task 6: Introduce V2 Protocol (`rapidbyte.v2`) And Build Wiring

**Files:**
- Create: `proto/rapidbyte/v2/controller.proto`
- Modify: `crates/rapidbyte-controller/build.rs`
- Modify: `crates/rapidbyte-controller/src/proto.rs`
- Modify: `crates/rapidbyte-agent/src/proto.rs`
- Test: `crates/rapidbyte-controller/tests/v2/proto_generation.rs`

**Step 1: Write failing protocol generation test**

Add a compile-time sanity test that imports `crate::proto::rapidbyte::v2::control_plane_server::ControlPlane` and `agent_session_server::AgentSession`.

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller proto_generation -- --nocapture
```

Expected:
- FAIL (`rapidbyte.v2` types missing)

**Step 3: Add V2 proto and generate**

Define V2 services/messages with session-based agent protocol and unified `ApiError` message.

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-controller proto_generation -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add proto/rapidbyte/v2/controller.proto crates/rapidbyte-controller/build.rs crates/rapidbyte-controller/src/proto.rs crates/rapidbyte-agent/src/proto.rs crates/rapidbyte-controller/tests/v2/proto_generation.rs
git commit -m "feat(proto): add rapidbyte.v2 control-plane contract"
```

### Task 7: Implement Postgres Adapter For V2 Repositories

**Files:**
- Create: `crates/rapidbyte-controller/src/adapters/postgres/mod.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/run_repo.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/task_repo.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/agent_repo.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/preview_repo.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/unit_of_work.rs`
- Create: `crates/rapidbyte-controller/src/adapters/postgres/migrations/0002_controller_v2.sql`
- Modify: `crates/rapidbyte-controller/src/store/mod.rs`
- Test: `crates/rapidbyte-controller/tests/v2/postgres_repository_roundtrip.rs`

**Step 1: Write failing Postgres repository test**

Create ignored integration test that verifies:
- run+task assignment commit atomically
- stale lease completion is rejected
- retry insertion creates next attempt and closes prior attempt

**Step 2: Run to verify failure**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller postgres_repository_roundtrip -- --ignored --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal Postgres adapters**

Implement repositories and transaction manager using explicit SQL per aggregate; keep business decisions in app/domain.

**Step 4: Re-run focused integration test**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller postgres_repository_roundtrip -- --ignored --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/adapters/postgres/*.rs crates/rapidbyte-controller/src/adapters/postgres/migrations/0002_controller_v2.sql crates/rapidbyte-controller/src/store/mod.rs crates/rapidbyte-controller/tests/v2/postgres_repository_roundtrip.rs
git commit -m "feat(controller): add v2 postgres repository adapters"
```

### Task 8: Implement V2 gRPC Adapters (ControlPlane + AgentSession)

**Files:**
- Create: `crates/rapidbyte-controller/src/adapters/grpc/mod.rs`
- Create: `crates/rapidbyte-controller/src/adapters/grpc/control_plane.rs`
- Create: `crates/rapidbyte-controller/src/adapters/grpc/agent_session.rs`
- Create: `crates/rapidbyte-controller/src/adapters/grpc/convert.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/middleware.rs`
- Test: `crates/rapidbyte-controller/tests/v2/grpc_control_plane_contract.rs`
- Test: `crates/rapidbyte-controller/tests/v2/grpc_agent_session_contract.rs`

**Step 1: Write failing gRPC contract tests**

Add tests for:
- submit/get/list/cancel/retry semantics
- session handshake + assignment + progress + complete flow
- uniform error envelope serialization

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller grpc_control_plane_contract grpc_agent_session_contract -- --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal gRPC adapters**

Map proto DTOs to use-case commands and back; keep adapters thin:

```rust
async fn submit_run(
    &self,
    request: tonic::Request<SubmitRunRequest>,
) -> Result<tonic::Response<SubmitRunResponse>, tonic::Status> {
    let cmd = convert::submit_request_to_command(request.into_inner())?;
    let out = self.submit_use_case.execute(cmd).await.map_err(convert::to_status)?;
    Ok(tonic::Response::new(convert::submit_output_to_response(out)))
}
```

**Step 4: Re-run focused gRPC tests**

Run:

```bash
cargo test -p rapidbyte-controller grpc_control_plane_contract grpc_agent_session_contract -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/adapters/grpc/*.rs crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/middleware.rs crates/rapidbyte-controller/tests/v2/grpc_*_contract.rs
git commit -m "feat(controller): add v2 grpc adapters"
```

### Task 9: Rewire Background Jobs Through Use-Cases

**Files:**
- Create: `crates/rapidbyte-controller/src/app/reconcile_restart.rs`
- Create: `crates/rapidbyte-controller/src/app/timeout_attempt.rs`
- Create: `crates/rapidbyte-controller/src/app/cleanup_preview.rs`
- Create: `crates/rapidbyte-controller/src/adapters/background/mod.rs`
- Create: `crates/rapidbyte-controller/src/adapters/background/lease_sweep.rs`
- Create: `crates/rapidbyte-controller/src/adapters/background/reaper.rs`
- Create: `crates/rapidbyte-controller/src/adapters/background/preview_cleanup.rs`
- Test: `crates/rapidbyte-controller/tests/v2/background_use_case_integration.rs`

**Step 1: Write failing background integration tests**

Verify background loops call use-cases and preserve atomicity guarantees when persistence fails.

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller background_use_case_integration -- --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal background adapters**

Each loop should only trigger use-case methods; no direct repository mutations.

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-controller background_use_case_integration -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/app/{reconcile_restart.rs,timeout_attempt.rs,cleanup_preview.rs} crates/rapidbyte-controller/src/adapters/background/*.rs crates/rapidbyte-controller/tests/v2/background_use_case_integration.rs
git commit -m "refactor(controller): route background processing through v2 use-cases"
```

### Task 10: Update CLI + Agent To V2 API

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-agent/src/progress.rs`
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/commands/status.rs`
- Modify: `crates/rapidbyte-cli/src/commands/watch.rs`
- Modify: `crates/rapidbyte-cli/src/commands/list_runs.rs`
- Modify: `crates/rapidbyte-cli/tests/distributed.rs`
- Test: `crates/rapidbyte-agent/tests/session_flow.rs`

**Step 1: Write failing client/agent integration tests**

Add tests for:
- agent session flow assignment/progress/completion
- distributed CLI run/watch/status/list against V2 endpoints

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-agent session_flow -- --nocapture
cargo test -p rapidbyte-cli distributed -- --nocapture
```

Expected:
- FAIL

**Step 3: Implement minimal client changes**

Replace poll/heartbeat/progress RPC choreography with session stream client in agent.

**Step 4: Re-run focused tests**

Run:

```bash
cargo test -p rapidbyte-agent session_flow -- --nocapture
cargo test -p rapidbyte-cli distributed -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/{worker.rs,progress.rs,proto.rs} crates/rapidbyte-agent/tests/session_flow.rs crates/rapidbyte-cli/src/commands/{distributed_run.rs,status.rs,watch.rs,list_runs.rs} crates/rapidbyte-cli/tests/distributed.rs
git commit -m "feat(controller): migrate cli and agent to v2 protocol"
```

### Task 11: Remove Legacy V1 Code Paths (No Compatibility Layer)

**Files:**
- Delete: `crates/rapidbyte-controller/src/services/agent/mod.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/register.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/poll.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/heartbeat.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/complete.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/dispatch.rs`
- Delete: `crates/rapidbyte-controller/src/services/agent/secret.rs`
- Delete: `crates/rapidbyte-controller/src/services/pipeline/mod.rs`
- Delete: `crates/rapidbyte-controller/src/services/pipeline/submit.rs`
- Delete: `crates/rapidbyte-controller/src/services/pipeline/query.rs`
- Delete: `crates/rapidbyte-controller/src/services/pipeline/cancel.rs`
- Delete: `crates/rapidbyte-controller/src/services/pipeline/convert.rs`
- Delete: `crates/rapidbyte-controller/src/services/mod.rs`
- Delete: `proto/rapidbyte/v1/controller.proto`
- Modify: `crates/rapidbyte-controller/src/lib.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Test: `crates/rapidbyte-controller/tests/v2/no_legacy_modules.rs`

**Step 1: Write failing no-legacy test**

```rust
#[test]
fn v1_service_modules_are_removed() {
    assert!(!std::path::Path::new("crates/rapidbyte-controller/src/services/agent/mod.rs").exists());
    assert!(!std::path::Path::new("proto/rapidbyte/v1/controller.proto").exists());
}
```

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller no_legacy_modules -- --nocapture
```

Expected:
- FAIL

**Step 3: Delete legacy modules and rewire exports**

Remove V1 files, remove old module exports/imports, and ensure V2 adapters are the only runtime path.

**Step 4: Re-run focused test**

Run:

```bash
cargo test -p rapidbyte-controller no_legacy_modules -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add -A crates/rapidbyte-controller/src/services proto/rapidbyte/v1/controller.proto crates/rapidbyte-controller/src/{lib.rs,server.rs} crates/rapidbyte-controller/tests/v2/no_legacy_modules.rs
git commit -m "refactor(controller): remove legacy v1 services and proto"
```

### Task 12: Functionality Pruning Sweep (Remove What Does Not Make Sense)

**Files:**
- Modify: `docs/REFACTORING_CONTROLLER.md`
- Modify: `docs/plans/2026-03-17-controller-v2-big-bang-implementation-plan.md`
- Modify: `crates/rapidbyte-controller/src/config.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Test: `crates/rapidbyte-controller/tests/v2/feature_pruning_contract.rs`

**Step 1: Write failing pruning contract test**

Add tests that assert removed features are truly gone (example: deprecated flags or low-value states not present in V2 API/config).

**Step 2: Run to verify failure**

Run:

```bash
cargo test -p rapidbyte-controller feature_pruning_contract -- --nocapture
```

Expected:
- FAIL

**Step 3: Remove low-value features and simplify config/API surface**

Apply this decision rule:
- keep only features with clear user value + clear ownership + test coverage
- delete ambiguous toggles or duplicate state semantics

Document every deletion with reason and replacement in `docs/REFACTORING_CONTROLLER.md`.

Pruning executed in this branch:
- removed controller auth escape hatch `allow_unauthenticated` and CLI flag `--allow-unauthenticated`
- removed controller signing-key escape hatch `allow_insecure_default_signing_key` and CLI flag `--allow-insecure-signing-key`
- added regression guard `crates/rapidbyte-controller/tests/v2/feature_pruning_contract.rs`

**Step 4: Re-run pruning tests**

Run:

```bash
cargo test -p rapidbyte-controller feature_pruning_contract -- --nocapture
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add docs/REFACTORING_CONTROLLER.md docs/plans/2026-03-17-controller-v2-big-bang-implementation-plan.md crates/rapidbyte-controller/src/config.rs crates/rapidbyte-cli/src/commands/controller.rs crates/rapidbyte-controller/tests/v2/feature_pruning_contract.rs
git commit -m "refactor(controller): prune low-value functionality from v2 surface"
```

### Task 13: Full Workspace Verification + Release Notes

**Files:**
- Modify: `docs/plans/2026-03-17-controller-v2-big-bang-implementation-plan.md`
- Modify: `docs/REFACTORING_CONTROLLER.md`
- Modify: `README.md`
- Modify: `crates/rapidbyte-controller/src/lib.rs`

**Step 1: Format and lint**

Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- PASS

**Step 2: Run tests**

Run:

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-agent
cargo test -p rapidbyte-cli
cargo test --workspace
```

Expected:
- PASS

**Step 3: Run ignored Postgres-backed distributed tests**

Run:

```bash
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-controller -- --ignored
RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL="host=127.0.0.1 port=33270 user=postgres password=postgres dbname=postgres" cargo test -p rapidbyte-cli -- --ignored
```

Expected:
- PASS

Execution notes in this branch:
- Started local Postgres test container on `127.0.0.1:33270` to satisfy metadata-store ignored tests.
- `cargo test -p rapidbyte-controller -- --ignored` passed (store + v2 postgres roundtrip tests).
- `cargo test -p rapidbyte-cli -- --ignored` currently runs zero tests; distributed CLI restart coverage now lives in non-ignored `distributed_cli_contract::*` tests executed in Step 2.

**Step 4: Final docs and changelog update**

Document:
- V2 API changes
- removed V1 behavior
- migration notes for CLI/agent consumers

**Step 5: Commit**

```bash
git add README.md docs/REFACTORING_CONTROLLER.md docs/plans/2026-03-17-controller-v2-big-bang-implementation-plan.md crates/rapidbyte-controller/src/lib.rs
git commit -m "chore(controller): verify v2 cutover and publish migration notes"
```

---

Plan complete and saved to `docs/plans/2026-03-17-controller-v2-big-bang-implementation-plan.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
