# RAPIDBYTE-CONTROLLER — Complete Refactoring Guide

**Big-Bang V2 Blueprint · Superb DX · Clean API · Zero-Nonsense Boundaries**

Version 1.0 — March 2026
No backward compatibility assumed.

---

## Table Of Contents

1. Executive Summary
2. Refactoring Goals
3. Current Architecture Assessment
4. V2 Architecture Blueprint
5. V2 Domain Model And Invariants
6. V2 API Contract (CLI + Agent DX)
7. Persistence Strategy And Crate Boundaries
8. Observability, Errors, And Operations UX
9. Module Ownership And File Layout
10. Naming Standards And Code Conventions
11. Big-Bang Execution Plan
12. Verification And Acceptance Criteria
13. Risks And Mitigations

---

## 1. Executive Summary

This guide defines a full redesign of `rapidbyte-controller` as a **hexagonal, domain-first control plane** with strict boundaries between lifecycle logic, persistence, transport, and background orchestration.

The current controller is functional, but key behavior is spread across large modules and multiple orchestration layers, which increases cognitive load and raises the chance of behavioral drift as features grow. The refactor objective is to make the controller the easiest crate in the repository to read, modify, and trust.

### Core Decision Set

- **Architecture:** Hexagonal (ports and adapters) with a strongly typed domain core.
- **Migration style:** Big-bang V2 in one dedicated branch (no wire compatibility constraints).
- **API policy:** Breaking changes allowed; optimize for long-term API ergonomics.
- **Priority profile:** Balanced across contributor DX, external API DX, and operational correctness.
- **Persistence policy:** Controller-owned metadata persistence remains in `rapidbyte-controller` for V2, with clean extraction ports.

---

## 2. Refactoring Goals

The V2 controller must satisfy all goals simultaneously.

1. **Contributor DX:** New engineers can confidently change lifecycle behavior without touching transport internals.
2. **API DX:** CLI and agents interact through clear intent-level contracts, not hidden choreography.
3. **Operational integrity:** Durable state is canonical; restart and recovery behavior is deterministic.
4. **No conceptual debt:** Every module has one ownership reason and one test strategy.
5. **Predictable extensibility:** Adding features should mean adding one use-case and one adapter, not editing unrelated systems.

---

## 3. Current Architecture Assessment

Current hotspots and complexity concentration:

- `services/agent/mod.rs` (~3.2k lines)
- `store/mod.rs` (~1.8k lines)
- `services/pipeline/mod.rs` (~1.3k lines)
- `scheduler.rs` (~850 lines)
- `state.rs` (~730 lines)
- `run_state.rs` (~570 lines)

### Current Smell Patterns

1. **Responsibility blending:** lifecycle transitions, persistence, event fanout, and adapter details are mixed in request handlers.
2. **Cross-module invariants:** rules are spread across `run_state`, `scheduler`, terminal helpers, and background loops.
3. **Lock-shaped architecture:** `ControllerState` currently acts as a broad mutable service locator.
4. **Adapter leakage:** transport and storage details influence lifecycle code paths.
5. **Large-file gravity:** adding features nudges contributors into editing oversized files with wide blast radius.

### Consequence

Even with solid tests, the current shape makes it too easy to introduce subtle regressions in lease fencing, retry behavior, and recovery semantics when evolving APIs.

---

## 4. V2 Architecture Blueprint

V2 centers on a pure domain core and explicit boundary ports.

```text
crates/rapidbyte-controller/src/
├── api/
│   ├── mod.rs
│   ├── server.rs                  # Public bootstrap entry points
│   └── config.rs                  # Controller runtime config API
├── app/                           # Use-case orchestration (transaction boundaries)
│   ├── mod.rs
│   ├── submit_run.rs
│   ├── cancel_run.rs
│   ├── stream_run.rs
│   ├── agent_open_session.rs
│   ├── reconcile_restart.rs
│   └── retry_run.rs
├── domain/                        # Pure behavior, no tonic/postgres/tokio-postgres
│   ├── mod.rs
│   ├── run.rs
│   ├── task_attempt.rs
│   ├── agent.rs
│   ├── lease.rs
│   ├── preview.rs
│   ├── recovery.rs
│   ├── policy.rs                  # retry/fencing/transition policy
│   └── error.rs
├── ports/                         # Interfaces used by app layer
│   ├── mod.rs
│   ├── repositories.rs            # RunRepo/TaskRepo/AgentRepo/PreviewRepo
│   ├── unit_of_work.rs            # transactional boundary
│   ├── event_bus.rs               # run event publication
│   ├── telemetry.rs               # metrics + tracing façade
│   ├── secrets.rs                 # secret resolution abstraction
│   ├── clock.rs
│   └── id_generator.rs
├── adapters/
│   ├── grpc/                      # protobuf mapping + interceptors + streaming
│   ├── postgres/                  # durable metadata store implementation
│   ├── in_memory/                 # fast deterministic test adapters
│   ├── observability/             # rapidbyte-metrics integration
│   └── background/                # sweep/reaper/preview cleanup workers
├── bootstrap/
│   ├── mod.rs
│   ├── wiring.rs                  # production dependency composition
│   └── startup_checks.rs
└── lib.rs
```

### Design Rules

1. `domain` cannot import `tonic`, `tokio_postgres`, or generated protobuf modules.
2. `app` can depend only on `domain` + `ports`.
3. `adapters` implement `ports`, never domain behavior.
4. gRPC handlers are thin translators: request -> use-case -> response.
5. Background tasks invoke use-cases; they do not mutate repositories directly.

---

## 5. V2 Domain Model And Invariants

### Aggregates

1. `Run`: lifecycle state, attempts, current assignment pointer, terminal summary.
2. `TaskAttempt`: immutable execution payload + mutable execution state.
3. `Agent`: registration metadata, capacity, liveness, compatibility traits.
4. `Preview`: preview availability and TTL envelope.
5. `RecoverySession`: restart reconciliation decision state.

### Value Objects

- `Lease` (`epoch`, `owner_agent_id`, `expires_at`)
- `RunId`, `TaskId`, `AgentId`, `IdempotencyKey`
- `AttemptNumber`
- `FailureClassification`

### Non-Negotiable Invariants

1. A run can have only one active attempt at a time.
2. Every mutation of run+task assignment state is atomic.
3. A stale lease can never alter durable state.
4. Terminal events are emitted only after durable commit success.
5. Retry eligibility is decided by domain policy, not transport handlers.
6. Recovery timeout decisions are explicit terminal outcomes.

### Canonical Run States (V2)

```text
Accepted -> Queued -> Dispatched -> Executing -> PreviewReady -> Succeeded
                                   -> FailedRetryable -> Queued (next attempt)
                                   -> FailedTerminal
                                   -> CancelRequested -> Cancelled
                                   -> Reconciliation -> Executing | FailedTerminal
```

### Canonical TaskAttempt States (V2)

```text
Created -> Leased -> Running -> Completed
                     -> Failed
                     -> Cancelled
                     -> LeaseExpired
                     -> ReconciliationExpired
```

---

## 6. V2 API Contract (CLI + Agent DX)

V2 should expose intent-level APIs instead of multi-RPC choreography.

### Service Layout

```proto
service ControlPlane {
  rpc SubmitRun(SubmitRunRequest) returns (SubmitRunResponse);
  rpc GetRun(GetRunRequest) returns (GetRunResponse);
  rpc ListRuns(ListRunsRequest) returns (ListRunsResponse);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc RetryRun(RetryRunRequest) returns (RetryRunResponse);
  rpc StreamRun(StreamRunRequest) returns (stream RunEvent);
}

service AgentSession {
  rpc OpenSession(stream AgentMessage) returns (stream ControllerMessage);
}
```

### API Principles

1. CLI sees stable user-oriented semantics: submit, watch, cancel, retry, inspect.
2. Agent lifecycle operates through one bidirectional session protocol.
3. Lease and fencing mechanics remain internal protocol fields, not user UX concepts.
4. All failures return typed machine-readable error envelopes.

### Unified Error Envelope

```text
ApiError {
  code: VALIDATION | AUTH | CONFLICT | LEASE_STALE | STATE_VIOLATION | INTERNAL
  category: CLIENT | TRANSIENT | OPERATIONAL
  message: string
  details: map<string,string>
  retry_hint: NO_RETRY | RETRY_IMMEDIATE | RETRY_BACKOFF | RETRY_AFTER_RECONCILIATION
  correlation_id: string
}
```

### Why This Is Better

1. Reduces race-prone client choreography.
2. Shrinks agent implementation complexity.
3. Produces richer, consistent diagnostics for operators and CLI consumers.

---

## 7. Persistence Strategy And Crate Boundaries

### Recommendation

Keep controller persistence controller-owned in V2, with extraction-friendly ports.

### Rationale

1. `rapidbyte-state` is currently optimized for engine data-plane state semantics.
2. Controller needs control-plane invariants (leases, assignments, reconciliation windows, idempotency transactions) that should not be shoehorned into unrelated abstractions.
3. Ports preserve future optional extraction without forcing premature coupling.

### Cross-Crate Integration Pattern

1. `rapidbyte-metrics`: used via telemetry adapter behind `TelemetryPort`.
2. `rapidbyte-secrets`: used via `SecretResolverPort` in dispatch use-case.
3. `rapidbyte-types`: shared value types only.
4. `rapidbyte-state`: no direct dependency in domain logic; optional future backing for selected repository concerns only when models align.

### Transaction Policy

- All run+task mutation flows use `UnitOfWork` port.
- Transaction ownership sits in app layer use-cases.
- Repositories perform row persistence only; business decisions stay domain-side.

---

## 8. Observability, Errors, And Operations UX

### Telemetry Contract

Every use-case emits a consistent lifecycle span and result metric:

1. `submit_run`
2. `dispatch_assignment`
3. `report_progress`
4. `complete_attempt`
5. `reconcile_restart`
6. `timeout_attempt`
7. `cleanup_preview`

### Required Labels

- `controller.operation`
- `controller.result` (`ok`, `error`, `cancelled`, `retryable`)
- `controller.failure_code` (when present)
- `controller.state_before`
- `controller.state_after`

### Operator UX Rules

1. Every terminal failure includes code, retry intent, and reconciliation guidance.
2. Recovery-timeout failures are visibly distinct from execution failures.
3. API responses and stream events must include correlation IDs for tracing.

---

## 9. Module Ownership And File Layout

### Current -> V2 Ownership Mapping

1. `services/agent/*` -> `adapters/grpc/agent_session` + `app/agent_open_session`.
2. `services/pipeline/*` -> `adapters/grpc/control_plane` + dedicated use-cases in `app/`.
3. `run_state.rs` + parts of `scheduler.rs` -> `domain/run.rs`, `domain/task_attempt.rs`, `domain/policy.rs`.
4. `state.rs` -> split into `ports/*` contracts and `bootstrap/wiring.rs` composition.
5. `store/mod.rs` -> `adapters/postgres/*` with focused repository files per aggregate.
6. `background/*` -> `adapters/background/*` that call app use-cases only.
7. `terminal.rs` + watcher interactions -> event bus adapter and terminal transition use-cases.

### Max File Size Targets

1. Domain files: 300 lines soft cap.
2. Use-case files: 250 lines soft cap.
3. Adapter files: 400 lines soft cap.
4. Test modules: split once beyond 500 lines.

---

## 10. Naming Standards And Code Conventions

### Naming Rules

1. Use `*UseCase` for app orchestration objects.
2. Use `*Repo` for persistence interfaces.
3. Use `*Record` only in adapter/database rows, never in domain.
4. Use `*State` for domain finite state enums only.
5. Use `*Event` for domain events and stream payloads, never for commands.

### Anti-Patterns To Ban

1. Domain code importing protobuf-generated modules.
2. Repositories deciding retry/cancel policy.
3. gRPC handlers acquiring multiple locks and performing workflow logic.
4. Background jobs mutating storage without going through use-cases.
5. New files containing unrelated concern bundles.

---

## 11. Big-Bang Execution Plan

This is a **single coordinated V2 branch cutover**, but still executed in ordered stages for safety.

### Stage A: Skeleton And Guardrails

1. Create V2 directory skeleton (`domain`, `app`, `ports`, `adapters`, `bootstrap`, `api`).
2. Introduce crate-level boundary lint conventions (module-level docs and dependency discipline).
3. Add architecture tests that fail if domain imports transport/persistence crates.

### Stage B: Domain + Ports

1. Implement domain aggregates, state machines, policies, and typed errors.
2. Define repository, unit-of-work, event bus, telemetry, clock, and ID generation ports.
3. Add pure domain tests for all state transitions and policy decisions.

### Stage C: App Use-Cases

1. Implement submit, cancel, retry, progress, complete, and reconciliation use-cases.
2. Ensure transaction boundaries are explicit and owned by use-cases.
3. Add use-case tests with in-memory adapters.

### Stage D: Adapters

1. Implement Postgres repositories and transactions.
2. Implement gRPC API adapters and protobuf mapping.
3. Implement background adapters and event streaming adapter.
4. Wire rapidbyte-metrics through telemetry adapter.

### Stage E: Bootstrap + Cutover

1. Build new composition root and startup checks.
2. Remove legacy v1 modules after parity tests pass.
3. Regenerate proto bindings and update CLI/agent integration points.

### Stage F: Hardening

1. Run end-to-end restart/recovery scenarios.
2. Validate observability outputs and operational docs.
3. Freeze public V2 contract and write upgrade notes.

---

## 12. Verification And Acceptance Criteria

Refactor is done only when every criterion passes.

### Architecture Criteria

1. Domain layer has zero transport/persistence dependency leaks.
2. All lifecycle transitions are defined in one domain policy package.
3. No file above 1,000 lines in V2 code paths.

### Correctness Criteria

1. Lease stale updates are rejected deterministically after restart.
2. Run/task terminal durability is atomic in all paths (foreground and background).
3. Reconciliation timeout behavior is consistent and explicitly represented.

### API/DX Criteria

1. Agent integration requires one session-oriented protocol implementation.
2. CLI run lifecycle commands map 1:1 to service intents.
3. All error responses include `code`, `retry_hint`, and `correlation_id`.

### Quality Gates

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test -p rapidbyte-controller`
4. `cargo test --workspace`
5. Existing distributed restart and metadata durability scenarios pass.

---

## 13. Risks And Mitigations

1. **Risk:** Big-bang branch drifts too long from mainline. **Mitigation:** short, time-boxed branch with daily rebases and strict scope lock.
2. **Risk:** API redesign introduces hidden client friction. **Mitigation:** provide v2 client fixtures and protocol contract tests before merge.
3. **Risk:** Over-abstracted ports become ceremony. **Mitigation:** enforce “one business reason per port” and delete speculative interfaces.
4. **Risk:** Postgres adapter complexity grows uncontrolled. **Mitigation:** split by aggregate repository and keep SQL transaction helpers local.
5. **Risk:** Recovery policy regressions under load. **Mitigation:** add deterministic concurrency/restart simulation tests as merge blockers.

---

## 14. Task 12 Pruning Notes

1. Removed controller auth escape hatch `allow_unauthenticated` and CLI flag `--allow-unauthenticated`. Reason: this toggle weakens production-safe defaults and creates ambiguous ownership of security posture. Replacement: controller startup now always requires `--auth-token` / `RAPIDBYTE_AUTH_TOKEN`.
2. Removed controller signing-key escape hatch `allow_insecure_default_signing_key` and CLI flag `--allow-insecure-signing-key`. Reason: this keeps an unsafe default path in the public surface and duplicates deployment policy decisions. Replacement: controller startup now always requires explicit `--signing-key` / `RAPIDBYTE_SIGNING_KEY`.
3. Added pruning guardrails in `crates/rapidbyte-controller/tests/v2/feature_pruning_contract.rs` to ensure removed controller toggles and flags do not regress back into source surfaces.

---

## 15. Task 13 Verification And Migration Notes

1. Verification gates passed: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p rapidbyte-controller`, `cargo test -p rapidbyte-agent`, `cargo test -p rapidbyte-cli`, and `cargo test --workspace`.
2. Postgres-backed ignored controller tests passed against local Postgres on `127.0.0.1:33270` via `RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL`.
3. Legacy ignored distributed CLI restart tests were removed during V2 cutover; distributed contract coverage is now provided by non-ignored `distributed_cli_contract::*` tests in `crates/rapidbyte-cli/tests/distributed.rs`.
4. V2 migration surface: `proto/rapidbyte/v1/controller.proto` and legacy controller service modules were removed; controller/agent integration now targets `rapidbyte.v2` session and control-plane APIs.
5. CLI migration surface: controller startup now requires explicit metadata DB URL, auth token, and signing key; insecure controller flags (`--allow-unauthenticated`, `--allow-insecure-signing-key`) are no longer part of the public controller interface.

---

## Final Note

This V2 refactor should be treated as foundational platform work. The target is not to merely “clean up files,” but to establish a controller architecture where **behavior is obvious, boundaries are enforceable, and new features are naturally incremental**.
