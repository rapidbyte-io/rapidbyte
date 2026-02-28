# Rapidbyte Coding Style Blueprint

Maintainer-focused standards for writing high-performance, correctness-critical Rust across engine, runtime, state, SDK, and connectors.

## 1) Purpose and Non-Goals

### Purpose

- Define merge-quality coding standards for ingestion/runtime paths.
- Encode DRY, SOLID, and YAGNI in a way that improves throughput and correctness.
- Provide a consistent refactor rubric to prioritize technical debt reduction.

### Non-Goals

- This is not a Rust language tutorial.
- This is not a preference guide for low-impact style choices.
- This does not replace protocol contracts in `docs/PROTOCOL.md`.

## 2) Rule Semantics

- `MUST`: Required for merge. Deviations require explicit maintainer exception.
- `MUST NOT`: Prohibited pattern.
- `SHOULD`: Strong default; deviation requires rationale in PR.
- `MAY`: Optional pattern for context-specific use.

Every `MUST`/`MUST NOT` rule includes:

- Failure mode prevented
- Scope (host, connector, tests, benches)
- Verification method

## 3) Core Principles for Rapidbyte

### DRY (Behavioral DRY)

- `MUST` deduplicate invariants and semantics, not just text.
- `MUST NOT` duplicate protocol mapping logic across crates.
- `SHOULD` share domain-specific helpers only when at least two concrete call sites exist.

Failure mode prevented: semantic drift across connectors/host boundaries.

### SOLID (Applied, Not Dogmatic)

- `MUST` keep external boundaries explicit: connector protocol, runtime host imports, state backends.
- `SHOULD` introduce traits/interfaces only for real substitution points already needed in code.
- `MUST NOT` add abstraction layers to satisfy hypothetical future connectors.

Failure mode prevented: indirection-heavy code that obscures correctness and slows hot paths.

### YAGNI (Perf-Aware)

- `MUST` implement only behavior required by protocol/spec and active roadmap.
- `MUST NOT` add speculative extension hooks in hot-path code.
- `SHOULD` favor simple concrete implementations until a second use case forces generalization.

Failure mode prevented: complexity debt and unnecessary runtime overhead.

## 4) Correctness Standards (Ingestion-Critical)

### 4.1 Data Semantics

- `MUST` make row drop behavior explicit and policy-driven (`fail`/`skip`/`dlq`).
- `MUST` keep batch/stream scope semantics deterministic and documented.
- `MUST` preserve all configured validation failure reasons when reporting invalid rows.

Failure mode prevented: silent data loss and nondeterministic pipeline outcomes.

Verification: unit tests + e2e policy matrix coverage.

### 4.2 Error Modeling

- `MUST` preserve error category boundaries (`config`, `data`, `infrastructure`, etc.).
- `MUST` include actionable context in public/boundary errors.
- `MUST NOT` collapse typed errors into opaque strings at boundary interfaces.
- `MUST NOT` panic in runtime data paths.

Failure mode prevented: retry misclassification, unrecoverable crashes, opaque incidents.

Verification: typed error assertions in tests, review checklist.

### 4.3 Numeric and Type Safety

- `MUST` define behavior for non-finite floats (`NaN`, `+/-Inf`) in rule evaluation.
- `MUST` preserve Decimal semantics (scale-aware comparisons).
- `MUST NOT` rely on lossy conversions without explicit acceptance of precision tradeoffs.

Failure mode prevented: acceptance/rejection drift and production data-quality regressions.

Verification: edge-case unit tests for numeric boundaries and decimal variants.

### 4.4 State, Checkpoints, and Idempotency

- `MUST` treat checkpoint writes and commit coordination as correctness-critical.
- `MUST` make idempotency assumptions explicit when retries can replay effects.
- `MUST NOT` advance cursor/state on partial or ambiguous destination outcomes.

Failure mode prevented: duplicate writes, data gaps, irreversible cursor corruption.

Verification: engine integration tests and checkpoint correlation tests.

## 5) Performance Standards (Hot-Path Rust)

### 5.1 Allocation Discipline

- `MUST` avoid per-row heap allocation in tight loops unless justified.
- `SHOULD` preallocate with known capacities.
- `SHOULD` prefer borrowed/sliced data over cloning in data-path code.
- `MUST` annotate unavoidable allocation-heavy code with rationale.

Failure mode prevented: throughput collapse from allocator pressure.

Verification: benchmark evidence + targeted review.

### 5.2 Throughput and Backpressure

- `MUST` use bounded channels and explicit backpressure semantics.
- `MUST NOT` introduce unbounded buffering between stages.
- `SHOULD` keep stage coupling visible in orchestration code.

Failure mode prevented: memory blowups and unstable tail latency.

Verification: architecture review + load/e2e behavior checks.

### 5.3 Async/Blocking Boundaries

- `MUST` make blocking operations explicit in async contexts.
- `MUST NOT` hide blocking I/O in async hot paths.
- `SHOULD` keep CPU-heavy transforms off latency-sensitive control paths.

Failure mode prevented: scheduler starvation and unpredictable throughput.

Verification: review + focused perf runs.

### 5.4 Observability in Performance Work

- `MUST` emit metrics for critical failure and throughput dimensions.
- `SHOULD` include stable labels for high-value cardinality dimensions (for example `rule`, `field`).
- `MUST NOT` emit high-cardinality labels with uncontrolled user values.

Failure mode prevented: invisible regressions and unusable monitoring.

Verification: metrics unit tests + e2e assertions where practical.

## 6) Code Structure and API Conventions

### Visibility and API Surface

- `MUST` keep crate-internal types/functions `pub(crate)` unless external use is intentional.
- `SHOULD` re-export only stable, high-value API types from crate `lib.rs`.
- `MUST NOT` leak experimental internals through public modules.

### Function and Module Design

- `SHOULD` keep function responsibilities narrow and directly testable.
- `MUST` separate parsing/validation/evaluation concerns when behavior differs.
- `MUST NOT` centralize unrelated concerns in giant utility modules.

### Comments and Documentation

- `SHOULD` comment invariants and non-obvious constraints, not line-by-line mechanics.
- `MUST` update protocol/config docs when behavior contracts change.
- `MUST NOT` leave stale docs after interface changes.

## 7) Testing and Verification Standards

### Required Coverage by Change Type

- Rule/config behavior changes: unit tests for happy path + invalid shape + edge values.
- Engine/runtime wiring changes: integration tests and relevant e2e path.
- Policy behavior changes (`fail`/`skip`/`dlq`): e2e assertions for all affected modes.
- Perf-sensitive changes: benchmark or targeted throughput evidence.

### Minimum Verification Commands

- `just fmt`
- `just lint`
- Relevant crate tests (`cargo test -p <crate>` or manifest-path)
- Relevant e2e suite (`cargo test --manifest-path tests/e2e/Cargo.toml --test <suite>`)

`MUST` include command evidence in PR description for non-trivial behavior/perf changes.

## 8) Enforcement Matrix

### Merge-Blocking

- Formatting/lint failures
- Test regressions in affected scope
- Missing coverage for changed correctness semantics
- Undocumented behavior changes to protocol/config contracts

### Review-Blocking Checklist

- Correctness semantics preserved or intentionally changed and documented
- Error categories preserved at boundaries
- No unbounded buffers introduced
- Hot-path allocation impact considered
- Metrics/logging implications considered

### Exceptions

- `SHOULD` deviations: allowed with rationale in PR
- `MUST`/`MUST NOT` deviations: require maintainer sign-off with risk + rollback note

## 9) Refactor Priority Rubric (Blueprint Driver)

Score each crate/module from `0..5` in each dimension:

- Correctness risk
- Performance impact
- Blast radius
- Maintainability debt
- Observability gap

Priority score:

```text
Priority = 2*Correctness + 2*Performance + BlastRadius + Maintainability + Observability
```

Classes:

- `P0`: high correctness + high performance pressure; refactor first
- `P1`: meaningful debt reduction with moderate operational risk
- `P2`: opportunistic cleanup and consistency work

### Suggested Initial Focus Areas

- P0 candidates: orchestrator stage coupling and checkpoint/commit coordination paths
- P1 candidates: connector validation helpers and shared metric-shape utilities
- P2 candidates: low-impact module decomposition and naming consistency

## 10) Good and Bad Patterns

### Good: hot-loop preallocation

```rust
let mut failures = Vec::with_capacity(config.rules.len());
```

### Bad: hidden per-iteration temporary allocation

```rust
for row in 0..batch.num_rows() {
    let failures = Vec::new(); // repeated allocation in hot path
}
```

### Good: explicit non-finite handling

```rust
if !value.is_finite() {
    return Err(DataError::non_finite(field));
}
```

### Bad: implicit comparison semantics with NaN

```rust
if value < min || value > max {
    // NaN can bypass both checks
}
```

## 11) PR Template Snippet

Use this in maintainers' PR descriptions for non-trivial changes:

```markdown
## Coding Style Compliance
- [ ] Correctness semantics unchanged or documented
- [ ] Error categories preserved
- [ ] No unbounded buffering introduced
- [ ] Hot-path allocation impact reviewed
- [ ] Metrics/logging impact reviewed

## Verification
- [ ] just fmt
- [ ] just lint
- [ ] crate tests
- [ ] relevant e2e
- [ ] perf evidence (if hot path touched)
```
