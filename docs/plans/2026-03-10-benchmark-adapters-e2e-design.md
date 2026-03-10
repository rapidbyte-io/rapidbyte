# Benchmark Adapters And E2E PR Benchmark Design

## Summary

Complete the missing benchmark adapter architecture and move the benchmark
platform off its current hardcoded Postgres pipeline path plus synthetic PR
smoke stub. The benchmark core should execute scenarios through adapter
contracts, with Postgres as the first concrete implementation, and the PR suite
should become a real end-to-end Rapidbyte benchmark rather than a synthetic
artifact generator.

## Goals

- Add real adapter contracts for source, destination, and transform benchmark
  behavior.
- Remove direct Postgres-specific execution knowledge from benchmark-core flow.
- Make the PR smoke benchmark run the actual engine/plugins end to end.
- Keep Postgres as the first supported connector set while preserving clean
  extension points for other plugins and isolated benchmark kinds.

## Non-Goals

- Fully implementing every benchmark kind in one tranche.
- Adding non-Postgres connector implementations in this slice.
- Making unsupported workload families silently fall back to synthetic output.
- Expanding CI scope beyond a deterministic, reproducible PR smoke benchmark.

## Alternatives Considered

### 1. Recommended: adapter contracts first, then rewire execution

Define benchmark adapter interfaces first, implement Postgres-backed adapters,
then move pipeline execution and PR smoke onto those interfaces.

Pros:
- Aligns the codebase with the original benchmark design.
- Avoids baking more connector-specific logic into the benchmark core.
- Leaves a clean path for future isolated source/destination/transform runs.

Cons:
- Slightly more up-front refactoring before visible benchmark behavior changes.

### 2. Make PR smoke end to end first, then generalize later

Replace the synthetic stub with a real Postgres PR benchmark immediately and
defer adapter cleanup.

Pros:
- Faster short-term progress for CI signal.

Cons:
- Duplicates more Postgres-specific logic that would have to be extracted later.
- Increases the risk of a second special-case execution path.

### 3. Generalize every benchmark kind at once

Implement full source, destination, transform, and pipeline execution for all
declared workload families in one pass.

Pros:
- Most complete architectural outcome.

Cons:
- Too large and risky for a single tranche given the current benchmark state.

## Recommended Design

### Adapter Architecture

Add real contracts under `benchmarks/src/adapters/` that own
connector-specific benchmark behavior. The benchmark core should depend on
adapter interfaces rather than direct Postgres helpers.

The contracts should cover:

- scenario support and validation
- connector-specific config materialization
- fixture and seed preparation
- benchmark metadata relevant to artifact labeling and diagnostics

For this tranche, implement only Postgres-backed source and destination
adapters plus the contract surface needed for future transform adapters.

### Execution Model

Split the runner into:

1. benchmark-core orchestration
2. adapter-driven scenario execution

Benchmark-core remains responsible for:

- discovering and filtering scenarios
- resolving environment profiles
- managing warmups and measured iterations
- collecting raw benchmark output
- emitting artifacts and enforcing correctness assertions

Adapter-driven execution becomes responsible for:

- constructing the executable benchmark plan for a scenario kind
- seeding fixtures or preparing runtime inputs
- materializing connector-specific config
- telling the core how to run the scenario through the real runtime

For this tranche, `pipeline` should be fully implemented through adapters.
`source`, `destination`, and `transform` should become explicit first-class
execution modes in the control flow, but they may return a clear
"not implemented" error until concrete harnesses land.

### PR Smoke Benchmark

Replace the current synthetic PR smoke behavior with a real deterministic e2e
pipeline benchmark that uses the engine and plugins directly.

The PR smoke scenario should:

- stop depending on the `synthetic` tag for stub execution
- resolve a committed local benchmark environment profile
- seed deterministic fixture data through the Postgres adapter
- render an ephemeral pipeline from adapter output
- run `rapidbyte run` and emit real benchmark artifacts

This keeps PR benchmark artifacts meaningful for regression analysis instead of
reporting placeholder metrics.

### Workloads And Fixtures

Move workload-specific fixture generation behind the adapter layer as well.

For this tranche:

- `WorkloadFamily::NarrowAppend` becomes the first fully real workload
- other families remain declared in the model but should fail explicitly for
  real execution until their fixture generation exists
- synthetic fallback should not hide missing implementation for supposedly real
  runs

### Scenario Model

The existing declarative scenario model can remain largely intact, but scenario
validation must become stricter:

- `kind` must align with the available adapter-backed execution path
- connector refs must resolve to supported adapter implementations
- unsupported benchmark kinds must fail clearly
- unsupported workload-family plus adapter combinations must fail clearly

This slice may also need a transform options block or reserved transform config
surface so `BenchmarkKind::Transform` is not structurally blocked by the schema.

## Change Scope

### New or substantially rewritten areas

- `benchmarks/src/adapters/mod.rs`
- `benchmarks/src/adapters/source.rs`
- `benchmarks/src/adapters/destination.rs`
- `benchmarks/src/adapters/transform.rs`

### Modified files

- `benchmarks/src/runner.rs`
- `benchmarks/src/pipeline.rs`
- `benchmarks/src/scenario.rs`
- `benchmarks/src/workload.rs`
- `benchmarks/src/environment.rs`
- `benchmarks/scenarios/pr/smoke.yaml`
- benchmark tests covering runner, workload, and scenario validation

## Error Handling

- Unsupported benchmark kinds should return explicit "not implemented" errors.
- Unsupported workload-family and adapter combinations should fail explicitly.
- Missing environment data for a real e2e run should fail before execution.
- Adapter resolution failures should surface the scenario id, connector kind,
  and plugin name involved.

## Verification Strategy

- unit tests for adapter resolution and scenario validation
- unit tests for Postgres adapter config and fixture generation
- runner tests proving pipeline execution routes through adapters
- smoke verification that a PR benchmark path invokes the real runtime and
  emits artifacts from actual engine/plugin execution
- negative tests proving unsupported benchmark kinds and workloads fail clearly

## Outcome

After this slice, the benchmark system will have:

- a real adapter architecture instead of placeholder files
- a benchmark core that is no longer hardcoded to one Postgres execution path
- a real end-to-end PR smoke benchmark with meaningful artifacts
- explicit extension points for isolated source, destination, and transform
  benchmarks without another large refactor
