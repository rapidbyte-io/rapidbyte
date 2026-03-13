# Distributed `pg_dest_copy_release` Benchmark Design

## Goal

Add a clean distributed analogue of `benchmarks/scenarios/lab/pg_dest_copy_release.yaml` that measures the same PostgreSQL COPY destination workload through the controller/agent runtime on a single machine, while staying directly comparable to the existing local benchmark.

## Summary

The distributed analogue should be a first-class benchmark scenario inside the existing `rapidbyte-benchmarks` runner, not a one-off shell script or separate harness. It should reuse the current scenario-driven workflow, benchmark-owned infrastructure model, artifact output format, warmup/iteration semantics, and correctness checks.

The benchmark should provision a benchmark-owned single-node distributed stack:

- PostgreSQL
- controller
- one agent

The benchmark runner should remain host-driven. It should prepare fixtures, render the pipeline YAML, submit the run through the controller, wait for terminal completion, validate correctness, and emit the normal benchmark artifact with a small distributed-specific metrics section.

## Non-Goals

- Multi-agent scaling benchmarks
- Cancellation/retry/failure-injection benchmarks
- Dry-run/preview benchmarks
- Replacing the existing local `pg_dest_copy_release` benchmark
- Building a separate distributed benchmark tool outside `benchmarks/`

## Why This Shape

The existing benchmark system is already declarative and scenario-driven. The local `pg_dest_copy_release` benchmark is useful as an engine baseline. A distributed analogue should preserve that structure so the result answers a clean question:

What is the end-to-end cost of running this same workload through the distributed runtime on one machine?

That comparison is only credible if both local and distributed benchmarks:

- share the same workload profile
- share the same correctness contract
- share the same artifact model
- differ mainly in execution path

## Recommended Approach

Implement a first-class distributed pipeline benchmark mode inside the current benchmark runner.

This is preferable to:

- shell-driven orchestration around existing CLI commands, which would be faster to prototype but harder to keep reproducible and comparable
- a separate distributed benchmark system, which would duplicate the current runner and add maintenance debt before it is justified

## Benchmark Shape

Add a new lab scenario that is conceptually the distributed counterpart of `pg_dest_copy_release`.

Its important properties should match the local release scenario:

- same workload family
- same row count
- same source and destination connectors
- same destination load method (`copy`)
- same write mode (`append`)
- same build mode (`release`)
- same AOT setting
- same correctness assertions

The key intentional difference should be execution mode:

- local benchmark path: invoke local `rapidbyte run <pipeline>`
- distributed benchmark path: submit the rendered pipeline to the controller and wait for a benchmark-owned agent to execute it

## Timing Boundary

Infrastructure startup and teardown should not be part of measured benchmark duration.

Measured timing should begin immediately before `SubmitPipeline` and end when the run reaches a terminal successful state. This keeps the metric focused on distributed execution overhead and runtime behavior rather than process boot latency.

This measured duration should include:

- controller RPC overhead
- queueing and assignment latency
- agent execution
- controller-side finalization before terminal completion is observed

This timing boundary preserves comparability with the local benchmark while still reflecting real distributed runtime cost.

## Environment Model

The environment should remain benchmark-owned, similar to `local-bench-postgres`, but expanded to support distributed runtime components.

The benchmark environment should provision:

- `postgres`
- `controller`
- `agent`

The benchmark runner should continue to run on the host machine. Compose-managed services should remain long-lived for the benchmark session so startup noise does not pollute warmups and measured iterations.

This also gives a clean path to future expansion, such as multiple agents, without redesigning the basic runner model.

## Security And Configuration

The benchmark environment should exercise the supported distributed path rather than insecure development shortcuts.

That means the benchmark-owned distributed environment should use:

- explicit non-default controller auth token
- explicit non-default preview signing key
- explicit controller and agent configuration owned by the benchmark environment

For the first version, TLS is not required unless the benchmark is specifically intended to measure TLS overhead. The important part is that the benchmark does not rely on deprecated or insecure defaults such as unauthenticated controller startup or shared built-in signing keys.

## Runner Data Flow

The distributed benchmark execution path should follow this shape:

1. Resolve scenario and environment profile.
2. Provision benchmark-owned infrastructure if required.
3. Wait for controller and agent readiness.
4. Prepare PostgreSQL fixtures for the scenario workload.
5. Render the benchmark pipeline YAML.
6. Submit the pipeline through the controller.
7. Start measured timing.
8. Watch the run until it reaches a terminal state.
9. Stop measured timing on successful completion.
10. Validate correctness using the existing row-count contract and destination state.
11. Emit the benchmark artifact.

The benchmark should fail loudly on:

- controller or agent readiness failures
- submission failures
- watch interruptions
- non-terminal hangs
- terminal failed/cancelled outcomes
- correctness assertion failures

The harness should not silently retry benchmark iterations internally. Benchmarks should surface instability rather than mask it.

## Artifact Contract

The existing artifact shape should remain primary.

The distributed benchmark should continue to emit comparable top-level metrics:

- `duration_secs`
- records/sec
- MB/sec
- correctness
- build mode
- execution flags

It may add a small distributed-specific metrics section inside connector metrics, for example:

- `distributed.submit_to_terminal_secs`
- `distributed.queue_wait_secs` when derivable from controller timestamps
- `distributed.execution_secs` when derivable from run metadata
- `distributed.agent_count`
- `distributed.transport`

These should be additive metrics, not a replacement for the current canonical metrics. The benchmark must remain directly comparable to local pipeline artifacts.

## Scenario And Environment Scope

The first version should stay deliberately narrow:

- one local benchmark scenario
- one controller
- one agent
- one benchmark-owned PostgreSQL instance
- one end-to-end pipeline workload matching `pg_dest_copy_release`

This should not yet include:

- multi-agent scheduling comparisons
- queue-depth stress
- preview-serving paths
- cancellation semantics
- retry/failure matrices

Those may become follow-on benchmark scenarios later, but they should not be bundled into the first distributed analogue.

## Testing Strategy

The implementation should validate three layers:

1. Scenario and environment parsing
- distributed scenario manifest fields deserialize and validate correctly
- distributed environment profile resolves and exposes controller/agent connection details

2. Runner execution path
- the benchmark runner can provision distributed benchmark infrastructure
- it can submit a rendered pipeline, watch terminal completion, and collect benchmark output
- failure paths abort cleanly

3. Artifact compatibility
- distributed benchmark results still materialize into the current artifact format
- local and distributed scenarios remain comparable at the canonical metric layer

## Future Extensions

Once the single-node distributed analogue is stable and useful, likely follow-on work would be:

- multi-agent scaling variants
- separate distributed latency-breakdown scenarios
- TLS-on benchmarking
- failure-mode operational benchmarks

Those should come after the core analogue exists and is producing stable, trustworthy measurements.
