# Benchmark Mode Split Design

## Summary

Split the current native Postgres COPY lab benchmark into two explicit modes:
`pg_dest_copy_regression` for cheap regression tracking and
`pg_dest_copy_release` for production-like release measurements. Keep the
measurement contract end-to-end and wall-clock based, but make the benchmark
artifacts and summary output report truthful execution metadata such as build
mode, AOT status, actual execution parallelism, total bytes, and derived
bytes-per-row.

## Goals

- Separate regression detection from release reporting without changing the
  core measurement model.
- Preserve comparability by keeping the same workload shape across both modes.
- Make benchmark artifacts self-describing so `debug` vs `release`, `aot:
  false` vs `aot: true`, and effective parallelism are visible without manual
  code inspection.
- Keep the benchmark summary focused on end-to-end throughput and latency while
  adding the context needed to interpret those numbers correctly.

## Non-Goals

- Redefining `records/sec` or `MB/sec` away from the current end-to-end
  wall-clock formulas.
- Introducing a third "peak" benchmark class in this slice.
- Changing the seeded workload family or row shape for the COPY benchmark.
- Building historical trend views or dashboarding.

## Current Problems

- The benchmark runner currently hardcodes artifact `build_mode` to `debug`
  and `execution_flags.aot` to `false`, regardless of how the underlying
  runtime actually executed.
- The benchmark runner invokes the CLI through `cargo run` without `--release`,
  so the current lab benchmark is debug-biased by construction.
- The runtime itself defaults AOT on unless `RAPIDBYTE_WASMTIME_AOT=0`, so the
  current artifact metadata can be factually wrong.
- The current summary shows a top-level `parallelism` value that can disagree
  with stream shard metrics, which makes the report misleading.
- The current bandwidth number is difficult to interpret because the report
  does not also show total bytes written or bytes per row.

## Alternatives Considered

### 1. Recommended: Split by benchmark intent, keep workload shape constant

Add two explicit scenario manifests for the same logical workload:
`pg_dest_copy_regression` and `pg_dest_copy_release`. Extend scenario metadata
so each manifest can declare benchmark execution mode such as build profile and
AOT policy.

Pros:
- Clean separation of purpose.
- Preserves comparability because workload shape does not drift.
- Makes summary grouping natural because `build_mode` and `execution_flags`
  already participate in artifact identity.

Cons:
- Requires small schema and runner changes.

### 2. Single scenario plus ad hoc CLI flags

Keep one `pg_dest_copy` manifest and rely on command-line flags or environment
variables to switch between regression and release behavior.

Pros:
- Fewer scenario files.

Cons:
- Easier to run inconsistently.
- Harder to compare artifacts because the mode is no longer encoded by the
  scenario contract.
- Pushes correctness burden onto the human operator.

### 3. Split by both mode and workload shape immediately

Introduce separate regression and release scenarios and also change the release
workload to be wider or otherwise more production-like.

Pros:
- Potentially closer to a production traffic profile.

Cons:
- Conflates two variables at once: runtime mode and dataset shape.
- Makes the first release numbers harder to compare against existing runs.

## Recommended Design

### Scenario Taxonomy

Create two lab COPY scenarios:

- `pg_dest_copy_regression`
  - purpose: stable regression signal
  - build mode: debug
  - AOT: disabled explicitly
  - workload: existing `narrow_append`, `1_000_000` rows
  - sample budget: small and cheap

- `pg_dest_copy_release`
  - purpose: production-like release measurement
  - build mode: release
  - AOT: enabled explicitly
  - workload: same `narrow_append`, `1_000_000` rows
  - sample budget: credible local measurement, not necessarily CI-cheap

Both scenarios should preserve the same connector behavior and correctness
assertions as the current `pg_dest_copy` benchmark.

### Scenario Execution Profile

Extend the scenario manifest schema with benchmark execution metadata that the
runner can consume directly. The minimal execution profile should include:

- `build_mode`: `debug` or `release`
- `aot`: `true` or `false`

This profile should be part of the manifest rather than only a CLI override so
that:

- the benchmark contract is versioned alongside the scenario
- artifacts can always be traced back to an intended execution mode
- local and CI runs stay consistent

### Runner Contract

Keep the current end-to-end measurement contract:

- `records_per_sec = records_written / duration_secs`
- `mb_per_sec = bytes_written / duration_secs / 1024 / 1024`
- `duration_secs` remains total wall-clock runtime for the full pipeline

Change only how the runner launches and records the run:

- use `cargo run --release` for release scenarios
- use explicit `RAPIDBYTE_WASMTIME_AOT=1` or `0` for each scenario instead of
  relying on runtime defaults
- write the actual build mode and AOT flag into the artifact

This keeps historical interpretation stable while removing ambiguity.

### Parallelism Truthfulness

Fix the emitted benchmark JSON and benchmark artifact so top-level parallelism
reflects actual effective execution parallelism. The current engine path can
report `1` even when the stream metrics clearly show multiple shards. The
top-level metric should match the parallelism actually used to execute the run,
not a fallback resolution path that ignores partitioned-read support.

### Summary Reporting

Extend the benchmark summary output so each scenario group includes:

- `build_mode`
- `aot`
- effective parallelism
- total records written
- total bytes written
- bytes per row
- existing throughput, bandwidth, latency, correctness, and connector metric
  keys

The summary should also make it explicit that throughput and bandwidth are
derived from end-to-end wall-clock runtime, not an isolated destination COPY
inner loop.

### Commands and Workflow

The existing `just bench-lab <scenario>` flow can remain the primary local
entrypoint. Documentation should steer users toward:

- `just bench-lab pg_dest_copy_regression`
- `just bench-lab pg_dest_copy_release`
- `just bench-summary target/benchmarks/lab/<scenario>.jsonl`

No separate "peak" command should be introduced in this slice.

## Change Scope

### Modified files

- `benchmarks/src/scenario.rs`
- `benchmarks/src/runner.rs`
- `benchmarks/src/summary.rs`
- `benchmarks/scenarios/lab/pg_dest_copy.yaml` or replacement scenarios
- `benchmarks/scenarios/README.md`
- `docs/BENCHMARKING.md`
- `README.md`
- `CONTRIBUTING.md`
- `crates/rapidbyte-engine/src/orchestrator.rs`
- `crates/rapidbyte-cli/src/commands/run.rs`

### New files

- `benchmarks/scenarios/lab/pg_dest_copy_regression.yaml`
- `benchmarks/scenarios/lab/pg_dest_copy_release.yaml`

## Verification Strategy

- unit tests for scenario parsing of the new benchmark execution profile
- runner tests that verify debug/release invocation and recorded artifact
  metadata
- engine or CLI tests that verify effective parallelism is reported truthfully
- summary tests that verify build mode, AOT, total bytes, and bytes-per-row are
  rendered
- targeted local runs of both scenarios
- summary output review against real artifact files

## Outcome

After this change, Rapidbyte will have one COPY benchmark optimized for
regression tracking and one optimized for release reporting, both using the
same workload semantics. The resulting artifacts and summaries will be honest
enough to explain observed performance without reverse-engineering the runner
implementation.
