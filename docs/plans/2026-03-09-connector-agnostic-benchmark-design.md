# Connector-Agnostic Benchmark Platform Design

## Problem

The current benchmark harness is useful for local experiments, but it is not a durable benchmark platform:

- it is centered on one hard-coded connector pairing
- it mixes end-to-end timing with connector-specific timing in ways that are hard to interpret
- it does not scale cleanly to many sources, destinations, and transforms
- it does not provide a strong regression-gating model against `main`
- it leaves too much benchmark meaning encoded in ad hoc Rust logic and legacy scripts

We need a benchmark system that can answer two different questions without conflating them:

1. How fast is a real Rapidbyte pipeline end to end?
2. How efficient is an individual connector implementation in isolation?

## Goals

- Support connector-agnostic benchmarking for sources, destinations, transforms, and full pipelines.
- Keep benchmark execution close to the real Rapidbyte runtime and plugin contracts.
- Provide a small, stable PR benchmark gate for catching obvious performance regressions.
- Provide a broader lab suite for release analysis and connector bring-up.
- Use a rolling `main` baseline for regression comparison.
- Make benchmark results attributable to source, transform, destination, startup, or runtime overhead.
- Retire the legacy benchmark implementation instead of maintaining two overlapping systems.

## Non-Goals

- No attempt to make every connector expose identical diagnostic metrics.
- No requirement that every benchmark run against a real external system.
- No UI/dashboard implementation in this tranche beyond artifact schemas and report outputs.
- No indefinite backward-compatibility guarantee for the current `tests/bench` command surface.

## Recommendation

Use a hybrid architecture:

- Rust for scenario validation, fixture generation, execution orchestration, and raw metric capture
- Python plus DuckDB for artifact ingestion, comparison, regression analysis, and report generation

Rust is the right execution language because the engine, plugin protocol, Arrow data paths, and capability model already live there. Reusing that stack keeps benchmark overhead small and lets the benchmark exercise real runtime behavior instead of a sidecar approximation. Python and DuckDB are the right analysis layer because they are faster to evolve for slicing, aggregation, statistical comparison, and historical reporting.

## Benchmark Model

The benchmark unit should be a declarative scenario, not a connector-specific Rust code path.

Each scenario lives under a stable directory such as `benchmarks/scenarios/` and declares:

- benchmark kind: `pipeline`, `source`, `destination`, `transform`
- stable scenario id and human-readable name
- tags such as `pr`, `lab`, `synthetic`, `real`, `cdc`, `partitioned`, `bulk_load`
- connector references and capability requirements
- workload shape: row count, target bytes per row, schema family, null density, key distribution, skew
- execution settings: iterations, warmups, concurrency, batch limits, checkpoint profile, AOT mode
- environment bindings: synthetic fixture provider or real service binding
- correctness assertions: row counts, checksums, cursor expectations, CDC invariants
- required canonical metrics

This keeps the core platform connector-agnostic. Connector-specific behavior stays inside adapter layers that map a logical workload to concrete connector configuration.

## Two First-Class Suites

### PR Gate Suite

A small, stable subset of scenarios intended to catch obvious regressions quickly:

- synthetic-first
- fixed hardware class
- fixed datasets and execution settings
- strict scenario ids
- coarse fail thresholds, for example throughput down more than 10 percent or latency up more than 15 percent

This suite should be optimized for signal and repeatability, not for breadth.

### Lab Suite

A broader suite intended for release analysis, connector bring-up, and deep performance work:

- larger datasets
- real-system scenarios where needed
- CDC and partitioned-read coverage
- broader connector matrices
- warm and cold runs
- memory and skew stress cases

This suite should produce artifacts suitable for trend analysis, not just pass/fail gating.

## Metrics Model

Every run emits two metric layers.

### Canonical Metrics

These are required for cross-connector comparison:

- total duration
- cold-start duration
- steady-state duration
- records per second
- megabytes per second
- CPU seconds
- peak RSS
- source active time
- transform active time
- destination active time
- retry count
- batch count
- correctness status
- output checksum or equivalent validation token

### Connector Metrics

These are optional diagnostics:

- insert statement count
- copy flush count
- rows per batch
- page count
- CDC lag
- partition skew
- compressed versus uncompressed bytes

Connector metrics must never be required for benchmark comparability, but they should be preserved for diagnosis.

## Artifact And Baseline Design

Each benchmark run should emit a versioned JSON artifact with:

- benchmark suite id
- scenario id
- git SHA
- build mode
- hardware class
- connector versions
- execution settings
- canonical metrics
- connector metrics
- correctness results
- sample quality metadata

Comparison happens against a rolling baseline from `main`, matched on:

- scenario id
- suite id
- hardware class
- build mode
- relevant execution flags

Use medians as the primary comparator and require a minimum sample count before hard pass/fail decisions. If sample quality is weak, report insufficient confidence instead of a noisy regression verdict.

## Rust/Python Responsibility Split

### Rust Responsibilities

- load and validate benchmark scenarios
- generate or replay synthetic fixtures
- provision benchmark execution plans
- invoke Rapidbyte with real plugin/runtime paths
- collect raw timing, resource, and correctness artifacts
- emit stable machine-readable output

### Python And DuckDB Responsibilities

- ingest benchmark artifacts
- store rolling baselines
- perform regression comparison
- produce CLI, Markdown, and HTML summaries
- support ad hoc analysis across time, connector, scenario, and hardware dimensions

## Connector-Agnostic Workload Library

Provide reusable logical workload families rather than connector-specific cases:

- narrow append
- wide append
- JSON-heavy rows
- high-null-density
- high-cardinality keys
- partitioned scan
- CDC micro-batch
- CDC backfill
- transform-heavy row expansion or filtering

New connectors should plug into these workload families through adapters instead of requiring a bespoke benchmark harness.

## Legacy Cleanup

The new system should replace, not supplement, the existing benchmark harness.

Migration should end with:

- removal of `tests/bench/src/main.rs`
- removal of legacy reporting scripts under `tests/bench/`
- replacement of existing `just bench` plumbing with a thin entrypoint that calls the new benchmark platform
- movement of any reusable fixture assets into the new benchmark directory structure

The only legacy code that should survive is code explicitly migrated into the new platform.

## Testing Strategy

- unit tests for scenario parsing and validation
- unit tests for workload adapters and fixture generation
- integration tests for canonical artifact emission
- regression tests for comparison logic in the analysis layer
- smoke tests for the PR suite on CI
- optional lab verification jobs for broader scenarios

## Outcome

After this redesign, Rapidbyte should have:

- a benchmark platform that scales across connector types
- a clean separation between end-to-end and isolated connector benchmarking
- a credible regression-gating path against `main`
- benchmark artifacts that explain regressions instead of hiding them
- no duplicated legacy benchmark stack lingering beside the new one
