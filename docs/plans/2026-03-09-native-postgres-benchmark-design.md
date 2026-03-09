# Native Postgres Benchmark Design

## Problem

The new benchmark platform has the right structure, but it cannot yet execute a
real connector benchmark. It only emits synthetic artifacts from scenario
metadata. That means it cannot currently answer the concrete question we still
care about for PostgreSQL destinations:

- how does `load_method: insert` compare to `load_method: copy` under the new framework?

We need a real benchmark path inside the new system, without reviving the
retired `tests/bench` harness or reusing its runtime fixtures as active inputs.

## Goal

Add a native pipeline benchmark execution path to the new benchmark framework
and define two explicit PostgreSQL lab scenarios:

- `pg_dest_insert`
- `pg_dest_copy`

Both scenarios must use identical workload, environment, and execution
settings. They should differ only in the destination write-path override.

## Non-Goals

- No generalized multi-database environment layer in this slice.
- No CDC or partitioned-read lab execution in this slice.
- No PR gate changes in this slice.
- No reuse of legacy benchmark fixture YAMLs as runtime inputs.

## Design

### Scenario Extensions

Extend the benchmark scenario schema with three new sections:

- `environment`
- `connector_options`
- `assertions`

For this slice, the only concrete environment type is Postgres:

- source database connection settings
- destination database connection settings
- seed table name
- schema name

`connector_options` provides explicit source and destination config overrides.
For the two new lab scenarios, the only differing field is:

- `destination.load_method: insert`
- `destination.load_method: copy`

`assertions` defines minimum correctness checks:

- expected records written
- expected records read

### Runtime Execution Path

Add a real `pipeline` execution path to the benchmark runner:

1. Resolve the scenario.
2. Provision or validate the Postgres environment.
3. Seed the benchmark source table according to the logical workload.
4. Render a temporary pipeline YAML from the scenario data.
5. Run `rapidbyte run <rendered-pipeline>` using the built host and plugins.
6. Parse the emitted bench JSON.
7. Convert the run result into a native benchmark artifact.

The rendered pipeline must be ephemeral. The source of truth is the scenario
manifest, not a committed benchmark YAML file.

### Workload Mapping

For this vertical slice, wire `WorkloadFamily::NarrowAppend` to a concrete
Postgres seed strategy:

- create or replace a `bench_events` source table
- populate it with deterministic rows sized to the intended benchmark profile
- record expected row counts for assertions

This keeps the first real benchmark simple and auditable.

### Native Scenarios

Add two lab scenarios under `benchmarks/scenarios/lab/`:

- `pg_dest_insert.yaml`
- `pg_dest_copy.yaml`

Both should declare:

- `kind: pipeline`
- source connector `postgres`
- destination connector `postgres`
- identical `workload`, `execution`, and `environment`
- identical correctness assertions

They should differ only in `connector_options.destination.load_method`.

## Testing Strategy

- unit tests for scenario parsing with the new sections
- runner tests for pipeline rendering from scenario data
- runner tests for correctness assertion enforcement
- a smoke integration path that runs the new lab scenarios against local Docker
  Postgres and produces real artifacts

## Outcome

After this slice, the new benchmark platform will be able to execute a real
Postgres destination benchmark natively, and the `insert` versus `copy`
comparison will live entirely inside the new framework.
