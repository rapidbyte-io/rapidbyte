# Benchmark Scenarios

This directory holds declarative benchmark scenarios for the new connector-agnostic
benchmark runner.

- `pr/` will contain the small, stable regression-gating suite.
- `lab/` will contain broader exploratory and release-oriented scenarios.

Scenario files are YAML manifests consumed by `rapidbyte-benchmarks`.

Current notable lab scenarios:

- `lab/pg_dest_insert.yaml` benchmarks PostgreSQL destination writes with `load_method: insert`.
- `lab/pg_dest_copy_regression.yaml` benchmarks PostgreSQL COPY in debug mode with AOT disabled for cheap regression tracking.
- `lab/pg_dest_copy_release.yaml` benchmarks the same workload in release mode with AOT enabled for production-like measurements.
- `../environments/local-bench-postgres.yaml` defines the benchmark-owned local benchmark environment.
- `../environments/local-dev-postgres.yaml` remains available as a shared/manual profile for ad hoc runs against an existing stack.

When `--env-profile local-bench-postgres` is used, the benchmark runner treats
that environment as benchmark-owned infrastructure: it provisions the isolated
benchmark Docker Compose project before running warmups and measured
iterations, then tears it down afterward on both success and failure.

Run a specific scenario with:

```bash
just bench --suite lab --scenario pg_dest_insert --env-profile local-bench-postgres --output target/benchmarks/lab/insert.jsonl
just bench --suite lab --scenario pg_dest_copy_regression --env-profile local-bench-postgres --output target/benchmarks/lab/pg_dest_copy_regression.jsonl
just bench --suite lab --scenario pg_dest_copy_release --env-profile local-bench-postgres --output target/benchmarks/lab/pg_dest_copy_release.jsonl
```
