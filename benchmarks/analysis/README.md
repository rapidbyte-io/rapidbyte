# Benchmark Analysis

This directory contains the analysis layer for the connector-agnostic benchmark
platform.

- `compare.py` loads JSON benchmark artifacts, matches candidate runs against a
  rolling baseline from `main`, and evaluates regression thresholds.
- `schema.sql` defines the DuckDB table used for artifact history.
- `requirements.txt` lists optional analysis dependencies for richer local and CI
  environments.

The local implementation keeps DuckDB optional so comparison logic can still run
in minimal environments that only have the Python standard library.

## Comparing Local vs Distributed COPY Benchmarks

The lab suite now has two directly comparable PostgreSQL COPY scenarios:

- `pg_dest_copy_release`
- `pg_dest_copy_release_distributed`

Run them separately:

```bash
just bench --suite lab --scenario pg_dest_copy_release --env-profile local-bench-postgres --output target/benchmarks/lab/pg_dest_copy_release.jsonl
just bench --suite lab --scenario pg_dest_copy_release_distributed --env-profile local-bench-distributed-postgres --output target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl
```

Then summarize or compare the artifacts:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- summary target/benchmarks/lab/pg_dest_copy_release.jsonl
cargo run --manifest-path benchmarks/Cargo.toml -- summary target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl
cargo run --manifest-path benchmarks/Cargo.toml -- compare target/benchmarks/lab/pg_dest_copy_release.jsonl target/benchmarks/lab/pg_dest_copy_release_distributed.jsonl
```

Canonical metrics remain comparable across both scenarios. Distributed-specific
runtime details are attached as additive connector metadata under
`connector_metrics.distributed`.
