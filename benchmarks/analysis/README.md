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
