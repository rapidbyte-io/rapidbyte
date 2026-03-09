# Benchmark Load Method Design

## Goal

Restore the benchmark contract so the `insert` mode always measures PostgreSQL row-by-row inserts and the `copy` mode always measures PostgreSQL COPY loading, even when the destination plugin default changes.

## Design

Keep the existing benchmark mode names and make their semantics explicit in the pipeline fixtures:

- `tests/bench/fixtures/pipelines/bench_pg.yaml` will set `destination.config.load_method: insert`
- `tests/bench/fixtures/pipelines/bench_pg_copy.yaml` will continue to set `destination.config.load_method: copy`

This keeps the benchmark harness simple because `tests/bench/src/main.rs` already maps modes to dedicated fixture files. The fix belongs in the fixtures, not in the harness, because the YAML should remain the source of truth for how each benchmark mode is configured.

## Regression Coverage

Add a benchmark-unit regression test in `tests/bench/src/main.rs` that renders the `insert` fixture and asserts the destination config still contains `load_method: insert`. This protects against future default changes silently collapsing the `insert` and `copy` modes back onto the same path.
