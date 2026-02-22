#!/usr/bin/env bash
# Postgres connector benchmark configuration.
# Sourced by bench.sh and compare.sh orchestrators.

# Default row count for benchmarks
BENCH_DEFAULT_ROWS=10000

# Row count for git-ref comparisons (smaller for speed)
BENCH_COMPARE_ROWS=10000

# Benchmark modes (pipeline variants to run)
BENCH_MODES=(insert copy)

# Default iterations per mode
BENCH_DEFAULT_ITERS=3

# Seed SQL (uses :bench_rows psql variable)
BENCH_SEED_SQL="$BENCH_DIR/fixtures/sql/bench_seed.sql"

# Pipeline YAML mapping: mode -> file
declare -A BENCH_PIPELINES
BENCH_PIPELINES[insert]="$BENCH_DIR/fixtures/pipelines/bench_pg.yaml"
BENCH_PIPELINES[copy]="$BENCH_DIR/fixtures/pipelines/bench_pg_copy.yaml"
BENCH_PIPELINES[lz4]="$BENCH_DIR/fixtures/pipelines/bench_pg_lz4.yaml"
BENCH_PIPELINES[zstd]="$BENCH_DIR/fixtures/pipelines/bench_pg_zstd.yaml"
