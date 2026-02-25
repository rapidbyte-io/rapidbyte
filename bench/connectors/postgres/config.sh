#!/usr/bin/env bash
# Postgres connector benchmark configuration.
# Sourced by bench.sh and compare.sh orchestrators.
# Requires BENCH_PROFILE to be set (small|medium|large).

# Profile -> seed SQL mapping
declare -A BENCH_PROFILE_SEEDS
BENCH_PROFILE_SEEDS[small]="$BENCH_DIR/fixtures/sql/bench_seed_small.sql"
BENCH_PROFILE_SEEDS[medium]="$BENCH_DIR/fixtures/sql/bench_seed_medium.sql"
BENCH_PROFILE_SEEDS[large]="$BENCH_DIR/fixtures/sql/bench_seed_large.sql"

# Profile -> default row counts
declare -A BENCH_PROFILE_ROWS
BENCH_PROFILE_ROWS[small]=100000
BENCH_PROFILE_ROWS[medium]=50000
BENCH_PROFILE_ROWS[large]=10000

# Seed SQL derived from active profile (set by orchestrator via BENCH_PROFILE)
BENCH_SEED_SQL="${BENCH_PROFILE_SEEDS[$BENCH_PROFILE]}"

# Default row count (profile-aware, overridable by CLI)
BENCH_DEFAULT_ROWS="${BENCH_PROFILE_ROWS[$BENCH_PROFILE]:-10000}"

# Row count for git-ref comparisons (smaller for speed)
BENCH_COMPARE_ROWS=10000

# Benchmark modes (pipeline variants to run)
BENCH_MODES=(insert copy transform transform_filter)

# Default iterations per mode
BENCH_DEFAULT_ITERS=3

# Pipeline YAML mapping: mode -> file
declare -A BENCH_PIPELINES
BENCH_PIPELINES[insert]="$BENCH_DIR/fixtures/pipelines/bench_pg.yaml"
BENCH_PIPELINES[copy]="$BENCH_DIR/fixtures/pipelines/bench_pg_copy.yaml"
BENCH_PIPELINES[lz4]="$BENCH_DIR/fixtures/pipelines/bench_pg_lz4.yaml"
BENCH_PIPELINES[zstd]="$BENCH_DIR/fixtures/pipelines/bench_pg_zstd.yaml"
BENCH_PIPELINES[transform]="$BENCH_DIR/fixtures/pipelines/bench_pg_transform.yaml"
BENCH_PIPELINES[transform_filter]="$BENCH_DIR/fixtures/pipelines/bench_pg_transform_filter.yaml"
