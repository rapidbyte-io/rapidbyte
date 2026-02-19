# Benchmarking

Guide to running benchmarks, profiling, and tracking performance regressions in Rapidbyte.

## Quick Start

```bash
just bench-connector-postgres
```

This runs 3 iterations each of INSERT and COPY modes against 10,000 rows with AOT-compiled WASM modules, then prints a comparison report.

## Prerequisites

- **Docker** — PostgreSQL runs via `docker-compose.yml`
- **WasmEdge** — runtime + `wasmedge compile` for AOT (source `~/.wasmedge/env`)
- **Python 3** — used by the reporting scripts
- **cargo-flamegraph** (optional) — `cargo install flamegraph` for CPU profiling

## Running Benchmarks

### bench.sh

The main benchmark script. Builds everything, starts PostgreSQL, seeds data, runs both INSERT and COPY modes, and prints a comparison table.

```bash
./tests/bench.sh [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--rows N` | 10000 | Number of rows to seed and benchmark |
| `--iters N` | 3 | Iterations per mode (INSERT and COPY) |
| `--no-aot` | (AOT on) | Skip AOT compilation of WASM modules |
| `--debug` | (release) | Use debug builds instead of release |
| `--profile` | (off) | Generate a flamegraph after benchmark runs |

Examples:

```bash
# 100K rows, 5 iterations
./tests/bench.sh --rows 100000 --iters 5

# Debug build, no AOT
./tests/bench.sh --debug --no-aot

# Benchmark + flamegraph at the end
./tests/bench.sh --rows 50000 --profile
```

### Justfile Targets

| Target | Description |
|--------|-------------|
| `just bench-connector-postgres` | INSERT vs COPY comparison (10K rows, 3 iters) |
| `just bench-connector-postgres 100000` | Same with custom row count |
| `just bench-connector-postgres-profile` | Benchmark + flamegraph |
| `just bench-connector-postgres-profile 50000` | Profiled run with custom rows |
| `just bench-profile` | Standalone flamegraph (10K rows, INSERT) |
| `just bench-profile 50000 copy` | Flamegraph with custom rows and mode |
| `just bench-matrix` | Row-count sweep (1K, 10K, 100K) |
| `just bench-compare ref1 ref2` | Benchmark two git refs and compare |
| `just bench-results` | View/compare past benchmark results |

## Profiling with Flamegraphs

### Standalone Profiler

`bench_profile.sh` generates a CPU flamegraph for a single pipeline run. It rebuilds the host binary with debug symbols (`CARGO_PROFILE_RELEASE_DEBUG=2`) and invokes `cargo flamegraph`.

```bash
./tests/bench_profile.sh [ROWS] [MODE]
```

- `ROWS` — row count (default: 10000)
- `MODE` — `insert` or `copy` (default: insert)

```bash
# Profile COPY mode with 50K rows
./tests/bench_profile.sh 50000 copy

# Or via just
just bench-profile 50000 copy
```

Output: `target/profiles/flamegraph_<rows>rows_<mode>_<timestamp>.svg`

Open the SVG in a browser for an interactive flamegraph.

### Integrated Profiling

Pass `--profile` to `bench.sh` to run the normal benchmark iterations first, then generate a flamegraph:

```bash
./tests/bench.sh --rows 10000 --profile
# or
just bench-connector-postgres-profile
```

This uses the INSERT pipeline for the profiling run.

## Benchmark Matrix

`bench_matrix.sh` sweeps across multiple row counts, running `bench.sh` for each:

```bash
./tests/bench_matrix.sh [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--rows "N1 N2 ..."` | `"1000 10000 100000"` | Space-separated row counts |
| `--iters N` | 3 | Iterations per mode per row count |

Environment variables also work:

```bash
BENCH_MATRIX_ROWS="5000 50000 500000" BENCH_MATRIX_ITERS=5 ./tests/bench_matrix.sh
```

Or via just:

```bash
just bench-matrix
just bench-matrix --rows "1000 50000" --iters 2
```

## Regression Tracking

Every benchmark run appends enriched JSON results to `target/bench_results/results.jsonl`. Each line includes:

- All `PipelineResult` timing fields
- `timestamp` — ISO 8601
- `mode` — `insert` or `copy`
- `bench_rows` — row count
- `aot` — whether AOT was enabled
- `git_sha` — short commit hash
- `git_branch` — current branch

### Comparing Two Git Refs

The easiest way to compare performance across commits is `bench_compare.sh`, which checks out each ref, builds, benchmarks, and shows a comparison — all in one command:

```bash
# Compare main vs your feature branch
just bench-compare main my-feature

# Compare last two commits with 100K rows
just bench-compare HEAD~1 HEAD --rows 100000

# Compare tags
just bench-compare v0.1.0 v0.2.0 --iters 5
```

The script:
1. Validates both refs exist and the working tree is clean
2. Starts PostgreSQL once (shared across both refs)
3. For each ref: checks out, builds (release), seeds, runs INSERT + COPY iterations
4. Restores your original branch
5. Shows a side-by-side comparison report

| Flag | Default | Description |
|------|---------|-------------|
| `--rows N` | 10000 | Number of rows to benchmark |
| `--iters N` | 3 | Iterations per mode |
| `--no-aot` | (AOT on) | Skip AOT compilation |

### Viewing Past Results

Use `bench_compare.py` to view and compare results already stored in `results.jsonl`:

```bash
# Compare last 2 runs
just bench-results

# Show last 5 runs
just bench-results --last 5

# Compare specific commits
just bench-results --sha abc1234 def5678

# Filter by row count
just bench-results --rows 100000
```

The output shows per-metric averages for each SHA and, when comparing exactly 2, the percentage change:

```
============================================================
  Mode: INSERT
============================================================
  Metric                    abc1234     def5678      Change
  ----------------------  ----------  ----------  ----------
  Duration (s)                 1.234       1.100     -10.9%
  Source (s)                   0.450       0.440      -2.2%
  Dest (s)                     0.780       0.655     -16.0%
  ...
```

### Manual Workflow

If you prefer to benchmark refs separately (e.g., to avoid checkout):

```bash
# 1. Benchmark on main
git checkout main
just bench-connector-postgres

# 2. Benchmark on your branch
git checkout my-feature
just bench-connector-postgres

# 3. Compare
just bench-results
```

## Understanding the Output

### Human-Readable Output

Each `rapidbyte run` prints a breakdown after completion:

```
Pipeline 'bench_pg' completed successfully.
  Records read:    10000
  Records written: 0
  Bytes read:      1.14 MB
  Bytes written:   0 B
  Avg row size:    120 B
  Duration:        1.23s
  Throughput:      8130 rows/sec, 0.93 MB/s
  Source duration:  0.45s
    Connect:       0.012s
    Query:         0.001s
    Fetch:         0.430s
  Dest duration:   0.78s
    VM setup:      0.005s
    Recv loop:     0.770s
    Connect:       0.015s
    Flush:         0.700s
    Commit:        0.020s
    WASM overhead: 0.030s
  Host emit_batch: 0.050s (10 calls)
  Host next_batch: 0.040s (10 calls)
  Source load:     4ms
  Dest load:       4ms
```

### Machine-Readable JSON

Every run also emits a JSON line prefixed with `@@BENCH_JSON@@`:

```
@@BENCH_JSON@@{"records_read":10000,"duration_secs":1.23,...}
```

`bench.sh` parses this line to collect per-iteration results. The JSON contains all fields from `PipelineResult`.

### Comparison Report

`bench.sh` prints a side-by-side table after all iterations:

```
  Dataset:     10000 rows, 1.14 MB (120 B/row)
  Iterations:  3 per mode

  Metric (avg)              INSERT        COPY     Speedup
  ----------------------  ----------  ----------  ----------
  Total duration               1.234       0.890       1.4x
  Dest duration                0.780       0.430       1.8x
    Flush                      0.700       0.350       2.0x
    ...
  Throughput (rows/s)         8,130      11,236       1.4x
  Throughput (MB/s)            0.93        1.28       1.4x
```

### Timing Hierarchy

```
duration_secs
├── source_duration_secs          (source thread wall time)
│   ├── source_connect_secs       (PG connection)
│   ├── source_query_secs         (query execution)
│   └── source_fetch_secs         (row fetching + Arrow encoding)
├── dest_duration_secs            (dest thread wall time)
│   ├── dest_vm_setup_secs        (WASM VM + import registration)
│   ├── dest_recv_secs            (channel recv loop)
│   ├── dest_connect_secs         (PG connection)
│   ├── dest_flush_secs           (INSERT/COPY writes)
│   ├── dest_commit_secs          (transaction commit)
│   └── wasm_overhead_secs        (dest_duration - connect - flush - commit)
├── source_module_load_ms         (WASM module load/parse)
├── dest_module_load_ms           (WASM module load/parse)
├── source_emit_nanos             (host_emit_batch total time)
│   └── source_compress_nanos     (LZ4/Zstd compression, if enabled)
└── dest_recv_nanos               (host_next_batch total time)
    └── dest_decompress_nanos     (LZ4/Zstd decompression, if enabled)
```

Note: source and destination run on separate threads. `duration_secs` is wall-clock time, not the sum of source + dest.

## Pipeline Variants

Four benchmark pipeline YAMLs are provided in `tests/fixtures/pipelines/`:

| File | Load Method | Compression | Notes |
|------|-------------|-------------|-------|
| `bench_pg.yaml` | INSERT (default) | None | Baseline |
| `bench_pg_copy.yaml` | COPY | None | Faster bulk loading |
| `bench_pg_lz4.yaml` | INSERT | LZ4 | Fast compression |
| `bench_pg_zstd.yaml` | INSERT | Zstd | Higher compression ratio |

### INSERT vs COPY

The dest connector supports two PostgreSQL load methods:

- **INSERT** (default) — standard `INSERT INTO ... VALUES` statements
- **COPY** — PostgreSQL's `COPY` protocol for bulk loading

Set via `destination.config.load_method` in the pipeline YAML:

```yaml
destination:
  use: dest-postgres
  config:
    load_method: copy  # or "insert" (default)
```

`bench.sh` runs both modes automatically and prints a comparison.

### Compression

IPC compression between source and destination stages is configured via the `resources` section:

```yaml
resources:
  compression: lz4   # or "zstd", omit for no compression
```

Compression/decompression happens in host functions (`host_emit_batch` / `host_next_batch`), transparent to WASI connectors. Compression time is tracked in `source_compress_nanos` and `dest_decompress_nanos`.

To benchmark with compression, run the pipeline directly:

```bash
rapidbyte run tests/fixtures/pipelines/bench_pg_lz4.yaml
rapidbyte run tests/fixtures/pipelines/bench_pg_zstd.yaml
```

### Backpressure

Channel capacity between source and destination is configurable:

```yaml
resources:
  max_inflight_batches: 16  # default
```

## Internals

### HostTimings

Defined in `crates/rapidbyte-core/src/runtime/host_functions.rs`:

```rust
pub struct HostTimings {
    pub emit_batch_nanos: u64,    // total time in host_emit_batch
    pub next_batch_nanos: u64,    // total time in host_next_batch
    pub compress_nanos: u64,      // time spent compressing (LZ4/Zstd)
    pub decompress_nanos: u64,    // time spent decompressing
    pub emit_batch_count: u64,    // number of emit_batch calls
    pub next_batch_count: u64,    // number of next_batch calls
}
```

These are accumulated by the host functions during a pipeline run and reported in `PipelineResult`.

### PipelineResult

Defined in `crates/rapidbyte-core/src/engine/runner.rs`. Contains all timing and stats fields emitted in the `@@BENCH_JSON@@` output. Key categories:

- **Counters**: `records_read`, `records_written`, `bytes_read`, `bytes_written`
- **Wall-clock**: `duration_secs`, `source_duration_secs`, `dest_duration_secs`
- **Module loading**: `source_module_load_ms`, `dest_module_load_ms`
- **Source sub-phases**: `source_connect_secs`, `source_query_secs`, `source_fetch_secs`
- **Dest sub-phases**: `dest_connect_secs`, `dest_flush_secs`, `dest_commit_secs`, `dest_vm_setup_secs`, `dest_recv_secs`, `wasm_overhead_secs`
- **Host functions**: `source_emit_nanos/count`, `source_compress_nanos`, `dest_recv_nanos/count`, `dest_decompress_nanos`
- **Transforms**: `transform_count`, `transform_duration_secs`, `transform_module_load_ms`
- **Retries**: `retry_count`

### Benchmark Dataset

`tests/fixtures/sql/bench_seed.sql` creates a `bench_events` table:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Auto-increment PK |
| `event_type` | TEXT | Cycles: click, purchase, view, signup, logout |
| `user_id` | INTEGER | Modulo 1000 |
| `amount` | INTEGER | Deterministic from row index |
| `is_active` | BOOLEAN | 2/3 true, 1/3 false |
| `payload` | TEXT | ~60 bytes per row (`payload-N-xxx...`) |
| `created_at` | TIMESTAMP | Spread across 24 hours |

Row count is controlled by the `bench_rows` psql variable. Average row size is ~120 bytes in Arrow IPC format.

## Troubleshooting

**PostgreSQL not ready** — The scripts use `psql -c "SELECT 1"` to check readiness, not `pg_isready`. If seeding fails, increase the wait time or check Docker logs with `docker compose logs postgres`.

**AOT compilation not found** — Ensure WasmEdge is installed and `source ~/.wasmedge/env` is in your shell profile. Without AOT, module load takes ~500ms instead of ~4ms.

**Debug build panics** — WASI connectors panic in debug builds on unsupported socket ops (e.g., `set_nodelay`). Always use release builds for benchmarking (the default). Debug mode is available via `--debug` but is only useful for testing the benchmark harness itself.

**`records_written` shows 0** — Known issue. The dest connector stats tracking doesn't update this counter. Use `records_read` for throughput calculations (the scripts already do this).

**Flamegraph permission denied (macOS)** — `cargo flamegraph` uses `dtrace` on macOS, which requires root. Run with `sudo` or use `--root` flag.

**No results for bench-compare** — Results are stored in `target/bench_results/results.jsonl`. This file is created on the first `bench.sh` run. Clean builds (`cargo clean`) do not delete it since it's under `target/bench_results/`, but manually deleting `target/` will.

**Compression fields are 0** — Compression timing only appears when a pipeline has `resources.compression` set. The default `bench_pg.yaml` and `bench_pg_copy.yaml` do not use compression.
