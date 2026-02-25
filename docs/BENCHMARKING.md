# Benchmarking

Guide to running benchmarks, profiling, and tracking performance regressions in Rapidbyte.

## Quick Start

```bash
just bench
```

This runs 3 iterations each of INSERT and COPY modes against 10,000 rows, then prints a criterion-style comparison report.

## Prerequisites

- **Docker** — PostgreSQL runs via `docker-compose.yml`
- **Wasmtime** — component runtime used by the host
- **Python 3** — used by the reporting scripts
- **samply** (optional) — `cargo install samply` for CPU profiling
- **Criterion** — Rust micro-benchmarks use criterion 0.5 (dev-dependency)

## Running Benchmarks

### E2E Pipeline Benchmarks

The main benchmark orchestrator. Builds everything, starts PostgreSQL, seeds data, runs all configured modes, and prints a comparison table.

```bash
./bench/bench.sh [CONNECTOR] [ROWS] [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `CONNECTOR` | (all) | Connector name (e.g. postgres). Omit to bench all. |
| `ROWS` | 10000 | Number of rows to seed and benchmark |
| `--iters N` | 3 | Iterations per mode (INSERT and COPY) |
| `--aot` | on | Enable Wasmtime AOT cache |
| `--no-aot` | off override | Disable Wasmtime AOT cache |
| `--debug` | (release) | Use debug builds instead of release |
| `--profile` | (off) | Generate a flamegraph after benchmark runs |

Examples:

```bash
# All connectors, default settings
just bench

# Postgres only, 100K rows, 5 iterations
just bench postgres 100000 --iters 5

# Debug build
just bench postgres --debug --no-aot

# Benchmark + flamegraph at the end
just bench postgres 50000 --profile
```

### Criterion Micro-Benchmarks

Rust-native benchmarks using Criterion 0.5 for hot-path operations:

```bash
# Run all criterion benchmarks
cargo bench

# Arrow IPC codec benchmarks only
cargo bench --bench arrow_codec

# State backend benchmarks only
cargo bench --bench state_backend
```

HTML reports are generated in `target/criterion/`.

### Justfile Targets

| Target | Description |
|--------|-------------|
| `just bench` | Run E2E pipeline benchmarks (all connectors, defaults) |
| `just bench postgres 50000` | Benchmark postgres with 50K rows |
| `just bench-compare ref1 ref2` | Compare benchmarks between two git refs |

## Profiling with samply

Uses [samply](https://github.com/mstange/samply) for CPU profiling with full symbol resolution on macOS (unlike `cargo flamegraph` which produces mostly unresolved hex addresses due to SIP).

Install: `cargo install samply`

### Integrated Profiling

Pass `--profile` to `bench.sh` to run the normal benchmark iterations first, then record a profile:

```bash
just bench postgres --profile
```

Output: `target/profiles/profile_<rows>rows_insert_<timestamp>.json`

View with: `samply load <profile.json>` — opens Firefox Profiler in your browser with full call stacks, source-level mapping, and flame charts.

## Regression Tracking

### Comparing Two Git Refs

`compare.sh` checks out each ref, builds, benchmarks, and shows a comparison — all in one command:

```bash
# Compare main vs your feature branch
just bench-compare main my-feature

# Compare with options
just bench-compare HEAD~1 HEAD --connector postgres --rows 100000
```

The script:
1. Validates both refs exist and the working tree is clean
2. Starts PostgreSQL once (shared across both refs)
3. For each ref: checks out, builds (release), seeds, runs INSERT + COPY iterations
4. Restores your original branch
5. Shows a side-by-side comparison report

| Flag | Default | Description |
|------|---------|-------------|
| `--connector NAME` | (all) | Benchmark only this connector |
| `--rows N` | 10000 | Number of rows to benchmark |
| `--iters N` | 3 | Iterations per mode |
| `--aot` | on | Enable Wasmtime AOT cache |
| `--no-aot` | off override | Disable Wasmtime AOT cache |

**Note:** Both refs must have the new `bench/` directory structure. Comparing against refs from before the benchmark refactor is not supported.

### Viewing Past Results

Use `bench/analyze.py` to view and compare results already stored in `results.jsonl`:

```bash
# Compare last 2 runs
python3 bench/analyze.py

# Show last 5 runs
python3 bench/analyze.py --last 5

# Compare specific commits
python3 bench/analyze.py --sha abc1234 def5678

# Filter by row count
python3 bench/analyze.py --rows 100000
```

Every benchmark run appends enriched JSON results to `target/bench_results/results.jsonl`. Each line includes all `PipelineResult` timing fields plus metadata: `timestamp`, `mode`, `bench_rows`, `aot`, `git_sha`, `git_branch`, and resource metrics (`cpu_cores_*`, `cpu_total_util_pct_*`, `mem_rss_mb_*`, `resource_samples`).

### Resource Sampling

`bench.sh` samples host process resources during each `rapidbyte run` iteration:

- CPU: average/peak process CPU, converted to core-equivalent usage and total-host utilization percent
- Memory: average/peak RSS (resident memory) in MB
- Sampling cadence: `0.20s` by default, configurable via `BENCH_RESOURCE_SAMPLE_INTERVAL`

These metrics are printed in the per-iteration console output, included in the criterion-style table, and persisted into `results.jsonl`.

## Understanding the Output

### Criterion-Style Report

`bench.sh` prints a statistical report after all iterations:

```
  Dataset:     10,000 rows, 1.14 MB (120 B/row)
  Samples:     3 INSERT, 3 COPY

  connector-postgres/insert/10000
                        time:   [1.1902 1.2340 1.2778] s
                        thrpt:  [7,826 8,104 8,401] rows/s
                                [0.89 0.93 0.96] MB/s

  connector-postgres/copy/10000
                        time:   [0.8512 0.8900 0.9288] s
                        thrpt:  [10,766 11,236 11,748] rows/s
                                [1.23 1.28 1.34] MB/s

  COPY vs INSERT:  1.39x faster

  Metric (mean)           INSERT          COPY     Speedup
  ----------------------  ----------  ----------  ----------
  Total duration            1.2340s       0.8900s      1.4x
  ...
```

### Timing Hierarchy

```
duration_secs
├── source_duration_secs          (source thread wall time)
│   ├── source_connect_secs       (PG connection)
│   ├── source_query_secs         (query execution)
│   ├── source_fetch_secs         (row fetching + Arrow encoding)
│   └── source_arrow_encode_secs  (Arrow IPC serialization, subset of fetch)
├── dest_duration_secs            (dest thread wall time)
│   ├── dest_vm_setup_secs        (WASM VM + import registration)
│   ├── dest_recv_secs            (channel recv loop)
│   ├── dest_connect_secs         (PG connection)
│   ├── dest_flush_secs           (INSERT/COPY writes + Arrow decoding)
│   │   └── dest_arrow_decode_secs (Arrow IPC deserialization, subset of flush)
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

Four benchmark pipeline YAMLs are provided in `bench/fixtures/pipelines/`:

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

To benchmark with compression, run the pipeline directly:

```bash
rapidbyte run bench/fixtures/pipelines/bench_pg_lz4.yaml
rapidbyte run bench/fixtures/pipelines/bench_pg_zstd.yaml
```

## Adding a New Connector Benchmark

1. Create `bench/connectors/<name>/` with 4 files:
   - `config.sh` — `BENCH_DEFAULT_ROWS`, `BENCH_MODES`, `BENCH_PIPELINES`
   - `setup.sh` — Start infrastructure, seed data
   - `run.sh` — Iterate modes, collect results, call `criterion_report`
   - `teardown.sh` — Clean per-run state
2. Add pipeline YAMLs to `bench/fixtures/pipelines/`
3. Run: `just bench <name>`

## Troubleshooting

**PostgreSQL not ready** — The scripts use `psql -c "SELECT 1"` to check readiness, not `pg_isready`. If seeding fails, increase the wait time or check Docker logs with `docker compose logs postgres`.

**AOT compilation flags** — Bench scripts enable Wasmtime AOT cache by default. Use `--no-aot` to disable it for A/B comparisons.

**Debug build panics** — WASI connectors panic in debug builds on unsupported socket ops (e.g., `set_nodelay`). Always use release builds for benchmarking (the default). Debug mode is available via `--debug` but is only useful for testing the benchmark harness itself.

**`records_written` shows 0** — Known issue. The dest connector stats tracking doesn't update this counter. Use `records_read` for throughput calculations (the scripts already do this).

**Profiling shows no symbols** — If using `cargo flamegraph` on macOS, SIP prevents dtrace from resolving symbols. Use `samply` instead (`cargo install samply`), which resolves symbols from DWARF debug info independently.

**No results for bench-compare** — Results are stored in `target/bench_results/results.jsonl`. This file is created on the first `bench.sh` run. Clean builds (`cargo clean`) do not delete it since it's under `target/bench_results/`, but manually deleting `target/` will.

**Bash 4+ required** — The connector config scripts use associative arrays (`declare -A`) which require bash 4+. macOS ships with bash 3.2; install a modern bash via Homebrew (`brew install bash`).
