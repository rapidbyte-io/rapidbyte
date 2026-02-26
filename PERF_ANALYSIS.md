# Rapidbyte Performance Analysis (Handoff)

Last updated: 2026-02-25

This document captures performance profiling work completed in this session so future agents can continue from evidence instead of re-running discovery.

## Scope Covered

- End-to-end benchmark profiling (`small`, `medium`, `large` datasets)
- Stage-level timing decomposition (source / transform / destination)
- AOT vs non-AOT impact
- Compression mode impact (`none`, `lz4`, `zstd`)
- Destination policy tuning (`copy_flush_bytes`, checkpoint intervals)
- Host `next_batch` wait-vs-process split instrumentation
- Initial adaptive destination flush policy implementation

## Critical Findings

1. Destination stage is the dominant critical path in nearly all AOT-on runs.
2. CPU utilization remains low relative to host core count (typically under ~1 core average), indicating structural under-parallelization.
3. AOT is mandatory for competitive performance, especially for transform-heavy workloads.
4. Compression is generally a net negative in local-loop benchmarks for `small`/`medium` data, mixed for `large`.
5. Host `next_batch` is almost entirely blocking wait time (not compute time).
6. Destination write policy strongly affects throughput:
   - Tiny `copy_flush_bytes` (128KB/256KB) is catastrophic.
   - Best observed in medium profile around 1MB-16MB.

## Evidence and Artifacts

### Benchmark and profile outputs

- Main bench history: `target/bench_results/results.jsonl`
- Phase 3 matrix (profiles x modes x AOT): `/tmp/rapidbyte_phase3_matrix.jsonl`
- Phase 3 run log: `/tmp/phase3_run.log`
- Phase 4 wait/process split runs: `/tmp/phase4_waitsplit.jsonl`
- Phase 4 wait/process split log: `/tmp/phase4_waitsplit.log`
- Destination policy sweep (medium): `/tmp/phase4_dest_policy_sweep.jsonl`
- Destination policy sweep log (medium): `/tmp/phase4_dest_policy_sweep.log`
- Destination policy sweep (large): `/tmp/phase4_dest_policy_sweep_large.jsonl`
- Destination policy sweep log (large): `/tmp/phase4_dest_policy_sweep_large.log`
- Samply profile artifact: `target/profiles/profile_small_copy_20260225_214547.json`

## Key Quantitative Results

### AOT impact (Phase 3)

- `small/transform`: ~8.5x faster with AOT
- `small/transform_filter`: ~15.7x faster with AOT
- `medium/transform`: ~5.9x faster with AOT
- `medium/copy`: ~2.3x faster with AOT

Conclusion: keep AOT enabled by default for performance-sensitive workloads.

### Host `next_batch` wait/process split (Phase 4)

Using new instrumentation fields:

- `dest_recv_wait_nanos`
- `dest_recv_process_nanos`

Observed across representative runs:

- `wait_share` of `dest_recv_nanos`: effectively ~100%
- `process` time: near-zero (microseconds)

Conclusion: host-side `next_batch` CPU is not the bottleneck; destination is waiting on upstream/downstream flow and write path timing.

### Destination policy sweep (medium, 50k rows, AOT on)

`copy_flush_bytes` sweep with checkpointing disabled:

- 128KB: ~12.4s avg (very poor)
- 256KB: ~5.1s avg
- 512KB: ~2.0s avg
- 1MB: ~1.81s avg (best observed)
- 4MB: ~1.83s avg
- 16MB: ~1.83s avg
- 64MB: ~1.98s avg

Checkpoint interval sweep (`copy_flush_bytes=4MB`):

- 0MB (disabled): ~1.96s avg (best in this medium sweep)
- 16MB: ~2.24s avg
- 64MB: ~2.79s avg
- 256MB: ~3.20s avg

### Destination policy spot-check (large, 10k rows, AOT on)

- 1MB flush performs poorly (~16.2s)
- 4MB/16MB are much better (~10.5s/~10.2s)
- Best observed in this limited sample: checkpoint interval 64MB (~8.9s)

Conclusion: optimal tuning is workload-dependent, but avoiding tiny flush thresholds is universally important.

## Code Changes Already Made (Performance Instrumentation/Tuning)

### Transform benchmark validity fixes

- Updated benchmark transform queries to match seeded schemas:
  - `bench/fixtures/pipelines/bench_pg_transform.yaml`
  - `bench/fixtures/pipelines/bench_pg_transform_filter.yaml`

### Source/destination perf metric emission

- Source perf metrics emitted:
  - `connectors/source-postgres/src/metrics.rs`
  - `connectors/source-postgres/src/reader.rs`
- Destination perf metrics emitted:
  - `connectors/dest-postgres/src/writer.rs`

### Host timing aggregation + pipeline exposure

- Host metric aggregation and timing fields:
  - `crates/rapidbyte-runtime/src/host_state.rs`
- Orchestrator aggregation/fallback wiring:
  - `crates/rapidbyte-engine/src/orchestrator.rs`
- CLI/bench JSON exposure:
  - `crates/rapidbyte-cli/src/commands/run.rs`

### `next_batch` wait vs process split

- Added and surfaced:
  - `next_batch_wait_nanos`
  - `next_batch_process_nanos`
  - CLI JSON fields: `dest_recv_wait_nanos`, `dest_recv_process_nanos`

### Adaptive destination flush policy prototype

- Implemented in `connectors/dest-postgres/src/writer.rs`
  - If user sets `copy_flush_bytes`, respect it.
  - Otherwise choose by observed avg row size:
    - `<8KB`: 1MB
    - `8KB..64KB`: 4MB
    - `>=64KB`: 16MB

## Validation Status

- `cargo fmt`: passed
- `cargo test --workspace`: passed
- Connector `cargo check`: passed
- Note: connector-local `cargo test` for `dest-postgres` compiles but cannot execute wasm artifact in this environment (permission/runtime limitation).

## Current Interpretation (First Principles)

1. Throughput ceiling is dominated by destination write-path behavior and pipeline stage coupling, not host-side `next_batch` CPU.
2. Current architecture is under-utilizing available CPU cores for single-stream workloads.
3. Tuning helps materially, but major additional gains likely require architectural parallelism (source partition fan-out + downstream fan-in coordination).

## Recommended Next Work (Priority Order)

1. Implement source partition fan-out prototype for full refresh (`id % N = shard`) and benchmark core utilization + throughput uplift.
2. Keep AOT on by default in benchmark/perf paths; add guardrails to detect accidental non-AOT runs.
3. Add benchmark profile-specific default tuning for destination (`copy_flush_bytes`, checkpoint behavior), while preserving explicit user overrides.
4. Re-run Phase 3 matrix after partition fan-out and compare against this baseline artifacts set.

## Reproduction Commands

- Full connector bench:
  - `just bench postgres --profile medium --iters 3`
- Profile matrix baseline (manual harness used in session):
  - see scripts/logs under `/tmp/phase3_run.log`, `/tmp/phase4_*.log`
- Micro-benchs:
  - `cargo bench --workspace --bench '*'`

## 2026-02-26 Destination Parallel Correctness + Perf Gate

### Correctness Gate (parallel destination)

- Command used: `for i in 1 2 3 4 5; do tests/connectors/postgres/setup.sh; RAPIDBYTE_CONNECTOR_DIR=target/connectors tests/connectors/postgres/scenarios/10_parallelism.sh; done`
- Result: 5/5 passed.
- Observed: zero destination retry/failure markers (`Retryable error`, `WRITE_FAILED`, `COMMIT_FAILED`, `SESSION_BEGIN_FAILED`).
- Observed: stable destination row counts on every run (`users=3`, `orders=3`, `all_types=4`).

### Benchmark Gate Runs (AOT on, release)

- Command: `just bench postgres --profile medium --iters 3`
  - Medium INSERT mean: `4.1110s` (`12,452 rows/s`, `19.91 MB/s`)
  - Medium COPY mean: `2.9011s` (`17,328 rows/s`, `27.70 MB/s`)
  - COPY vs INSERT: `1.42x`

- Command: `just bench postgres --profile large --iters 3`
  - Large INSERT mean: `15.6081s` (`645 rows/s`, `42.43 MB/s`)
  - Large COPY mean: `14.8206s` (`675 rows/s`, `44.43 MB/s`)
  - COPY vs INSERT: `1.05x`

### Utilization Snapshot

- Medium run shows destination worker fields present in bench JSON (`worker_records_skew_ratio`, `worker_dest_recv_secs`, `worker_dest_vm_setup_secs`, `worker_dest_active_secs`).
- Process-level CPU fields present in bench JSON (`process_cpu_secs`, `process_cpu_pct_one_core`, `process_cpu_pct_available_cores`, `available_cores`).

### Notes

- Bench runs were blocked initially by Docker compose port contention with the postgres test stack (`5433`); resolved by tearing down `tests/connectors/postgres/docker-compose.yml` before benchmark runs.
