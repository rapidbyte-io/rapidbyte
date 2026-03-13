# Benchmarking

Rapidbyte now uses the connector-agnostic benchmark platform under `benchmarks/`.

## Commands

Run the benchmark runner:

```bash
just bench --suite pr --output target/benchmarks/pr/results.jsonl
```

Run the PR smoke suite and compare it against the checked-in baseline artifact
set:

```bash
just bench-pr
```

Run the explicit native PostgreSQL destination benchmarks:

```bash
just bench-lab pg_dest_insert
just bench-lab pg_dest_copy_regression
just bench-lab pg_dest_copy_release
just bench-lab pg_dest_copy_release_distributed
```

Print a readable summary for a single artifact set:

```bash
just bench-summary target/benchmarks/lab/pg_dest_copy_regression.jsonl
just bench-summary target/benchmarks/lab/pg_dest_copy_release.jsonl
```

Compare the two generated artifact sets directly:

```bash
just bench-compare target/benchmarks/lab/pg-copy.jsonl target/benchmarks/lab/pg-insert.jsonl --min-samples 1
```

Compare two artifact sets directly:

```bash
just bench-compare benchmarks/baselines/main/pr.jsonl target/benchmarks/pr/candidate.jsonl --min-samples 1
```

## Layout

- `benchmarks/scenarios/` contains declarative benchmark scenarios
- `benchmarks/environments/` contains benchmark environment profiles
- `benchmarks/analysis/` contains comparison and reporting logic
- `benchmarks/baselines/` contains checked-in smoke baselines

## Notes

- The checked-in baseline is a local and CI smoke mechanism.
- The long-term comparison model is rolling artifacts from `main`.
- Native lab scenarios currently include `pg_dest_insert`,
  `pg_dest_copy_regression`, and `pg_dest_copy_release`.
- `pg_dest_copy_regression` is the cheap regression-tracking COPY benchmark.
- `pg_dest_copy_release` is the release-mode COPY benchmark with AOT enabled.
- Use `just bench-summary <artifact>` when you only have one JSONL artifact set
  and want to inspect latency and throughput without a saved baseline.
- Summary throughput and bandwidth are end-to-end wall-clock metrics for the
  whole pipeline run, not isolated inner-loop COPY-only rates. The rendered
  bandwidth label uses `MiB/sec` because the value is computed as
  `bytes / 1024 / 1024`.
- Native lab scenarios should be run with `--env-profile <id>` or the `just bench-lab` wrapper.
- The benchmark-owned local profile is `local-bench-postgres`.
- `just bench-lab` uses the scenario manifest's `environment.ref` by default.
  `just bench-pr` still defaults to `local-bench-postgres`.
- `local-bench-postgres` provisions an isolated Docker Compose project under
  `benchmarks/` and tears it down afterward for local benchmark scenarios.
- `local-bench-distributed-postgres` does the same for the single-node
  distributed benchmark stack.
- `local-dev-postgres` remains a shared/manual profile for ad hoc runs against
  an already running stack, but it is not the default benchmark-owned path.
- Override local profile settings with:
  `RB_BENCH_PG_HOST`, `RB_BENCH_PG_PORT`, `RB_BENCH_PG_USER`,
  `RB_BENCH_PG_PASSWORD`, `RB_BENCH_PG_DATABASE`,
  `RB_BENCH_PG_SOURCE_SCHEMA`, and `RB_BENCH_PG_DEST_SCHEMA`.
- The benchmark runner auto-provisions benchmark-owned Docker environments for
  profiles like `local-bench-postgres`, while shared/manual profiles like
  `local-dev-postgres` are treated as existing environments.
- The retired benchmark harness should not be reintroduced.
