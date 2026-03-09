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
docker compose up -d --wait
just build-all
just bench --suite lab --scenario pg_dest_insert --output target/benchmarks/lab/pg-insert.jsonl
just bench --suite lab --scenario pg_dest_copy --output target/benchmarks/lab/pg-copy.jsonl
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
- `benchmarks/analysis/` contains comparison and reporting logic
- `benchmarks/baselines/` contains checked-in smoke baselines

## Notes

- The checked-in baseline is a local and CI smoke mechanism.
- The long-term comparison model is rolling artifacts from `main`.
- Native lab scenarios currently include `pg_dest_insert` and `pg_dest_copy`.
- Use `--scenario <id>` to run one lab scenario without executing the entire suite.
- The retired benchmark harness should not be reintroduced.
