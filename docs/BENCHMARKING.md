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

Compare two artifact sets directly:

```bash
just bench-compare benchmarks/baselines/main/pr.jsonl target/benchmarks/pr/candidate.jsonl --min-samples 1
```

## Layout

- `benchmarks/scenarios/` contains declarative benchmark scenarios
- `benchmarks/analysis/` contains comparison and reporting logic
- `benchmarks/baselines/` contains checked-in smoke baselines
- `benchmarks/fixtures/legacy/` holds migrated fixtures from the retired `tests/bench` harness

## Notes

- The checked-in baseline is a local and CI smoke mechanism.
- The long-term comparison model is rolling artifacts from `main`.
- The legacy `tests/bench` harness has been retired and should not be reintroduced.
