# Benchmark Summary Design

## Summary

Add a first-class benchmark summary command so a single JSONL artifact set can
be inspected without needing a saved baseline. The summary should present
readable terminal output for one or more scenario groups, emphasizing latency
and throughput in both records/sec and MB/sec.

## Goals

- Make a single benchmark artifact file easy to review locally.
- Keep the user-facing benchmark workflow inside the Rust benchmark CLI.
- Reuse the existing artifact schema and metrics already emitted by the runner.
- Provide a stable terminal-oriented summary for lab and PR benchmark outputs.

## Non-Goals

- Adding JSON summary output in this slice.
- Replacing the Python comparison layer.
- Changing the benchmark artifact format.
- Building historical trend analysis or charts.

## Alternatives Considered

### 1. Recommended: Rust `summary` subcommand

Add a new benchmark CLI subcommand that loads a JSONL artifact file, groups
rows by scenario identity, computes summary statistics, and prints a compact
report.

Pros:
- Keeps benchmark UX in one primary CLI.
- Works in minimal environments without adding more Python entrypoints.
- Reuses the same artifact parsing and schema assumptions as the benchmark
  runner.

Cons:
- Requires small new Rust surface area for parsing and rendering summaries.

### 2. Python `summary.py`

Add a second Python analysis script beside `compare.py`.

Pros:
- Fast to prototype.

Cons:
- Splits basic benchmark inspection across Rust and Python.
- Makes the core benchmark UX less cohesive.

### 3. `just` wrapper with inline scripting

Add only a shell-level helper around ad hoc Python.

Pros:
- Lowest initial code change.

Cons:
- Weak interface, harder to test, and poor long-term ergonomics.

## Recommended Design

### CLI Surface

Extend the benchmark CLI with a `summary` subcommand:

- `cargo run --manifest-path benchmarks/Cargo.toml -- summary <artifact.jsonl>`
- `just bench-summary <artifact.jsonl>`

This command should accept a single artifact path and print terminal-formatted
summary output.

### Summary Data Model

Load the JSONL file and group artifact rows by the same identity used by the
comparison layer:

- `scenario_id`
- `suite_id`
- `hardware_class`
- `build_mode`
- `execution_flags`

For each group, compute:

- sample count
- throughput in records/sec: median, min, max
- throughput in MB/sec: median, min, max
- latency in seconds: median, min, max
- correctness pass/fail counts
- connector metric top-level keys present in the group

The existing canonical metrics already include:

- `records_per_sec`
- `mb_per_sec`
- `duration_secs`

So no artifact schema change is required.

### Output Format

Render a readable plain-text report, for example:

```text
# Benchmark Summary

- scenario: pg_dest_copy
  suite: lab
  samples: 3
  throughput: 435363.1 rec/s median (407715.1 min, 452100.4 max)
  bandwidth: 53.1 MiB/s median (49.8 min, 55.2 max)
  latency: 2.297s median (2.211s min, 2.453s max)
  correctness: 3 passed, 0 failed
  connector metrics: destination, process
```

If multiple identity groups exist in one file, print one block per group.

### Error Handling

- Missing or malformed JSONL should fail with actionable parsing errors.
- Missing required canonical metrics should fail clearly, naming the missing
  metric.
- Empty files should fail clearly instead of printing a misleading empty report.

### Change Scope

### Modified files

- `benchmarks/src/cli.rs`
- `benchmarks/src/main.rs`
- `Justfile`
- `docs/BENCHMARKING.md`

### New files

- `benchmarks/src/summary.rs`

## Verification Strategy

- unit tests for CLI parsing of the `summary` subcommand
- unit tests for loading/grouping artifact rows and computing summary stats
- unit tests for rendered output including records/sec and MB/sec
- run `cargo test -p rapidbyte-benchmarks`
- run the new summary command against a real artifact file such as
  `target/benchmarks/lab/pg_dest_copy.jsonl`

## Outcome

After this change, a developer can inspect a single benchmark artifact set
directly without first creating or preserving a baseline, while still using the
existing Python comparison tools when two artifact sets need regression
analysis.
