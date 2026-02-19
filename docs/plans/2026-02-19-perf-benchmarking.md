# Performance Benchmarking & Profiling Framework Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a repeatable benchmark harness that produces flamegraphs, tracks regression history, and supports multiple dataset sizes — replacing the ad-hoc bench.sh with a structured framework.

**Architecture:** Extend the existing bench.sh with profiling integration (`cargo-flamegraph` / `dtrace`), add a structured JSON results store for cross-run comparison, add configurable benchmark scenarios (varying row counts, batch sizes, compression, inflight), and capture host-side timing at the hot-path level (host function call durations).

**Tech Stack:** Rust, bash, cargo-flamegraph, dtrace (macOS), Python 3 (reporting), Docker Compose

---

## Current State Assessment

### What exists today

| Component | Status | Location |
|-----------|--------|----------|
| bench.sh | Working | `tests/bench.sh` — runs INSERT vs COPY, 3 iters, comparison report |
| @@BENCH_JSON@@ | Working | Machine-readable per-run JSON in CLI output |
| PipelineResult timing | Good | source/dest duration, connect/query/fetch, flush/commit, VM setup, recv loop, WASM overhead |
| ReadPerf / WritePerf | Good | Connector-reported sub-phase timing |
| AOT compilation | Working | `wasmedge compile` in bench.sh (reduces module load ~500ms → ~4ms) |
| cargo-flamegraph | Installed | v0.6.4 — generates flamegraphs from dtrace sampling |

### What's missing

1. **No profiling integration** — bench.sh doesn't capture flamegraphs or dtrace profiles
2. **No host-function-level timing** — we measure connector sub-phases but not individual host FFI call durations (emit_batch, next_batch, compression time)
3. **No regression tracking** — results are ephemeral (temp files, discarded after run)
4. **Single scenario** — only benchmarks INSERT vs COPY at one row count per invocation
5. **No batch-size / compression / inflight sweeps** — can't answer "what's the optimal max_batch_bytes?" or "does lz4 help at 100K rows?"

---

### Task 1: Add host-function-level timing instrumentation

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/host_functions.rs:93-152` (host_emit_batch)
- Modify: `crates/rapidbyte-core/src/runtime/host_functions.rs:158-230` (host_next_batch)
- Modify: `crates/rapidbyte-core/src/runtime/host_functions.rs:28-68` (HostState)
- Modify: `crates/rapidbyte-core/src/engine/runner.rs:26-53` (PipelineResult)
- Modify: `crates/rapidbyte-core/src/engine/runner.rs:76-184` (run_source)
- Modify: `crates/rapidbyte-core/src/engine/runner.rs:187-309` (run_destination)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs:448-470` (PipelineResult construction)
- Modify: `crates/rapidbyte-cli/src/commands/run.rs` (CLI output + bench JSON)

This adds cumulative timing for the hot-path host functions: how much wall time is spent in emit_batch, next_batch, compression, and decompression. These are key to understanding where host overhead lives.

**Step 1: Add timing accumulators to HostState**

In `crates/rapidbyte-core/src/runtime/host_functions.rs`, add timing fields to `HostState`:

```rust
    // --- Performance counters ---
    /// Cumulative time spent in host_emit_batch (including compression + channel send).
    pub emit_batch_nanos: u64,
    /// Cumulative time spent in host_next_batch (including decompression + channel recv).
    pub next_batch_nanos: u64,
    /// Cumulative time spent compressing batches in emit_batch.
    pub compress_nanos: u64,
    /// Cumulative time spent decompressing batches in next_batch.
    pub decompress_nanos: u64,
    /// Number of host_emit_batch calls.
    pub emit_batch_count: u64,
    /// Number of host_next_batch calls that returned data.
    pub next_batch_count: u64,
```

**Step 2: Instrument host_emit_batch**

Wrap the body of `host_emit_batch` in timing:

```rust
pub fn host_emit_batch(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let fn_start = std::time::Instant::now();
    data.clear_last_error();

    // ... existing code for memory read, empty check ...

    // Compress if a codec is configured
    let compress_start = std::time::Instant::now();
    let batch_bytes = if let Some(codec) = data.compression {
        super::compression::compress(codec, &batch_bytes)
    } else {
        batch_bytes
    };
    data.compress_nanos += compress_start.elapsed().as_nanos() as u64;

    // ... existing sender code ...

    data.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
    data.emit_batch_count += 1;
    Ok(vec![WasmValue::from_i32(0)])
}
```

**Step 3: Instrument host_next_batch**

Same pattern for `host_next_batch` — measure total call time and decompression time.

**Step 4: Initialize timing fields in all HostState construction sites**

In `runner.rs`, add the new fields (all `0`) to every `HostState { ... }` block:
- `run_source` (line ~88)
- `run_destination` (line ~209)
- `run_transform` (line ~333)
- `validate_connector` (line ~431)
- `run_discover` (line ~488)

**Step 5: Extract timing from HostState and thread to PipelineResult**

The tricky part: HostState is moved into the import object, so we can't read it after the VM runs. Use `Arc<Mutex<HostTimings>>` shared between HostState and the runner, same pattern as `stats`.

Add a new struct:

```rust
#[derive(Debug, Clone, Default)]
pub struct HostTimings {
    pub emit_batch_nanos: u64,
    pub next_batch_nanos: u64,
    pub compress_nanos: u64,
    pub decompress_nanos: u64,
    pub emit_batch_count: u64,
    pub next_batch_count: u64,
}
```

Change HostState timing fields to use `Arc<Mutex<HostTimings>>` (one shared ref).

Add to PipelineResult:

```rust
    // Host function timing (nanoseconds)
    pub source_emit_nanos: u64,
    pub source_compress_nanos: u64,
    pub source_emit_count: u64,
    pub dest_recv_nanos: u64,
    pub dest_decompress_nanos: u64,
    pub dest_recv_count: u64,
```

Thread from HostTimings into PipelineResult in the orchestrator.

**Step 6: Display in CLI output and bench JSON**

Add to `run.rs` output:

```rust
    println!("  Host emit_batch: {:.3}s ({} calls)", result.source_emit_nanos as f64 / 1e9, result.source_emit_count);
    println!("    Compression:   {:.3}s", result.source_compress_nanos as f64 / 1e9);
    println!("  Host next_batch: {:.3}s ({} calls)", result.dest_recv_nanos as f64 / 1e9, result.dest_recv_count);
    println!("    Decompression: {:.3}s", result.dest_decompress_nanos as f64 / 1e9);
```

And to the bench JSON.

**Step 7: Run tests**

Run: `cargo test --workspace`
Expected: All pass

**Step 8: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/host_functions.rs \
       crates/rapidbyte-core/src/engine/runner.rs \
       crates/rapidbyte-core/src/engine/orchestrator.rs \
       crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(core): add host-function-level timing instrumentation for perf analysis"
```

---

### Task 2: Add flamegraph profiling to bench.sh

**Files:**
- Modify: `tests/bench.sh`
- Create: `tests/bench_profile.sh`

**Step 1: Create a dedicated profiling script**

Create `tests/bench_profile.sh` — a slimmed-down version of bench.sh that runs a single benchmark iteration with `cargo-flamegraph` attached:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"
PROFILE_DIR="$PROJECT_ROOT/target/profiles"

# Source WasmEdge environment
if [ -f "$HOME/.wasmedge/env" ]; then
    set +u; source "$HOME/.wasmedge/env"; set -u
fi

BENCH_ROWS="${1:-10000}"
LOAD_METHOD="${2:-insert}"

mkdir -p "$PROFILE_DIR"

# Build with debug symbols in release mode
echo "[INFO] Building with debug symbols (release + debuginfo)..."
CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release

# Build connectors
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --release)
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --release)

# Stage connectors
mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip1/release/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip1/release/dest_postgres.wasm" "$CONNECTOR_DIR/"

# Start PG, seed data (same as bench.sh)
docker compose -f "$PROJECT_ROOT/docker-compose.yml" up -d
cleanup() { docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true; }
trap cleanup EXIT

echo "[INFO] Waiting for PostgreSQL..."
for i in $(seq 1 30); do
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "SELECT 1" > /dev/null 2>&1 && break
    [ "$i" -eq 30 ] && { echo "FAIL: PG not ready"; exit 1; }
    sleep 1
done
sleep 2

echo "[INFO] Seeding $BENCH_ROWS rows..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
    < "$PROJECT_ROOT/tests/fixtures/sql/bench_seed.sql" > /dev/null

# Clean dest
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
rm -f /tmp/rapidbyte_bench_state.db

# Select pipeline YAML
PIPELINE="bench_pg.yaml"
[ "$LOAD_METHOD" = "copy" ] && PIPELINE="bench_pg_copy.yaml"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SVG_FILE="$PROFILE_DIR/flamegraph_${BENCH_ROWS}rows_${LOAD_METHOD}_${TIMESTAMP}.svg"

echo "[INFO] Running with flamegraph profiling..."
echo "[INFO] Output: $SVG_FILE"

cargo flamegraph \
    --bin rapidbyte \
    --release \
    -o "$SVG_FILE" \
    -- run "$PROJECT_ROOT/tests/fixtures/pipelines/$PIPELINE" \
    --log-level warn

echo "[INFO] Flamegraph saved to: $SVG_FILE"
echo "[INFO] Open with: open $SVG_FILE"
```

**Step 2: Add `--profile` flag to bench.sh**

Add a `--profile` flag to the existing bench.sh that runs a single iteration with flamegraph after the regular benchmark:

In the argument parsing section, add:

```bash
PROFILE="false"
# In the case block:
--profile) PROFILE="true"; shift ;;
```

After the regular benchmark runs, add:

```bash
if [ "$PROFILE" = "true" ]; then
    info "Running profiling iteration with flamegraph..."
    # Clean dest
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
    rm -f /tmp/rapidbyte_bench_state.db

    PROFILE_DIR="$PROJECT_ROOT/target/profiles"
    mkdir -p "$PROFILE_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    SVG_FILE="$PROFILE_DIR/flamegraph_${BENCH_ROWS}rows_insert_${TIMESTAMP}.svg"

    # Rebuild with debug symbols for flamegraph
    CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release 2>&1 | tail -1

    cargo flamegraph \
        --bin rapidbyte \
        --release \
        -o "$SVG_FILE" \
        -- run "$PROJECT_ROOT/tests/fixtures/pipelines/bench_pg.yaml" \
        --log-level warn 2>&1

    info "Flamegraph saved to: $SVG_FILE"
fi
```

**Step 3: Add justfile targets**

Add to `justfile`:

```makefile
# Profile a benchmark run and generate flamegraph
bench-profile rows="10000" mode="insert":
    ./tests/bench_profile.sh {{rows}} {{mode}}

# Benchmark with profiling at the end
bench-connector-postgres-profile rows="10000":
    ./tests/bench.sh --rows {{rows}} --iters 3 --profile
```

**Step 4: Commit**

```bash
git add tests/bench_profile.sh tests/bench.sh justfile
git commit -m "feat(bench): add flamegraph profiling integration to benchmark harness"
```

---

### Task 3: Add benchmark results persistence and regression tracking

**Files:**
- Modify: `tests/bench.sh`
- Create: `tests/bench_compare.py`

**Step 1: Save results to a persistent store**

Modify bench.sh to append results to a JSONL file instead of temp files:

```bash
RESULTS_DIR="$PROJECT_ROOT/target/bench_results"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/results.jsonl"
```

After each iteration, write an enriched result:

```bash
# In run_mode(), after extracting JSON_LINE:
ENRICHED=$(echo "$JSON_LINE" | python3 -c "
import sys, json, datetime
d = json.load(sys.stdin)
d['timestamp'] = datetime.datetime.now().isoformat()
d['mode'] = '$mode'
d['bench_rows'] = $BENCH_ROWS
d['aot'] = $( [ '$BENCH_AOT' = 'true' ] && echo 'true' || echo 'false' )
d['git_sha'] = '$(git rev-parse --short HEAD 2>/dev/null || echo unknown)'
d['git_branch'] = '$(git branch --show-current 2>/dev/null || echo unknown)'
print(json.dumps(d))
")
echo "$ENRICHED" >> "$RESULTS_FILE"
```

**Step 2: Create comparison script**

Create `tests/bench_compare.py` that loads the results JSONL and compares the last N runs:

```python
#!/usr/bin/env python3
"""Compare benchmark results across runs.

Usage:
    python3 tests/bench_compare.py                    # Compare last 2 runs
    python3 tests/bench_compare.py --last 5           # Show last 5 runs
    python3 tests/bench_compare.py --sha abc123 def456  # Compare specific commits
"""
import argparse
import json
import sys
from pathlib import Path
from collections import defaultdict

RESULTS_FILE = Path(__file__).parent.parent / "target" / "bench_results" / "results.jsonl"

def load_results():
    if not RESULTS_FILE.exists():
        print(f"No results file at {RESULTS_FILE}")
        sys.exit(1)
    results = []
    for line in RESULTS_FILE.read_text().splitlines():
        if line.strip():
            results.append(json.loads(line))
    return results

def group_by_run(results):
    """Group results by (git_sha, mode, bench_rows)."""
    groups = defaultdict(list)
    for r in results:
        key = (r.get("git_sha", "?"), r.get("mode", "?"), r.get("bench_rows", 0))
        groups[key].append(r)
    return groups

def avg(results, key):
    vals = [r.get(key, 0) for r in results]
    return sum(vals) / len(vals) if vals else 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--last", type=int, default=2, help="Compare last N runs")
    parser.add_argument("--sha", nargs="+", help="Compare specific git SHAs")
    parser.add_argument("--rows", type=int, help="Filter by row count")
    args = parser.parse_args()

    results = load_results()
    if args.rows:
        results = [r for r in results if r.get("bench_rows") == args.rows]

    if args.sha:
        results = [r for r in results if r.get("git_sha") in args.sha]

    groups = group_by_run(results)
    if not groups:
        print("No matching results found.")
        return

    # Get unique SHAs in order
    seen = []
    for r in results:
        sha = r.get("git_sha", "?")
        if sha not in seen:
            seen.append(sha)

    shas = seen[-args.last:] if not args.sha else args.sha

    metrics = [
        ("Duration (s)", "duration_secs"),
        ("Source (s)", "source_duration_secs"),
        ("Dest (s)", "dest_duration_secs"),
        ("  Flush (s)", "dest_flush_secs"),
        ("  Commit (s)", "dest_commit_secs"),
        ("  WASM overhead (s)", "wasm_overhead_secs"),
    ]

    for mode in ["insert", "copy"]:
        print(f"\n{'=' * 60}")
        print(f"  Mode: {mode.upper()}")
        print(f"{'=' * 60}")

        header = f"  {'Metric':<22s}"
        for sha in shas:
            header += f"  {sha:>10s}"
        if len(shas) == 2:
            header += f"  {'Change':>10s}"
        print(header)
        print(f"  {'-' * 22}" + f"  {'-' * 10}" * len(shas) + ("  " + "-" * 10 if len(shas) == 2 else ""))

        for label, key in metrics:
            line = f"  {label:<22s}"
            vals = []
            for sha in shas:
                matching = [r for r in results if r.get("git_sha") == sha and r.get("mode") == mode]
                v = avg(matching, key)
                vals.append(v)
                line += f"  {v:>10.3f}"
            if len(vals) == 2 and vals[0] > 0:
                pct = (vals[1] - vals[0]) / vals[0] * 100
                sign = "+" if pct > 0 else ""
                line += f"  {sign}{pct:.1f}%"
            print(line)

if __name__ == "__main__":
    main()
```

**Step 3: Add justfile target**

```makefile
# Compare benchmark results across runs
bench-compare *args="":
    python3 tests/bench_compare.py {{args}}
```

**Step 4: Commit**

```bash
git add tests/bench.sh tests/bench_compare.py justfile
git commit -m "feat(bench): add results persistence and cross-run comparison"
```

---

### Task 4: Add multi-scenario benchmark matrix

**Files:**
- Create: `tests/bench_matrix.sh`
- Create: `tests/fixtures/pipelines/bench_pg_lz4.yaml`
- Create: `tests/fixtures/pipelines/bench_pg_zstd.yaml`
- Modify: `justfile`

**Step 1: Create variant pipeline YAMLs**

Create `tests/fixtures/pipelines/bench_pg_lz4.yaml`:

```yaml
version: "1.0"
pipeline: bench_pg_lz4

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: bench_events
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw
  write_mode: append

resources:
  compression: lz4

state:
  backend: sqlite
  connection: /tmp/rapidbyte_bench_state.db
```

Same for `bench_pg_zstd.yaml` with `compression: zstd`.

**Step 2: Create matrix benchmark script**

Create `tests/bench_matrix.sh` that runs a configurable matrix:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Matrix benchmark: sweeps across row counts, load methods, compression
#
# Usage: ./tests/bench_matrix.sh [--rows "1000 10000 100000"] [--iters 3]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ROW_COUNTS="${BENCH_MATRIX_ROWS:-1000 10000 100000}"
ITERS="${BENCH_MATRIX_ITERS:-3}"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)   ROW_COUNTS="$2"; shift 2 ;;
        --iters)  ITERS="$2"; shift 2 ;;
        *)        echo "Unknown: $1"; exit 1 ;;
    esac
done

SCENARIOS=(
    "bench_pg.yaml:insert:none"
    "bench_pg_copy.yaml:copy:none"
    "bench_pg_lz4.yaml:insert:lz4"
    "bench_pg_zstd.yaml:insert:zstd"
)

echo ""
echo "═══════════════════════════════════════════════"
echo "  Rapidbyte Benchmark Matrix"
echo "  Rows: $ROW_COUNTS"
echo "  Scenarios: ${#SCENARIOS[@]}"
echo "  Iterations: $ITERS per scenario"
echo "═══════════════════════════════════════════════"
echo ""

for ROW_COUNT in $ROW_COUNTS; do
    echo "--- $ROW_COUNT rows ---"
    "$PROJECT_ROOT/tests/bench.sh" --rows "$ROW_COUNT" --iters "$ITERS"
    echo ""
done

echo ""
echo "Matrix complete. Run 'just bench-compare' to see results."
```

**Step 3: Add justfile targets**

```makefile
# Run benchmark matrix across row counts and modes
bench-matrix:
    ./tests/bench_matrix.sh

# Run benchmark matrix with custom row counts
bench-matrix-rows rows="1000 10000 100000":
    ./tests/bench_matrix.sh --rows "{{rows}}"
```

**Step 4: Commit**

```bash
git add tests/bench_matrix.sh tests/fixtures/pipelines/bench_pg_lz4.yaml \
       tests/fixtures/pipelines/bench_pg_zstd.yaml justfile
git commit -m "feat(bench): add multi-scenario benchmark matrix with compression variants"
```

---

### Task 5: Verification

**Step 1: Run all host tests**

Run: `cargo test --workspace`
Expected: All pass, no regressions

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Build both connectors**

Run:
```bash
cd connectors/source-postgres && cargo build --target wasm32-wasip1 --release
cd connectors/dest-postgres && cargo build --target wasm32-wasip1 --release
```
Expected: Both build cleanly

**Step 4: Smoke test the benchmark**

Run: `just bench-connector-postgres 1000`
Expected: Completes, shows timing including new host function metrics

**Step 5: Verify flamegraph script**

Run: `just bench-profile 1000 insert`
Expected: Produces SVG flamegraph in `target/profiles/`
