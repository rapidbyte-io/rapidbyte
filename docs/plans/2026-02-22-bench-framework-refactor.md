# Benchmark Framework Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the scattered benchmark scripts in `tests/` with a structured, extensible benchmark framework in `bench/` that follows the same orchestrator pattern as `tests/e2e.sh`, with Criterion micro-benchmarks for core components and criterion-style statistical output for E2E pipeline benchmarks.

**Architecture:** Two-tier benchmark system: (1) Criterion Rust benchmarks for hot-path component code (Arrow codec, WASM loading, state backend) run via `cargo bench`, and (2) E2E pipeline benchmarks via shell orchestrator in `bench/` that run full pipelines against Docker Postgres with criterion-inspired statistical output. The shell orchestrator auto-discovers connector benchmarks from `bench/connectors/*/config.sh`, making it trivial to add new connector benchmarks.

**Tech Stack:** Criterion 0.5 (Rust micro-benchmarks), Bash (E2E orchestrator), Python 3 (statistical analysis + results viewer), Docker Compose (Postgres infrastructure).

---

## Phase 1: Create bench/ directory structure and shared helpers

### Task 1: Create bench directory scaffolding

**Files:**
- Create: `bench/` (directory)
- Create: `bench/connectors/postgres/` (directory)
- Create: `bench/fixtures/sql/` (directory)
- Create: `bench/fixtures/pipelines/` (directory)
- Create: `bench/lib/` (directory)

**Step 1: Create the directory tree**

Run:
```bash
mkdir -p bench/lib bench/connectors/postgres bench/fixtures/sql bench/fixtures/pipelines
```

Expected: Directories created, no output.

**Step 2: Commit scaffolding**

```bash
git add bench/
git commit -m "bench: create directory scaffolding for benchmark framework"
```

---

### Task 2: Create bench/lib/helpers.sh

This file provides benchmark-specific helpers plus sources the existing test helpers for shared functions (`pg_exec`, `pg_cmd`, color codes, `info`, `fail`, `section`, etc.).

**Files:**
- Create: `bench/lib/helpers.sh`

**Step 1: Write the helpers file**

```bash
#!/usr/bin/env bash
# Benchmark framework shared helpers.
# Sources test helpers for pg_exec, colors, etc. and adds bench-specific functions.

_BENCH_HELPERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$_BENCH_HELPERS_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$BENCH_DIR/.." && pwd)"

# Re-use test helpers (pg_exec, pg_cmd, colors, info/warn/fail/section, assertions)
source "$PROJECT_ROOT/tests/lib/helpers.sh"

CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"
RESULTS_DIR="$PROJECT_ROOT/target/bench_results"
RESULTS_FILE="$RESULTS_DIR/results.jsonl"
mkdir -p "$RESULTS_DIR"

# ── Deterministic build environment ──────────────────────────────
setup_deterministic_env() {
    if [ "${RAPIDBYTE_BENCH_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
        export RUSTC_WRAPPER=""
        export CARGO_BUILD_RUSTC_WRAPPER=""
    fi
    if [ "${RAPIDBYTE_BENCH_RESET_RUSTFLAGS:-1}" = "1" ]; then
        export RUSTFLAGS=""
        export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
    fi
}

# ── Build helpers ────────────────────────────────────────────────
build_host() {
    local mode="${1:-release}"
    info "Building host ($mode)..."
    if [ "$mode" = "release" ]; then
        (cd "$PROJECT_ROOT" && cargo build --release --quiet)
    else
        (cd "$PROJECT_ROOT" && cargo build --quiet)
    fi
}

build_connectors() {
    local mode="${1:-release}"
    local flag=""
    [ "$mode" = "release" ] && flag="--release"

    info "Building source-postgres connector ($mode)..."
    (cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build $flag --quiet)

    info "Building dest-postgres connector ($mode)..."
    (cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build $flag --quiet)
}

stage_connectors() {
    local mode="${1:-release}"
    local wasm_dir="wasm32-wasip2/$mode"

    mkdir -p "$CONNECTOR_DIR"
    cp "$PROJECT_ROOT/connectors/source-postgres/target/$wasm_dir/source_postgres.wasm" \
       "$CONNECTOR_DIR/"
    cp "$PROJECT_ROOT/connectors/dest-postgres/target/$wasm_dir/dest_postgres.wasm" \
       "$CONNECTOR_DIR/"
    cp "$PROJECT_ROOT/connectors/source-postgres/manifest.json" \
       "$CONNECTOR_DIR/source_postgres.manifest.json"
    cp "$PROJECT_ROOT/connectors/dest-postgres/manifest.json" \
       "$CONNECTOR_DIR/dest_postgres.manifest.json"
}

# ── Docker helpers ───────────────────────────────────────────────
start_postgres() {
    info "Starting Docker Compose..."
    docker compose -f "$COMPOSE_FILE" up -d --wait 2>/dev/null

    info "Waiting for PostgreSQL readiness..."
    local retries=30
    while ! pg_exec "SELECT 1" > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [ "$retries" -le 0 ]; then
            fail "PostgreSQL did not become ready within 30 seconds"
        fi
        sleep 1
    done
    sleep 2
    info "PostgreSQL ready"
}

stop_postgres() {
    info "Stopping Docker Compose..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}

clean_dest_schema() {
    pg_cmd "DROP SCHEMA IF EXISTS raw CASCADE"
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}

# ── Git metadata ─────────────────────────────────────────────────
git_sha() {
    git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

git_branch() {
    git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "unknown"
}

# ── Benchmark execution ─────────────────────────────────────────
# Run a single pipeline iteration and return the JSON metrics line.
# Usage: run_pipeline_bench <pipeline_yaml>
# Outputs JSON to stdout, status messages to stderr.
run_pipeline_bench() {
    local pipeline_yaml="$1"
    local build_mode="${2:-release}"
    local target_dir="release"
    [ "$build_mode" != "release" ] && target_dir="debug"

    local output
    output=$("$PROJECT_ROOT/target/$target_dir/rapidbyte" run \
        "$pipeline_yaml" \
        --log-level warn 2>&1)

    local json_line
    json_line=$(echo "$output" | grep "@@BENCH_JSON@@" | sed 's/@@BENCH_JSON@@//')

    if [ -z "$json_line" ]; then
        echo "" # Empty signals failure
        return 1
    fi

    echo "$json_line"
}

# Enrich a JSON result line with metadata.
# Usage: enrich_result <json_line> <mode> <rows> <aot> [extra_json_fields]
enrich_result() {
    local json_line="$1"
    local mode="$2"
    local rows="$3"
    local aot="$4"

    python3 -c "
import sys, json, datetime
d = json.loads(sys.argv[1])
d['timestamp'] = datetime.datetime.now().isoformat()
d['mode'] = sys.argv[2]
d['bench_rows'] = int(sys.argv[3])
d['aot'] = sys.argv[4] == 'true'
d['git_sha'] = sys.argv[5]
d['git_branch'] = sys.argv[6]
print(json.dumps(d))
" "$json_line" "$mode" "$rows" "$aot" "$(git_sha)" "$(git_branch)"
}

# ── Statistical output (criterion-style) ────────────────────────
# Generate criterion-style statistical report from collected results.
# Usage: criterion_report <insert_results_file> <copy_results_file> <rows>
criterion_report() {
    local insert_file="$1"
    local copy_file="$2"
    local rows="$3"

    python3 "$BENCH_DIR/lib/report.py" "$insert_file" "$copy_file" "$rows"
}
```

**Step 2: Verify the file sources correctly**

Run:
```bash
bash -c 'source bench/lib/helpers.sh && echo "PROJECT_ROOT=$PROJECT_ROOT" && echo "BENCH_DIR=$BENCH_DIR"'
```

Expected: Both paths printed correctly.

**Step 3: Commit**

```bash
git add bench/lib/helpers.sh
git commit -m "bench: add shared benchmark helpers with build/docker/stats functions"
```

---

### Task 3: Create bench/lib/report.py - criterion-style statistical report

This replaces the inline Python in `bench.sh` with a standalone module that produces criterion-style output with confidence intervals, standard deviation, and throughput.

**Files:**
- Create: `bench/lib/report.py`

**Step 1: Write report.py**

```python
#!/usr/bin/env python3
"""Criterion-style statistical report for benchmark results.

Usage:
    python3 report.py <insert_results.jsonl> <copy_results.jsonl> <rows>

Produces formatted output with mean +/- std dev, confidence intervals,
throughput in rows/s and MB/s, and speedup ratios.
"""

import json
import math
import sys


def load_results(path: str) -> list[dict]:
    results = []
    try:
        for line in open(path):
            line = line.strip()
            if line:
                results.append(json.loads(line))
    except FileNotFoundError:
        pass
    return results


def stats(values: list[float]) -> dict:
    """Compute mean, std dev, min, max, and 95% confidence interval."""
    n = len(values)
    if n == 0:
        return {"mean": 0, "std": 0, "min": 0, "max": 0, "ci_lo": 0, "ci_hi": 0, "n": 0}
    mean = sum(values) / n
    if n > 1:
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        std = math.sqrt(variance)
        # 95% CI using t-distribution approximation (t ~ 2.0 for small n)
        t_val = {2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365}.get(n, 1.96)
        margin = t_val * std / math.sqrt(n)
    else:
        std = 0
        margin = 0
    return {
        "mean": mean,
        "std": std,
        "min": min(values),
        "max": max(values),
        "ci_lo": mean - margin,
        "ci_hi": mean + margin,
        "n": n,
    }


def fmt_ci(s: dict, unit: str = "s") -> str:
    """Format as criterion-style: [lo mean hi]"""
    if s["n"] == 0:
        return "no data"
    if unit == "ms":
        return f'[{s["ci_lo"]:.1f} {s["mean"]:.1f} {s["ci_hi"]:.1f}] ms'
    return f'[{s["ci_lo"]:.4f} {s["mean"]:.4f} {s["ci_hi"]:.4f}] {unit}'


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <insert_file> <copy_file> <rows>", file=sys.stderr)
        sys.exit(1)

    insert_results = load_results(sys.argv[1])
    copy_results = load_results(sys.argv[2])
    rows = int(sys.argv[3])

    if not insert_results and not copy_results:
        print("  No results collected")
        sys.exit(1)

    ref = insert_results[0] if insert_results else copy_results[0]
    bytes_read = ref.get("bytes_read", 0)
    avg_row_bytes = bytes_read // rows if rows > 0 else 0

    # ── Header ────────────────────────────────────────────────────
    print(f"  Dataset:     {rows:,} rows, {bytes_read / 1048576:.2f} MB ({avg_row_bytes} B/row)")
    print(f"  Samples:     {len(insert_results)} INSERT, {len(copy_results)} COPY")
    print()

    # ── Criterion-style per-mode output ───────────────────────────
    for label, results in [("INSERT", insert_results), ("COPY", copy_results)]:
        if not results:
            continue
        durations = [r["duration_secs"] for r in results]
        s = stats(durations)
        rps_vals = [rows / d for d in durations if d > 0]
        rps = stats(rps_vals)
        mbps_vals = [bytes_read / d / 1048576 for d in durations if d > 0]
        mbps = stats(mbps_vals)

        print(f"  connector-postgres/{label.lower()}/{rows}")
        print(f"                        time:   {fmt_ci(s)}")
        print(f"                        thrpt:  [{rps['ci_lo']:,.0f} {rps['mean']:,.0f} {rps['ci_hi']:,.0f}] rows/s")
        print(f"                                [{mbps['ci_lo']:.2f} {mbps['mean']:.2f} {mbps['ci_hi']:.2f}] MB/s")
        print()

    # ── Speedup comparison ────────────────────────────────────────
    if insert_results and copy_results:
        i_avg = stats([r["duration_secs"] for r in insert_results])["mean"]
        c_avg = stats([r["duration_secs"] for r in copy_results])["mean"]
        if c_avg > 0.001:
            pct = (c_avg - i_avg) / i_avg * 100
            print(f"  COPY vs INSERT:  {pct:+.1f}% ({i_avg/c_avg:.2f}x speedup)")
            print()

    # ── Detailed metrics table ────────────────────────────────────
    hdr = "  {:<22s}  {:>12s}  {:>12s}  {:>8s}"
    print(hdr.format("Metric (mean)", "INSERT", "COPY", "Speedup"))
    print(hdr.format("-" * 22, "-" * 12, "-" * 12, "-" * 8))

    metrics = [
        ("Total duration", "duration_secs", "s"),
        ("Dest duration", "dest_duration_secs", "s"),
        ("  Connect", "dest_connect_secs", "s"),
        ("  Flush", "dest_flush_secs", "s"),
        ("  Arrow decode", "dest_arrow_decode_secs", "s"),
        ("  Commit", "dest_commit_secs", "s"),
        ("  VM setup", "dest_vm_setup_secs", "s"),
        ("  Recv loop", "dest_recv_secs", "s"),
        ("  WASM overhead", "wasm_overhead_secs", "s"),
        ("Source duration", "source_duration_secs", "s"),
        ("  Connect", "source_connect_secs", "s"),
        ("  Query", "source_query_secs", "s"),
        ("  Fetch", "source_fetch_secs", "s"),
        ("  Arrow encode", "source_arrow_encode_secs", "s"),
        ("Source module load", "source_module_load_ms", "ms"),
        ("Dest module load", "dest_module_load_ms", "ms"),
    ]

    for label, key, unit in metrics:
        i_s = stats([r.get(key, 0) for r in insert_results]) if insert_results else stats([])
        c_s = stats([r.get(key, 0) for r in copy_results]) if copy_results else stats([])
        speedup = f'{i_s["mean"]/c_s["mean"]:.1f}x' if c_s["mean"] > 0.001 else "-"
        i_val = f'{i_s["mean"]:.4f}{unit}' if i_s["n"] > 0 else "-"
        c_val = f'{c_s["mean"]:.4f}{unit}' if c_s["n"] > 0 else "-"
        print(hdr.format(label, i_val, c_val, speedup))

    # ── Throughput summary ────────────────────────────────────────
    print()
    if insert_results:
        i_rps = sum(rows / r["duration_secs"] for r in insert_results) / len(insert_results)
        i_mbps = sum(bytes_read / r["duration_secs"] / 1048576 for r in insert_results) / len(insert_results)
    else:
        i_rps = i_mbps = 0

    if copy_results:
        c_rps = sum(rows / r["duration_secs"] for r in copy_results) / len(copy_results)
        c_mbps = sum(bytes_read / r["duration_secs"] / 1048576 for r in copy_results) / len(copy_results)
    else:
        c_rps = c_mbps = 0

    rps_su = f"{c_rps/i_rps:.1f}x" if i_rps > 0 else "-"
    mbps_su = f"{c_mbps/i_mbps:.1f}x" if i_mbps > 0 else "-"
    print(hdr.format("Throughput (rows/s)", f"{i_rps:,.0f}", f"{c_rps:,.0f}", rps_su))
    print(hdr.format("Throughput (MB/s)", f"{i_mbps:.2f}", f"{c_mbps:.2f}", mbps_su))


if __name__ == "__main__":
    main()
```

**Step 2: Test with mock data**

Run:
```bash
echo '{"duration_secs":2.5,"records_read":10000,"bytes_read":520000,"dest_duration_secs":1.2,"source_duration_secs":1.3,"dest_connect_secs":0.01,"dest_flush_secs":0.3,"dest_arrow_decode_secs":0.2,"dest_commit_secs":0.1,"dest_vm_setup_secs":0.05,"dest_recv_secs":0.54,"wasm_overhead_secs":0.1,"source_connect_secs":0.01,"source_query_secs":0.05,"source_fetch_secs":0.8,"source_arrow_encode_secs":0.44,"source_module_load_ms":45,"dest_module_load_ms":52}' > /tmp/bench_test_insert.jsonl
echo '{"duration_secs":1.8,"records_read":10000,"bytes_read":520000,"dest_duration_secs":0.8,"source_duration_secs":1.0,"dest_connect_secs":0.01,"dest_flush_secs":0.15,"dest_arrow_decode_secs":0.15,"dest_commit_secs":0.08,"dest_vm_setup_secs":0.04,"dest_recv_secs":0.37,"wasm_overhead_secs":0.08,"source_connect_secs":0.01,"source_query_secs":0.04,"source_fetch_secs":0.6,"source_arrow_encode_secs":0.35,"source_module_load_ms":44,"dest_module_load_ms":50}' > /tmp/bench_test_copy.jsonl
python3 bench/lib/report.py /tmp/bench_test_insert.jsonl /tmp/bench_test_copy.jsonl 10000
rm /tmp/bench_test_insert.jsonl /tmp/bench_test_copy.jsonl
```

Expected: Formatted criterion-style report with dataset info, per-mode stats, speedup, and detailed metrics table.

**Step 3: Commit**

```bash
git add bench/lib/report.py
git commit -m "bench: add criterion-style statistical report generator"
```

---

## Phase 2: Move fixtures from tests/ to bench/

### Task 4: Move benchmark fixtures to bench/fixtures/

**Files:**
- Move: `tests/fixtures/sql/bench_seed.sql` -> `bench/fixtures/sql/bench_seed.sql`
- Move: `tests/fixtures/pipelines/bench_pg.yaml` -> `bench/fixtures/pipelines/bench_pg.yaml`
- Move: `tests/fixtures/pipelines/bench_pg_copy.yaml` -> `bench/fixtures/pipelines/bench_pg_copy.yaml`
- Move: `tests/fixtures/pipelines/bench_pg_lz4.yaml` -> `bench/fixtures/pipelines/bench_pg_lz4.yaml`
- Move: `tests/fixtures/pipelines/bench_pg_zstd.yaml` -> `bench/fixtures/pipelines/bench_pg_zstd.yaml`

**Step 1: Move the files**

Run:
```bash
mv tests/fixtures/sql/bench_seed.sql bench/fixtures/sql/bench_seed.sql
mv tests/fixtures/pipelines/bench_pg.yaml bench/fixtures/pipelines/bench_pg.yaml
mv tests/fixtures/pipelines/bench_pg_copy.yaml bench/fixtures/pipelines/bench_pg_copy.yaml
mv tests/fixtures/pipelines/bench_pg_lz4.yaml bench/fixtures/pipelines/bench_pg_lz4.yaml
mv tests/fixtures/pipelines/bench_pg_zstd.yaml bench/fixtures/pipelines/bench_pg_zstd.yaml
```

**Step 2: Verify no e2e tests reference moved files**

Run:
```bash
grep -r "bench_seed\|bench_pg" tests/e2e/ tests/lib/ tests/e2e.sh || echo "No references found (good)"
```

Expected: "No references found (good)" - e2e tests don't use bench fixtures.

**Step 3: Commit**

```bash
git add bench/fixtures/ tests/fixtures/
git commit -m "bench: move benchmark fixtures from tests/ to bench/fixtures/"
```

---

## Phase 3: Create connector benchmark scripts

### Task 5: Create bench/connectors/postgres/config.sh

Per-connector configuration file. Defines defaults for row counts, modes, seed SQL, and pipeline paths.

**Files:**
- Create: `bench/connectors/postgres/config.sh`

**Step 1: Write config.sh**

```bash
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
```

**Step 2: Commit**

```bash
git add bench/connectors/postgres/config.sh
git commit -m "bench: add postgres connector benchmark config"
```

---

### Task 6: Create bench/connectors/postgres/setup.sh

Handles infrastructure setup for postgres benchmarks: Docker PG start, data seeding, row count verification.

**Files:**
- Create: `bench/connectors/postgres/setup.sh`

**Step 1: Write setup.sh**

```bash
#!/usr/bin/env bash
# Postgres connector benchmark setup: start Docker PG and seed data.
# Expects: BENCH_ROWS, BENCH_SEED_SQL, BENCH_DIR (set by orchestrator or helpers)
set -euo pipefail

source "$(cd "$(dirname "$0")/../../lib" && pwd)/helpers.sh"
source "$(cd "$(dirname "$0")" && pwd)/config.sh"

BENCH_ROWS="${BENCH_ROWS:-$BENCH_DEFAULT_ROWS}"

section "Setting up postgres benchmark ($BENCH_ROWS rows)"

start_postgres

# Seed benchmark data
info "Seeding $BENCH_ROWS benchmark rows..."
docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
    -f - < "$BENCH_SEED_SQL"

# Verify row count
ACTUAL=$(pg_exec "SELECT COUNT(*) FROM public.bench_events")
if [ "$ACTUAL" != "$BENCH_ROWS" ]; then
    fail "Expected $BENCH_ROWS rows, got $ACTUAL"
fi
info "Verified: $ACTUAL rows seeded"
```

**Step 2: Commit**

```bash
git add bench/connectors/postgres/setup.sh
git commit -m "bench: add postgres connector setup (Docker PG + seed)"
```

---

### Task 7: Create bench/connectors/postgres/run.sh

Core benchmark runner for the postgres connector. Runs all configured modes, collects results, produces criterion-style output.

**Files:**
- Create: `bench/connectors/postgres/run.sh`

**Step 1: Write run.sh**

```bash
#!/usr/bin/env bash
# Postgres connector benchmark: runs configured modes (INSERT, COPY) and reports results.
# Expects: BENCH_ROWS, BENCH_ITERS (set by orchestrator)
set -euo pipefail

source "$(cd "$(dirname "$0")/../../lib" && pwd)/helpers.sh"
source "$(cd "$(dirname "$0")" && pwd)/config.sh"

BENCH_ROWS="${BENCH_ROWS:-$BENCH_DEFAULT_ROWS}"
BENCH_ITERS="${BENCH_ITERS:-$BENCH_DEFAULT_ITERS}"
BUILD_MODE="${BUILD_MODE:-release}"
BENCH_AOT="${BENCH_AOT:-true}"

if [ "$BENCH_AOT" = "true" ]; then
    export RAPIDBYTE_WASMTIME_AOT="1"
else
    export RAPIDBYTE_WASMTIME_AOT="0"
fi
info "Wasmtime AOT cache: ${BENCH_AOT}"

# Create temp files for collecting per-mode results
declare -A MODE_RESULTS
for mode in "${BENCH_MODES[@]}"; do
    MODE_RESULTS[$mode]=$(mktemp)
done

for mode in "${BENCH_MODES[@]}"; do
    pipeline="${BENCH_PIPELINES[$mode]}"
    if [ ! -f "$pipeline" ]; then
        warn "Pipeline not found for mode '$mode': $pipeline (skipping)"
        continue
    fi

    info "Running $BENCH_ITERS iterations ($mode mode, $BENCH_ROWS rows)..."

    for i in $(seq 1 "$BENCH_ITERS"); do
        # Clean destination between runs
        clean_dest_schema

        echo -n "  [$mode] Iteration $i/$BENCH_ITERS ... "

        json_line=$(run_pipeline_bench "$pipeline" "$BUILD_MODE")

        if [ -z "$json_line" ]; then
            echo "FAILED (no JSON output)"
            continue
        fi

        duration=$(echo "$json_line" | python3 -c "import sys,json; print(f'{json.load(sys.stdin)[\"duration_secs\"]:.3f}')")
        echo "done (${duration}s)"

        # Collect raw result for mode report
        echo "$json_line" >> "${MODE_RESULTS[$mode]}"

        # Persist enriched result for historical tracking
        enriched=$(enrich_result "$json_line" "$mode" "$BENCH_ROWS" "$BENCH_AOT")
        echo "$enriched" >> "$RESULTS_FILE"
    done
    echo ""
done

# ── Generate criterion-style report ──────────────────────────────
section "Benchmark Report"

# For the standard INSERT vs COPY comparison, use report.py
INSERT_FILE="${MODE_RESULTS[insert]:-/dev/null}"
COPY_FILE="${MODE_RESULTS[copy]:-/dev/null}"
criterion_report "$INSERT_FILE" "$COPY_FILE" "$BENCH_ROWS"

# Cleanup temp files
for mode in "${BENCH_MODES[@]}"; do
    rm -f "${MODE_RESULTS[$mode]}"
done
```

**Step 2: Commit**

```bash
git add bench/connectors/postgres/run.sh
git commit -m "bench: add postgres connector benchmark runner with criterion-style output"
```

---

### Task 8: Create bench/connectors/postgres/teardown.sh

Minimal teardown for postgres connector benchmarks.

**Files:**
- Create: `bench/connectors/postgres/teardown.sh`

**Step 1: Write teardown.sh**

```bash
#!/usr/bin/env bash
# Postgres connector benchmark teardown.
# Docker is stopped by the main orchestrator's EXIT trap, not here.
# This handles connector-specific cleanup only.
set -euo pipefail

source "$(cd "$(dirname "$0")/../../lib" && pwd)/helpers.sh"

# Clean state DB
rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
    sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
```

**Step 2: Commit**

```bash
git add bench/connectors/postgres/teardown.sh
git commit -m "bench: add postgres connector teardown"
```

---

## Phase 4: Create orchestrator scripts

### Task 9: Create bench/bench.sh - main orchestrator

The primary entry point. Auto-discovers connectors, handles build, delegates to connector-specific scripts.

**Files:**
- Create: `bench/bench.sh`

**Step 1: Write bench.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$BENCH_DIR/lib/helpers.sh"

setup_deterministic_env

# ── Parse arguments ──────────────────────────────────────────────
CONNECTOR=""
BENCH_ROWS=""
BENCH_ITERS=""
BUILD_MODE="release"
BENCH_AOT="true"
PROFILE="false"

usage() {
    cat <<EOF
Usage: bench.sh [CONNECTOR] [ROWS] [OPTIONS]

Arguments:
  CONNECTOR   Connector name (e.g. postgres). Omit to bench all connectors.
  ROWS        Number of rows to benchmark. Omit for connector default.

Options:
  --iters N         Number of iterations per mode (default: from connector config)
  --debug           Build in debug mode
  --aot / --no-aot  Enable/disable Wasmtime AOT cache (default: enabled)
  --profile         Run profiling iteration after benchmark
  -h, --help        Show this help

Examples:
  bench.sh                       # All connectors, default rows
  bench.sh postgres              # Postgres, default rows
  bench.sh postgres 50000        # Postgres, 50K rows
  bench.sh postgres 50000 --iters 5
EOF
    exit 0
}

# Parse positional args first, then flags
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --iters)    BENCH_ITERS="$2"; shift 2 ;;
        --debug)    BUILD_MODE="debug"; shift ;;
        --aot)      BENCH_AOT="true"; shift ;;
        --no-aot)   BENCH_AOT="false"; shift ;;
        --profile)  PROFILE="true"; shift ;;
        -h|--help)  usage ;;
        -*)         fail "Unknown option: $1" ;;
        *)          POSITIONAL+=("$1"); shift ;;
    esac
done

# Assign positional args
[ ${#POSITIONAL[@]} -ge 1 ] && CONNECTOR="${POSITIONAL[0]}"
[ ${#POSITIONAL[@]} -ge 2 ] && BENCH_ROWS="${POSITIONAL[1]}"

# ── Discover connectors ──────────────────────────────────────────
CONNECTORS=()
if [[ -n "$CONNECTOR" ]]; then
    if [[ ! -f "$BENCH_DIR/connectors/$CONNECTOR/config.sh" ]]; then
        fail "Unknown connector: $CONNECTOR (no config.sh found in bench/connectors/$CONNECTOR/)"
    fi
    CONNECTORS=("$CONNECTOR")
else
    for config_file in "$BENCH_DIR"/connectors/*/config.sh; do
        name="$(basename "$(dirname "$config_file")")"
        CONNECTORS+=("$name")
    done
fi

if [ ${#CONNECTORS[@]} -eq 0 ]; then
    fail "No connector benchmarks found in bench/connectors/"
fi

# ── Banner ───────────────────────────────────────────────────────
echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark"
cyan "  Connectors: ${CONNECTORS[*]}"
cyan "  Build: $BUILD_MODE | AOT: $BENCH_AOT"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Build ────────────────────────────────────────────────────────
build_host "$BUILD_MODE"
build_connectors "$BUILD_MODE"
stage_connectors "$BUILD_MODE"

# ── Cleanup trap ─────────────────────────────────────────────────
cleanup() {
    stop_postgres
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}
trap cleanup EXIT

# ── Run each connector ───────────────────────────────────────────
export BUILD_MODE BENCH_AOT

for connector in "${CONNECTORS[@]}"; do
    connector_dir="$BENCH_DIR/connectors/$connector"

    # Source connector config to get defaults
    source "$connector_dir/config.sh"

    # Apply overrides or use connector defaults
    export BENCH_ROWS="${BENCH_ROWS:-$BENCH_DEFAULT_ROWS}"
    export BENCH_ITERS="${BENCH_ITERS:-$BENCH_DEFAULT_ITERS}"

    # Setup infrastructure
    bash "$connector_dir/setup.sh"

    # Run benchmark
    bash "$connector_dir/run.sh"

    # Teardown connector-specific state
    bash "$connector_dir/teardown.sh"
done

# ── Optional profiling ───────────────────────────────────────────
if [ "$PROFILE" = "true" ]; then
    if ! command -v samply &> /dev/null; then
        fail "samply not found. Install with: cargo install samply"
    fi

    info "Running profiling iteration with samply..."
    clean_dest_schema

    PROFILE_DIR="$PROJECT_ROOT/target/profiles"
    mkdir -p "$PROFILE_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    PROFILE_FILE="$PROFILE_DIR/profile_${BENCH_ROWS}rows_insert_${TIMESTAMP}.json"

    info "Rebuilding with debug symbols for profiling..."
    CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed \
        cargo build --release --quiet

    samply record \
        --save-only \
        -o "$PROFILE_FILE" \
        -- "$PROJECT_ROOT/target/release/rapidbyte" run \
        "$BENCH_DIR/fixtures/pipelines/bench_pg.yaml" \
        --log-level warn 2>&1

    info "Profile saved to: $PROFILE_FILE"
    info "View with: samply load $PROFILE_FILE"
fi

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Benchmark complete"
cyan "═══════════════════════════════════════════════"
```

**Step 2: Make executable**

Run:
```bash
chmod +x bench/bench.sh bench/connectors/postgres/setup.sh bench/connectors/postgres/run.sh bench/connectors/postgres/teardown.sh
```

**Step 3: Commit**

```bash
git add bench/bench.sh
git commit -m "bench: add main orchestrator with auto-discovery and profiling"
```

---

### Task 10: Create bench/compare.sh - git ref comparison

Benchmarks two git refs and generates a comparison report. Follows the same connector auto-discovery pattern as bench.sh.

**Files:**
- Create: `bench/compare.sh`

**Step 1: Write compare.sh**

```bash
#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$BENCH_DIR/lib/helpers.sh"

setup_deterministic_env

# ── Parse arguments ──────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: compare.sh <ref1> <ref2> [OPTIONS]

Compare benchmark results between two git refs (branches, tags, or SHAs).

Arguments:
  ref1        First git ref (e.g. main, HEAD~1, v0.1.0)
  ref2        Second git ref

Options:
  --connector NAME    Benchmark only this connector (default: all)
  --rows N            Number of rows (default: from connector config)
  --iters N           Iterations per mode (default: from connector config)
  --aot / --no-aot    Wasmtime AOT cache (default: enabled)
  -h, --help          Show this help

Examples:
  compare.sh main my-feature
  compare.sh HEAD~1 HEAD --connector postgres --rows 50000
  compare.sh v0.1.0 v0.2.0 --no-aot
EOF
    exit 0
}

REF1=""
REF2=""
CONNECTOR=""
BENCH_ROWS=""
BENCH_ITERS=""
BENCH_AOT="true"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connector) CONNECTOR="$2"; shift 2 ;;
        --rows)      BENCH_ROWS="$2"; shift 2 ;;
        --iters)     BENCH_ITERS="$2"; shift 2 ;;
        --aot)       BENCH_AOT="true"; shift ;;
        --no-aot)    BENCH_AOT="false"; shift ;;
        -h|--help)   usage ;;
        -*)          fail "Unknown option: $1" ;;
        *)           POSITIONAL+=("$1"); shift ;;
    esac
done

[ ${#POSITIONAL[@]} -ge 1 ] && REF1="${POSITIONAL[0]}"
[ ${#POSITIONAL[@]} -ge 2 ] && REF2="${POSITIONAL[1]}"

if [[ -z "$REF1" || -z "$REF2" ]]; then
    fail "Usage: compare.sh <ref1> <ref2> [OPTIONS]"
fi

# ── Validate refs and working tree ───────────────────────────────
SHA1=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF1" 2>/dev/null) || fail "Invalid ref: $REF1"
SHA2=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF2" 2>/dev/null) || fail "Invalid ref: $REF2"

if [ "$SHA1" = "$SHA2" ]; then
    fail "Both refs resolve to the same commit: $SHA1"
fi

if ! git -C "$PROJECT_ROOT" diff --quiet 2>/dev/null || \
   ! git -C "$PROJECT_ROOT" diff --cached --quiet 2>/dev/null; then
    fail "Working tree is dirty. Commit or stash changes before comparing."
fi

# Save original ref for restoration
ORIGINAL_REF=$(git -C "$PROJECT_ROOT" symbolic-ref --short HEAD 2>/dev/null || \
               git -C "$PROJECT_ROOT" rev-parse HEAD)

# ── Discover connectors ──────────────────────────────────────────
CONNECTORS=()
if [[ -n "$CONNECTOR" ]]; then
    [[ -f "$BENCH_DIR/connectors/$CONNECTOR/config.sh" ]] || fail "Unknown connector: $CONNECTOR"
    CONNECTORS=("$CONNECTOR")
else
    for config_file in "$BENCH_DIR"/connectors/*/config.sh; do
        name="$(basename "$(dirname "$config_file")")"
        CONNECTORS+=("$name")
    done
fi

# ── Banner ───────────────────────────────────────────────────────
echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark Comparison"
cyan "  Ref 1: $REF1 ($SHA1)"
cyan "  Ref 2: $REF2 ($SHA2)"
cyan "  Connectors: ${CONNECTORS[*]}"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Cleanup trap ─────────────────────────────────────────────────
cleanup() {
    info "Restoring original ref: $ORIGINAL_REF"
    git -C "$PROJECT_ROOT" checkout "$ORIGINAL_REF" --quiet 2>/dev/null || true
    stop_postgres
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}
trap cleanup EXIT

# Start shared infrastructure once
start_postgres

# ── Run benchmarks for each ref ──────────────────────────────────
for ref in "$REF1" "$REF2"; do
    sha=$(git -C "$PROJECT_ROOT" rev-parse --short "$ref")
    section "Benchmarking ref: $ref ($sha)"

    git -C "$PROJECT_ROOT" checkout "$ref" --quiet

    # Build from this ref
    build_host release
    build_connectors release
    stage_connectors release

    for connector in "${CONNECTORS[@]}"; do
        # Source config (from current ref — config.sh may differ between refs)
        # Fall back to known defaults if config.sh doesn't exist in this ref
        if [[ -f "$BENCH_DIR/connectors/$connector/config.sh" ]]; then
            source "$BENCH_DIR/connectors/$connector/config.sh"
        fi

        export BENCH_ROWS="${BENCH_ROWS:-$BENCH_DEFAULT_ROWS}"
        export BENCH_ITERS="${BENCH_ITERS:-$BENCH_DEFAULT_ITERS}"
        export BENCH_AOT BUILD_MODE="release"

        # Seed data
        info "Seeding $BENCH_ROWS benchmark rows..."
        docker compose -f "$COMPOSE_FILE" exec -T postgres \
            psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
            -f - < "$BENCH_SEED_SQL"

        # Run benchmark (inline to share env)
        bash "$BENCH_DIR/connectors/$connector/run.sh"

        # Clean for next ref
        bash "$BENCH_DIR/connectors/$connector/teardown.sh"
    done
done

# ── Generate comparison ──────────────────────────────────────────
section "Comparison Report"
python3 "$BENCH_DIR/analyze.py" --sha "$SHA1" "$SHA2"
```

**Step 2: Make executable**

Run:
```bash
chmod +x bench/compare.sh
```

**Step 3: Commit**

```bash
git add bench/compare.sh
git commit -m "bench: add git ref comparison script"
```

---

### Task 11: Move and update bench/analyze.py

Move `tests/bench_compare.py` to `bench/analyze.py` with no functional changes (it reads from `target/bench_results/results.jsonl` which is the same path).

**Files:**
- Move: `tests/bench_compare.py` -> `bench/analyze.py`

**Step 1: Move the file**

Run:
```bash
mv tests/bench_compare.py bench/analyze.py
```

**Step 2: Verify it still works**

Run:
```bash
python3 bench/analyze.py --help 2>&1 || echo "Script loaded (may need results file)"
```

Expected: Usage info or graceful error about missing results file.

**Step 3: Commit**

```bash
git add bench/analyze.py tests/bench_compare.py
git commit -m "bench: move results analyzer from tests/ to bench/"
```

---

## Phase 5: Add Criterion micro-benchmarks

### Task 12: Add criterion dependency to workspace

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/rapidbyte-core/Cargo.toml`

**Step 1: Add criterion to workspace dependencies**

In `Cargo.toml` (root), add to `[workspace.dependencies]`:

```toml
criterion = { version = "0.5", features = ["html_reports"] }
```

**Step 2: Add criterion dev-dependency and bench target to rapidbyte-core**

In `crates/rapidbyte-core/Cargo.toml`, add:

```toml
[dev-dependencies]
tempfile = "3"
criterion = { workspace = true }

[[bench]]
name = "arrow_codec"
harness = false

[[bench]]
name = "state_backend"
harness = false
```

Note: Replace the existing `[dev-dependencies]` section (which only has `tempfile = "3"`).

**Step 3: Verify workspace resolves**

Run:
```bash
cargo check --workspace 2>&1 | tail -5
```

Expected: Successful check (or only warnings, no errors).

**Step 4: Commit**

```bash
git add Cargo.toml crates/rapidbyte-core/Cargo.toml
git commit -m "bench: add criterion dependency for micro-benchmarks"
```

---

### Task 13: Create Arrow codec criterion benchmark

Benchmarks Arrow IPC encoding and decoding at various batch sizes. Tests the hot path in the pipeline where source encodes to Arrow and destination decodes from Arrow.

**Files:**
- Create: `crates/rapidbyte-core/benches/arrow_codec.rs`

**Step 1: Write the benchmark**

```rust
//! Criterion benchmarks for Arrow IPC encoding and decoding.
//!
//! These measure the hot path: source -> Arrow IPC encode -> channel -> Arrow IPC decode -> dest.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("user_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let ids: Vec<i32> = (0..rows as i32).collect();
    let events: Vec<&str> = (0..rows)
        .map(|i| match i % 5 {
            0 => "click",
            1 => "purchase",
            2 => "view",
            3 => "signup",
            _ => "logout",
        })
        .collect();
    let user_ids: Vec<i64> = (0..rows).map(|i| (i % 1000) as i64 + 1).collect();
    let amounts: Vec<f64> = (0..rows).map(|i| i as f64 * 1.5).collect();
    let actives: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let timestamps: Vec<i64> = (0..rows)
        .map(|i| 1_700_000_000_000_000 + i as i64 * 1_000_000)
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(events)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(BooleanArray::from(actives)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
        ],
    )
    .unwrap()
}

fn encode_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn bench_arrow_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/encode");

    for rows in [100, 1_000, 10_000, 100_000] {
        let batch = create_test_batch(rows);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| encode_batch(batch));
        });
    }
    group.finish();
}

fn bench_arrow_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/decode");

    for rows in [100, 1_000, 10_000, 100_000] {
        let batch = create_test_batch(rows);
        let encoded = encode_batch(&batch);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
                    let _batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_arrow_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/roundtrip");

    for rows in [1_000, 10_000] {
        let batch = create_test_batch(rows);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| {
                let encoded = encode_batch(batch);
                let reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
                let _batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_arrow_encode, bench_arrow_decode, bench_arrow_roundtrip);
criterion_main!(benches);
```

**Step 2: Run the benchmark**

Run:
```bash
cargo bench --bench arrow_codec 2>&1 | head -30
```

Expected: Criterion output with timing for arrow/encode, arrow/decode, arrow/roundtrip at various row counts.

**Step 3: Commit**

```bash
git add crates/rapidbyte-core/benches/arrow_codec.rs
git commit -m "bench: add criterion Arrow IPC codec benchmarks"
```

---

### Task 14: Create state backend criterion benchmark

Benchmarks SQLite state backend operations (run tracking, cursor persistence, checkpoint writes). These are on the critical path for incremental pipelines.

**Files:**
- Create: `crates/rapidbyte-core/benches/state_backend.rs`

**Step 1: Write the benchmark**

```rust
//! Criterion benchmarks for the SQLite state backend.
//!
//! Measures run tracking, cursor persistence, and checkpoint operations
//! which are on the critical path for incremental pipelines.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rapidbyte_core::state::backend::{PipelineId, RunStatus, StateBackend, StreamName};
use rapidbyte_core::state::sqlite::SqliteStateBackend;
use tempfile::NamedTempFile;

fn create_backend() -> (SqliteStateBackend, NamedTempFile) {
    let tmp = NamedTempFile::new().unwrap();
    let backend = SqliteStateBackend::new(tmp.path()).unwrap();
    (backend, tmp)
}

fn bench_start_and_finish_run(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/run_lifecycle");

    group.bench_function("start_and_finish", |b| {
        let (backend, _tmp) = create_backend();
        let pipeline = PipelineId("bench_pipeline".to_string());
        let stream = StreamName("bench_stream".to_string());

        b.iter(|| {
            let run_id = backend.start_run(&pipeline, &stream).unwrap();
            backend
                .finish_run(
                    &pipeline,
                    &stream,
                    run_id,
                    RunStatus::Completed,
                    0.1,
                    100,
                    100,
                    5000,
                    5000,
                )
                .unwrap();
        });
    });
    group.finish();
}

fn bench_cursor_persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/cursor");

    for n in [1, 10, 50] {
        group.bench_with_input(BenchmarkId::new("persist", n), &n, |b, &n| {
            let (backend, _tmp) = create_backend();
            let pipeline = PipelineId("bench_pipeline".to_string());

            b.iter(|| {
                for i in 0..n {
                    let stream = StreamName(format!("stream_{}", i));
                    backend
                        .persist_cursor(&pipeline, &stream, "id", &format!("{}", i * 1000))
                        .unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_cursor_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/cursor_read");

    group.bench_function("get_cursor", |b| {
        let (backend, _tmp) = create_backend();
        let pipeline = PipelineId("bench_pipeline".to_string());
        let stream = StreamName("bench_stream".to_string());
        backend
            .persist_cursor(&pipeline, &stream, "id", "12345")
            .unwrap();

        b.iter(|| {
            let _cursor = backend.get_cursor(&pipeline, &stream).unwrap();
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_start_and_finish_run,
    bench_cursor_persistence,
    bench_cursor_read
);
criterion_main!(benches);
```

**Step 2: Verify the state backend API matches**

Before running, confirm that the `StateBackend` trait and `SqliteStateBackend` have the methods used above. The benchmark uses:
- `SqliteStateBackend::new(path)`
- `backend.start_run(&pipeline_id, &stream_name)`
- `backend.finish_run(&pipeline_id, &stream_name, run_id, status, duration, reads, writes, bytes_read, bytes_written)`
- `backend.persist_cursor(&pipeline_id, &stream_name, field, value)`
- `backend.get_cursor(&pipeline_id, &stream_name)`

Check the actual trait in `crates/rapidbyte-core/src/state/backend.rs` and adjust method signatures as needed. The exact parameters may differ — match them to the real API.

**Step 3: Run the benchmark**

Run:
```bash
cargo bench --bench state_backend 2>&1 | head -20
```

Expected: Criterion output with timing for state operations.

**Step 4: Commit**

```bash
git add crates/rapidbyte-core/benches/state_backend.rs
git commit -m "bench: add criterion state backend benchmarks"
```

---

## Phase 6: Update justfile and cleanup

### Task 15: Update justfile - replace 6 targets with 2

**Files:**
- Modify: `justfile`

**Step 1: Remove old bench targets and add new ones**

Remove these targets from the justfile:
- `bench-connector-postgres` (line 42-43)
- `bench-profile` (line 53-54)
- `bench-connector-postgres-profile` (line 57-58)
- `bench-matrix` (line 61-62)
- `bench-compare` (line 65-66)
- `bench-results` (line 69-70)

Add these new targets:

```just
# Run benchmarks: bench.sh [CONNECTOR] [ROWS] [--iters N] [--profile]
bench *args="":
    ./bench/bench.sh {{args}}

# Compare benchmarks between two git refs
bench-compare ref1 ref2 *args="":
    ./bench/compare.sh {{ref1}} {{ref2}} {{args}}
```

The final justfile should have these targets (in order):
1. `default` - list targets
2. `build` - cargo build
3. `build-no-sccache` - build without sccache
4. `build-connectors` - build WASM connectors
5. `test` - cargo test
6. `check` - cargo check
7. `check-no-sccache` - check without sccache
8. `fmt` - cargo fmt
9. `lint` - cargo clippy
10. `e2e` - E2E tests
11. `e2e-scenario` - single E2E scenario
12. `bench` - benchmark (NEW)
13. `bench-compare` - compare refs (NEW)
14. `scaffold` - connector scaffold
15. `clean` - cargo clean
16. `sccache-stats` - sccache stats

**Step 2: Verify justfile parses**

Run:
```bash
just --list
```

Expected: All targets listed including new `bench` and `bench-compare`.

**Step 3: Commit**

```bash
git add justfile
git commit -m "bench: simplify justfile from 6 bench targets to 2"
```

---

### Task 16: Remove old bench scripts from tests/

**Files:**
- Delete: `tests/bench.sh`
- Delete: `tests/bench_profile.sh`
- Delete: `tests/bench_matrix.sh`
- Delete: `tests/bench_compare.sh`

**Step 1: Remove the old scripts**

Run:
```bash
rm tests/bench.sh tests/bench_profile.sh tests/bench_matrix.sh tests/bench_compare.sh
```

**Step 2: Verify tests/ is clean (only e2e and lib remain)**

Run:
```bash
ls tests/
```

Expected:
```
e2e/
e2e.sh
fixtures/
lib/
```

No `bench*.sh` or `bench*.py` files remain.

**Step 3: Commit**

```bash
git add tests/
git commit -m "bench: remove old benchmark scripts from tests/"
```

---

### Task 17: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Update the Project Structure section**

Add `bench/` to the directory listing:

```markdown
bench/
  bench.sh             # Benchmark orchestrator
  compare.sh           # Compare benchmarks between git refs
  lib/                 # Shared helpers, criterion-style report
  connectors/postgres/ # Postgres connector benchmarks
  fixtures/            # Bench SQL seeds, pipeline YAMLs
  analyze.py           # Historical results viewer
```

**Step 2: Update the Commands section**

Replace the old bench commands:

```bash
just bench                          # All connectors, default rows
just bench postgres                 # Postgres connector, default rows
just bench postgres 50000           # Postgres, 50K rows
just bench postgres 50000 --iters 5 # Postgres, 50K rows, 5 iterations
just bench-compare main feature     # Compare benchmarks between refs
cargo bench                         # Criterion micro-benchmarks (Arrow, state)
```

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with new benchmark framework"
```

---

## Phase 7: Verify and final commit

### Task 18: Verify criterion benchmarks compile

Run:
```bash
cargo bench --bench arrow_codec --no-run 2>&1 | tail -5
cargo bench --bench state_backend --no-run 2>&1 | tail -5
```

Expected: Both compile successfully (may show warnings but no errors).

### Task 19: Verify bench.sh --help works

Run:
```bash
./bench/bench.sh --help
```

Expected: Usage text showing all options.

### Task 20: Verify compare.sh --help works

Run:
```bash
./bench/compare.sh --help
```

Expected: Usage text showing all options.

### Task 21: Run a quick benchmark (if Docker available)

Run:
```bash
just bench postgres 1000 --iters 1
```

Expected: Benchmark completes with criterion-style output showing INSERT and COPY results.

### Task 22: Squash into clean commit series (optional)

If desired, the individual phase commits can remain for clean history, or squash into logical groups:

1. `bench: create benchmark framework structure and helpers`
2. `bench: add postgres connector benchmarks`
3. `bench: add criterion micro-benchmarks for Arrow and state`
4. `bench: simplify justfile and remove old scripts`
5. `docs: update CLAUDE.md with new benchmark framework`

---

## Summary of changes

### New files created
| File | Purpose |
|------|---------|
| `bench/bench.sh` | Main orchestrator (replaces 4 scripts) |
| `bench/compare.sh` | Git ref comparison |
| `bench/analyze.py` | Moved from `tests/bench_compare.py` |
| `bench/lib/helpers.sh` | Shared helpers (sources test helpers) |
| `bench/lib/report.py` | Criterion-style statistical report |
| `bench/connectors/postgres/config.sh` | Connector defaults |
| `bench/connectors/postgres/setup.sh` | Docker PG + seed |
| `bench/connectors/postgres/run.sh` | Benchmark runner |
| `bench/connectors/postgres/teardown.sh` | Cleanup |
| `bench/fixtures/sql/bench_seed.sql` | Moved from tests/ |
| `bench/fixtures/pipelines/bench_pg*.yaml` | Moved from tests/ |
| `crates/rapidbyte-core/benches/arrow_codec.rs` | Criterion Arrow IPC bench |
| `crates/rapidbyte-core/benches/state_backend.rs` | Criterion state backend bench |

### Files removed
| File | Replaced by |
|------|-------------|
| `tests/bench.sh` | `bench/bench.sh` + `bench/connectors/postgres/run.sh` |
| `tests/bench_profile.sh` | `bench/bench.sh --profile` |
| `tests/bench_matrix.sh` | `bench/bench.sh postgres 1000` then `bench/bench.sh postgres 10000` etc. |
| `tests/bench_compare.sh` | `bench/compare.sh` |
| `tests/bench_compare.py` | `bench/analyze.py` |

### Justfile changes
| Before (6 targets) | After (2 targets) |
|-------|-------|
| `bench-connector-postgres rows` | `bench [connector] [rows]` |
| `bench-profile rows mode` | `bench [connector] [rows] --profile` |
| `bench-connector-postgres-profile rows` | `bench postgres [rows] --profile` |
| `bench-matrix *args` | Run `bench` multiple times with different rows |
| `bench-compare ref1 ref2 *args` | `bench-compare ref1 ref2 [--connector x] [--rows n]` |
| `bench-results *args` | `python3 bench/analyze.py *args` |

### Adding a new connector benchmark
1. Create `bench/connectors/<name>/config.sh` (set BENCH_DEFAULT_ROWS, BENCH_MODES, etc.)
2. Create `bench/connectors/<name>/setup.sh` (infrastructure setup)
3. Create `bench/connectors/<name>/run.sh` (benchmark runner)
4. Create `bench/connectors/<name>/teardown.sh` (cleanup)
5. Add fixtures to `bench/fixtures/`
6. Run `just bench <name>` - auto-discovered by orchestrator
