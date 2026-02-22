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
