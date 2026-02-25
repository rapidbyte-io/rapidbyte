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

        json_line=$(run_pipeline_bench "$pipeline" "$BUILD_MODE") || true

        if [ -z "$json_line" ]; then
            echo "FAILED (see error above)"
            continue
        fi

        summary=$(echo "$json_line" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('|'.join([
    f'{d[\"duration_secs\"]:.3f}',
    f'{d.get(\"cpu_cores_mean\", 0.0):.2f}',
    f'{d.get(\"cpu_cores_max\", 0.0):.2f}',
    f'{d.get(\"cpu_total_util_pct_mean\", 0.0):.1f}',
    f'{d.get(\"cpu_total_util_pct_max\", 0.0):.1f}',
    f'{d.get(\"mem_rss_mb_mean\", 0.0):.1f}',
    f'{d.get(\"mem_rss_mb_max\", 0.0):.1f}',
]))
")
        IFS='|' read -r duration cpu_cores_mean cpu_cores_max cpu_util_mean cpu_util_max mem_mean_mb mem_max_mb <<< "$summary"
        echo "done (${duration}s, CPU ${cpu_cores_mean}/${cpu_cores_max} cores avg/max, util ${cpu_util_mean}%/${cpu_util_max}% total, RSS ${mem_mean_mb}/${mem_max_mb} MB avg/max)"

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

# Build mode:file argument pairs for report.py
REPORT_ARGS=()
for mode in "${BENCH_MODES[@]}"; do
    result_file="${MODE_RESULTS[$mode]}"
    if [ -s "$result_file" ]; then
        REPORT_ARGS+=("$mode:$result_file")
    fi
done

if [ ${#REPORT_ARGS[@]} -gt 0 ]; then
    python3 "$BENCH_DIR/lib/report.py" "$BENCH_ROWS" "${BENCH_PROFILE:-unknown}" "${REPORT_ARGS[@]}"
else
    echo "  No results collected"
fi

# Cleanup temp files
for mode in "${BENCH_MODES[@]}"; do
    rm -f "${MODE_RESULTS[$mode]}"
done
