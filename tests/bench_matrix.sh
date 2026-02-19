#!/usr/bin/env bash
set -euo pipefail

# Matrix benchmark: sweeps across row counts
#
# Usage: ./tests/bench_matrix.sh [--rows "1000 10000 100000"] [--iters 3]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ROW_COUNTS="${BENCH_MATRIX_ROWS:-1000 10000 100000}"
ITERS="${BENCH_MATRIX_ITERS:-3}"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
cyan()  { echo -e "${CYAN}$*${NC}"; }

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)   ROW_COUNTS="$2"; shift 2 ;;
        --iters)  ITERS="$2"; shift 2 ;;
        --help)   echo "Usage: $0 [--rows \"1000 10000\"] [--iters N]"; exit 0 ;;
        *)        echo "Unknown: $1"; exit 1 ;;
    esac
done

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark Matrix"
cyan "  Rows: $ROW_COUNTS"
cyan "  Iterations: $ITERS per scenario"
cyan "═══════════════════════════════════════════════"
echo ""

for ROW_COUNT in $ROW_COUNTS; do
    echo ""
    cyan "--- $ROW_COUNT rows ---"
    "$PROJECT_ROOT/tests/bench.sh" --rows "$ROW_COUNT" --iters "$ITERS"
    echo ""
done

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Matrix complete"
cyan "═══════════════════════════════════════════════"
echo ""
info "Run 'just bench-compare' to see results across runs."
