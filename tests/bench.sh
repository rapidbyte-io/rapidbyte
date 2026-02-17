#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"

# Source WasmEdge environment
if [ -f "$HOME/.wasmedge/env" ]; then
    set +u; source "$HOME/.wasmedge/env"; set -u
fi

# Defaults
BENCH_ROWS="${BENCH_ROWS:-1000}"
BENCH_ITERS="${BENCH_ITERS:-3}"
BUILD_MODE="${BUILD_MODE:-release}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
cyan()  { echo -e "${CYAN}$*${NC}"; }

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
    rm -f /tmp/rapidbyte_bench_state.db
}

usage() {
    echo "Usage: $0 [--rows N] [--iters N] [--debug]"
    echo ""
    echo "Options:"
    echo "  --rows N    Number of rows to benchmark (default: 1000)"
    echo "  --iters N   Number of iterations (default: 3)"
    echo "  --debug     Use debug builds instead of release"
    exit 0
}

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)  BENCH_ROWS="$2"; shift 2 ;;
        --iters) BENCH_ITERS="$2"; shift 2 ;;
        --debug) BUILD_MODE="debug"; shift ;;
        --help)  usage ;;
        *)       fail "Unknown option: $1" ;;
    esac
done

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark"
cyan "  Rows: $BENCH_ROWS | Iterations: $BENCH_ITERS | Build: $BUILD_MODE"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Build ─────────────────────────────────────────────────────────
BUILD_FLAG=""
TARGET_DIR="debug"
if [ "$BUILD_MODE" = "release" ]; then
    BUILD_FLAG="--release"
    TARGET_DIR="release"
fi

info "Building host binary ($BUILD_MODE)..."
(cd "$PROJECT_ROOT" && cargo build $BUILD_FLAG 2>&1 | tail -1)

info "Building source-postgres connector ($BUILD_MODE)..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build $BUILD_FLAG 2>&1 | tail -1)

info "Building dest-postgres connector ($BUILD_MODE)..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build $BUILD_FLAG 2>&1 | tail -1)

# Stage .wasm files
mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip1/$TARGET_DIR/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip1/$TARGET_DIR/dest_postgres.wasm" "$CONNECTOR_DIR/"
info "Connectors staged in $CONNECTOR_DIR"

# ── Start PostgreSQL ──────────────────────────────────────────────
info "Starting PostgreSQL via Docker Compose..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" up -d
trap cleanup EXIT

info "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "SELECT 1" > /dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "PostgreSQL did not become ready in time"
    fi
    sleep 1
done
sleep 2
info "PostgreSQL is ready"

# ── Seed benchmark data ──────────────────────────────────────────
info "Seeding $BENCH_ROWS rows into bench_events..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
    < "$PROJECT_ROOT/tests/fixtures/sql/bench_seed.sql" > /dev/null

ROW_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM public.bench_events")
info "Seeded $ROW_COUNT rows"

# ── Run benchmark iterations ─────────────────────────────────────
export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
RESULTS_FILE=$(mktemp)

info "Running $BENCH_ITERS iterations..."
echo ""

for i in $(seq 1 "$BENCH_ITERS"); do
    # Clean destination between runs
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
    rm -f /tmp/rapidbyte_bench_state.db

    echo -n "  Iteration $i/$BENCH_ITERS ... "

    OUTPUT=$("$PROJECT_ROOT/target/$TARGET_DIR/rapidbyte" run \
        "$PROJECT_ROOT/tests/fixtures/pipelines/bench_pg.yaml" \
        --log-level warn 2>&1)

    # Extract JSON line
    JSON_LINE=$(echo "$OUTPUT" | grep "@@BENCH_JSON@@" | sed 's/@@BENCH_JSON@@//')

    if [ -z "$JSON_LINE" ]; then
        echo "FAILED (no JSON output)"
        continue
    fi

    DURATION=$(echo "$JSON_LINE" | python3 -c "import sys,json; print(f'{json.load(sys.stdin)[\"duration_secs\"]:.2f}')")
    echo "done (${DURATION}s)"
    echo "$JSON_LINE" >> "$RESULTS_FILE"
done

echo ""

# ── Generate report ──────────────────────────────────────────────
info "Benchmark Report:"
echo ""

python3 -c "
import json, sys

results = []
for line in open('$RESULTS_FILE'):
    line = line.strip()
    if line:
        results.append(json.loads(line))

if not results:
    print('  No results collected')
    sys.exit(1)

def stats(key):
    vals = [r[key] for r in results]
    return min(vals), sum(vals)/len(vals), max(vals)

n = len(results)
rows = results[0].get('records_read', 0)

print(f'  Dataset:     {rows} rows')
print(f'  Iterations:  {n}')
print()

fmt = '  {:<25s}  {:>8s}  {:>8s}  {:>8s}'
print(fmt.format('Metric', 'Min', 'Avg', 'Max'))
print(fmt.format('-' * 25, '-' * 8, '-' * 8, '-' * 8))

for label, key, unit in [
    ('Total duration', 'duration_secs', 's'),
    ('Source duration', 'source_duration_secs', 's'),
    ('Dest duration', 'dest_duration_secs', 's'),
    ('Source module load', 'source_module_load_ms', 'ms'),
    ('Dest module load', 'dest_module_load_ms', 'ms'),
]:
    lo, avg, hi = stats(key)
    print(fmt.format(label, f'{lo:.2f}{unit}', f'{avg:.2f}{unit}', f'{hi:.2f}{unit}'))

# Throughput
durations = [r['duration_secs'] for r in results]
rps = [rows / d if d > 0 else 0 for d in durations]
print()
print(f'  Throughput:  {min(rps):.0f} - {sum(rps)/len(rps):.0f} - {max(rps):.0f} rows/sec')
"

rm -f "$RESULTS_FILE"

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Benchmark complete"
cyan "═══════════════════════════════════════════════"
