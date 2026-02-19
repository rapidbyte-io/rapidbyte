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
BENCH_ROWS="${BENCH_ROWS:-10000}"
BENCH_ITERS="${BENCH_ITERS:-3}"
BUILD_MODE="${BUILD_MODE:-release}"
BENCH_AOT="${BENCH_AOT:-true}"
PROFILE="false"
RESULTS_DIR="$PROJECT_ROOT/target/bench_results"
mkdir -p "$RESULTS_DIR"
RESULTS_FILE="$RESULTS_DIR/results.jsonl"

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
    echo "Usage: $0 [--rows N] [--iters N] [--no-aot] [--debug] [--profile]"
    echo ""
    echo "Options:"
    echo "  --rows N    Number of rows to benchmark (default: 10000)"
    echo "  --iters N   Number of iterations per mode (default: 3)"
    echo "  --no-aot    Skip AOT compilation of WASM modules"
    echo "  --debug     Use debug builds instead of release"
    echo "  --profile   Generate flamegraph after benchmark runs"
    echo ""
    echo "Runs both INSERT and COPY modes automatically."
    exit 0
}

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)   BENCH_ROWS="$2"; shift 2 ;;
        --iters)  BENCH_ITERS="$2"; shift 2 ;;
        --no-aot) BENCH_AOT="false"; shift ;;
        --debug)  BUILD_MODE="debug"; shift ;;
        --profile) PROFILE="true"; shift ;;
        --help)   usage ;;
        *)        fail "Unknown option: $1" ;;
    esac
done

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Postgres Connector Benchmark"
cyan "  Rows: $BENCH_ROWS | Iterations: $BENCH_ITERS | Build: $BUILD_MODE | AOT: $BENCH_AOT"
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

# ── AOT-compile WASM modules ────────────────────────────────────
if [ "$BENCH_AOT" = "true" ] && command -v wasmedge &> /dev/null; then
    info "AOT-compiling source connector..."
    wasmedge compile "$CONNECTOR_DIR/source_postgres.wasm" "$CONNECTOR_DIR/source_postgres.wasm" 2>&1 | tail -1
    info "AOT-compiling dest connector..."
    wasmedge compile "$CONNECTOR_DIR/dest_postgres.wasm" "$CONNECTOR_DIR/dest_postgres.wasm" 2>&1 | tail -1
    info "AOT compilation complete"
elif [ "$BENCH_AOT" = "true" ]; then
    warn "wasmedge CLI not found, skipping AOT compilation"
fi

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

run_mode() {
    local mode="$1"
    local pipeline="bench_pg.yaml"
    if [ "$mode" = "copy" ]; then
        pipeline="bench_pg_copy.yaml"
    fi
    local results_file="$2"

    info "Running $BENCH_ITERS iterations ($mode mode)..."

    for i in $(seq 1 "$BENCH_ITERS"); do
        # Clean destination between runs
        docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
            psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
        rm -f /tmp/rapidbyte_bench_state.db

        echo -n "  [$mode] Iteration $i/$BENCH_ITERS ... "

        OUTPUT=$("$PROJECT_ROOT/target/$TARGET_DIR/rapidbyte" run \
            "$PROJECT_ROOT/tests/fixtures/pipelines/$pipeline" \
            --log-level warn 2>&1)

        JSON_LINE=$(echo "$OUTPUT" | grep "@@BENCH_JSON@@" | sed 's/@@BENCH_JSON@@//')

        if [ -z "$JSON_LINE" ]; then
            echo "FAILED (no JSON output)"
            continue
        fi

        DURATION=$(echo "$JSON_LINE" | python3 -c "import sys,json; print(f'{json.load(sys.stdin)[\"duration_secs\"]:.2f}')")
        echo "done (${DURATION}s)"
        echo "$JSON_LINE" >> "$results_file"

        # Persist enriched result for regression tracking
        GIT_SHA=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
        GIT_BRANCH=$(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "unknown")
        ENRICHED=$(echo "$JSON_LINE" | python3 -c "
import sys, json, datetime
d = json.load(sys.stdin)
d['timestamp'] = datetime.datetime.now().isoformat()
d['mode'] = '$mode'
d['bench_rows'] = $BENCH_ROWS
d['aot'] = $( [ '$BENCH_AOT' = 'true' ] && echo 'True' || echo 'False' )
d['git_sha'] = '$GIT_SHA'
d['git_branch'] = '$GIT_BRANCH'
print(json.dumps(d))
")
        echo "$ENRICHED" >> "$RESULTS_FILE"
    done
}

INSERT_RESULTS=$(mktemp)
COPY_RESULTS=$(mktemp)

echo ""
run_mode "insert" "$INSERT_RESULTS"
echo ""
run_mode "copy" "$COPY_RESULTS"
echo ""

# ── Generate comparison report ────────────────────────────────────
info "Benchmark Report:"
echo ""

python3 -c "
import json, sys

def load_results(path):
    results = []
    for line in open(path):
        line = line.strip()
        if line:
            results.append(json.loads(line))
    return results

def stats(results, key):
    vals = [r.get(key, 0) for r in results]
    if not vals:
        return 0, 0, 0
    return min(vals), sum(vals)/len(vals), max(vals)

insert_results = load_results('$INSERT_RESULTS')
copy_results = load_results('$COPY_RESULTS')

if not insert_results and not copy_results:
    print('  No results collected')
    sys.exit(1)

# Use insert results for dataset info (same data for both)
ref = insert_results[0] if insert_results else copy_results[0]
rows = ref.get('records_read', 0)
bytes_read = ref.get('bytes_read', 0)
avg_row_bytes = bytes_read // rows if rows > 0 else 0

print(f'  Dataset:     {rows} rows, {bytes_read / 1048576:.2f} MB ({avg_row_bytes} B/row)')
print(f'  Iterations:  {len(insert_results)} per mode')
print()

# Comparison table
hdr = '  {:<22s}  {:>10s}  {:>10s}  {:>10s}'
print(hdr.format('Metric (avg)', 'INSERT', 'COPY', 'Speedup'))
print(hdr.format('-' * 22, '-' * 10, '-' * 10, '-' * 10))

for label, key, unit in [
    ('Total duration', 'duration_secs', 's'),
    ('Dest duration', 'dest_duration_secs', 's'),
    ('  Connect', 'dest_connect_secs', 's'),
    ('  Flush', 'dest_flush_secs', 's'),
    ('  Commit', 'dest_commit_secs', 's'),
    ('  VM setup', 'dest_vm_setup_secs', 's'),
    ('  Recv loop', 'dest_recv_secs', 's'),
    ('  WASM overhead', 'wasm_overhead_secs', 's'),
    ('Source duration', 'source_duration_secs', 's'),
    ('  Connect', 'source_connect_secs', 's'),
    ('  Query', 'source_query_secs', 's'),
    ('  Fetch', 'source_fetch_secs', 's'),
    ('Source module load', 'source_module_load_ms', 'ms'),
    ('Dest module load', 'dest_module_load_ms', 'ms'),
]:
    _, i_avg, _ = stats(insert_results, key)
    _, c_avg, _ = stats(copy_results, key)
    speedup = f'{i_avg/c_avg:.1f}x' if c_avg > 0.001 else '-'
    print(hdr.format(label, f'{i_avg:.3f}{unit}', f'{c_avg:.3f}{unit}', speedup))

# Throughput comparison
print()
i_durations = [r['duration_secs'] for r in insert_results]
c_durations = [r['duration_secs'] for r in copy_results]
i_rps_avg = sum(rows / d for d in i_durations) / len(i_durations) if i_durations else 0
c_rps_avg = sum(rows / d for d in c_durations) / len(c_durations) if c_durations else 0
i_mbps_avg = sum(bytes_read / d / 1048576 for d in i_durations) / len(i_durations) if i_durations else 0
c_mbps_avg = sum(bytes_read / d / 1048576 for d in c_durations) / len(c_durations) if c_durations else 0
rps_speedup = f'{c_rps_avg/i_rps_avg:.1f}x' if i_rps_avg > 0 else '-'
mbps_speedup = f'{c_mbps_avg/i_mbps_avg:.1f}x' if i_mbps_avg > 0 else '-'

print(hdr.format('Throughput (rows/s)', f'{i_rps_avg:,.0f}', f'{c_rps_avg:,.0f}', rps_speedup))
print(hdr.format('Throughput (MB/s)', f'{i_mbps_avg:.2f}', f'{c_mbps_avg:.2f}', mbps_speedup))
"

rm -f "$INSERT_RESULTS" "$COPY_RESULTS"

# ── Optional flamegraph profiling ─────────────────────────────────
if [ "$PROFILE" = "true" ]; then
    info "Running profiling iteration with flamegraph..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
    rm -f /tmp/rapidbyte_bench_state.db

    PROFILE_DIR="$PROJECT_ROOT/target/profiles"
    mkdir -p "$PROFILE_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    SVG_FILE="$PROFILE_DIR/flamegraph_${BENCH_ROWS}rows_insert_${TIMESTAMP}.svg"

    info "Rebuilding with debug symbols for flamegraph..."
    CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release 2>&1 | tail -1

    cargo flamegraph \
        --bin rapidbyte \
        --release \
        -o "$SVG_FILE" \
        -- run "$PROJECT_ROOT/tests/fixtures/pipelines/bench_pg.yaml" \
        --log-level warn 2>&1

    info "Flamegraph saved to: $SVG_FILE"
    info "Open with: open $SVG_FILE"
fi

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Benchmark complete"
cyan "═══════════════════════════════════════════════"
