#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"

# Defaults
BENCH_ROWS="10000"
BENCH_ITERS="3"
BENCH_AOT="true"
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

# Keep benchmark compare deterministic across developer environments.
if [ "${RAPIDBYTE_BENCH_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
    export RUSTC_WRAPPER=""
    export CARGO_BUILD_RUSTC_WRAPPER=""
fi
if [ "${RAPIDBYTE_BENCH_RESET_RUSTFLAGS:-1}" = "1" ]; then
    export RUSTFLAGS=""
    export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
fi

ORIGINAL_REF=""
PG_STARTED="false"

cleanup() {
    if [ -n "$ORIGINAL_REF" ]; then
        info "Restoring original branch/ref: $ORIGINAL_REF"
        git -C "$PROJECT_ROOT" checkout "$ORIGINAL_REF" 2>/dev/null || true
    fi
    if [ "$PG_STARTED" = "true" ]; then
        info "Stopping Docker Compose..."
        docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
    fi
    rm -f /tmp/rapidbyte_bench_state.db
}

usage() {
    echo "Usage: $0 <ref1> <ref2> [--rows N] [--iters N] [--aot|--no-aot]"
    echo ""
    echo "Benchmark two git refs and compare results."
    echo ""
    echo "Arguments:"
    echo "  ref1        First git ref (branch, tag, or commit)"
    echo "  ref2        Second git ref (branch, tag, or commit)"
    echo ""
    echo "Options:"
    echo "  --rows N    Number of rows to benchmark (default: 10000)"
    echo "  --iters N   Number of iterations per mode (default: 3)"
    echo "  --aot       Enable Wasmtime AOT cache (default)"
    echo "  --no-aot    Disable Wasmtime AOT cache"
    echo ""
    echo "Examples:"
    echo "  $0 main my-feature"
    echo "  $0 HEAD~1 HEAD --rows 100000 --iters 5"
    echo "  $0 v0.1.0 v0.2.0 --no-aot"
    exit 0
}

# ── Parse args ────────────────────────────────────────────────────
if [[ $# -lt 2 ]]; then
    usage
fi

REF1="$1"; shift
REF2="$1"; shift

while [[ $# -gt 0 ]]; do
    case $1 in
        --rows)   BENCH_ROWS="$2"; shift 2 ;;
        --iters)  BENCH_ITERS="$2"; shift 2 ;;
        --aot)    BENCH_AOT="true"; shift ;;
        --no-aot) BENCH_AOT="false"; shift ;;
        --help)   usage ;;
        *)        fail "Unknown option: $1" ;;
    esac
done

# ── Resolve refs ──────────────────────────────────────────────────
SHA1=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF1" 2>/dev/null) || fail "Cannot resolve ref: $REF1"
SHA2=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF2" 2>/dev/null) || fail "Cannot resolve ref: $REF2"

if [ "$SHA1" = "$SHA2" ]; then
    fail "Both refs resolve to the same commit: $SHA1"
fi

# ── Check working tree ────────────────────────────────────────────
if ! git -C "$PROJECT_ROOT" diff --quiet || ! git -C "$PROJECT_ROOT" diff --cached --quiet; then
    fail "Working tree has uncommitted changes. Commit or stash them first."
fi

# ── Save current branch ──────────────────────────────────────────
ORIGINAL_REF=$(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || true)
if [ -z "$ORIGINAL_REF" ]; then
    ORIGINAL_REF=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD)
fi
trap cleanup EXIT

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark Comparison"
cyan "  $REF1 ($SHA1) vs $REF2 ($SHA2)"
cyan "  Rows: $BENCH_ROWS | Iterations: $BENCH_ITERS | AOT: $BENCH_AOT"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Start PostgreSQL (shared for both refs) ───────────────────────
info "Starting PostgreSQL via Docker Compose..."
docker compose -f "$PROJECT_ROOT/docker-compose.yml" up -d
PG_STARTED="true"

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

# ── Benchmark a single ref ────────────────────────────────────────
bench_ref() {
    local ref="$1"
    local sha="$2"

    echo ""
    cyan "───────────────────────────────────────────────"
    cyan "  Benchmarking: $ref ($sha)"
    cyan "───────────────────────────────────────────────"
    echo ""

    # Checkout
    info "Checking out $ref..."
    git -C "$PROJECT_ROOT" checkout "$ref" --quiet

    # Build
    info "Building host binary (release)..."
    (cd "$PROJECT_ROOT" && cargo build --release --quiet)

    info "Building source-postgres connector (release)..."
    (cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --release --quiet)

    info "Building dest-postgres connector (release)..."
    (cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --release --quiet)

    # Stage .wasm files
    mkdir -p "$CONNECTOR_DIR"
    cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip2/release/source_postgres.wasm" "$CONNECTOR_DIR/"
    cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip2/release/dest_postgres.wasm" "$CONNECTOR_DIR/"
    info "Connectors staged"

    # Seed benchmark data
    info "Seeding $BENCH_ROWS rows into bench_events..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
        < "$PROJECT_ROOT/tests/fixtures/sql/bench_seed.sql" > /dev/null

    ROW_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
        psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM public.bench_events")
    info "Seeded $ROW_COUNT rows"

    # Run iterations
    export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
    if [ "$BENCH_AOT" = "true" ]; then
        export RAPIDBYTE_WASMTIME_AOT="1"
    else
        export RAPIDBYTE_WASMTIME_AOT="0"
    fi

    for mode in insert copy; do
        local pipeline="bench_pg.yaml"
        if [ "$mode" = "copy" ]; then
            pipeline="bench_pg_copy.yaml"
        fi

        info "Running $BENCH_ITERS iterations ($mode mode)..."

        for i in $(seq 1 "$BENCH_ITERS"); do
            # Clean destination between runs
            docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
                psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
            rm -f /tmp/rapidbyte_bench_state.db

            echo -n "  [$mode] Iteration $i/$BENCH_ITERS ... "

            OUTPUT=$("$PROJECT_ROOT/target/release/rapidbyte" run \
                "$PROJECT_ROOT/tests/fixtures/pipelines/$pipeline" \
                --log-level warn 2>&1)

            JSON_LINE=$(echo "$OUTPUT" | grep "@@BENCH_JSON@@" | sed 's/@@BENCH_JSON@@//')

            if [ -z "$JSON_LINE" ]; then
                echo "FAILED (no JSON output)"
                continue
            fi

            DURATION=$(echo "$JSON_LINE" | python3 -c "import sys,json; print(f'{json.load(sys.stdin)[\"duration_secs\"]:.2f}')")
            echo "done (${DURATION}s)"

            # Enrich and persist result
            ENRICHED=$(echo "$JSON_LINE" | python3 -c "
import sys, json, datetime
d = json.load(sys.stdin)
d['timestamp'] = datetime.datetime.now().isoformat()
d['mode'] = '$mode'
d['bench_rows'] = $BENCH_ROWS
d['aot'] = $( [ '$BENCH_AOT' = 'true' ] && echo 'True' || echo 'False' )
d['git_sha'] = '$sha'
d['git_branch'] = '$ref'
print(json.dumps(d))
")
            echo "$ENRICHED" >> "$RESULTS_FILE"
        done
    done
}

# ── Run benchmarks for both refs ──────────────────────────────────
bench_ref "$REF1" "$SHA1"
bench_ref "$REF2" "$SHA2"

# ── Restore original branch ──────────────────────────────────────
info "Restoring original branch: $ORIGINAL_REF"
git -C "$PROJECT_ROOT" checkout "$ORIGINAL_REF" --quiet
ORIGINAL_REF=""  # prevent double-restore in cleanup trap

echo ""
cyan "───────────────────────────────────────────────"
cyan "  Comparison Report"
cyan "───────────────────────────────────────────────"
echo ""

# ── Run comparison ────────────────────────────────────────────────
python3 "$PROJECT_ROOT/tests/bench_compare.py" --sha "$SHA1" "$SHA2" --rows "$BENCH_ROWS"

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Benchmark comparison complete"
cyan "  Results saved to: $RESULTS_FILE"
cyan "═══════════════════════════════════════════════"
