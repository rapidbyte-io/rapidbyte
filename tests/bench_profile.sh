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
BENCH_ROWS="${1:-10000}"
LOAD_METHOD="${2:-insert}"

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

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Flamegraph Profiler"
cyan "  Rows: $BENCH_ROWS | Mode: $LOAD_METHOD"
cyan "═══════════════════════════════════════════════"
echo ""

# Validate load method
if [ "$LOAD_METHOD" != "insert" ] && [ "$LOAD_METHOD" != "copy" ]; then
    fail "Invalid load method: $LOAD_METHOD (must be 'insert' or 'copy')"
fi

# Check cargo-flamegraph is available
if ! command -v cargo-flamegraph &> /dev/null && ! cargo flamegraph --help &> /dev/null 2>&1; then
    fail "cargo-flamegraph not found. Install with: cargo install flamegraph"
fi

# ── Build with debug symbols ─────────────────────────────────────
info "Building host binary (release + debug symbols)..."
(cd "$PROJECT_ROOT" && CARGO_PROFILE_RELEASE_DEBUG=2 cargo build --release 2>&1 | tail -1)

info "Building source-postgres connector (release)..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --release 2>&1 | tail -1)

info "Building dest-postgres connector (release)..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --release 2>&1 | tail -1)

# Stage .wasm files
mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip1/release/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip1/release/dest_postgres.wasm" "$CONNECTOR_DIR/"
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

# ── Clean dest schema ─────────────────────────────────────────────
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -c "DROP SCHEMA IF EXISTS raw CASCADE" > /dev/null 2>&1
rm -f /tmp/rapidbyte_bench_state.db

# ── Select pipeline YAML ─────────────────────────────────────────
PIPELINE="bench_pg.yaml"
if [ "$LOAD_METHOD" = "copy" ]; then
    PIPELINE="bench_pg_copy.yaml"
fi
PIPELINE_PATH="$PROJECT_ROOT/tests/fixtures/pipelines/$PIPELINE"

# ── Set connector dir ─────────────────────────────────────────────
export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

# ── Output setup ──────────────────────────────────────────────────
PROFILE_DIR="$PROJECT_ROOT/target/profiles"
mkdir -p "$PROFILE_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SVG_FILE="$PROFILE_DIR/flamegraph_${BENCH_ROWS}rows_${LOAD_METHOD}_${TIMESTAMP}.svg"

# ── Run flamegraph ────────────────────────────────────────────────
info "Generating flamegraph ($LOAD_METHOD mode, $BENCH_ROWS rows)..."
info "This may take a while..."

cargo flamegraph \
    --bin rapidbyte \
    --release \
    -o "$SVG_FILE" \
    -- run "$PIPELINE_PATH" \
    --log-level warn 2>&1

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Flamegraph complete"
cyan "═══════════════════════════════════════════════"
echo ""
info "Flamegraph saved to: $SVG_FILE"
info "Open with: open $SVG_FILE"
