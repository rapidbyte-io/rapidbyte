#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"

# Defaults
BENCH_ROWS="${1:-10000}"
LOAD_METHOD="${2:-insert}"
BENCH_AOT="${BENCH_AOT:-true}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
cyan()  { echo -e "${CYAN}$*${NC}"; }

# Keep profiling runs deterministic across developer environments.
if [ "${RAPIDBYTE_BENCH_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
    export RUSTC_WRAPPER=""
    export CARGO_BUILD_RUSTC_WRAPPER=""
fi
if [ "${RAPIDBYTE_BENCH_RESET_RUSTFLAGS:-1}" = "1" ]; then
    export RUSTFLAGS=""
    export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
fi

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Profiler (samply)"
cyan "  Rows: $BENCH_ROWS | Mode: $LOAD_METHOD | AOT: $BENCH_AOT"
cyan "═══════════════════════════════════════════════"
echo ""

# Validate load method
if [ "$LOAD_METHOD" != "insert" ] && [ "$LOAD_METHOD" != "copy" ]; then
    fail "Invalid load method: $LOAD_METHOD (must be 'insert' or 'copy')"
fi

# Check samply is available
if ! command -v samply &> /dev/null; then
    fail "samply not found. Install with: cargo install samply"
fi

# ── Build with debug symbols ─────────────────────────────────────
# split-debuginfo=packed creates a .dSYM bundle on macOS for samply symbol resolution
info "Building host binary (release + debug symbols)..."
(cd "$PROJECT_ROOT" && CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed \
    cargo build --release --quiet)

info "Building source-postgres connector (release)..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --release --quiet)

info "Building dest-postgres connector (release)..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --release --quiet)

# Stage .wasm files
mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip2/release/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip2/release/dest_postgres.wasm" "$CONNECTOR_DIR/"
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
if [ "$BENCH_AOT" = "true" ]; then
    export RAPIDBYTE_WASMTIME_AOT="1"
else
    export RAPIDBYTE_WASMTIME_AOT="0"
fi

# ── Output setup ──────────────────────────────────────────────────
PROFILE_DIR="$PROJECT_ROOT/target/profiles"
mkdir -p "$PROFILE_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE_FILE="$PROFILE_DIR/profile_${BENCH_ROWS}rows_${LOAD_METHOD}_${TIMESTAMP}.json"

# ── Run profiler ─────────────────────────────────────────────────
info "Recording profile ($LOAD_METHOD mode, $BENCH_ROWS rows)..."

samply record \
    --save-only \
    -o "$PROFILE_FILE" \
    -- "$PROJECT_ROOT/target/release/rapidbyte" run "$PIPELINE_PATH" \
    --log-level warn 2>&1

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Profile complete"
cyan "═══════════════════════════════════════════════"
echo ""
info "Profile saved to: $PROFILE_FILE"
info "View with: samply load $PROFILE_FILE"
