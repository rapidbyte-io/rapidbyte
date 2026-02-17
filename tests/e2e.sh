#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"

# Source WasmEdge environment (DYLD_LIBRARY_PATH, PATH)
# Temporarily relax nounset — the env script checks variables that may be unset
if [ -f "$HOME/.wasmedge/env" ]; then
    set +u
    source "$HOME/.wasmedge/env"
    set -u
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
    rm -f /tmp/rapidbyte_e2e_state.db
}

# ── Step 1: Build everything ────────────────────────────────────────

info "Building host binary..."
(cd "$PROJECT_ROOT" && cargo build 2>&1 | tail -1)

info "Building source-postgres connector..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build 2>&1 | tail -1)

info "Building dest-postgres connector..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build 2>&1 | tail -1)

# ── Step 2: Stage .wasm files ───────────────────────────────────────

mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip1/debug/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip1/debug/dest_postgres.wasm" "$CONNECTOR_DIR/"
info "Connectors staged in $CONNECTOR_DIR"
ls -lh "$CONNECTOR_DIR"/*.wasm

# ── Step 3: Start PostgreSQL ────────────────────────────────────────

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
# Extra wait to ensure init scripts have completed
sleep 2
info "PostgreSQL is ready"

# ── Step 4: Verify seed data ────────────────────────────────────────

info "Verifying source seed data..."
USER_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM public.users")
ORDER_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM public.orders")

echo "  Source: users=$USER_COUNT, orders=$ORDER_COUNT"

if [ "$USER_COUNT" -ne 3 ] || [ "$ORDER_COUNT" -ne 3 ]; then
    fail "Seed data mismatch: expected users=3, orders=3"
fi

# ── Step 5: Run rapidbyte pipeline ──────────────────────────────────

info "Running rapidbyte pipeline..."
export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_single_pg.yaml" \
    --log-level debug 2>&1

# ── Step 6: Verify destination data ─────────────────────────────────

info "Verifying destination data..."

# Check raw schema exists
SCHEMA_EXISTS=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw'")

if [ "$SCHEMA_EXISTS" -ne 1 ]; then
    fail "Destination schema 'raw' was not created"
fi

# Check table row counts
DEST_USERS=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw.users")
DEST_ORDERS=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw.orders")

echo "  Destination: raw.users=$DEST_USERS, raw.orders=$DEST_ORDERS"

if [ "$DEST_USERS" -ne 3 ]; then
    fail "raw.users row count mismatch: expected 3, got $DEST_USERS"
fi
if [ "$DEST_ORDERS" -ne 3 ]; then
    fail "raw.orders row count mismatch: expected 3, got $DEST_ORDERS"
fi

# Spot-check data values
ALICE_EMAIL=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT email FROM raw.users WHERE name = 'Alice'")

if [ "$ALICE_EMAIL" != "alice@example.com" ]; then
    fail "Data integrity check failed: Alice's email = '$ALICE_EMAIL'"
fi

MAX_ORDER=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT MAX(amount_cents) FROM raw.orders")

if [ "$MAX_ORDER" -ne 12000 ]; then
    fail "Data integrity check failed: max order = '$MAX_ORDER', expected 12000"
fi

# ── Step 7: Verify state backend ────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_state.db ]; then
    RUN_COUNT=$(sqlite3 /tmp/rapidbyte_e2e_state.db "SELECT COUNT(*) FROM sync_runs")
    RUN_STATUS=$(sqlite3 /tmp/rapidbyte_e2e_state.db "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: runs=$RUN_COUNT, last_status=$RUN_STATUS"

    if [ "$RUN_STATUS" != "completed" ]; then
        fail "Last run status is '$RUN_STATUS', expected 'completed'"
    fi
else
    warn "State DB not found at /tmp/rapidbyte_e2e_state.db (state not persisted)"
fi

# ── Done ────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  E2E TEST PASSED                          ${NC}"
echo -e "${GREEN}  Source: public.users(3) + public.orders(3)${NC}"
echo -e "${GREEN}  Dest:   raw.users($DEST_USERS) + raw.orders($DEST_ORDERS) ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
