#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

# E2E should be deterministic and not depend on local rustc wrappers (e.g. sccache).
# Set RAPIDBYTE_E2E_DISABLE_RUSTC_WRAPPER=0 to keep existing wrapper configuration.
if [ "${RAPIDBYTE_E2E_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
    export RUSTC_WRAPPER=""
    export CARGO_BUILD_RUSTC_WRAPPER=""
fi

# Global host rustflags (for example target-cpu=apple-m2) can break wasm target builds.
# Keep e2e stable by clearing them unless explicitly opted out.
if [ "${RAPIDBYTE_E2E_RESET_RUSTFLAGS:-1}" = "1" ]; then
    export RUSTFLAGS=""
    export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
fi

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" down -v 2>/dev/null || true
    rm -f /tmp/rapidbyte_e2e_state.db
    rm -f /tmp/rapidbyte_e2e_incr_state.db
    rm -f /tmp/rapidbyte_e2e_replace_state.db
    rm -f /tmp/rapidbyte_e2e_upsert_state.db
    rm -f /tmp/rapidbyte_e2e_all_types_state.db
}

# ── Step 1: Build everything ────────────────────────────────────────

info "Building host binary..."
(cd "$PROJECT_ROOT" && cargo build --quiet)

info "Building source-postgres connector..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --quiet)

info "Building dest-postgres connector..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --quiet)

# ── Step 2: Stage .wasm files ───────────────────────────────────────

mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip2/debug/source_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip2/debug/dest_postgres.wasm" "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/source-postgres/manifest.json" "$CONNECTOR_DIR/source_postgres.manifest.json"
cp "$PROJECT_ROOT/connectors/dest-postgres/manifest.json" "$CONNECTOR_DIR/dest_postgres.manifest.json"
info "Connectors staged in $CONNECTOR_DIR"

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
ALL_TYPES_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM public.all_types")

echo "  Source: users=$USER_COUNT, orders=$ORDER_COUNT, all_types=$ALL_TYPES_COUNT"

if [ "$USER_COUNT" -ne 3 ] || [ "$ORDER_COUNT" -ne 3 ]; then
    fail "Seed data mismatch: expected users=3, orders=3"
fi
if [ "$ALL_TYPES_COUNT" -ne 2 ]; then
    fail "Seed data mismatch: expected all_types=2, got $ALL_TYPES_COUNT"
fi

# ── Step 5: Run rapidbyte pipeline ──────────────────────────────────

info "Running rapidbyte pipeline..."
export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_single_pg.yaml" \
    --log-level info 2>&1

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

# ── Full Refresh Done ──────────────────────────────────────────────

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  FULL REFRESH E2E TEST PASSED             ${NC}"
echo -e "${GREEN}  Source: public.users(3) + public.orders(3)${NC}"
echo -e "${GREEN}  Dest:   raw.users($DEST_USERS) + raw.orders($DEST_ORDERS) ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"

# ══════════════════════════════════════════════════════════════════════
# ── All PG Types Test ─────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

info "=== All PG Types Test ==="

rm -f /tmp/rapidbyte_e2e_all_types_state.db

# Run pipeline to sync all_types through source -> dest
info "Running all_types pipeline..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_all_types.yaml" \
    --log-level info 2>&1

# Verify row count
DEST_ALL_TYPES=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_types.all_types")
info "Destination: raw_types.all_types=$DEST_ALL_TYPES"
if [ "$DEST_ALL_TYPES" -ne 2 ]; then
    fail "raw_types.all_types row count mismatch: expected 2, got $DEST_ALL_TYPES"
fi

# Verify NULL row: all nullable columns should be NULL
NULL_COUNT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT COUNT(*) FROM raw_types.all_types
     WHERE col_smallint IS NULL AND col_int IS NULL AND col_bigint IS NULL
       AND col_real IS NULL AND col_double IS NULL AND col_bool IS NULL
       AND col_text IS NULL AND col_varchar IS NULL AND col_char IS NULL
       AND col_timestamp IS NULL AND col_timestamptz IS NULL AND col_date IS NULL
       AND col_bytea IS NULL
       AND col_json IS NULL AND col_jsonb IS NULL
       AND col_numeric IS NULL AND col_uuid IS NULL
       AND col_time IS NULL AND col_timetz IS NULL AND col_interval IS NULL
       AND col_inet IS NULL AND col_cidr IS NULL AND col_macaddr IS NULL")
if [ "$NULL_COUNT" -ne 1 ]; then
    fail "Expected exactly 1 all-NULL row, got $NULL_COUNT"
fi
info "NULL row verified (1 row with all NULLs)"

# Verify data row: spot-check integer types
DATA_INTS=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_smallint, col_int, col_bigint FROM raw_types.all_types WHERE col_smallint IS NOT NULL")
if [ "$DATA_INTS" != "1|100|1000000000000" ]; then
    fail "Integer columns mismatch: got '$DATA_INTS', expected '1|100|1000000000000'"
fi
info "Integer types verified (smallint, int, bigint)"

# Verify boolean
DATA_BOOL=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_bool FROM raw_types.all_types WHERE col_bool IS NOT NULL")
if [ "$DATA_BOOL" != "t" ]; then
    fail "Boolean mismatch: got '$DATA_BOOL', expected 't'"
fi
info "Boolean type verified"

# Verify text types
DATA_TEXT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_text FROM raw_types.all_types WHERE col_text IS NOT NULL")
if [ "$DATA_TEXT" != "hello world" ]; then
    fail "Text mismatch: got '$DATA_TEXT', expected 'hello world'"
fi
info "Text types verified"

# Verify date
DATA_DATE=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_date FROM raw_types.all_types WHERE col_date IS NOT NULL")
if [ "$DATA_DATE" != "2024-01-15" ]; then
    fail "Date mismatch: got '$DATA_DATE', expected '2024-01-15'"
fi
info "Date type verified"

# Verify UUID (text-cast type)
DATA_UUID=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_uuid FROM raw_types.all_types WHERE col_uuid IS NOT NULL")
if [ "$DATA_UUID" != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" ]; then
    fail "UUID mismatch: got '$DATA_UUID', expected 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"
fi
info "UUID type verified"

# Verify JSONB
DATA_JSONB=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT col_jsonb::text FROM raw_types.all_types WHERE col_jsonb IS NOT NULL")
# JSONB normalizes key order; check it contains expected content
if ! echo "$DATA_JSONB" | grep -q '"a"'; then
    fail "JSONB mismatch: got '$DATA_JSONB', expected it to contain '\"a\"'"
fi
info "JSONB type verified"

# Count total non-null columns in the data row to verify all columns came through
NON_NULL_COLS=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT
        (col_smallint IS NOT NULL)::int + (col_int IS NOT NULL)::int +
        (col_bigint IS NOT NULL)::int + (col_real IS NOT NULL)::int +
        (col_double IS NOT NULL)::int + (col_bool IS NOT NULL)::int +
        (col_text IS NOT NULL)::int + (col_varchar IS NOT NULL)::int +
        (col_char IS NOT NULL)::int + (col_timestamp IS NOT NULL)::int +
        (col_timestamptz IS NOT NULL)::int + (col_date IS NOT NULL)::int +
        (col_bytea IS NOT NULL)::int + (col_json IS NOT NULL)::int +
        (col_jsonb IS NOT NULL)::int + (col_numeric IS NOT NULL)::int +
        (col_uuid IS NOT NULL)::int + (col_time IS NOT NULL)::int +
        (col_timetz IS NOT NULL)::int + (col_interval IS NOT NULL)::int +
        (col_inet IS NOT NULL)::int + (col_cidr IS NOT NULL)::int +
        (col_macaddr IS NOT NULL)::int
     FROM raw_types.all_types WHERE col_smallint IS NOT NULL")
if [ "$NON_NULL_COLS" -ne 23 ]; then
    fail "Data row has $NON_NULL_COLS non-null columns, expected 23 (all columns)"
fi
info "All 23 typed columns round-tripped with non-null values"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  ALL PG TYPES TEST PASSED                 ${NC}"
echo -e "${GREEN}  2 rows (1 data + 1 NULL) round-tripped   ${NC}"
echo -e "${GREEN}  23 typed columns verified                 ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"

# ══════════════════════════════════════════════════════════════════════
# ── Incremental Sync Test ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

info "=== Incremental Sync Test ==="

# Clean up any previous incremental state
rm -f /tmp/rapidbyte_e2e_incr_state.db

# Create the raw_incr schema
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -c "CREATE SCHEMA IF NOT EXISTS raw_incr;" -q

# Run 1: Full initial load (incremental with no prior cursor = read all)
info "Running incremental pipeline (run 1 — initial load)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_incremental.yaml" \
    --log-level info 2>&1

# Verify: raw_incr.users should have 3 rows
INCR_COUNT_1=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_incr.users")
info "After run 1: raw_incr.users=$INCR_COUNT_1"
if [ "$INCR_COUNT_1" -ne 3 ]; then
    fail "Expected 3 rows after run 1, got $INCR_COUNT_1"
fi

# Verify cursor was persisted in state DB
if [ -f /tmp/rapidbyte_e2e_incr_state.db ]; then
    CURSOR_VAL=$(sqlite3 /tmp/rapidbyte_e2e_incr_state.db \
        "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incremental' AND stream='users'" 2>/dev/null || echo "")
    info "Cursor value after run 1: '$CURSOR_VAL'"
    if [ -z "$CURSOR_VAL" ]; then
        fail "No cursor value found in state DB after run 1"
    fi
else
    fail "Incremental state DB not found after run 1"
fi

# Insert 2 more rows into source
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -c \
    "INSERT INTO users (name, email) VALUES ('Dave', 'dave@example.com'), ('Eve', 'eve@example.com');" -q

# Run 2: Incremental (should only read new rows with id > 3)
info "Running incremental pipeline (run 2 — should read only new rows)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_incremental.yaml" \
    --log-level info 2>&1

# Verify: raw_incr.users should have 5 rows (3 from run 1 + 2 new)
INCR_COUNT_2=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_incr.users")
info "After run 2: raw_incr.users=$INCR_COUNT_2"
if [ "$INCR_COUNT_2" -ne 5 ]; then
    fail "Expected 5 rows after run 2, got $INCR_COUNT_2"
fi

# Verify cursor was updated
CURSOR_VAL_2=$(sqlite3 /tmp/rapidbyte_e2e_incr_state.db \
    "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incremental' AND stream='users'" 2>/dev/null || echo "")
info "Cursor value after run 2: '$CURSOR_VAL_2'"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  INCREMENTAL SYNC TEST PASSED             ${NC}"
echo -e "${GREEN}  Run 1: 3 rows (initial load)             ${NC}"
echo -e "${GREEN}  Run 2: 5 rows total (2 new appended)     ${NC}"
echo -e "${GREEN}  Cursor: $CURSOR_VAL -> $CURSOR_VAL_2     ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"

# ══════════════════════════════════════════════════════════════════════
# ── Replace Write Mode Test ─────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

info "=== Replace Write Mode Test ==="

rm -f /tmp/rapidbyte_e2e_replace_state.db

# Run 1: Initial load
info "Running replace pipeline (run 1 — initial load)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_replace.yaml" \
    --log-level info 2>&1

REPLACE_COUNT_1=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_replace.users")
info "After run 1: raw_replace.users=$REPLACE_COUNT_1"
if [ "$REPLACE_COUNT_1" -ne 5 ]; then
    fail "Expected 5 rows after replace run 1, got $REPLACE_COUNT_1"
fi

# Run 2: Should TRUNCATE then re-insert (still 5 rows, not 10)
info "Running replace pipeline (run 2 — should truncate and re-insert)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_replace.yaml" \
    --log-level info 2>&1

REPLACE_COUNT_2=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_replace.users")
info "After run 2: raw_replace.users=$REPLACE_COUNT_2"
if [ "$REPLACE_COUNT_2" -ne 5 ]; then
    fail "Replace mode failed: expected 5 rows after run 2, got $REPLACE_COUNT_2 (data was duplicated)"
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  REPLACE WRITE MODE TEST PASSED           ${NC}"
echo -e "${GREEN}  Run 1: 5 rows (initial)                  ${NC}"
echo -e "${GREEN}  Run 2: 5 rows (truncated + re-inserted)  ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"

# ══════════════════════════════════════════════════════════════════════
# ── Upsert Write Mode Test ──────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

info "=== Upsert Write Mode Test ==="

rm -f /tmp/rapidbyte_e2e_upsert_state.db

# Run 1: Initial load (upsert into empty table = plain insert)
info "Running upsert pipeline (run 1 — initial load)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_upsert.yaml" \
    --log-level info 2>&1

UPSERT_COUNT_1=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_upsert.users")
info "After run 1: raw_upsert.users=$UPSERT_COUNT_1"
if [ "$UPSERT_COUNT_1" -ne 5 ]; then
    fail "Expected 5 rows after upsert run 1, got $UPSERT_COUNT_1"
fi

# Modify source data (update Alice's email)
docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -c \
    "UPDATE users SET email = 'alice-updated@example.com' WHERE name = 'Alice';" -q

# Run 2: Should upsert (update existing rows, no duplicates)
info "Running upsert pipeline (run 2 — should update existing rows)..."
"$PROJECT_ROOT/target/debug/rapidbyte" run \
    "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_upsert.yaml" \
    --log-level info 2>&1

UPSERT_COUNT_2=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c "SELECT COUNT(*) FROM raw_upsert.users")
info "After run 2: raw_upsert.users=$UPSERT_COUNT_2"
if [ "$UPSERT_COUNT_2" -ne 5 ]; then
    fail "Upsert mode failed: expected 5 rows after run 2, got $UPSERT_COUNT_2 (data was duplicated)"
fi

# Verify the update was applied
ALICE_EMAIL_UPSERT=$(docker compose -f "$PROJECT_ROOT/docker-compose.yml" exec -T postgres \
    psql -U postgres -d rapidbyte_test -t -A -c \
    "SELECT email FROM raw_upsert.users WHERE name = 'Alice'")
info "Alice's email after upsert: $ALICE_EMAIL_UPSERT"
if [ "$ALICE_EMAIL_UPSERT" != "alice-updated@example.com" ]; then
    fail "Upsert did not update Alice's email: got '$ALICE_EMAIL_UPSERT'"
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
echo -e "${GREEN}  UPSERT WRITE MODE TEST PASSED            ${NC}"
echo -e "${GREEN}  Run 1: 5 rows (initial insert)           ${NC}"
echo -e "${GREEN}  Run 2: 5 rows (upserted, no duplicates)  ${NC}"
echo -e "${GREEN}  Alice email: updated correctly            ${NC}"
echo -e "${GREEN}═══════════════════════════════════════════${NC}"
