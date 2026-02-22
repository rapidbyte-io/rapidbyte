#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Parallelism Test (3 concurrent streams)"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_parallel_state.db

# ── Run pipeline with parallelism=3 ─────────────────────────────────

info "Running rapidbyte pipeline with parallelism=3..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_parallel.yaml"

# ── Verify destination schema exists ────────────────────────────────

info "Verifying destination schema..."
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw_parallel'")
assert_eq_num "$SCHEMA_EXISTS" 1 "Destination schema 'raw_parallel' exists"

# ── Verify all 3 tables exist ───────────────────────────────────────

info "Verifying all 3 tables exist in raw_parallel..."
USERS_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.tables \
     WHERE table_schema = 'raw_parallel' AND table_name = 'users'")
ORDERS_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.tables \
     WHERE table_schema = 'raw_parallel' AND table_name = 'orders'")
ALL_TYPES_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.tables \
     WHERE table_schema = 'raw_parallel' AND table_name = 'all_types'")

assert_eq_num "$USERS_EXISTS" 1 "raw_parallel.users table exists"
assert_eq_num "$ORDERS_EXISTS" 1 "raw_parallel.orders table exists"
assert_eq_num "$ALL_TYPES_EXISTS" 1 "raw_parallel.all_types table exists"

# ── Verify row counts ────────────────────────────────────────────────

info "Verifying row counts..."
DEST_USERS=$(pg_exec "SELECT COUNT(*) FROM raw_parallel.users")
DEST_ORDERS=$(pg_exec "SELECT COUNT(*) FROM raw_parallel.orders")
DEST_ALL_TYPES=$(pg_exec "SELECT COUNT(*) FROM raw_parallel.all_types")

echo "  Destination: raw_parallel.users=$DEST_USERS, raw_parallel.orders=$DEST_ORDERS, raw_parallel.all_types=$DEST_ALL_TYPES"

# users may have Dave/Eve from earlier tests (>= 3)
if [ "$DEST_USERS" -lt 3 ] 2>/dev/null; then
    fail "raw_parallel.users row count: got '$DEST_USERS', expected >= 3"
fi
pass "raw_parallel.users row count >= 3"

assert_eq_num "$DEST_ORDERS" 3 "raw_parallel.orders row count"
assert_eq_num "$DEST_ALL_TYPES" 4 "raw_parallel.all_types row count"

# ── Spot-check data integrity (no corruption from concurrent writes) ─

info "Spot-checking data integrity..."
ALICE_EMAIL=$(pg_exec "SELECT email FROM raw_parallel.users WHERE name = 'Alice'")
assert_eq "$ALICE_EMAIL" "alice@example.com" "Alice email spot-check (no corruption)"

MAX_ORDER=$(pg_exec "SELECT MAX(amount_cents) FROM raw_parallel.orders")
assert_eq_num "$MAX_ORDER" 12000 "Max order amount spot-check (no corruption)"

# ── Verify state backend ─────────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_parallel_state.db ]; then
    RUN_COUNT=$(state_query /tmp/rapidbyte_e2e_parallel_state.db \
        "SELECT COUNT(*) FROM sync_runs")
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_parallel_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: runs=$RUN_COUNT, last_status=$RUN_STATUS"

    assert_eq_num "$RUN_COUNT" 1 "State DB has 1 completed run"
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found at /tmp/rapidbyte_e2e_parallel_state.db (state not persisted)"
fi

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  PARALLELISM E2E TEST PASSED                    ${NC}"
echo -e "${GREEN}  parallelism=3, streams: users/orders/all_types ${NC}"
echo -e "${GREEN}  users=$DEST_USERS  orders=$DEST_ORDERS  all_types=$DEST_ALL_TYPES ${NC}"
echo -e "${GREEN}================================================${NC}"
