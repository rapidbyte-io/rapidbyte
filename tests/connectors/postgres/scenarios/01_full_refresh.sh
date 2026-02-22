#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Full Refresh Test"

info "Running rapidbyte pipeline..."
run_pipeline "$PG_PIPELINES/e2e_single_pg.yaml"

# ── Verify destination data ─────────────────────────────────────────

info "Verifying destination data..."

# Check raw schema exists
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw'")

if [ "$SCHEMA_EXISTS" -ne 1 ]; then
    fail "Destination schema 'raw' was not created"
fi

# Check table row counts
DEST_USERS=$(pg_exec "SELECT COUNT(*) FROM raw.users")
DEST_ORDERS=$(pg_exec "SELECT COUNT(*) FROM raw.orders")

echo "  Destination: raw.users=$DEST_USERS, raw.orders=$DEST_ORDERS"

assert_eq_num "$DEST_USERS" 3 "raw.users row count"
assert_eq_num "$DEST_ORDERS" 3 "raw.orders row count"

# Spot-check data values
ALICE_EMAIL=$(pg_exec "SELECT email FROM raw.users WHERE name = 'Alice'")
assert_eq "$ALICE_EMAIL" "alice@example.com" "Alice email spot-check"

MAX_ORDER=$(pg_exec "SELECT MAX(amount_cents) FROM raw.orders")
assert_eq_num "$MAX_ORDER" 12000 "Max order amount spot-check"

# ── Verify state backend ────────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_state.db ]; then
    RUN_COUNT=$(state_query /tmp/rapidbyte_e2e_state.db "SELECT COUNT(*) FROM sync_runs")
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_state.db "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: runs=$RUN_COUNT, last_status=$RUN_STATUS"

    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found at /tmp/rapidbyte_e2e_state.db (state not persisted)"
fi

# ── Full Refresh Done ───────────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  FULL REFRESH E2E TEST PASSED                   ${NC}"
echo -e "${GREEN}  Source: public.users(3) + public.orders(3)     ${NC}"
echo -e "${GREEN}  Dest:   raw.users($DEST_USERS) + raw.orders($DEST_ORDERS) ${NC}"
echo -e "${GREEN}================================================${NC}"
