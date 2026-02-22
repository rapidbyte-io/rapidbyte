#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "CDC (Change Data Capture) Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

# Clean up any previous CDC state
clean_state /tmp/rapidbyte_e2e_cdc_state.db

# Create the raw_cdc schema
pg_cmd "CREATE SCHEMA IF NOT EXISTS raw_cdc;"

# Drop replication slot if leftover from a previous test run
pg_cmd "SELECT pg_drop_replication_slot('rapidbyte_users') FROM pg_replication_slots WHERE slot_name = 'rapidbyte_users';" 2>/dev/null || true

# ── Run 1: Capture initial state ─────────────────────────────────
# The replication slot is created on first run. Changes made BEFORE slot
# creation are NOT captured -- this is expected CDC behavior.
# So we first create the slot (via run 1 with no pending changes),
# then make changes and capture them in run 2.

info "Running CDC pipeline (run 1 -- create slot, capture any pending WAL)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_cdc.yaml"

# Run 1 might capture 0 rows (slot just created, no new WAL since)
# or some rows if there were WAL entries. Either way is fine.
RUN1_EXISTS=$(pg_exec "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='raw_cdc' AND table_name='users')::text")
info "After run 1: raw_cdc.users table exists=$RUN1_EXISTS"

# Count rows if table exists
if [ "$RUN1_EXISTS" = "t" ]; then
    RUN1_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users")
else
    RUN1_COUNT=0
fi
info "After run 1: raw_cdc.users rows=$RUN1_COUNT"

# Verify state DB was created and LSN was saved (if changes were captured)
if [ -f /tmp/rapidbyte_e2e_cdc_state.db ]; then
    CDC_LSN_1=$(state_query /tmp/rapidbyte_e2e_cdc_state.db \
        "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_cdc' AND stream='users'" || echo "")
    info "CDC LSN after run 1: '${CDC_LSN_1:-none}'"
else
    info "No state DB after run 1 (no changes captured -- expected)"
fi

# ── Make changes to source (these will be captured by slot) ──────
info "Inserting new row into source..."
pg_cmd "INSERT INTO users (name, email) VALUES ('Frank', 'frank@example.com');"

info "Updating existing row..."
pg_cmd "UPDATE users SET email = 'alice-updated@example.com' WHERE name = 'Alice';"

info "Deleting a row..."
pg_cmd "DELETE FROM users WHERE name = 'Carol';"

# ── Run 2: Capture the changes ──────────────────────────────────
info "Running CDC pipeline (run 2 -- capture INSERT, UPDATE, DELETE)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_cdc.yaml"

# Verify: raw_cdc.users should have the 3 change rows from run 2
CDC_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users")
# Total = run1 rows + 3 new changes (1 INSERT + 1 UPDATE + 1 DELETE)
EXPECTED_TOTAL=$((RUN1_COUNT + 3))
info "After run 2: raw_cdc.users=$CDC_COUNT_2 (expected $EXPECTED_TOTAL)"
assert_eq_num "$CDC_COUNT_2" "$EXPECTED_TOTAL" "CDC run 2 total row count"

# Verify the _rb_op column exists and has correct values
INSERT_OPS=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users WHERE _rb_op = 'insert'")
UPDATE_OPS=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users WHERE _rb_op = 'update'")
DELETE_OPS=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users WHERE _rb_op = 'delete'")
info "CDC operations: insert=$INSERT_OPS update=$UPDATE_OPS delete=$DELETE_OPS"

# Run 2 should have at least 1 of each operation type
if [ "$INSERT_OPS" -lt 1 ]; then
    fail "Expected at least 1 INSERT operation, got $INSERT_OPS"
fi
pass "CDC captured INSERT operations"

if [ "$UPDATE_OPS" -lt 1 ]; then
    fail "Expected at least 1 UPDATE operation, got $UPDATE_OPS"
fi
pass "CDC captured UPDATE operations"

if [ "$DELETE_OPS" -lt 1 ]; then
    fail "Expected at least 1 DELETE operation, got $DELETE_OPS"
fi
pass "CDC captured DELETE operations"

# Verify the Frank insert has correct data
FRANK_NAME=$(pg_exec "SELECT name FROM raw_cdc.users WHERE _rb_op = 'insert' AND name = 'Frank' LIMIT 1")
assert_eq "$FRANK_NAME" "Frank" "CDC INSERT captured correct name"

# Verify the Alice update
ALICE_UPDATE=$(pg_exec "SELECT email FROM raw_cdc.users WHERE _rb_op = 'update' AND name = 'Alice' LIMIT 1")
assert_eq "$ALICE_UPDATE" "alice-updated@example.com" "CDC UPDATE captured new email"

# Verify LSN advanced in state DB
if [ -f /tmp/rapidbyte_e2e_cdc_state.db ]; then
    CDC_LSN_2=$(state_query /tmp/rapidbyte_e2e_cdc_state.db \
        "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_cdc' AND stream='users'")
    info "CDC LSN after run 2: '$CDC_LSN_2'"
    if [ -z "$CDC_LSN_2" ]; then
        fail "No CDC LSN found in state DB after run 2"
    fi
    pass "CDC LSN persisted"
else
    fail "State DB not found after run 2"
fi

# ── Run 3: No new changes → no new rows ─────────────────────────
info "Running CDC pipeline (run 3 -- no new changes)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_cdc.yaml"

CDC_COUNT_3=$(pg_exec "SELECT COUNT(*) FROM raw_cdc.users")
info "After run 3: raw_cdc.users=$CDC_COUNT_3 (expected $EXPECTED_TOTAL -- unchanged)"
assert_eq_num "$CDC_COUNT_3" "$EXPECTED_TOTAL" "CDC run 3 row count unchanged"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  CDC TEST PASSED                                ${NC}"
echo -e "${GREEN}  Run 1: Slot created ($RUN1_COUNT rows)         ${NC}"
echo -e "${GREEN}  Run 2: +3 changes (I=$INSERT_OPS U=$UPDATE_OPS D=$DELETE_OPS)${NC}"
echo -e "${GREEN}  Run 3: No new changes (stable)                 ${NC}"
echo -e "${GREEN}  LSN: ${CDC_LSN_1:-none} -> $CDC_LSN_2          ${NC}"
echo -e "${GREEN}================================================${NC}"
