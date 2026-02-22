#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Incremental Sync Test"

# Clean up any previous incremental state
clean_state /tmp/rapidbyte_e2e_incr_state.db

# Create the raw_incr schema
pg_cmd "CREATE SCHEMA IF NOT EXISTS raw_incr;"

# Run 1: Full initial load (incremental with no prior cursor = read all)
info "Running incremental pipeline (run 1 -- initial load)..."
run_pipeline "$PG_PIPELINES/e2e_incremental.yaml"

# Verify: raw_incr.users should have 3 rows
INCR_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_incr.users")
info "After run 1: raw_incr.users=$INCR_COUNT_1"
assert_eq_num "$INCR_COUNT_1" 3 "Incremental run 1 row count"

# Verify cursor was persisted in state DB
if [ -f /tmp/rapidbyte_e2e_incr_state.db ]; then
    CURSOR_VAL=$(state_query /tmp/rapidbyte_e2e_incr_state.db \
        "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incremental' AND stream='users'")
    info "Cursor value after run 1: '$CURSOR_VAL'"
    if [ -z "$CURSOR_VAL" ]; then
        fail "No cursor value found in state DB after run 1"
    fi
else
    fail "Incremental state DB not found after run 1"
fi

# Insert 2 more rows into source
pg_cmd "INSERT INTO users (name, email) VALUES ('Dave', 'dave@example.com'), ('Eve', 'eve@example.com');"

# Run 2: Incremental (should only read new rows with id > 3)
info "Running incremental pipeline (run 2 -- should read only new rows)..."
run_pipeline "$PG_PIPELINES/e2e_incremental.yaml"

# Verify: raw_incr.users should have 5 rows (3 from run 1 + 2 new)
INCR_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_incr.users")
info "After run 2: raw_incr.users=$INCR_COUNT_2"
assert_eq_num "$INCR_COUNT_2" 5 "Incremental run 2 row count"

# Verify cursor was updated
CURSOR_VAL_2=$(state_query /tmp/rapidbyte_e2e_incr_state.db \
    "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incremental' AND stream='users'")
info "Cursor value after run 2: '$CURSOR_VAL_2'"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  INCREMENTAL SYNC TEST PASSED                   ${NC}"
echo -e "${GREEN}  Run 1: 3 rows (initial load)                   ${NC}"
echo -e "${GREEN}  Run 2: 5 rows total (2 new appended)           ${NC}"
echo -e "${GREEN}  Cursor: $CURSOR_VAL -> $CURSOR_VAL_2           ${NC}"
echo -e "${GREEN}================================================${NC}"
