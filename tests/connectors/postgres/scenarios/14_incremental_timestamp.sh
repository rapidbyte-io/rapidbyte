#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Incremental Sync with Timestamp Cursor Test"

# Clean up any previous state
clean_state /tmp/rapidbyte_e2e_incr_ts_state.db

# Create the destination schema
pg_cmd "CREATE SCHEMA IF NOT EXISTS raw_incr_ts;"

# Count how many users exist in the source right now (may include Dave/Eve from earlier tests)
BASELINE_COUNT=$(pg_exec "SELECT COUNT(*) FROM users")
info "Baseline source user count: $BASELINE_COUNT"

# Run 1: Full initial load (incremental with no prior cursor = read all)
info "Running incremental timestamp pipeline (run 1 -- initial load)..."
run_pipeline "$PG_PIPELINES/e2e_incr_timestamp.yaml"

# Verify: destination should have all baseline rows
DEST_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_incr_ts.users")
info "After run 1: raw_incr_ts.users=$DEST_COUNT_1"
assert_eq_num "$DEST_COUNT_1" "$BASELINE_COUNT" "Incremental timestamp run 1 row count"

# Verify cursor was persisted in state DB
if [ -f /tmp/rapidbyte_e2e_incr_ts_state.db ]; then
    CURSOR_VAL=$(state_query /tmp/rapidbyte_e2e_incr_ts_state.db \
        "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incr_timestamp' AND stream='users'")
    info "Cursor value after run 1: '$CURSOR_VAL'"
    if [ -z "$CURSOR_VAL" ]; then
        fail "No cursor value found in state DB after run 1"
    fi
else
    fail "Incremental state DB not found after run 1"
fi

# Insert 1 more user into the source (will have a newer created_at timestamp)
pg_cmd "INSERT INTO users (name, email) VALUES ('Timestamp_Test_User', 'ts_test@example.com');"

# Run 2: Incremental (should only read new row with created_at > stored cursor)
info "Running incremental timestamp pipeline (run 2 -- should read only new row)..."
run_pipeline "$PG_PIPELINES/e2e_incr_timestamp.yaml"

# Verify: destination should have baseline + 1 rows
EXPECTED_TOTAL=$((BASELINE_COUNT + 1))
DEST_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_incr_ts.users")
info "After run 2: raw_incr_ts.users=$DEST_COUNT_2"
assert_eq_num "$DEST_COUNT_2" "$EXPECTED_TOTAL" "Incremental timestamp run 2 row count"

# Verify Timestamp_Test_User exists in destination
TS_USER=$(pg_exec "SELECT name FROM raw_incr_ts.users WHERE name = 'Timestamp_Test_User'")
assert_eq "$TS_USER" "Timestamp_Test_User" "Timestamp_Test_User exists in destination"

# Verify cursor was updated
CURSOR_VAL_2=$(state_query /tmp/rapidbyte_e2e_incr_ts_state.db \
    "SELECT cursor_value FROM sync_cursors WHERE pipeline='e2e_incr_timestamp' AND stream='users'")
info "Cursor value after run 2: '$CURSOR_VAL_2'"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  INCREMENTAL TIMESTAMP CURSOR TEST PASSED       ${NC}"
echo -e "${GREEN}  Run 1: $BASELINE_COUNT rows (initial load)     ${NC}"
echo -e "${GREEN}  Run 2: $EXPECTED_TOTAL rows total (1 new)      ${NC}"
echo -e "${GREEN}  Cursor: $CURSOR_VAL -> $CURSOR_VAL_2           ${NC}"
echo -e "${GREEN}================================================${NC}"
