#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Upsert Write Mode Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_upsert_state.db

# Run 1: Initial load (upsert into empty table = plain insert)
info "Running upsert pipeline (run 1 -- initial load)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_upsert.yaml"

UPSERT_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_upsert.users")
info "After run 1: raw_upsert.users=$UPSERT_COUNT_1"
assert_eq_num "$UPSERT_COUNT_1" 5 "Upsert run 1 row count"

# Modify source data (update Alice's email)
pg_cmd "UPDATE users SET email = 'alice-updated@example.com' WHERE name = 'Alice';"

# Run 2: Should upsert (update existing rows, no duplicates)
info "Running upsert pipeline (run 2 -- should update existing rows)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_upsert.yaml"

UPSERT_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_upsert.users")
info "After run 2: raw_upsert.users=$UPSERT_COUNT_2"
if [ "$UPSERT_COUNT_2" -ne 5 ]; then
    fail "Upsert mode failed: expected 5 rows after run 2, got $UPSERT_COUNT_2 (data was duplicated)"
fi
pass "Upsert run 2 row count (no duplicates)"

# Verify the update was applied
ALICE_EMAIL_UPSERT=$(pg_exec "SELECT email FROM raw_upsert.users WHERE name = 'Alice'")
info "Alice's email after upsert: $ALICE_EMAIL_UPSERT"
assert_eq "$ALICE_EMAIL_UPSERT" "alice-updated@example.com" "Alice email updated via upsert"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  UPSERT WRITE MODE TEST PASSED                  ${NC}"
echo -e "${GREEN}  Run 1: 5 rows (initial insert)                 ${NC}"
echo -e "${GREEN}  Run 2: 5 rows (upserted, no duplicates)        ${NC}"
echo -e "${GREEN}  Alice email: updated correctly                  ${NC}"
echo -e "${GREEN}================================================${NC}"
