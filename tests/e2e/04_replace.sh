#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Replace Write Mode Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_replace_state.db

# Run 1: Initial load
info "Running replace pipeline (run 1 -- initial load)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_replace.yaml"

REPLACE_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_replace.users")
info "After run 1: raw_replace.users=$REPLACE_COUNT_1"
assert_eq_num "$REPLACE_COUNT_1" 5 "Replace run 1 row count"

# Run 2: Should TRUNCATE then re-insert (still 5 rows, not 10)
info "Running replace pipeline (run 2 -- should truncate and re-insert)..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_replace.yaml"

REPLACE_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_replace.users")
info "After run 2: raw_replace.users=$REPLACE_COUNT_2"
if [ "$REPLACE_COUNT_2" -ne 5 ]; then
    fail "Replace mode failed: expected 5 rows after run 2, got $REPLACE_COUNT_2 (data was duplicated)"
fi
pass "Replace run 2 row count (no duplicates)"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  REPLACE WRITE MODE TEST PASSED                 ${NC}"
echo -e "${GREEN}  Run 1: 5 rows (initial)                        ${NC}"
echo -e "${GREEN}  Run 2: 5 rows (truncated + re-inserted)        ${NC}"
echo -e "${GREEN}================================================${NC}"
