#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Upsert Write Mode With All PG Types Test"

clean_state /tmp/rapidbyte_e2e_upsert_types_state.db

# Run 1: Initial load (upsert into empty table = plain insert)
info "Running upsert_types pipeline (run 1 -- initial load)..."
run_pipeline "$PG_PIPELINES/e2e_upsert_types.yaml"

COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_upsert_types.all_types")
info "After run 1: raw_upsert_types.all_types=$COUNT_1"
assert_eq_num "$COUNT_1" 4 "Upsert run 1 row count (1 data + 1 NULL + 2 edge)"

# Run 2: Re-run without changes -- upsert should update existing rows, not duplicate
info "Running upsert_types pipeline (run 2 -- should update, not duplicate)..."
run_pipeline "$PG_PIPELINES/e2e_upsert_types.yaml"

COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_upsert_types.all_types")
info "After run 2: raw_upsert_types.all_types=$COUNT_2"
if [ "$COUNT_2" -ne 4 ]; then
    fail "Upsert mode failed: expected 4 rows after run 2, got $COUNT_2 (data was duplicated)"
fi
pass "Upsert run 2 row count (no duplicates)"

# Spot-check: data row values survived the upsert round-trip
DATA_TEXT=$(pg_exec "SELECT col_text FROM raw_upsert_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_TEXT" "hello world" "col_text value correct after upsert (col_smallint=1)"

DATA_DATE=$(pg_exec "SELECT col_date FROM raw_upsert_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_DATE" "2024-01-15" "col_date value correct after upsert"

DATA_UUID=$(pg_exec "SELECT col_uuid FROM raw_upsert_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_UUID" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "col_uuid value correct after upsert"

DATA_JSONB=$(pg_exec "SELECT col_jsonb::text FROM raw_upsert_types.all_types WHERE col_smallint = 1")
assert_contains "$DATA_JSONB" '"a"' "col_jsonb contains key 'a' after upsert"

# Verify NULL row still intact
NULL_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM raw_upsert_types.all_types
     WHERE col_smallint IS NULL AND col_int IS NULL AND col_bigint IS NULL
       AND col_real IS NULL AND col_double IS NULL AND col_bool IS NULL
       AND col_text IS NULL AND col_varchar IS NULL AND col_char IS NULL
       AND col_timestamp IS NULL AND col_timestamptz IS NULL AND col_date IS NULL
       AND col_bytea IS NULL
       AND col_json IS NULL AND col_jsonb IS NULL
       AND col_numeric IS NULL AND col_uuid IS NULL
       AND col_time IS NULL AND col_timetz IS NULL AND col_interval IS NULL
       AND col_inet IS NULL AND col_cidr IS NULL AND col_macaddr IS NULL")
assert_eq_num "$NULL_COUNT" 1 "Exactly 1 all-NULL row preserved after upsert"
info "NULL row verified intact after upsert"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  UPSERT ALL TYPES TEST PASSED                   ${NC}"
echo -e "${GREEN}  Run 1: 4 rows (initial insert)                 ${NC}"
echo -e "${GREEN}  Run 2: 4 rows (upserted, no duplicates)        ${NC}"
echo -e "${GREEN}  Spot-checks: text, date, uuid, jsonb OK        ${NC}"
echo -e "${GREEN}  NULL row: preserved correctly                   ${NC}"
echo -e "${GREEN}================================================${NC}"
