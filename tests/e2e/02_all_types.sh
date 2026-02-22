#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "All PG Types Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_all_types_state.db

# Run pipeline to sync all_types through source -> dest
info "Running all_types pipeline..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_all_types.yaml"

# Verify row count
DEST_ALL_TYPES=$(pg_exec "SELECT COUNT(*) FROM raw_types.all_types")
info "Destination: raw_types.all_types=$DEST_ALL_TYPES"
assert_eq_num "$DEST_ALL_TYPES" 4 "raw_types.all_types row count"

# Verify NULL row: all nullable columns should be NULL
NULL_COUNT=$(pg_exec \
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
assert_eq_num "$NULL_COUNT" 1 "Exactly 1 all-NULL row"
info "NULL row verified (1 row with all NULLs)"

# Verify original data row: spot-check integer types (use col_smallint=1 to target original row)
DATA_INTS=$(pg_exec \
    "SELECT col_smallint, col_int, col_bigint FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_INTS" "1|100|1000000000000" "Integer columns (smallint, int, bigint)"
info "Integer types verified (smallint, int, bigint)"

# Verify boolean
DATA_BOOL=$(pg_exec \
    "SELECT col_bool FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_BOOL" "t" "Boolean value"
info "Boolean type verified"

# Verify text types
DATA_TEXT=$(pg_exec \
    "SELECT col_text FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_TEXT" "hello world" "Text value"
info "Text types verified"

# Verify date
DATA_DATE=$(pg_exec \
    "SELECT col_date FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_DATE" "2024-01-15" "Date value"
info "Date type verified"

# Verify UUID (text-cast type)
DATA_UUID=$(pg_exec \
    "SELECT col_uuid FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq "$DATA_UUID" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "UUID value"
info "UUID type verified"

# Verify JSONB
DATA_JSONB=$(pg_exec \
    "SELECT col_jsonb::text FROM raw_types.all_types WHERE col_smallint = 1")
# JSONB normalizes key order; check it contains expected content
assert_contains "$DATA_JSONB" '"a"' "JSONB contains key 'a'"
info "JSONB type verified"

# Count total non-null columns in the original data row to verify all columns came through
NON_NULL_COLS=$(pg_exec \
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
     FROM raw_types.all_types WHERE col_smallint = 1")
assert_eq_num "$NON_NULL_COLS" 23 "Data row has 23 non-null columns"
info "All 23 typed columns round-tripped with non-null values"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  ALL PG TYPES TEST PASSED                       ${NC}"
echo -e "${GREEN}  4 rows (1 data + 1 NULL + 2 edge) round-tripped ${NC}"
echo -e "${GREEN}  23 typed columns verified                       ${NC}"
echo -e "${GREEN}================================================${NC}"
