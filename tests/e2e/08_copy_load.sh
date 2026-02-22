#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "COPY Load Method Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_copy_state.db

# Run pipeline using COPY FROM STDIN load method
info "Running COPY load pipeline..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_copy_load.yaml"

# ── Verify total row count ───────────────────────────────────────────

DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_copy.all_types")
info "Destination: raw_copy.all_types=$DEST_COUNT"
assert_eq_num "$DEST_COUNT" 4 "raw_copy.all_types row count"

# ── Spot-check integers from original row ────────────────────────────

info "Verifying integer columns from original row..."
DATA_INTS=$(pg_exec \
    "SELECT col_smallint, col_int, col_bigint FROM raw_copy.all_types WHERE col_smallint = 1")
assert_eq "$DATA_INTS" "1|100|1000000000000" "Integer columns (smallint=1, int=100, bigint=1000000000000)"

# ── Spot-check text from original row ────────────────────────────────

info "Verifying text column from original row..."
DATA_TEXT=$(pg_exec \
    "SELECT col_text FROM raw_copy.all_types WHERE col_smallint = 1")
assert_eq "$DATA_TEXT" "hello world" "col_text = 'hello world'"

# ── Spot-check date from original row ────────────────────────────────

info "Verifying date column from original row..."
DATA_DATE=$(pg_exec \
    "SELECT col_date FROM raw_copy.all_types WHERE col_smallint = 1")
assert_eq "$DATA_DATE" "2024-01-15" "col_date = '2024-01-15'"

# ── Spot-check UUID from original row ────────────────────────────────

info "Verifying UUID column from original row..."
DATA_UUID=$(pg_exec \
    "SELECT col_uuid FROM raw_copy.all_types WHERE col_smallint = 1")
assert_eq "$DATA_UUID" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "col_uuid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"

# ── Verify NULL row exists ────────────────────────────────────────────

info "Verifying NULL row round-trips through COPY..."
NULL_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM raw_copy.all_types WHERE col_smallint IS NULL AND col_int IS NULL")
assert_eq_num "$NULL_COUNT" 1 "Exactly 1 row where col_smallint IS NULL AND col_int IS NULL"

# ── COPY Load Done ────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  COPY LOAD METHOD E2E TEST PASSED              ${NC}"
echo -e "${GREEN}  4 rows loaded via COPY FROM STDIN             ${NC}"
echo -e "${GREEN}  Integers, text, date, UUID, NULLs verified    ${NC}"
echo -e "${GREEN}================================================${NC}"
