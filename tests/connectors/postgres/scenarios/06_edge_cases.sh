#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Edge Cases Test"

clean_state /tmp/rapidbyte_e2e_edges_state.db

# Run pipeline to sync all_types (with edge case rows) into raw_edges schema
info "Running edge cases pipeline..."
run_pipeline "$PG_PIPELINES/e2e_edge_cases.yaml"

# ── Verify total row count ───────────────────────────────────────────

DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_edges.all_types")
info "Destination: raw_edges.all_types=$DEST_COUNT"
assert_eq_num "$DEST_COUNT" 4 "raw_edges.all_types row count"

# ── Integer boundaries (min) ─────────────────────────────────────────

info "Verifying integer min boundaries..."
MIN_INT=$(pg_exec \
    "SELECT col_int FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$MIN_INT" "-2147483648" "col_int = INT_MIN"

MIN_BIGINT=$(pg_exec \
    "SELECT col_bigint FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$MIN_BIGINT" "-9223372036854775808" "col_bigint = BIGINT_MIN"

# ── Integer boundaries (max) ─────────────────────────────────────────

info "Verifying integer max boundaries..."
MAX_INT=$(pg_exec \
    "SELECT col_int FROM raw_edges.all_types WHERE col_smallint = 32767")
assert_eq "$MAX_INT" "2147483647" "col_int = INT_MAX"

MAX_BIGINT=$(pg_exec \
    "SELECT col_bigint FROM raw_edges.all_types WHERE col_smallint = 32767")
assert_eq "$MAX_BIGINT" "9223372036854775807" "col_bigint = BIGINT_MAX"

# ── Float NaN ────────────────────────────────────────────────────────

info "Verifying float NaN round-trip..."
NAN_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM raw_edges.all_types WHERE col_real = 'NaN'")
assert_eq_num "$NAN_COUNT" 1 "Exactly 1 row with col_real = NaN"

# ── Empty text ───────────────────────────────────────────────────────

info "Verifying empty text round-trip..."
EMPTY_TEXT=$(pg_exec \
    "SELECT col_text FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$EMPTY_TEXT" "" "col_text is empty string"

# ── Unicode text ─────────────────────────────────────────────────────

info "Verifying Unicode text round-trip..."
UNICODE_TEXT=$(pg_exec \
    "SELECT col_text FROM raw_edges.all_types WHERE col_smallint = 32767")
assert_contains "$UNICODE_TEXT" "rocket" "col_text contains 'rocket'"

# ── Epoch timestamp ──────────────────────────────────────────────────

info "Verifying epoch timestamp round-trip..."
EPOCH_TS=$(pg_exec \
    "SELECT col_timestamp FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$EPOCH_TS" "1970-01-01 00:00:00" "col_timestamp = epoch"

# ── Epoch date ───────────────────────────────────────────────────────

info "Verifying epoch date round-trip..."
EPOCH_DATE=$(pg_exec \
    "SELECT col_date FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$EPOCH_DATE" "1970-01-01" "col_date = epoch"

# ── Empty JSON ───────────────────────────────────────────────────────

info "Verifying empty JSON round-trip..."
EMPTY_JSON=$(pg_exec \
    "SELECT col_json::text FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$EMPTY_JSON" "{}" "col_json = empty object"

# ── Nil UUID ─────────────────────────────────────────────────────────

info "Verifying nil UUID round-trip..."
NIL_UUID=$(pg_exec \
    "SELECT col_uuid FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$NIL_UUID" "00000000-0000-0000-0000-000000000000" "col_uuid = nil UUID"

# ── Numeric zero ─────────────────────────────────────────────────────

info "Verifying numeric zero round-trip..."
ZERO_NUMERIC=$(pg_exec \
    "SELECT col_numeric FROM raw_edges.all_types WHERE col_smallint = -32768")
assert_eq "$ZERO_NUMERIC" "0.000000" "col_numeric = 0.000000"

# ── Edge Cases Done ──────────────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  EDGE CASES E2E TEST PASSED                     ${NC}"
echo -e "${GREEN}  Int boundaries, NaN, Unicode, epoch, empty     ${NC}"
echo -e "${GREEN}  4 rows verified in raw_edges.all_types         ${NC}"
echo -e "${GREEN}================================================${NC}"
