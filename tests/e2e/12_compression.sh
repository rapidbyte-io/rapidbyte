#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Compression E2E Test (LZ4 and Zstd data integrity)"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

# ── Clean state DBs ──────────────────────────────────────────────────

clean_state /tmp/rapidbyte_e2e_lz4_state.db /tmp/rapidbyte_e2e_zstd_state.db

# ── LZ4 Pipeline ─────────────────────────────────────────────────────

section "LZ4 Compression"

info "Running rapidbyte pipeline with LZ4 compression..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_lz4.yaml"

info "Verifying destination schema raw_lz4..."
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw_lz4'")
assert_eq_num "$SCHEMA_EXISTS" 1 "Destination schema 'raw_lz4' exists"

info "Verifying row counts..."
DEST_USERS_LZ4=$(pg_exec "SELECT COUNT(*) FROM raw_lz4.users")
DEST_ORDERS_LZ4=$(pg_exec "SELECT COUNT(*) FROM raw_lz4.orders")
echo "  Destination: raw_lz4.users=$DEST_USERS_LZ4, raw_lz4.orders=$DEST_ORDERS_LZ4"

if [ "$DEST_USERS_LZ4" -lt 3 ] 2>/dev/null; then
    fail "raw_lz4.users row count: got '$DEST_USERS_LZ4', expected >= 3"
fi
pass "raw_lz4.users row count >= 3"

assert_eq_num "$DEST_ORDERS_LZ4" 3 "raw_lz4.orders row count"

info "Spot-checking data integrity (LZ4)..."
ALICE_EMAIL_LZ4=$(pg_exec "SELECT email FROM raw_lz4.users WHERE name = 'Alice'")
assert_eq "$ALICE_EMAIL_LZ4" "alice@example.com" "Alice email spot-check (LZ4)"

# ── Zstd Pipeline ────────────────────────────────────────────────────

section "Zstd Compression"

info "Running rapidbyte pipeline with Zstd compression..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_zstd.yaml"

info "Verifying destination schema raw_zstd..."
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw_zstd'")
assert_eq_num "$SCHEMA_EXISTS" 1 "Destination schema 'raw_zstd' exists"

info "Verifying row counts..."
DEST_USERS_ZSTD=$(pg_exec "SELECT COUNT(*) FROM raw_zstd.users")
DEST_ORDERS_ZSTD=$(pg_exec "SELECT COUNT(*) FROM raw_zstd.orders")
echo "  Destination: raw_zstd.users=$DEST_USERS_ZSTD, raw_zstd.orders=$DEST_ORDERS_ZSTD"

if [ "$DEST_USERS_ZSTD" -lt 3 ] 2>/dev/null; then
    fail "raw_zstd.users row count: got '$DEST_USERS_ZSTD', expected >= 3"
fi
pass "raw_zstd.users row count >= 3"

assert_eq_num "$DEST_ORDERS_ZSTD" 3 "raw_zstd.orders row count"

info "Spot-checking data integrity (Zstd)..."
ALICE_EMAIL_ZSTD=$(pg_exec "SELECT email FROM raw_zstd.users WHERE name = 'Alice'")
assert_eq "$ALICE_EMAIL_ZSTD" "alice@example.com" "Alice email spot-check (Zstd)"

# ── Compression E2E Done ─────────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  COMPRESSION E2E TEST PASSED                   ${NC}"
echo -e "${GREEN}  LZ4:  raw_lz4.users($DEST_USERS_LZ4) + raw_lz4.orders($DEST_ORDERS_LZ4)   ${NC}"
echo -e "${GREEN}  Zstd: raw_zstd.users($DEST_USERS_ZSTD) + raw_zstd.orders($DEST_ORDERS_ZSTD) ${NC}"
echo -e "${GREEN}  Arrow IPC compression/decompression verified  ${NC}"
echo -e "${GREEN}================================================${NC}"
