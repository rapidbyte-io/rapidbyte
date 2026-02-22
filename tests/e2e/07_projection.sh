#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Projection Pushdown Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_projection_state.db

# Run pipeline with column projection: only id and name selected
info "Running projection pipeline..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_projection.yaml"

# ── Verify destination row count ─────────────────────────────────────

DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_projection.users")
info "Destination: raw_projection.users=$DEST_COUNT"

# Source may have up to 5 users (Alice, Bob, Carol + Dave/Eve from incremental tests)
if [ "$DEST_COUNT" -lt 3 ]; then
    fail "raw_projection.users row count: got '$DEST_COUNT', expected at least 3"
fi
pass "raw_projection.users row count >= 3"

# ── Verify only 2 columns exist ──────────────────────────────────────

info "Verifying column count (projection)..."
COL_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users'")
assert_eq_num "$COL_COUNT" 2 "raw_projection.users column count"

# ── Verify correct column names ──────────────────────────────────────

info "Verifying column names..."
HAS_ID=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users' AND column_name='id'")
assert_eq_num "$HAS_ID" 1 "column 'id' exists"

HAS_NAME=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users' AND column_name='name'")
assert_eq_num "$HAS_NAME" 1 "column 'name' exists"

# ── Verify excluded columns are absent ───────────────────────────────

info "Verifying excluded columns are absent..."
HAS_EMAIL=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users' AND column_name='email'")
assert_eq_num "$HAS_EMAIL" 0 "column 'email' absent (projected out)"

HAS_CREATED_AT=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users' AND column_name='created_at'")
assert_eq_num "$HAS_CREATED_AT" 0 "column 'created_at' absent (projected out)"

# ── Spot-check data values ────────────────────────────────────────────

info "Verifying expected user names are present..."
ALICE_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_projection.users WHERE name = 'Alice'")
assert_eq_num "$ALICE_COUNT" 1 "Alice is present"

BOB_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_projection.users WHERE name = 'Bob'")
assert_eq_num "$BOB_COUNT" 1 "Bob is present"

CAROL_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_projection.users WHERE name = 'Carol'")
assert_eq_num "$CAROL_COUNT" 1 "Carol is present"

# ── Verify state backend ──────────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_projection_state.db ]; then
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_projection_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: last_status=$RUN_STATUS"
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found at /tmp/rapidbyte_e2e_projection_state.db (state not persisted)"
fi

# ── Projection Done ───────────────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  PROJECTION PUSHDOWN E2E TEST PASSED           ${NC}"
echo -e "${GREEN}  Columns: id, name (email + created_at absent) ${NC}"
echo -e "${GREEN}  Users: Alice, Bob, Carol present              ${NC}"
echo -e "${GREEN}================================================${NC}"
