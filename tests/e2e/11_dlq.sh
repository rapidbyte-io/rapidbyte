#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "DLQ Test (on_data_error: dlq configuration)"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

clean_state /tmp/rapidbyte_e2e_dlq_state.db

# ── Run pipeline with on_data_error: dlq ────────────────────────────

info "Running rapidbyte pipeline with on_data_error: dlq..."
run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_dlq.yaml"

# ── Verify destination schema exists ────────────────────────────────

info "Verifying destination schema..."
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw_dlq'")
assert_eq_num "$SCHEMA_EXISTS" 1 "Destination schema 'raw_dlq' exists"

# ── Verify row counts ────────────────────────────────────────────────

info "Verifying row counts..."
DEST_USERS=$(pg_exec "SELECT COUNT(*) FROM raw_dlq.users")
echo "  Destination: raw_dlq.users=$DEST_USERS"

if [ "$DEST_USERS" -lt 3 ] 2>/dev/null; then
    fail "raw_dlq.users row count: got '$DEST_USERS', expected >= 3"
fi
pass "raw_dlq.users row count >= 3"

# ── Verify state backend ─────────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_dlq_state.db ]; then
    RUN_COUNT=$(state_query /tmp/rapidbyte_e2e_dlq_state.db \
        "SELECT COUNT(*) FROM sync_runs")
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_dlq_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: runs=$RUN_COUNT, last_status=$RUN_STATUS"

    assert_eq_num "$RUN_COUNT" 1 "State DB has 1 completed run"
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found at /tmp/rapidbyte_e2e_dlq_state.db (state not persisted)"
fi

# ── Check DLQ table in state DB ──────────────────────────────────────

info "Checking DLQ table in state DB..."
if [ -f /tmp/rapidbyte_e2e_dlq_state.db ]; then
    DLQ_TABLE_EXISTS=$(state_query /tmp/rapidbyte_e2e_dlq_state.db \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='dlq_records'")
    if [ "${DLQ_TABLE_EXISTS:-0}" -ge 1 ] 2>/dev/null; then
        DLQ_COUNT=$(state_query /tmp/rapidbyte_e2e_dlq_state.db \
            "SELECT COUNT(*) FROM dlq_records")
        echo "  DLQ table exists, records=$DLQ_COUNT"
        pass "DLQ table exists in state DB"
    else
        echo "  DLQ table not present (no errors occurred, this is acceptable)"
        pass "Pipeline completed without DLQ errors"
    fi
else
    warn "State DB not found, skipping DLQ table check"
fi

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  DLQ E2E TEST PASSED                          ${NC}"
echo -e "${GREEN}  on_data_error: dlq accepted and pipeline ran ${NC}"
echo -e "${GREEN}  Dest: raw_dlq.users($DEST_USERS)             ${NC}"
echo -e "${GREEN}================================================${NC}"
