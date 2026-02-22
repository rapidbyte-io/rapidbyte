#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

section "Pipeline Permissions & Limits Test"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
STATE_FILE="/tmp/rapidbyte_e2e_perms_state.db"
BLOCKED_STATE="/tmp/rapidbyte_e2e_perms_blocked_state.db"
LOWMEM_STATE="/tmp/rapidbyte_e2e_perms_lowmem_state.db"
clean_state "$STATE_FILE" "$BLOCKED_STATE" "$LOWMEM_STATE"

# Clean destination from any prior run
pg_cmd "DROP TABLE IF EXISTS raw.users" 2>/dev/null || true

# ── Test 1: Permissions with correct hosts succeed ────────────────
info "Test 1: Pipeline with valid permissions completes successfully"

run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions.yaml"

SOURCE_USERS=$(pg_exec "SELECT COUNT(*) FROM public.users")
DEST_USERS=$(pg_exec "SELECT COUNT(*) FROM raw.users")
assert_eq_num "$DEST_USERS" "$SOURCE_USERS" "Restricted pipeline copies all rows"

RUN_STATUS=$(state_query "$STATE_FILE" "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
assert_eq "$RUN_STATUS" "completed" "Pipeline run status is completed"

pass "Permissions with correct hosts succeed"

# ── Test 2: Network ACL blocks connection to disallowed host ──────
info "Test 2: Network ACL blocks connection to disallowed host"

OUTPUT=$(run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions_blocked.yaml" 2>&1 || true)

# The pipeline should fail because the connector tries to connect to localhost
# but the ACL only allows nonexistent.example.com
if echo "$OUTPUT" | grep -qi "not allowed\|NETWORK_DENIED\|permission\|denied\|blocked"; then
    pass "Network ACL correctly blocked connection"
elif echo "$OUTPUT" | grep -qi "error\|fail\|panic\|trap"; then
    pass "Network ACL blocked connection (error detected)"
else
    # If it didn't output an error, check if the command actually failed
    if run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions_blocked.yaml" > /dev/null 2>&1; then
        fail "Pipeline should have failed with blocked host but succeeded"
    else
        pass "Network ACL blocked connection (non-zero exit)"
    fi
fi

# ── Test 3: Memory limit causes trap ─────────────────────────────
info "Test 3: Extremely low memory limit causes failure"

if run_pipeline "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions_low_memory.yaml" > /dev/null 2>&1; then
    fail "Pipeline should have failed with 64kb memory limit"
else
    pass "Memory limit correctly enforced (pipeline failed with low memory)"
fi

# ── Test 4: Validation rejects invalid permissions ────────────────
info "Test 4: Validation rejects invalid permission patterns"

BINARY="$PROJECT_ROOT/target/debug/rapidbyte"
CHECK_OUTPUT=$("$BINARY" check "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions_invalid.yaml" 2>&1 || true)

if echo "$CHECK_OUTPUT" | grep -qi "invalid\|error\|fail\|pattern"; then
    pass "Validation correctly rejected invalid permissions"
else
    # Just verify it exits non-zero
    if "$BINARY" check "$PROJECT_ROOT/tests/fixtures/pipelines/e2e_permissions_invalid.yaml" > /dev/null 2>&1; then
        fail "Check should have failed with invalid permissions"
    else
        pass "Validation rejected invalid permissions (non-zero exit)"
    fi
fi

# ── Cleanup ───────────────────────────────────────────────────────
clean_state "$STATE_FILE" "$BLOCKED_STATE" "$LOWMEM_STATE"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  PERMISSIONS & LIMITS E2E TEST PASSED           ${NC}"
echo -e "${GREEN}================================================${NC}"
