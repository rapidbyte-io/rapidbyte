#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/helpers.sh"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
PASSED=0; FAILED=0; SCENARIOS=()

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    clean_state /tmp/rapidbyte_e2e_state.db \
                /tmp/rapidbyte_e2e_incr_state.db \
                /tmp/rapidbyte_e2e_replace_state.db \
                /tmp/rapidbyte_e2e_upsert_state.db \
                /tmp/rapidbyte_e2e_all_types_state.db \
                /tmp/rapidbyte_e2e_cdc_state.db
}
trap cleanup EXIT

# Setup (build, start PG, verify seeds)
source "$SCRIPT_DIR/e2e/00_setup.sh"

# Run each scenario
for test_file in "$SCRIPT_DIR"/e2e/[0-9][0-9]_*.sh; do
    [ "$test_file" = "$SCRIPT_DIR/e2e/00_setup.sh" ] && continue
    test_name="$(basename "$test_file" .sh)"
    section "Running: $test_name"
    if bash "$test_file"; then
        PASSED=$((PASSED + 1))
        SCENARIOS+=("PASS: $test_name")
    else
        FAILED=$((FAILED + 1))
        SCENARIOS+=("FAIL: $test_name")
    fi
done

# Summary
section "E2E Test Summary"
for s in "${SCENARIOS[@]}"; do echo "  $s"; done
echo ""
echo "  Total: $((PASSED + FAILED)) | Passed: $PASSED | Failed: $FAILED"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
exit 0
