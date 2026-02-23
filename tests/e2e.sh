#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/helpers.sh"

# ── Parse arguments ─────────────────────────────────────────────
CONNECTOR=""
[ $# -ge 1 ] && CONNECTOR="$1"

# ── Deterministic build environment ─────────────────────────────
if [ "${RAPIDBYTE_E2E_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
    export RUSTC_WRAPPER="" CARGO_BUILD_RUSTC_WRAPPER=""
fi
if [ "${RAPIDBYTE_E2E_RESET_RUSTFLAGS:-1}" = "1" ]; then
    export RUSTFLAGS="" CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
fi

# ── Discover connectors ────────────────────────────────────────
CONNECTORS=()
if [[ -n "$CONNECTOR" ]]; then
    if [[ ! -f "$SCRIPT_DIR/connectors/$CONNECTOR/config.sh" ]]; then
        fail "Unknown connector: $CONNECTOR (no config.sh in tests/connectors/$CONNECTOR/)"
    fi
    CONNECTORS=("$CONNECTOR")
else
    for config_file in "$SCRIPT_DIR"/connectors/*/config.sh; do
        name="$(basename "$(dirname "$config_file")")"
        CONNECTORS+=("$name")
    done
fi

[ ${#CONNECTORS[@]} -eq 0 ] && fail "No E2E test connectors found in tests/connectors/"

# ── Build host binary ──────────────────────────────────────────
section "Build: Host"
info "Building host binary..."
(cd "$PROJECT_ROOT" && cargo build --quiet)

# ── Run each connector suite ───────────────────────────────────
TOTAL_PASSED=0; TOTAL_FAILED=0; ALL_SCENARIOS=()

for connector in "${CONNECTORS[@]}"; do
    CONNECTOR_DIR_PATH="$SCRIPT_DIR/connectors/$connector"
    export CONNECTOR_DIR_PATH

    # Source connector config
    source "$CONNECTOR_DIR_PATH/config.sh"

    # Build and stage connectors
    section "Build: $connector connectors"
    mkdir -p "$CONNECTOR_DIR"
    for entry in "${CONNECTOR_BUILDS[@]}"; do
        dir="${entry%%:*}"
        wasm="${entry##*:}"
        info "Building $dir..."
        (cd "$PROJECT_ROOT/connectors/$dir" && cargo build --quiet)
        cp "$PROJECT_ROOT/connectors/$dir/target/wasm32-wasip2/debug/${wasm}.wasm" "$CONNECTOR_DIR/"
    done
    export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"

    # Setup with teardown trap for safety
    _teardown_script="$CONNECTOR_DIR_PATH/teardown.sh"
    cleanup_connector() {
        bash "$_teardown_script" 2>/dev/null || true
    }
    trap cleanup_connector EXIT

    bash "$CONNECTOR_DIR_PATH/setup.sh"

    # Run scenarios
    PASSED=0; FAILED=0; SCENARIOS=()
    for test_file in "$CONNECTOR_DIR_PATH"/scenarios/[0-9][0-9]_*.sh; do
        test_name="$(basename "$test_file" .sh)"
        section "Running: $connector/$test_name"
        if bash "$test_file"; then
            PASSED=$((PASSED + 1))
            SCENARIOS+=("PASS: $connector/$test_name")
        else
            FAILED=$((FAILED + 1))
            SCENARIOS+=("FAIL: $connector/$test_name")
        fi
    done

    # Teardown
    bash "$CONNECTOR_DIR_PATH/teardown.sh"
    trap - EXIT

    TOTAL_PASSED=$((TOTAL_PASSED + PASSED))
    TOTAL_FAILED=$((TOTAL_FAILED + FAILED))
    ALL_SCENARIOS+=("${SCENARIOS[@]}")
done

# ── Summary ─────────────────────────────────────────────────────
section "E2E Test Summary"
for s in "${ALL_SCENARIOS[@]}"; do echo "  $s"; done
echo ""
echo "  Total: $((TOTAL_PASSED + TOTAL_FAILED)) | Passed: $TOTAL_PASSED | Failed: $TOTAL_FAILED"
[ "$TOTAL_FAILED" -gt 0 ] && exit 1
exit 0
