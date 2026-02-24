#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$BENCH_DIR/lib/helpers.sh"

setup_deterministic_env

# ── Parse arguments ──────────────────────────────────────────────
CONNECTOR=""
BENCH_ROWS=""
BENCH_ITERS=""
BUILD_MODE="release"
BENCH_AOT="true"
BENCH_PROFILE=""
CPU_PROFILE="false"

usage() {
    cat <<EOF
Usage: bench.sh [CONNECTOR] [ROWS] --profile PROFILE [OPTIONS]

Arguments:
  CONNECTOR   Connector name (e.g. postgres). Omit to bench all connectors.
  ROWS        Number of rows to benchmark. Omit for profile default.

Required:
  --profile PROFILE   Data profile: small (~500 B/row), medium (~4 KB/row), large (~50 KB/row)

Options:
  --iters N         Number of iterations per mode (default: from connector config)
  --debug           Build in debug mode
  --aot / --no-aot  Enable/disable Wasmtime AOT cache (default: enabled)
  --cpu-profile     Run profiling iteration after benchmark (requires samply)
  -h, --help        Show this help

Examples:
  bench.sh postgres --profile medium              # Postgres, medium profile, 50K rows
  bench.sh postgres 100000 --profile small        # Postgres, small profile, 100K rows
  bench.sh postgres --profile large --iters 5     # Postgres, large profile, 5 iters
  bench.sh --profile small                        # All connectors, small profile
EOF
    exit 0
}

# Parse positional args first, then flags
POSITIONAL=()
CLI_ROWS=""
CLI_ITERS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --iters)       CLI_ITERS="$2"; shift 2 ;;
        --debug)       BUILD_MODE="debug"; shift ;;
        --aot)         BENCH_AOT="true"; shift ;;
        --no-aot)      BENCH_AOT="false"; shift ;;
        --profile)     BENCH_PROFILE="$2"; shift 2 ;;
        --cpu-profile) CPU_PROFILE="true"; shift ;;
        -h|--help)     usage ;;
        -*)            fail "Unknown option: $1" ;;
        *)             POSITIONAL+=("$1"); shift ;;
    esac
done

# Assign positional args
[ ${#POSITIONAL[@]} -ge 1 ] && CONNECTOR="${POSITIONAL[0]}"
[ ${#POSITIONAL[@]} -ge 2 ] && CLI_ROWS="${POSITIONAL[1]}"

# Require --profile
if [[ -z "$BENCH_PROFILE" ]]; then
    fail "Missing required --profile flag. Use: --profile small|medium|large"
fi
if [[ "$BENCH_PROFILE" != "small" && "$BENCH_PROFILE" != "medium" && "$BENCH_PROFILE" != "large" ]]; then
    fail "Invalid profile: $BENCH_PROFILE. Must be one of: small, medium, large"
fi

export BENCH_PROFILE

# ── Discover connectors ──────────────────────────────────────────
CONNECTORS=()
if [[ -n "$CONNECTOR" ]]; then
    if [[ ! -f "$BENCH_DIR/connectors/$CONNECTOR/config.sh" ]]; then
        fail "Unknown connector: $CONNECTOR (no config.sh found in bench/connectors/$CONNECTOR/)"
    fi
    CONNECTORS=("$CONNECTOR")
else
    for config_file in "$BENCH_DIR"/connectors/*/config.sh; do
        name="$(basename "$(dirname "$config_file")")"
        CONNECTORS+=("$name")
    done
fi

if [ ${#CONNECTORS[@]} -eq 0 ]; then
    fail "No connector benchmarks found in bench/connectors/"
fi

# ── Banner ───────────────────────────────────────────────────────
echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark"
cyan "  Connectors: ${CONNECTORS[*]}"
cyan "  Profile: $BENCH_PROFILE"
cyan "  Build: $BUILD_MODE | AOT: $BENCH_AOT"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Build ────────────────────────────────────────────────────────
build_host "$BUILD_MODE"
build_connectors "$BUILD_MODE"
stage_connectors "$BUILD_MODE"
report_wasm_sizes

# ── Cleanup trap ─────────────────────────────────────────────────
cleanup() {
    stop_postgres
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}
trap cleanup EXIT

# ── Run each connector ───────────────────────────────────────────
export BUILD_MODE BENCH_AOT

for connector in "${CONNECTORS[@]}"; do
    connector_dir="$BENCH_DIR/connectors/$connector"

    # Source connector config to get defaults (profile-aware)
    source "$connector_dir/config.sh"

    # Use CLI override if given, otherwise connector default
    export BENCH_ROWS="${CLI_ROWS:-$BENCH_DEFAULT_ROWS}"
    export BENCH_ITERS="${CLI_ITERS:-$BENCH_DEFAULT_ITERS}"

    # Setup infrastructure
    bash "$connector_dir/setup.sh"

    # Run benchmark
    bash "$connector_dir/run.sh"

    # Teardown connector-specific state
    bash "$connector_dir/teardown.sh"
done

# ── Optional CPU profiling ──────────────────────────────────────
if [ "$CPU_PROFILE" = "true" ]; then
    if ! command -v samply &> /dev/null; then
        fail "samply not found. Install with: cargo install samply"
    fi

    info "Running profiling iteration with samply..."
    clean_dest_schema

    PROFILE_DIR="$PROJECT_ROOT/target/profiles"
    mkdir -p "$PROFILE_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    PROFILE_FILE="$PROFILE_DIR/profile_${BENCH_ROWS}rows_insert_${TIMESTAMP}.json"

    info "Rebuilding with debug symbols for profiling..."
    CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed \
        cargo build --release --quiet

    samply record \
        --save-only \
        -o "$PROFILE_FILE" \
        -- "$PROJECT_ROOT/target/release/rapidbyte" run \
        "$BENCH_DIR/fixtures/pipelines/bench_pg.yaml" \
        --log-level warn 2>&1

    info "Profile saved to: $PROFILE_FILE"
    info "View with: samply load $PROFILE_FILE"
fi

echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Benchmark complete"
cyan "═══════════════════════════════════════════════"
