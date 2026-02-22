#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$BENCH_DIR/lib/helpers.sh"

setup_deterministic_env

# ── Parse arguments ──────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: compare.sh <ref1> <ref2> [OPTIONS]

Compare benchmark results between two git refs (branches, tags, or SHAs).

Arguments:
  ref1        First git ref (e.g. main, HEAD~1, v0.1.0)
  ref2        Second git ref

Options:
  --connector NAME    Benchmark only this connector (default: all)
  --rows N            Number of rows (default: from connector config)
  --iters N           Iterations per mode (default: from connector config)
  --aot / --no-aot    Wasmtime AOT cache (default: enabled)
  -h, --help          Show this help

Examples:
  compare.sh main my-feature
  compare.sh HEAD~1 HEAD --connector postgres --rows 50000
  compare.sh v0.1.0 v0.2.0 --no-aot
EOF
    exit 0
}

REF1=""
REF2=""
CONNECTOR=""
CLI_ROWS=""
CLI_ITERS=""
BENCH_AOT="true"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connector) CONNECTOR="$2"; shift 2 ;;
        --rows)      CLI_ROWS="$2"; shift 2 ;;
        --iters)     CLI_ITERS="$2"; shift 2 ;;
        --aot)       BENCH_AOT="true"; shift ;;
        --no-aot)    BENCH_AOT="false"; shift ;;
        -h|--help)   usage ;;
        -*)          fail "Unknown option: $1" ;;
        *)           POSITIONAL+=("$1"); shift ;;
    esac
done

[ ${#POSITIONAL[@]} -ge 1 ] && REF1="${POSITIONAL[0]}"
[ ${#POSITIONAL[@]} -ge 2 ] && REF2="${POSITIONAL[1]}"

if [[ -z "$REF1" || -z "$REF2" ]]; then
    fail "Usage: compare.sh <ref1> <ref2> [OPTIONS]"
fi

# ── Validate refs and working tree ───────────────────────────────
SHA1=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF1" 2>/dev/null) || fail "Invalid ref: $REF1"
SHA2=$(git -C "$PROJECT_ROOT" rev-parse --short "$REF2" 2>/dev/null) || fail "Invalid ref: $REF2"

if [ "$SHA1" = "$SHA2" ]; then
    fail "Both refs resolve to the same commit: $SHA1"
fi

if ! git -C "$PROJECT_ROOT" diff --quiet 2>/dev/null || \
   ! git -C "$PROJECT_ROOT" diff --cached --quiet 2>/dev/null; then
    fail "Working tree is dirty. Commit or stash changes before comparing."
fi

# Save original ref for restoration
ORIGINAL_REF=$(git -C "$PROJECT_ROOT" symbolic-ref --short HEAD 2>/dev/null || \
               git -C "$PROJECT_ROOT" rev-parse HEAD)

# ── Discover connectors ──────────────────────────────────────────
CONNECTORS=()
if [[ -n "$CONNECTOR" ]]; then
    [[ -f "$BENCH_DIR/connectors/$CONNECTOR/config.sh" ]] || fail "Unknown connector: $CONNECTOR"
    CONNECTORS=("$CONNECTOR")
else
    for config_file in "$BENCH_DIR"/connectors/*/config.sh; do
        name="$(basename "$(dirname "$config_file")")"
        CONNECTORS+=("$name")
    done
fi

# ── Banner ───────────────────────────────────────────────────────
echo ""
cyan "═══════════════════════════════════════════════"
cyan "  Rapidbyte Benchmark Comparison"
cyan "  Ref 1: $REF1 ($SHA1)"
cyan "  Ref 2: $REF2 ($SHA2)"
cyan "  Connectors: ${CONNECTORS[*]}"
cyan "═══════════════════════════════════════════════"
echo ""

# ── Cleanup trap ─────────────────────────────────────────────────
cleanup() {
    info "Restoring original ref: $ORIGINAL_REF"
    git -C "$PROJECT_ROOT" checkout "$ORIGINAL_REF" --quiet 2>/dev/null || true
    stop_postgres
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}
trap cleanup EXIT

# Start shared infrastructure once
start_postgres

# ── Run benchmarks for each ref ──────────────────────────────────
for ref in "$REF1" "$REF2"; do
    sha=$(git -C "$PROJECT_ROOT" rev-parse --short "$ref")
    section "Benchmarking ref: $ref ($sha)"

    git -C "$PROJECT_ROOT" checkout "$ref" --quiet

    # Build from this ref
    build_host release
    build_connectors release
    stage_connectors release

    for connector in "${CONNECTORS[@]}"; do
        # Source config (from current ref — config.sh may differ between refs)
        if [[ -f "$BENCH_DIR/connectors/$connector/config.sh" ]]; then
            source "$BENCH_DIR/connectors/$connector/config.sh"
        fi

        export BENCH_ROWS="${CLI_ROWS:-$BENCH_DEFAULT_ROWS}"
        export BENCH_ITERS="${CLI_ITERS:-$BENCH_DEFAULT_ITERS}"
        export BENCH_AOT
        export BUILD_MODE="release"

        # Seed data
        info "Seeding $BENCH_ROWS benchmark rows..."
        docker compose -f "$COMPOSE_FILE" exec -T postgres \
            psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
            -f - < "$BENCH_SEED_SQL"

        # Run benchmark
        bash "$BENCH_DIR/connectors/$connector/run.sh"

        # Clean for next ref
        bash "$BENCH_DIR/connectors/$connector/teardown.sh"
    done
done

# ── Generate comparison ──────────────────────────────────────────
section "Comparison Report"
python3 "$BENCH_DIR/analyze.py" --sha "$SHA1" "$SHA2"
