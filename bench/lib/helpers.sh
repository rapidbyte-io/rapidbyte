#!/usr/bin/env bash
# Benchmark framework shared helpers.
# Sources test helpers for pg_exec, colors, etc. and adds bench-specific functions.

_BENCH_HELPERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$_BENCH_HELPERS_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$BENCH_DIR/.." && pwd)"

# Require bash 4+ for associative arrays used in connector configs
if ((BASH_VERSINFO[0] < 4)); then
    echo "ERROR: bash 4+ required for benchmark scripts. Found: $BASH_VERSION" >&2
    echo "  macOS: brew install bash" >&2
    exit 1
fi

# Re-use test helpers (pg_exec, pg_cmd, colors, info/warn/fail/section, assertions)
source "$PROJECT_ROOT/tests/lib/helpers.sh"

CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"
export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"
RESULTS_DIR="$PROJECT_ROOT/target/bench_results"
RESULTS_FILE="$RESULTS_DIR/results.jsonl"
mkdir -p "$RESULTS_DIR"

# ── Deterministic build environment ──────────────────────────────
setup_deterministic_env() {
    if [ "${RAPIDBYTE_BENCH_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
        export RUSTC_WRAPPER=""
        export CARGO_BUILD_RUSTC_WRAPPER=""
    fi
    if [ "${RAPIDBYTE_BENCH_RESET_RUSTFLAGS:-1}" = "1" ]; then
        export RUSTFLAGS=""
        export CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
    fi
}

# ── Build helpers ────────────────────────────────────────────────
build_host() {
    local mode="${1:-release}"
    info "Building host ($mode)..."
    if [ "$mode" = "release" ]; then
        (cd "$PROJECT_ROOT" && cargo build --release --quiet)
    else
        (cd "$PROJECT_ROOT" && cargo build --quiet)
    fi
}

build_connectors() {
    local mode="${1:-release}"
    local flag=""
    [ "$mode" = "release" ] && flag="--release"

    info "Building source-postgres connector ($mode)..."
    (cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build $flag --quiet)

    info "Building dest-postgres connector ($mode)..."
    (cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build $flag --quiet)
}

stage_connectors() {
    local mode="${1:-release}"
    local wasm_dir="wasm32-wasip2/$mode"

    mkdir -p "$CONNECTOR_DIR"
    cp "$PROJECT_ROOT/connectors/source-postgres/target/$wasm_dir/source_postgres.wasm" \
       "$CONNECTOR_DIR/"
    cp "$PROJECT_ROOT/connectors/dest-postgres/target/$wasm_dir/dest_postgres.wasm" \
       "$CONNECTOR_DIR/"

    # Strip custom sections from release builds, re-embed rapidbyte_* sections afterward
    if [ "$mode" = "release" ] && [ -x "$PROJECT_ROOT/scripts/strip-wasm.sh" ]; then
        "$PROJECT_ROOT/scripts/strip-wasm.sh" "$CONNECTOR_DIR/source_postgres.wasm" \
            "$CONNECTOR_DIR/source_postgres.wasm"
        "$PROJECT_ROOT/scripts/strip-wasm.sh" "$CONNECTOR_DIR/dest_postgres.wasm" \
            "$CONNECTOR_DIR/dest_postgres.wasm"
    fi
}

# Report staged connector binary sizes.
report_wasm_sizes() {
    if [ -d "$CONNECTOR_DIR" ]; then
        info "Connector binary sizes:"
        for wasm in "$CONNECTOR_DIR"/*.wasm; do
            [ -f "$wasm" ] || continue
            local name size
            name="$(basename "$wasm")"
            size="$(wc -c < "$wasm" | tr -d ' ')"
            local human
            if [ "$size" -ge 1048576 ]; then
                human="$(python3 -c "print(f'{$size/1048576:.1f} MB')")"
            else
                human="$(python3 -c "print(f'{$size/1024:.0f} KB')")"
            fi
            info "  $name: $human ($size bytes)"
        done
    fi
}

# ── Docker helpers ───────────────────────────────────────────────
start_postgres() {
    info "Starting Docker Compose..."
    docker compose -f "$COMPOSE_FILE" up -d --wait 2>/dev/null

    info "Waiting for PostgreSQL readiness..."
    local retries=30
    while ! pg_exec "SELECT 1" > /dev/null 2>&1; do
        retries=$((retries - 1))
        if [ "$retries" -le 0 ]; then
            fail "PostgreSQL did not become ready within 30 seconds"
        fi
        sleep 1
    done
    sleep 2
    info "PostgreSQL ready"
}

stop_postgres() {
    info "Stopping Docker Compose..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}

clean_dest_schema() {
    pg_cmd "DROP SCHEMA IF EXISTS raw CASCADE"
    rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || \
        sudo rm -f /tmp/rapidbyte_bench_state.db 2>/dev/null || true
}

# ── Git metadata ─────────────────────────────────────────────────
git_sha() {
    git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

git_branch() {
    git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "unknown"
}

# ── Benchmark execution ─────────────────────────────────────────
# Run a single pipeline iteration and return the JSON metrics line.
# Usage: run_pipeline_bench <pipeline_yaml>
# Outputs JSON to stdout, status messages to stderr.
run_pipeline_bench() {
    local pipeline_yaml="$1"
    local build_mode="${2:-release}"
    local target_dir="release"
    [ "$build_mode" != "release" ] && target_dir="debug"

    local output exit_code=0
    output=$("$PROJECT_ROOT/target/$target_dir/rapidbyte" run \
        "$pipeline_yaml" \
        --log-level warn 2>&1) || exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        echo "Pipeline failed (exit code $exit_code):" >&2
        echo "$output" >&2
        return 1
    fi

    local json_line
    json_line=$(echo "$output" | grep "@@BENCH_JSON@@" | sed 's/@@BENCH_JSON@@//') || true

    if [ -z "$json_line" ]; then
        echo "Pipeline succeeded but no @@BENCH_JSON@@ marker found. Output:" >&2
        echo "$output" >&2
        return 1
    fi

    echo "$json_line"
}

# Enrich a JSON result line with metadata.
# Usage: enrich_result <json_line> <mode> <rows> <aot>
enrich_result() {
    local json_line="$1"
    local mode="$2"
    local rows="$3"
    local aot="$4"

    python3 -c "
import sys, json, datetime
d = json.loads(sys.argv[1])
d['timestamp'] = datetime.datetime.now().isoformat()
d['mode'] = sys.argv[2]
d['bench_rows'] = int(sys.argv[3])
d['aot'] = sys.argv[4] == 'true'
d['git_sha'] = sys.argv[5]
d['git_branch'] = sys.argv[6]
d['profile'] = sys.argv[7]
print(json.dumps(d))
" "$json_line" "$mode" "$rows" "$aot" "$(git_sha)" "$(git_branch)" "${BENCH_PROFILE:-unknown}"
}

# ── Statistical output (criterion-style) ────────────────────────
# Generate criterion-style statistical report from collected results.
# Usage: criterion_report <insert_results_file> <copy_results_file> <rows>
criterion_report() {
    local insert_file="$1"
    local copy_file="$2"
    local rows="$3"

    python3 "$BENCH_DIR/lib/report.py" "$insert_file" "$copy_file" "$rows"
}
