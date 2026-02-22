#!/usr/bin/env bash
# Shared E2E test helpers. Source this file, do not execute directly.

_HELPERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$_HELPERS_DIR/../.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
cyan()  { echo -e "${CYAN}$*${NC}"; }
section() { echo -e "\n${CYAN}=== $* ===${NC}"; }

# Execute SQL against the test database, return trimmed result.
pg_exec() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U postgres -d rapidbyte_test -t -A -c "$1"
}

# Execute SQL command (no result needed, quiet mode).
pg_cmd() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "$1" -q
}

# Run a rapidbyte pipeline. Args: pipeline_yaml [extra_flags...]
run_pipeline() {
    local yaml="$1"; shift
    "$PROJECT_ROOT/target/debug/rapidbyte" run "$yaml" --log-level info "$@" 2>&1
}

# Assert equality. Args: actual expected description
assert_eq() {
    local actual="$1" expected="$2" desc="$3"
    if [ "$actual" != "$expected" ]; then
        fail "$desc: got '$actual', expected '$expected'"
    fi
    pass "$desc"
}

# Assert numeric equality.
assert_eq_num() {
    local actual="$1" expected="$2" desc="$3"
    if [ "$actual" -ne "$expected" ] 2>/dev/null; then
        fail "$desc: got '$actual', expected '$expected'"
    fi
    pass "$desc"
}

# Assert string contains substring.
assert_contains() {
    local actual="$1" expected="$2" desc="$3"
    if ! echo "$actual" | grep -q "$expected"; then
        fail "$desc: '$actual' does not contain '$expected'"
    fi
    pass "$desc"
}

# Query SQLite state DB. Args: db_path sql
state_query() {
    sqlite3 "$1" "$2" 2>/dev/null || echo ""
}

# Clean state DB files matching a pattern.
clean_state() {
    rm -f "$@"
}
