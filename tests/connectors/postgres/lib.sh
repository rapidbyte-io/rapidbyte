#!/usr/bin/env bash
# Postgres E2E test helpers. Source this from scenarios.
# Layers on top of tests/lib/helpers.sh with PG-specific functions.

_PG_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_PG_LIB_DIR/../../lib/helpers.sh"

# Override COMPOSE_FILE to point at postgres-specific compose
COMPOSE_FILE="$_PG_LIB_DIR/docker-compose.yml"

# PG fixture paths
PG_FIXTURES="$_PG_LIB_DIR/fixtures"
PG_PIPELINES="$PG_FIXTURES/pipelines"

# Execute SQL and return trimmed result.
pg_exec() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U postgres -d rapidbyte_test -t -A -c "$1"
}

# Execute SQL command (no result needed, quiet mode).
pg_cmd() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U postgres -d rapidbyte_test -c "$1" -q
}
