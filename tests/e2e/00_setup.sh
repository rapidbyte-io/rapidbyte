#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib/helpers.sh"

section "Setup: Build and Stage"

if [ "${RAPIDBYTE_E2E_DISABLE_RUSTC_WRAPPER:-1}" = "1" ]; then
    export RUSTC_WRAPPER="" CARGO_BUILD_RUSTC_WRAPPER=""
fi
if [ "${RAPIDBYTE_E2E_RESET_RUSTFLAGS:-1}" = "1" ]; then
    export RUSTFLAGS="" CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS=""
fi

info "Building host binary..."
(cd "$PROJECT_ROOT" && cargo build --quiet)

info "Building source-postgres connector..."
(cd "$PROJECT_ROOT/connectors/source-postgres" && cargo build --quiet)

info "Building dest-postgres connector..."
(cd "$PROJECT_ROOT/connectors/dest-postgres" && cargo build --quiet)

mkdir -p "$CONNECTOR_DIR"
cp "$PROJECT_ROOT/connectors/source-postgres/target/wasm32-wasip2/debug/source_postgres.wasm" \
   "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/dest-postgres/target/wasm32-wasip2/debug/dest_postgres.wasm" \
   "$CONNECTOR_DIR/"
cp "$PROJECT_ROOT/connectors/source-postgres/manifest.json" \
   "$CONNECTOR_DIR/source_postgres.manifest.json"
cp "$PROJECT_ROOT/connectors/dest-postgres/manifest.json" \
   "$CONNECTOR_DIR/dest_postgres.manifest.json"
info "Connectors staged in $CONNECTOR_DIR"

section "Setup: PostgreSQL"

info "Resetting PostgreSQL test environment..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true

info "Starting PostgreSQL via Docker Compose..."
docker compose -f "$COMPOSE_FILE" up -d

info "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
    if pg_exec "SELECT 1" > /dev/null 2>&1; then break; fi
    [ "$i" -eq 30 ] && fail "PostgreSQL did not become ready in time"
    sleep 1
done
# Extra wait to ensure init scripts have completed
sleep 2
info "PostgreSQL is ready"

section "Setup: Verify Seed Data"

info "Verifying source seed data..."
USER_COUNT=$(pg_exec 'SELECT COUNT(*) FROM public.users')
ORDER_COUNT=$(pg_exec 'SELECT COUNT(*) FROM public.orders')
ALL_TYPES_COUNT=$(pg_exec 'SELECT COUNT(*) FROM public.all_types')

echo "  Source: users=$USER_COUNT, orders=$ORDER_COUNT, all_types=$ALL_TYPES_COUNT"

assert_eq_num "$USER_COUNT" 3 "users seed count"
assert_eq_num "$ORDER_COUNT" 3 "orders seed count"
assert_eq_num "$ALL_TYPES_COUNT" 4 "all_types seed count"
