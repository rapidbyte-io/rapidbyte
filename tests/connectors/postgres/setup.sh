#!/usr/bin/env bash
# Postgres E2E setup: start Docker PG, wait for readiness, verify seeds.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

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
