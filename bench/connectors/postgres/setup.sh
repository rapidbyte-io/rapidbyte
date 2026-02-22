#!/usr/bin/env bash
# Postgres connector benchmark setup: start Docker PG and seed data.
# Expects: BENCH_ROWS, BENCH_SEED_SQL, BENCH_DIR (set by orchestrator or helpers)
set -euo pipefail

source "$(cd "$(dirname "$0")/../../lib" && pwd)/helpers.sh"
source "$(cd "$(dirname "$0")" && pwd)/config.sh"

BENCH_ROWS="${BENCH_ROWS:-$BENCH_DEFAULT_ROWS}"

section "Setting up postgres benchmark ($BENCH_ROWS rows)"

start_postgres

# Seed benchmark data
info "Seeding $BENCH_ROWS benchmark rows..."
docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d rapidbyte_test -v bench_rows="$BENCH_ROWS" \
    -f - < "$BENCH_SEED_SQL"

# Verify row count
ACTUAL=$(pg_exec "SELECT COUNT(*) FROM public.bench_events")
if [ "$ACTUAL" != "$BENCH_ROWS" ]; then
    fail "Expected $BENCH_ROWS rows, got $ACTUAL"
fi
info "Verified: $ACTUAL rows seeded"
