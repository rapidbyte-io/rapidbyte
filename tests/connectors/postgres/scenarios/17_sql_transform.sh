#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "SQL Transform Test"

clean_state /tmp/rapidbyte_e2e_sql_transform_state.db

# ── Build transform-sql connector if not already present ──────────

TRANSFORM_WASM="$CONNECTOR_DIR/transform_sql.wasm"
if [ ! -f "$TRANSFORM_WASM" ]; then
    info "Building transform-sql connector..."
    (cd "$PROJECT_ROOT/connectors/transform-sql" && cargo build --quiet)
    cp "$PROJECT_ROOT/connectors/transform-sql/target/wasm32-wasip2/debug/transform_sql.wasm" \
        "$CONNECTOR_DIR/"
fi

# ── Seed source data ─────────────────────────────────────────────

info "Seeding source table public.transform_test..."
pg_cmd "DROP TABLE IF EXISTS public.transform_test CASCADE"
pg_cmd "DROP SCHEMA IF EXISTS raw_transform CASCADE"

pg_cmd "CREATE TABLE IF NOT EXISTS public.transform_test (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER
)"
pg_cmd "TRUNCATE public.transform_test"
pg_cmd "INSERT INTO public.transform_test (name, age) VALUES
    ('alice', 30),
    ('bob', 25),
    ('charlie', 35)"

# Verify seed data
SRC_COUNT=$(pg_exec "SELECT COUNT(*) FROM public.transform_test")
assert_eq_num "$SRC_COUNT" 3 "source seed count"

# ── Run pipeline ─────────────────────────────────────────────────

info "Running SQL transform pipeline..."
run_pipeline "$PG_PIPELINES/e2e_sql_transform.yaml"

# ── Verify destination data ──────────────────────────────────────

info "Verifying destination data..."

# Check destination schema and table exist
SCHEMA_EXISTS=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'raw_transform'")
assert_eq_num "$SCHEMA_EXISTS" 1 "Destination schema 'raw_transform' exists"

# Check row count
DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_transform.transform_test")
info "Destination: raw_transform.transform_test=$DEST_COUNT"
assert_eq_num "$DEST_COUNT" 3 "raw_transform.transform_test row count"

# ── Verify names are uppercased by transform ─────────────────────

info "Verifying UPPER(name) transform applied..."

NAME_1=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 1")
assert_eq "$NAME_1" "ALICE" "name for id=1 is uppercased"

NAME_2=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 2")
assert_eq "$NAME_2" "BOB" "name for id=2 is uppercased"

NAME_3=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 3")
assert_eq "$NAME_3" "CHARLIE" "name for id=3 is uppercased"

# ── Verify age column passed through unchanged ───────────────────

info "Verifying age column passed through unchanged..."

AGE_1=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 1")
assert_eq_num "$AGE_1" 30 "age for id=1"

AGE_2=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 2")
assert_eq_num "$AGE_2" 25 "age for id=2"

AGE_3=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 3")
assert_eq_num "$AGE_3" 35 "age for id=3"

# ── Verify column count (id, name, age) ──────────────────────────

info "Verifying column count..."
COL_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_transform' AND table_name='transform_test'")
assert_eq_num "$COL_COUNT" 3 "raw_transform.transform_test column count"

# ── Verify state backend ─────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_sql_transform_state.db ]; then
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_sql_transform_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    echo "  State DB: last_status=$RUN_STATUS"
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found at /tmp/rapidbyte_e2e_sql_transform_state.db (state not persisted)"
fi

# ── Cleanup ──────────────────────────────────────────────────────

pg_cmd "DROP TABLE IF EXISTS public.transform_test CASCADE"

# ── SQL Transform Done ───────────────────────────────────────────

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  SQL TRANSFORM E2E TEST PASSED                  ${NC}"
echo -e "${GREEN}  Source: alice, bob, charlie (lowercase)        ${NC}"
echo -e "${GREEN}  Dest:   ALICE, BOB, CHARLIE (uppercased)      ${NC}"
echo -e "${GREEN}  Transform: SELECT id, UPPER(name), age        ${NC}"
echo -e "${GREEN}================================================${NC}"
