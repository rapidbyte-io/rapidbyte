#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "SQL Transform Test"

clean_state /tmp/rapidbyte_e2e_sql_transform_state.db

# ── Seed source data ─────────────────────────────────────────────

info "Seeding source table public.transform_test..."
pg_cmd "DROP TABLE IF EXISTS public.transform_test CASCADE"
pg_cmd "DROP SCHEMA IF EXISTS raw_transform CASCADE"

pg_cmd "CREATE TABLE public.transform_test (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER
)"
pg_cmd "INSERT INTO public.transform_test (name, age) VALUES
    ('alice', 30),
    ('bob', 25),
    ('charlie', 35)"

SRC_COUNT=$(pg_exec "SELECT COUNT(*) FROM public.transform_test")
assert_eq_num "$SRC_COUNT" 3 "source seed count"

# ── Run pipeline ─────────────────────────────────────────────────

info "Running SQL transform pipeline..."
run_pipeline "$PG_PIPELINES/e2e_sql_transform.yaml"

# ── Verify destination data ──────────────────────────────────────

info "Verifying destination data..."

DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_transform.transform_test")
assert_eq_num "$DEST_COUNT" 3 "raw_transform.transform_test row count"

# Verify names are uppercased by transform
NAME_1=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 1")
assert_eq "$NAME_1" "ALICE" "name for id=1 is uppercased"

NAME_2=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 2")
assert_eq "$NAME_2" "BOB" "name for id=2 is uppercased"

NAME_3=$(pg_exec "SELECT name FROM raw_transform.transform_test WHERE id = 3")
assert_eq "$NAME_3" "CHARLIE" "name for id=3 is uppercased"

# Verify age column passed through unchanged
AGE_1=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 1")
assert_eq_num "$AGE_1" 30 "age for id=1"

AGE_2=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 2")
assert_eq_num "$AGE_2" 25 "age for id=2"

AGE_3=$(pg_exec "SELECT age FROM raw_transform.transform_test WHERE id = 3")
assert_eq_num "$AGE_3" 35 "age for id=3"

# ── Verify state backend ─────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_sql_transform_state.db ]; then
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_sql_transform_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found (state not persisted)"
fi

# ── Cleanup ──────────────────────────────────────────────────────

pg_cmd "DROP TABLE IF EXISTS public.transform_test CASCADE"

pass "SQL Transform E2E test passed"
