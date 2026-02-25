#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Chained SQL Transforms Test"

clean_state /tmp/rapidbyte_e2e_sql_chain_state.db

# ── Seed source data ─────────────────────────────────────────────
# 5 rows: two will be filtered out by the second transform (age <= 25).

info "Seeding source table public.chain_test..."
pg_cmd "DROP TABLE IF EXISTS public.chain_test CASCADE"
pg_cmd "DROP SCHEMA IF EXISTS raw_chain CASCADE"

pg_cmd "CREATE TABLE public.chain_test (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER
)"
pg_cmd "INSERT INTO public.chain_test (name, age) VALUES
    ('alice', 30),
    ('bob', 20),
    ('charlie', 35),
    ('diana', 25),
    ('eve', 40)"

SRC_COUNT=$(pg_exec "SELECT COUNT(*) FROM public.chain_test")
assert_eq_num "$SRC_COUNT" 5 "source seed count"

# ── Run pipeline ─────────────────────────────────────────────────
# Transform chain:
#   Step 1: UPPER(name), add double_age = age * 2
#   Step 2: filter WHERE age > 25, drop age column → only id, name, double_age
#
# Expected survivors: alice(30), charlie(35), eve(40) — bob(20) and diana(25) filtered.

info "Running chained SQL transforms pipeline..."
run_pipeline "$PG_PIPELINES/e2e_sql_transform_chain.yaml"

# ── Verify destination data ──────────────────────────────────────

info "Verifying destination data..."

DEST_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_chain.chain_test")
assert_eq_num "$DEST_COUNT" 3 "raw_chain.chain_test row count (filtered from 5)"

# Verify names are uppercased (from transform 1)
NAME_1=$(pg_exec "SELECT name FROM raw_chain.chain_test WHERE id = 1")
assert_eq "$NAME_1" "ALICE" "name for id=1 is uppercased"

NAME_3=$(pg_exec "SELECT name FROM raw_chain.chain_test WHERE id = 3")
assert_eq "$NAME_3" "CHARLIE" "name for id=3 is uppercased"

NAME_5=$(pg_exec "SELECT name FROM raw_chain.chain_test WHERE id = 5")
assert_eq "$NAME_5" "EVE" "name for id=5 is uppercased"

# Verify bob(20) and diana(25) were filtered out by transform 2
BOB_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_chain.chain_test WHERE id = 2")
assert_eq_num "$BOB_COUNT" 0 "bob (age=20) filtered out"

DIANA_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_chain.chain_test WHERE id = 4")
assert_eq_num "$DIANA_COUNT" 0 "diana (age=25) filtered out"

# Verify computed column from transform 1 survived through transform 2
DOUBLE_AGE_1=$(pg_exec "SELECT double_age FROM raw_chain.chain_test WHERE id = 1")
assert_eq_num "$DOUBLE_AGE_1" 60 "double_age for alice (30*2)"

DOUBLE_AGE_3=$(pg_exec "SELECT double_age FROM raw_chain.chain_test WHERE id = 3")
assert_eq_num "$DOUBLE_AGE_3" 70 "double_age for charlie (35*2)"

DOUBLE_AGE_5=$(pg_exec "SELECT double_age FROM raw_chain.chain_test WHERE id = 5")
assert_eq_num "$DOUBLE_AGE_5" 80 "double_age for eve (40*2)"

# Verify column count: only id, name, double_age (age was dropped by transform 2)
COL_COUNT=$(pg_exec \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_chain' AND table_name='chain_test'")
assert_eq_num "$COL_COUNT" 3 "column count (id, name, double_age — age dropped)"

# ── Verify state backend ─────────────────────────────────────────

info "Verifying state backend..."
if [ -f /tmp/rapidbyte_e2e_sql_chain_state.db ]; then
    RUN_STATUS=$(state_query /tmp/rapidbyte_e2e_sql_chain_state.db \
        "SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1")
    assert_eq "$RUN_STATUS" "completed" "Last run status"
else
    warn "State DB not found (state not persisted)"
fi

# ── Cleanup ──────────────────────────────────────────────────────

pg_cmd "DROP TABLE IF EXISTS public.chain_test CASCADE"

pass "Chained SQL Transforms E2E test passed"
