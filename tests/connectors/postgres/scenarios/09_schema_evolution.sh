#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

section "Schema Evolution Test (new_column: add)"

clean_state /tmp/rapidbyte_e2e_evo_state.db

# ── Prepare: clean destination and create source table ─────────────

pg_cmd "DROP TABLE IF EXISTS public.evo_test CASCADE"
pg_cmd "DROP SCHEMA IF EXISTS raw_evo CASCADE"

pg_cmd "CREATE TABLE public.evo_test (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score INT NOT NULL
)"
pg_cmd "INSERT INTO public.evo_test (name, score) VALUES ('Alice', 90), ('Bob', 85)"

# ── Run 1: initial load (3 columns: id, name, score) ──────────────

info "Running schema evolution pipeline (run 1 -- initial 3-column schema)..."
run_pipeline "$PG_PIPELINES/e2e_schema_evo.yaml"

ROW_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM raw_evo.evo_test")
info "After run 1: raw_evo.evo_test rows=$ROW_COUNT_1"
assert_eq_num "$ROW_COUNT_1" 2 "Run 1 row count"

COL_COUNT_1=$(pg_exec "SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = 'raw_evo' AND table_name = 'evo_test'")
info "After run 1: column count=$COL_COUNT_1"
assert_eq_num "$COL_COUNT_1" 3 "Run 1 column count (id, name, score)"

# ── Alter source schema: add a new column ──────────────────────────

info "Altering source table: adding 'tag' column..."
pg_cmd "ALTER TABLE public.evo_test ADD COLUMN tag TEXT DEFAULT 'new'"
pg_cmd "INSERT INTO public.evo_test (name, score, tag) VALUES ('Carol', 95, 'added')"

# Verify source has 4 columns and 3 rows
SRC_COLS=$(pg_exec "SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'evo_test'")
SRC_ROWS=$(pg_exec "SELECT COUNT(*) FROM public.evo_test")
info "Source after ALTER: columns=$SRC_COLS, rows=$SRC_ROWS"
assert_eq_num "$SRC_COLS" 4 "Source column count after ALTER"
assert_eq_num "$SRC_ROWS" 3 "Source row count after INSERT"

# ── Run 2: schema drift detected, new_column=add policy fires ─────

info "Running schema evolution pipeline (run 2 -- 4-column schema, drift detection)..."
run_pipeline "$PG_PIPELINES/e2e_schema_evo.yaml"

# After run 2 with append mode: 2 rows from run 1 + 3 rows from run 2 = 5
ROW_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM raw_evo.evo_test")
info "After run 2: raw_evo.evo_test rows=$ROW_COUNT_2"
assert_eq_num "$ROW_COUNT_2" 5 "Run 2 row count (2 + 3 appended)"

COL_COUNT_2=$(pg_exec "SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = 'raw_evo' AND table_name = 'evo_test'")
info "After run 2: column count=$COL_COUNT_2"
assert_eq_num "$COL_COUNT_2" 4 "Run 2 column count (tag added via schema evolution)"

# Verify the 'tag' column exists
TAG_EXISTS=$(pg_exec "SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = 'raw_evo' AND table_name = 'evo_test' AND column_name = 'tag'")
assert_eq_num "$TAG_EXISTS" 1 "tag column exists in destination"

# Verify Carol's tag value
CAROL_TAG=$(pg_exec "SELECT tag FROM raw_evo.evo_test WHERE name = 'Carol' LIMIT 1")
assert_eq "$CAROL_TAG" "added" "Carol tag = 'added'"

# Verify Alice and Bob from run 2 have tag='new' (source default)
ALICE_TAG_RUN2=$(pg_exec "SELECT tag FROM raw_evo.evo_test WHERE name = 'Alice' AND tag IS NOT NULL LIMIT 1")
assert_eq "$ALICE_TAG_RUN2" "new" "Alice tag = 'new' (source default, run 2 row)"

# Rows from run 1 have tag=NULL (column was added after they were written)
NULL_TAG_COUNT=$(pg_exec "SELECT COUNT(*) FROM raw_evo.evo_test WHERE tag IS NULL")
assert_eq_num "$NULL_TAG_COUNT" 2 "2 rows from run 1 have tag=NULL"

# ── Cleanup ────────────────────────────────────────────────────────

pg_cmd "DROP TABLE IF EXISTS public.evo_test CASCADE"

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}  SCHEMA EVOLUTION E2E TEST PASSED               ${NC}"
echo -e "${GREEN}  Run 1: 2 rows, 3 columns                      ${NC}"
echo -e "${GREEN}  ALTER TABLE adds 'tag' column at source        ${NC}"
echo -e "${GREEN}  Run 2: 5 rows, 4 columns (new_column: add)    ${NC}"
echo -e "${GREEN}================================================${NC}"
