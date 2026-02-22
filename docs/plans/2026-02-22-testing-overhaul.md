# Testing Overhaul Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the `tests/` directory into a robust testing framework and fill every critical coverage gap — data types, edge cases, write modes, sync modes, schema evolution, DLQ, projection pushdown, parallelism, CDC, and sandbox/ACL security.

**Architecture:** Split the monolithic `e2e.sh` (470 lines) into a modular shell test runner with shared helpers, reusable SQL fixtures, and per-scenario pipeline configs. Add comprehensive unit tests for untested host modules (Network ACL, component runtime). Add connector inline tests for all pure-logic functions.

**Tech Stack:** Bash test runner, PostgreSQL 16 (Docker), `cargo test --workspace` (host crate unit/integration tests), `cargo check --tests` (connector compilation verification).

---

## Current State Assessment

### What exists
- **e2e.sh** (470 lines, monolithic): full_refresh, all_types, incremental, replace, upsert
- **Unit tests**: 78 host tests (parser, validator, compression, checkpoint, state, errors, protocol serde, arrow IPC)
- **Connector inline tests**: ~44 tests (schema mapping, arrow encode, CDC parsing, type_map, typed_col, copy_format, insert, query)
- **Benchmarks**: INSERT vs COPY, LZ4/Zstd, matrix, comparison

### What's missing
1. **E2E**: No projection pushdown, CDC, schema evolution, DLQ, parallelism, COPY load method, compression
2. **Data edge cases**: No boundary values (INT_MIN/MAX, epoch dates, empty strings, huge JSON, NaN/Infinity floats)
3. **Network ACL**: `allows()`, `add_host()`, wildcard matching, `derive_network_acl()`, `extract_host_from_url()`, `collect_runtime_hosts()` — ALL untested
4. **Schema evolution**: No DDL drift detection tests, no policy enforcement tests
5. **DLQ**: No E2E test for `on_data_error: dlq` routing
6. **Pipeline config**: No validation tests for schema_evolution config, load_method, compression, primary_key, on_data_error

---

## Task 1: Refactor tests/ directory structure

**Files:**
- Create: `tests/lib/helpers.sh`
- Create: `tests/e2e/00_setup.sh`
- Create: `tests/e2e/01_full_refresh.sh`
- Create: `tests/e2e/02_all_types.sh`
- Create: `tests/e2e/03_incremental.sh`
- Create: `tests/e2e/04_replace.sh`
- Create: `tests/e2e/05_upsert.sh`
- Modify: `tests/e2e.sh` — becomes orchestrator
- Modify: `justfile`

**Step 1: Create `tests/lib/helpers.sh`**

Extract shared utilities from the monolithic e2e.sh:

```bash
#!/usr/bin/env bash
# Shared E2E test helpers. Source this file, do not execute directly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONNECTOR_DIR="$PROJECT_ROOT/target/connectors"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
section() { echo -e "\n${CYAN}=== $* ===${NC}"; }

# Execute SQL against the test database, return trimmed result.
pg_exec() {
    docker compose -f "$COMPOSE_FILE" exec -T postgres \
        psql -U postgres -d rapidbyte_test -t -A -c "$1"
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
```

**Step 2: Create `tests/e2e/00_setup.sh`**

Build, stage connectors, start PG, verify seeds:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../lib/helpers.sh"

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

docker compose -f "$COMPOSE_FILE" up -d
for i in $(seq 1 30); do
    if pg_exec "SELECT 1" > /dev/null 2>&1; then break; fi
    [ "$i" -eq 30 ] && fail "PostgreSQL did not become ready in time"
    sleep 1
done
sleep 2
info "PostgreSQL is ready"

section "Setup: Verify Seed Data"

assert_eq_num "$(pg_exec 'SELECT COUNT(*) FROM public.users')" 3 "users seed count"
assert_eq_num "$(pg_exec 'SELECT COUNT(*) FROM public.orders')" 3 "orders seed count"
assert_eq_num "$(pg_exec 'SELECT COUNT(*) FROM public.all_types')" 2 "all_types seed count"
```

**Step 3: Split each existing E2E scenario into its own file**

Extract from the monolithic e2e.sh into `tests/e2e/01_full_refresh.sh`, `02_all_types.sh`, `03_incremental.sh`, `04_replace.sh`, `05_upsert.sh`. Each file sources `helpers.sh`, runs its scenario, uses `assert_eq`/`assert_eq_num` helpers.

**Step 4: Rewrite `tests/e2e.sh` as orchestrator**

```bash
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib/helpers.sh"

export RAPIDBYTE_CONNECTOR_DIR="$CONNECTOR_DIR"
PASSED=0; FAILED=0; SCENARIOS=()

cleanup() {
    info "Stopping Docker Compose..."
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    clean_state /tmp/rapidbyte_e2e_*.db
}
trap cleanup EXIT

# Setup
source "$SCRIPT_DIR/e2e/00_setup.sh"

# Run each scenario
for test_file in "$SCRIPT_DIR"/e2e/[0-9][0-9]_*.sh; do
    [ "$test_file" = "$SCRIPT_DIR/e2e/00_setup.sh" ] && continue
    test_name="$(basename "$test_file" .sh)"
    section "Running: $test_name"
    if bash "$test_file"; then
        PASSED=$((PASSED + 1))
        SCENARIOS+=("PASS: $test_name")
    else
        FAILED=$((FAILED + 1))
        SCENARIOS+=("FAIL: $test_name")
    fi
done

# Summary
section "E2E Test Summary"
for s in "${SCENARIOS[@]}"; do echo "  $s"; done
echo ""
echo "  Total: $((PASSED + FAILED)) | Passed: $PASSED | Failed: $FAILED"
[ "$FAILED" -gt 0 ] && exit 1
```

**Step 5: Update `justfile`**

Add `e2e-scenario` recipe:
```
e2e-scenario name:
    RAPIDBYTE_CONNECTOR_DIR=target/connectors bash tests/e2e/{{name}}.sh
```

**Step 6: Run to verify**

Run: `just e2e`
Expected: All 5 existing scenarios pass with same assertions.

**Step 7: Commit**

```bash
git add tests/lib/ tests/e2e/ tests/e2e.sh justfile
git commit -m "refactor(tests): split monolithic e2e.sh into modular test runner"
```

---

## Task 2: Add edge-case seed data fixture

**Files:**
- Create: `tests/fixtures/sql/edge_cases.sql`
- Modify: `docker-compose.yml`

**Step 1: Create `tests/fixtures/sql/edge_cases.sql`**

```sql
-- Edge case values for boundary testing.
CREATE TABLE IF NOT EXISTS edge_cases (
    id                SERIAL PRIMARY KEY,
    -- Integer boundaries
    col_smallint_min  SMALLINT,
    col_smallint_max  SMALLINT,
    col_int_min       INTEGER,
    col_int_max       INTEGER,
    col_bigint_min    BIGINT,
    col_bigint_max    BIGINT,
    -- Float special values
    col_real_nan      REAL,
    col_real_inf      REAL,
    col_real_neginf   REAL,
    col_real_tiny     REAL,
    col_double_nan    DOUBLE PRECISION,
    col_double_inf    DOUBLE PRECISION,
    col_double_neginf DOUBLE PRECISION,
    -- Text edge cases
    col_empty_text    TEXT,
    col_unicode       TEXT,
    col_long_text     TEXT,
    col_newlines      TEXT,
    -- Timestamp edge cases
    col_ts_epoch      TIMESTAMP,
    col_ts_far_future TIMESTAMP,
    -- Date edge cases
    col_date_epoch    DATE,
    col_date_max      DATE,
    -- JSON edge cases
    col_json_empty    JSON,
    col_json_array    JSON,
    col_json_nested   JSON,
    col_json_null_val JSON,
    -- Numeric edge cases
    col_numeric_zero  NUMERIC(18,6),
    col_numeric_neg   NUMERIC(18,6),
    col_numeric_large NUMERIC(18,6),
    -- Binary edge cases
    col_bytea_empty   BYTEA,
    col_bytea_zero    BYTEA
);

INSERT INTO edge_cases (
    col_smallint_min, col_smallint_max,
    col_int_min, col_int_max,
    col_bigint_min, col_bigint_max,
    col_real_nan, col_real_inf, col_real_neginf, col_real_tiny,
    col_double_nan, col_double_inf, col_double_neginf,
    col_empty_text, col_unicode, col_long_text, col_newlines,
    col_ts_epoch, col_ts_far_future,
    col_date_epoch, col_date_max,
    col_json_empty, col_json_array, col_json_nested, col_json_null_val,
    col_numeric_zero, col_numeric_neg, col_numeric_large,
    col_bytea_empty, col_bytea_zero
) VALUES (
    -32768, 32767,
    -2147483648, 2147483647,
    -9223372036854775808, 9223372036854775807,
    'NaN'::real, 'Infinity'::real, '-Infinity'::real, 1e-38::real,
    'NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision,
    '', E'\U0001F389\U0001F680 \u4E2D\u6587', repeat('x', 10000), E'line1\nline2\ttab',
    '1970-01-01 00:00:00', '9999-12-31 23:59:59',
    '1970-01-01', '9999-12-31',
    '{}', '[1,2,3]', '{"a":{"b":{"c":[1,null,"x"]}}}', '{"key":null}',
    0.000000, -999999999999.999999, 999999999999.999999,
    E'\\x', E'\\x00'
);
```

**Step 2: Add to docker-compose.yml**

Add `- ./tests/fixtures/sql/edge_cases.sql:/docker-entrypoint-initdb.d/03_edge_cases.sql:ro`

**Step 3: Commit**

```bash
git add tests/fixtures/sql/edge_cases.sql docker-compose.yml
git commit -m "test: add edge case seed data (boundaries, NaN, Unicode, epoch dates)"
```

---

## Task 3: Add edge cases E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_edge_cases.yaml`
- Create: `tests/e2e/06_edge_cases.sh`

**Step 1: Create pipeline YAML**

```yaml
version: "1.0"
pipeline: e2e_edge_cases

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: edge_cases
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw_edges
  write_mode: append

state:
  backend: sqlite
  connection: /tmp/rapidbyte_e2e_edges_state.db
```

**Step 2: Create `tests/e2e/06_edge_cases.sh`**

Verify:
- Row count = 1
- Integer boundaries: `col_smallint_min = -32768`, `col_smallint_max = 32767`, `col_int_min = -2147483648`, `col_int_max = 2147483647`, `col_bigint_min = -9223372036854775808`, `col_bigint_max = 9223372036854775807`
- Float special values: `col_real_nan = NaN`, `col_real_inf = Infinity`, `col_real_neginf = -Infinity`
- Empty string: `col_empty_text = ''`
- Unicode: `col_unicode` contains a rocket emoji character
- Long text length: `length(col_long_text) = 10000`
- Newlines survived: `col_newlines` contains `line1`
- Epoch timestamps: `col_ts_epoch = '1970-01-01 00:00:00'`
- Date boundaries: `col_date_epoch = '1970-01-01'`
- JSON edge cases: `col_json_empty::text = '{}'`, `col_json_array::text` contains `[1,2,3]`
- Numeric zero: `col_numeric_zero = 0.000000`
- Empty bytea: `col_bytea_empty = '\x'` (empty bytes)

**Step 3: Run to verify**

Run: `just e2e`
Expected: 06_edge_cases passes alongside existing scenarios.

**Step 4: Commit**

```bash
git add tests/fixtures/pipelines/e2e_edge_cases.yaml tests/e2e/06_edge_cases.sh
git commit -m "test: add edge case E2E (int boundaries, NaN, Unicode, epoch, empty)"
```

---

## Task 4: Add projection pushdown E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_projection.yaml`
- Create: `tests/e2e/07_projection.sh`

**Step 1: Create pipeline YAML with column selection**

```yaml
version: "1.0"
pipeline: e2e_projection

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: users
      sync_mode: full_refresh
      columns: [id, name]

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw_projection
  write_mode: append

state:
  backend: sqlite
  connection: /tmp/rapidbyte_e2e_projection_state.db
```

**Step 2: Create `tests/e2e/07_projection.sh`**

Verify:
- 3 rows in `raw_projection.users`
- Only 2 columns exist: `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='raw_projection' AND table_name='users'` = 2
- Column names are `id` and `name` (no `email`, no `created_at`)
- Data is correct: Alice, Bob, Carol exist

**Step 3: Commit**

```bash
git add tests/fixtures/pipelines/e2e_projection.yaml tests/e2e/07_projection.sh
git commit -m "test: add projection pushdown E2E (select subset of columns)"
```

---

## Task 5: Add COPY load method E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_copy_load.yaml`
- Create: `tests/e2e/08_copy_load.sh`

**Step 1: Create pipeline YAML with `load_method: copy`**

```yaml
version: "1.0"
pipeline: e2e_copy_load

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: all_types
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw_copy
    load_method: copy
  write_mode: append

state:
  backend: sqlite
  connection: /tmp/rapidbyte_e2e_copy_state.db
```

**Step 2: Create `tests/e2e/08_copy_load.sh`**

Verify:
- 2 rows in `raw_copy.all_types`
- Spot-check: integers, text, date, UUID, JSONB are correct (same assertions as 02_all_types but via COPY path)
- This validates the COPY text format serialization for timestamp, date, binary, all text-cast types

**Step 3: Commit**

```bash
git add tests/fixtures/pipelines/e2e_copy_load.yaml tests/e2e/08_copy_load.sh
git commit -m "test: add COPY load method E2E (all types via COPY FROM STDIN)"
```

---

## Task 6: Add schema evolution E2E test

**Files:**
- Create: `tests/e2e/09_schema_evolution.sh`
- Create: `tests/fixtures/pipelines/e2e_schema_evo.yaml`

**Step 1: Create pipeline YAML with schema evolution policies**

```yaml
version: "1.0"
pipeline: e2e_schema_evo

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: evo_test
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw_evo
  write_mode: replace
  schema_evolution:
    new_column: add
    removed_column: ignore
    type_change: coerce
    nullability_change: allow

state:
  backend: sqlite
  connection: /tmp/rapidbyte_e2e_evo_state.db
```

**Step 2: Create `tests/e2e/09_schema_evolution.sh`**

Test flow:
1. Create source table `evo_test` with (id SERIAL, name TEXT, score INT)
2. Insert 2 rows
3. Run pipeline: verify 2 rows, 3 columns in destination
4. ALTER TABLE: `ALTER TABLE evo_test ADD COLUMN tag TEXT DEFAULT 'new'`
5. Insert 1 more row (with tag)
6. Run pipeline again (full_refresh with replace): verify destination now has 4 columns (tag added), 3 rows, new row has tag='new'

This tests `new_column: add` policy.

**Step 3: Commit**

```bash
git add tests/e2e/09_schema_evolution.sh tests/fixtures/pipelines/e2e_schema_evo.yaml
git commit -m "test: add schema evolution E2E (new column added mid-pipeline)"
```

---

## Task 7: Add parallelism E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_parallel.yaml`
- Create: `tests/e2e/10_parallelism.sh`

**Step 1: Create pipeline YAML with parallelism > 1**

```yaml
version: "1.0"
pipeline: e2e_parallel

source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
  streams:
    - name: users
      sync_mode: full_refresh
    - name: orders
      sync_mode: full_refresh
    - name: all_types
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    schema: raw_parallel
  write_mode: append

resources:
  parallelism: 3

state:
  backend: sqlite
  connection: /tmp/rapidbyte_e2e_parallel_state.db
```

**Step 2: Create `tests/e2e/10_parallelism.sh`**

Verify:
- All 3 tables exist in `raw_parallel` schema
- Row counts: users=3, orders=3, all_types=2 (same as source)
- State DB has 1 completed run
- No data corruption from concurrent writes (spot-check Alice's email, max order)

**Step 3: Commit**

```bash
git add tests/fixtures/pipelines/e2e_parallel.yaml tests/e2e/10_parallelism.sh
git commit -m "test: add parallelism E2E (3 concurrent streams)"
```

---

## Task 8: Add comprehensive Network ACL unit tests

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/network_acl.rs`

**Step 1: Add tests for `NetworkAcl::allows()` with exact hosts and wildcards**

```rust
#[test]
fn acl_exact_host_match() {
    let mut acl = NetworkAcl::default();
    acl.add_host("db.example.com");
    assert!(acl.allows("db.example.com"));
    assert!(!acl.allows("other.example.com"));
    assert!(!acl.allows("example.com"));
}

#[test]
fn acl_wildcard_suffix_match() {
    let mut acl = NetworkAcl::default();
    acl.add_host("*.example.com");
    assert!(acl.allows("db.example.com"));
    assert!(acl.allows("foo.bar.example.com"));
    assert!(!acl.allows("example.com")); // no leading dot
    assert!(!acl.allows("evil.com"));
}

#[test]
fn acl_star_allows_all() {
    let mut acl = NetworkAcl::default();
    acl.add_host("*");
    assert!(acl.allows("anything.example.com"));
    assert!(acl.allows("127.0.0.1"));
}

#[test]
fn acl_allow_all_constructor() {
    let acl = NetworkAcl::allow_all();
    assert!(acl.allows("anything"));
}

#[test]
fn acl_empty_host_denied() {
    let acl = NetworkAcl::default();
    assert!(!acl.allows(""));
    assert!(!acl.allows("  "));
}

#[test]
fn acl_case_insensitive() {
    let mut acl = NetworkAcl::default();
    acl.add_host("DB.Example.COM");
    assert!(acl.allows("db.example.com"));
    assert!(acl.allows("DB.EXAMPLE.COM"));
}

#[test]
fn acl_host_with_port_stripped() {
    let mut acl = NetworkAcl::default();
    acl.add_host("db.example.com:5432");
    assert!(acl.allows("db.example.com"));
}
```

**Step 2: Add tests for `extract_host_from_url()`**

```rust
#[test]
fn extract_host_from_url_basic() {
    assert_eq!(
        extract_host_from_url("postgres://db.example.com:5432/mydb"),
        Some("db.example.com".to_string())
    );
    assert_eq!(
        extract_host_from_url("https://user:pass@api.example.com/path"),
        Some("api.example.com".to_string())
    );
    assert_eq!(
        extract_host_from_url("postgres://[::1]:5432/mydb"),
        Some("::1".to_string())
    );
    assert_eq!(extract_host_from_url("not-a-url"), None);
}
```

**Step 3: Add tests for `collect_runtime_hosts()`**

```rust
#[test]
fn collect_runtime_hosts_extracts_host_keys() {
    let config = serde_json::json!({
        "host": "db.example.com",
        "port": 5432,
        "nested": { "hostname": "nested.example.com" }
    });
    let mut hosts = std::collections::HashSet::new();
    collect_runtime_hosts(&config, &mut hosts);
    assert!(hosts.contains("db.example.com"));
    assert!(hosts.contains("nested.example.com"));
}

#[test]
fn collect_runtime_hosts_extracts_url_keys() {
    let config = serde_json::json!({
        "connection_url": "postgres://db.example.com:5432/mydb"
    });
    let mut hosts = std::collections::HashSet::new();
    collect_runtime_hosts(&config, &mut hosts);
    assert!(hosts.contains("db.example.com"));
}
```

**Step 4: Add tests for `derive_network_acl()`**

```rust
#[test]
fn derive_acl_no_permissions_allows_all() {
    let config = serde_json::json!({});
    let acl = derive_network_acl(None, &config);
    assert!(acl.allows("anything.com"));
}

#[test]
fn derive_acl_with_allowed_domains() {
    use rapidbyte_types::manifest::{Permissions, NetworkPermissions};
    let perms = Permissions {
        network: NetworkPermissions {
            allowed_domains: Some(vec!["db.example.com".to_string()]),
            allow_runtime_config_domains: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let config = serde_json::json!({});
    let acl = derive_network_acl(Some(&perms), &config);
    assert!(acl.allows("db.example.com"));
    assert!(!acl.allows("other.com"));
}

#[test]
fn derive_acl_with_runtime_config_domains() {
    use rapidbyte_types::manifest::{Permissions, NetworkPermissions};
    let perms = Permissions {
        network: NetworkPermissions {
            allowed_domains: None,
            allow_runtime_config_domains: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let config = serde_json::json!({"host": "dynamic.example.com", "port": 5432});
    let acl = derive_network_acl(Some(&perms), &config);
    assert!(acl.allows("dynamic.example.com"));
    assert!(!acl.allows("other.com"));
}
```

**Step 5: Run tests**

Run: `cargo test -p rapidbyte-core -- network_acl`
Expected: All new tests pass.

**Step 6: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/network_acl.rs
git commit -m "test(core): add comprehensive Network ACL unit tests"
```

---

## Task 9: Add pipeline validator tests for all config combinations

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/validator.rs`

**Step 1: Add tests for schema_evolution, on_data_error, load_method, compression**

```rust
#[test]
fn cdc_without_replication_slot_fails() {
    // Build pipeline with CDC sync mode but no replication_slot in source config
    // Expected: validation error mentioning replication_slot
}

#[test]
fn replace_mode_with_incremental_warns_or_works() {
    // Replace write mode + incremental sync mode
    // Verify behavior matches implementation
}

#[test]
fn valid_schema_evolution_policies() {
    // All valid combinations of schema_evolution fields pass validation
}

#[test]
fn on_data_error_dlq_is_valid() {
    // Verify on_data_error: dlq passes validation
}

#[test]
fn parallelism_zero_fails() {
    // resources.parallelism: 0 should fail validation
}

#[test]
fn valid_compression_values() {
    // lz4, zstd, null (or absent) all pass
}
```

**Step 2: Run tests**

Run: `cargo test -p rapidbyte-core -- validator`
Expected: All pass.

**Step 3: Commit**

```bash
git add crates/rapidbyte-core/src/pipeline/validator.rs
git commit -m "test(core): add pipeline validator tests for all config combinations"
```

---

## Task 10: Add connector type-mapping roundtrip tests

**Files:**
- Modify: `connectors/source-postgres/src/reader/arrow_encode.rs`
- Modify: `connectors/dest-postgres/src/batch/copy_format.rs`

**Step 1: Add unit tests in `arrow_encode.rs` for every type's NULL handling**

Add tests that construct Arrow arrays with nulls in each type and verify `build_arrow_schema` handles them correctly (tested via manual array construction since we can't use live PG rows in unit tests).

**Step 2: Add COPY format tests for timestamp, date, binary**

```rust
#[test]
fn copy_format_timestamp_microseconds() {
    // 2024-01-15 10:30:00 = specific microsecond value
    // Verify format is "2024-01-15 10:30:00.000000"
}

#[test]
fn copy_format_date32_epoch() {
    // 0 days since epoch = "1970-01-01"
}

#[test]
fn copy_format_binary_hex() {
    // [0xDE, 0xAD, 0xBE, 0xEF] -> "\\xdeadbeef"
}

#[test]
fn copy_format_timestamp_null() {
    // NULL timestamp -> "\\N"
}
```

**Step 3: Verify compilation**

Run: `cd connectors/source-postgres && cargo check --tests && cd ../dest-postgres && cargo check --tests`
Expected: Compiles cleanly.

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/reader/arrow_encode.rs \
        connectors/dest-postgres/src/batch/copy_format.rs
git commit -m "test(connectors): add type-mapping roundtrip and COPY format unit tests"
```

---

## Task 11: Add DLQ E2E test

**Files:**
- Create: `tests/fixtures/sql/dlq_test.sql`
- Create: `tests/fixtures/pipelines/e2e_dlq.yaml`
- Create: `tests/e2e/11_dlq.sh`

**Step 1: Create a scenario where records fail**

Create a source table with valid data and run with `on_data_error: dlq`:

```sql
CREATE TABLE IF NOT EXISTS dlq_source (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score INTEGER
);
INSERT INTO dlq_source (name, score) VALUES ('Alice', 100), ('Bob', NULL), ('Carol', 50);
```

Pipeline with DLQ:
```yaml
destination:
  on_data_error: dlq
```

**Step 2: Verify DLQ pipeline completes**

After running: check that the pipeline completed successfully and the data was written. Check `sqlite3 /tmp/...state.db "SELECT COUNT(*) FROM dlq_records"` for any DLQ entries. This validates that `on_data_error: dlq` is accepted and the pipeline runs correctly.

Note: This test's exact DLQ record count depends on whether the connector produces write errors. The primary value is configuration coverage.

**Step 3: Commit**

```bash
git add tests/fixtures/sql/dlq_test.sql tests/fixtures/pipelines/e2e_dlq.yaml tests/e2e/11_dlq.sh
git commit -m "test: add DLQ E2E test (on_data_error: dlq configuration)"
```

---

## Task 12: Add compression E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_lz4.yaml`
- Create: `tests/fixtures/pipelines/e2e_zstd.yaml`
- Create: `tests/e2e/12_compression.sh`

**Step 1: Create pipeline YAMLs with compression**

Same as `e2e_single_pg.yaml` but with `resources: { compression: lz4 }` and `resources: { compression: zstd }`.

**Step 2: Create `tests/e2e/12_compression.sh`**

Run both pipelines to different dest schemas. Verify data integrity (row counts, spot-check values) is identical to uncompressed. This validates that Arrow IPC compression/decompression does not corrupt data.

**Step 3: Commit**

```bash
git add tests/fixtures/pipelines/e2e_lz4.yaml tests/fixtures/pipelines/e2e_zstd.yaml \
        tests/e2e/12_compression.sh
git commit -m "test: add compression E2E tests (LZ4 and Zstd data integrity)"
```

---

## Task 13: Add upsert with all types E2E test

**Files:**
- Create: `tests/fixtures/pipelines/e2e_upsert_types.yaml`
- Create: `tests/e2e/13_upsert_types.sh`

**Step 1: Create pipeline YAML**

Upsert `all_types` table with `primary_key: [id]`. Run twice. Second run should not duplicate rows. Verifies that upsert works correctly with timestamp, date, bytea, json, numeric, uuid columns.

**Step 2: Verify**

- After run 1: 2 rows
- After run 2: still 2 rows (no duplicates)

**Step 3: Commit**

```bash
git add tests/fixtures/pipelines/e2e_upsert_types.yaml tests/e2e/13_upsert_types.sh
git commit -m "test: add upsert E2E with all PG types"
```

---

## Task 14: Add incremental with timestamp cursor E2E test

**Files:**
- Create: `tests/e2e/14_incremental_timestamp.sh`
- Create: `tests/fixtures/pipelines/e2e_incr_timestamp.yaml`

**Step 1: Create test**

Use `created_at` (TIMESTAMP) as cursor field instead of integer `id`. This verifies that timestamp cursor tracking and comparison works end-to-end.

1. Run 1: Read all 3 users (cursor = max created_at)
2. Insert 1 more user with `created_at = NOW()`
3. Run 2: Only new user read (cursor comparison with timestamp)
4. Verify 4 total rows in destination

**Step 2: Commit**

```bash
git add tests/e2e/14_incremental_timestamp.sh tests/fixtures/pipelines/e2e_incr_timestamp.yaml
git commit -m "test: add incremental sync E2E with timestamp cursor"
```

---

## Execution Order and Dependencies

```
Task 1  (refactor tests/ structure)
  |
  +---> Task 2  (edge case seed data)
  |      |
  |      +---> Task 3  (edge cases E2E)
  |
  +---> Task 4  (projection pushdown E2E)
  |
  +---> Task 5  (COPY load method E2E)
  |
  +---> Task 6  (schema evolution E2E)
  |
  +---> Task 7  (parallelism E2E)
  |
  +---> Task 8  (network ACL unit tests)     <-- independent, host crate only
  |
  +---> Task 9  (validator tests)            <-- independent, host crate only
  |
  +---> Task 10 (connector type roundtrip)   <-- independent, connector only
  |
  +---> Task 11 (DLQ E2E)
  |
  +---> Task 12 (compression E2E)
  |
  +---> Task 13 (upsert all types E2E)
  |
  +---> Task 14 (incremental timestamp cursor)
```

Tasks 8, 9, 10 are pure unit tests. No Docker dependency. Can be developed and run independently. All other tasks depend on Task 1 (the test framework refactor).

## Summary

| Task | Type | What it tests | Priority |
|------|------|---------------|----------|
| 1 | Refactor | Modular test runner, shared helpers | P0 |
| 2 | Fixture | Edge case data (boundaries, NaN, Unicode, epoch) | P0 |
| 3 | E2E | Edge case roundtrip | P0 |
| 4 | E2E | Projection pushdown (column selection) | P0 |
| 5 | E2E | COPY load method (all types via COPY) | P0 |
| 6 | E2E | Schema evolution (new_column: add) | P0 |
| 7 | E2E | Parallelism (3 concurrent streams) | P1 |
| 8 | Unit | Network ACL (allows, wildcards, derive) | P0 |
| 9 | Unit | Pipeline validator (all config combos) | P1 |
| 10 | Unit | Connector type-mapping roundtrip | P1 |
| 11 | E2E | DLQ (on_data_error: dlq) | P1 |
| 12 | E2E | Compression (LZ4/Zstd data integrity) | P1 |
| 13 | E2E | Upsert with all PG types | P1 |
| 14 | E2E | Incremental with timestamp cursor | P0 |
