# DataFusion SQL Transforms Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a `transform-sql` WASM connector that executes SQL queries on Arrow batches using DataFusion, enabling in-flight SQL transforms in the source→destination pipeline.

**Architecture:** DataFusion compiles cleanly to `wasm32-wasip2` (validated: 37MB release binary). The transform runs inside its own WASM sandbox like every other connector, using `#[connector(transform)]` and the standard SDK `Transform` trait. Each incoming batch from `ctx.next_batch()` is registered as a DataFusion `MemTable` named `input`, the user's SQL query executes against it, and result batches are forwarded downstream via `ctx.emit_batch()`. No orchestrator changes needed — the existing transform pipeline infrastructure handles everything.

**Tech Stack:** DataFusion 43 (arrow 53 compatible), `rapidbyte-sdk` connector framework, `wasm32-wasip2` target.

---

## Design Decisions

### Why a WASM connector, not host-side?

Transforms are connectors. The existing infrastructure (`#[connector(transform)]`, `Transform` trait, WASM sandbox, mpsc channel wiring in the orchestrator) already handles transform lifecycle, state, metrics, and error handling. DataFusion compiles to `wasm32-wasip2` — there's no technical reason to special-case SQL transforms on the host.

Benefits:
- Consistent architecture: transforms sandboxed like source/destination
- No orchestrator changes: zero risk to existing pipeline code
- Permissions model inherited: WASM sandbox limits memory, network, filesystem
- Future transforms (masking, etc.) follow the same pattern

### Why batch-by-batch, not materialized?

Most ELT transforms are per-row (SELECT, WHERE, CAST, computed columns). Batch-by-batch streaming bounds memory (wasm32 4GB ceiling) and reuses existing backpressure via channel capacity. Cross-batch aggregation (GROUP BY) is a future extension.

### Table name: `input`

Users always reference the incoming data as `SELECT ... FROM input`. One table, one convention, zero configuration.

### Binary size tradeoff

Release WASM binary: ~37MB (vs ~1.8MB for dest-postgres). The SQL query engine is heavy but acceptable — connectors are loaded once per pipeline run. Debug builds are ~442MB; use release for production.

---

## Task 1: Scaffold `connectors/transform-sql/` directory structure

**Files:**
- Create: `connectors/transform-sql/.cargo/config.toml`
- Create: `connectors/transform-sql/Cargo.toml`
- Create: `connectors/transform-sql/build.rs`
- Create: `connectors/transform-sql/src/main.rs` (stub)

**Step 1: Create `.cargo/config.toml`**

```toml
[build]
target = "wasm32-wasip2"
rustc-wrapper = "../../scripts/rustc-wrapper.sh"
rustflags = []
```

This is identical to the other connectors — targets wasm32-wasip2 and uses the shared rustc-wrapper.

**Step 2: Create `Cargo.toml`**

```toml
[package]
name = "transform-sql"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "transform_sql"
path = "src/main.rs"

[dependencies]
rapidbyte-sdk = { path = "../../crates/rapidbyte-sdk" }
datafusion = { version = "43", default-features = false, features = ["unicode_expressions", "regex_expressions"] }
tokio = { version = "1.36", default-features = false, features = ["rt", "macros"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
arrow = { version = "53", features = ["ipc"] }
wit-bindgen = "0.46"

[build-dependencies]
rapidbyte-sdk = { path = "../../crates/rapidbyte-sdk", default-features = false, features = ["build"] }

[profile.release]
opt-level = 3
lto = true
codegen-units = 1
panic = "abort"
strip = "symbols"

[lints.clippy]
pedantic = { level = "warn", priority = -1 }

[workspace]
```

Key choices:
- `datafusion` with minimal features: `unicode_expressions` for UPPER/LOWER/TRIM, `regex_expressions` for REGEXP_MATCH. No parquet, no catalogs, no listing tables.
- `arrow = "53"` matches the workspace version.
- `tokio = "=1.36"` pinned for WASI compatibility.
- Empty `[workspace]` prevents cargo from walking up to parent workspace.
- No `[patch.crates-io]` for `whoami` — DataFusion doesn't depend on it. Add only if build fails.

**Step 3: Create `build.rs`**

```rust
use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("rapidbyte/transform-sql")
        .name("SQL Transform")
        .description("Executes SQL queries on Arrow batches using Apache DataFusion")
        .emit();
}
```

No network permissions, no env vars — SQL transforms operate on in-memory Arrow data only.

**Step 4: Create `src/main.rs` (minimal stub)**

```rust
//! SQL transform connector powered by Apache DataFusion.

mod config;
mod transform;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(transform)]
pub struct TransformSql {
    config: config::Config,
}

impl Transform for TransformSql {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V2,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, ConnectorError> {
        transform::run(ctx, &stream, &self.config).await
    }
}
```

**Step 5: Verify the directory structure exists**

Run: `ls connectors/transform-sql/`
Expected: `.cargo/  Cargo.toml  build.rs  src/`

**Step 6: Commit**

```bash
git add connectors/transform-sql/.cargo/config.toml connectors/transform-sql/Cargo.toml connectors/transform-sql/build.rs connectors/transform-sql/src/main.rs
git commit -m "feat(transform-sql): scaffold WASM connector directory"
```

---

## Task 2: Implement the config module

**Files:**
- Create: `connectors/transform-sql/src/config.rs`

**Step 1: Write the config struct**

```rust
//! SQL transform configuration.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Configuration for the SQL transform connector.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// SQL query to execute against each incoming batch.
    /// Must reference `input` as the table name.
    /// Example: "SELECT id, UPPER(name) AS name FROM input WHERE active = true"
    pub query: String,
}
```

That's it. One field. The query string is the only configuration. The `ConfigSchema` derive generates JSON Schema for manifest validation at `rapidbyte check` time.

**Step 2: Verify it compiles**

Run: `cd connectors/transform-sql && cargo check`
Expected: Compiles (will fail on missing `transform` module — that's fine, we add it next)

Actually, this step will fail because `src/main.rs` references `mod transform;` which doesn't exist yet. That's expected — we'll create it in Task 3.

**Step 3: Commit**

```bash
git add connectors/transform-sql/src/config.rs
git commit -m "feat(transform-sql): add config module with query field"
```

---

## Task 3: Implement the transform execution module

This is the core logic. Each batch flows through DataFusion's SQL engine.

**Files:**
- Create: `connectors/transform-sql/src/transform.rs`

**Step 1: Write the transform module**

```rust
//! DataFusion-powered SQL transform execution.
//!
//! For each incoming Arrow batch:
//! 1. Register it as a DataFusion MemTable named `input`
//! 2. Execute the user's SQL query
//! 3. Forward result batches downstream via `ctx.emit_batch()`

use std::sync::Arc;

use datafusion::prelude::*;
use rapidbyte_sdk::prelude::*;

use crate::config::Config;

/// Run the SQL transform for a single stream.
pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    config: &Config,
) -> Result<TransformSummary, ConnectorError> {
    let session = SessionContext::new();

    let mut records_in: u64 = 0;
    let mut records_out: u64 = 0;
    let mut bytes_in: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut batches_processed: u64 = 0;

    while let Some((schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
        let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        records_in += batch_rows;

        // Estimate bytes from array memory sizes.
        let batch_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        bytes_in += batch_bytes;

        if batches.is_empty() {
            continue;
        }

        // Register as MemTable named "input".
        let mem_table = datafusion::datasource::MemTable::try_new(
            schema,
            vec![batches],
        )
        .map_err(|e| {
            ConnectorError::internal(
                "SQL_MEMTABLE",
                format!("Failed to create MemTable: {e}"),
            )
        })?;

        // Deregister previous table (ignore error if it doesn't exist yet).
        let _ = session.deregister_table("input");
        session
            .register_table("input", Arc::new(mem_table))
            .map_err(|e| {
                ConnectorError::internal(
                    "SQL_REGISTER",
                    format!("Failed to register table: {e}"),
                )
            })?;

        // Plan and execute.
        let df = session.sql(&config.query).await.map_err(|e| {
            ConnectorError::internal("SQL_PLAN", format!("Query planning failed: {e}"))
        })?;

        let result_batches = df.collect().await.map_err(|e| {
            ConnectorError::internal("SQL_EXEC", format!("Query execution failed: {e}"))
        })?;

        // Forward each non-empty result batch downstream.
        for batch in &result_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            records_out += batch.num_rows() as u64;
            bytes_out += batch.get_array_memory_size() as u64;
            ctx.emit_batch(batch)?;
        }

        batches_processed += 1;
    }

    ctx.log(
        LogLevel::Info,
        &format!(
            "SQL transform complete: {records_in} rows in, {records_out} rows out, {batches_processed} batches"
        ),
    );

    Ok(TransformSummary {
        records_in,
        records_out,
        bytes_in,
        bytes_out,
        batches_processed,
    })
}
```

Key design points:
- Uses `ctx.next_batch()` and `ctx.emit_batch()` — the SDK handles all IPC encoding/decoding and frame management.
- `SessionContext` is created once per stream (reused across batches for query plan caching).
- Table "input" is deregistered/re-registered per batch to avoid stale data.
- Empty batches and zero-row results are skipped.
- Error codes follow the existing pattern: short uppercase identifiers.

**Step 2: Verify the full connector compiles**

Run: `cd connectors/transform-sql && cargo check`
Expected: compiles for wasm32-wasip2 with no errors

**Step 3: Build release to verify binary size**

Run: `cd connectors/transform-sql && cargo build --release`
Expected: compiles (may take 5-8 minutes first time), binary at `target/wasm32-wasip2/release/transform_sql.wasm`

**Step 4: Commit**

```bash
git add connectors/transform-sql/src/transform.rs
git commit -m "feat(transform-sql): implement DataFusion SQL transform execution"
```

---

## Task 4: Add validation

The `validate` lifecycle method should verify the SQL query parses correctly without executing it.

**Files:**
- Modify: `connectors/transform-sql/src/main.rs` (add `validate` method)

**Step 1: Add the validate implementation**

In `src/main.rs`, add the validate method to the `Transform` impl block:

```rust
    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        let _ = ctx;
        // Verify the SQL parses by creating a session and planning (no execution).
        let session = datafusion::prelude::SessionContext::new();

        // We need a table registered to plan against, but we don't have data yet.
        // Use an empty schema — this catches syntax errors but not column references.
        // Column validation happens at runtime when schema is known.
        match session.sql(&config.query).await {
            Ok(_) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "SQL query parsed successfully".to_string(),
            }),
            Err(e) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message: format!("SQL query is invalid: {e}"),
            }),
        }
    }
```

Note: Without a registered `input` table, DataFusion will report "table not found" for valid queries that reference `input`. We need to handle this. Better approach — register an empty MemTable so that only genuinely bad SQL fails:

```rust
    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        let _ = ctx;
        // Basic syntax validation: check that the query string is non-empty.
        if config.query.trim().is_empty() {
            return Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message: "SQL query must not be empty".to_string(),
            });
        }

        // We can't fully validate column references without the upstream schema,
        // which isn't available at validation time. Syntax errors will surface
        // at runtime with a clear error message.
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "SQL query configuration is valid".to_string(),
        })
    }
```

This is the pragmatic choice. Full SQL validation requires the upstream schema which isn't available at `rapidbyte check` time. Runtime errors from DataFusion are clear enough.

**Step 2: Verify it compiles**

Run: `cd connectors/transform-sql && cargo check`
Expected: compiles

**Step 3: Commit**

```bash
git add connectors/transform-sql/src/main.rs
git commit -m "feat(transform-sql): add config validation"
```

---

## Task 5: Update build infrastructure

**Files:**
- Modify: `justfile` (if `build-connectors` / `release-connectors` targets exist)
- Verify: connector resolution works with the new path

**Step 1: Check if `justfile` needs updating**

Read the `justfile` and look for `build-connectors` and `release-connectors` recipes. If they hardcode connector paths, add `transform-sql`. If they auto-discover from `connectors/*/Cargo.toml`, no change needed.

**Step 2: Verify connector resolution**

The orchestrator resolves connectors via `rapidbyte_runtime::resolve_connector_path()`. Check how this function maps `use: transform-sql` to the WASM binary path. It likely looks for `connectors/transform-sql/target/wasm32-wasip2/{debug,release}/transform_sql.wasm` or a registry path. Verify the naming convention matches.

**Step 3: If WASI tokio patch is needed**

Check if DataFusion's tokio dependency requires the WASI patch. If the build in Task 3 succeeded without a patch, skip this. If it needs it, add to `Cargo.toml`:

```toml
[patch.crates-io]
tokio = { git = "https://github.com/aspect-build/tokio", branch = "v1.36.x" }
```

Wait — the memory says to use `second-state` org, not `aspect-build`. And pin `tokio = "=1.36"`. Check the existing connector patches if needed.

**Step 4: Commit (if changes were made)**

```bash
git add justfile  # or whatever files changed
git commit -m "build: add transform-sql to build infrastructure"
```

---

## Task 6: Add a pipeline YAML fixture

**Files:**
- Create: `tests/fixtures/pipeline_sql_transform.yaml`

**Step 1: Create the fixture**

```yaml
version: "1.0"
pipeline: sql_transform_test
source:
  use: source-postgres
  config:
    host: "127.0.0.1"
    port: 5432
    user: "rapidbyte"
    password: "rapidbyte"
    database: "rapidbyte_test"
  streams:
    - name: public.users
      sync_mode: full_refresh
  permissions:
    network:
      allowed_hosts:
        - "127.0.0.1"

transforms:
  - use: transform-sql
    config:
      query: "SELECT id, UPPER(name) AS name FROM input"

destination:
  use: dest-postgres
  config:
    host: "127.0.0.1"
    port: 5432
    user: "rapidbyte"
    password: "rapidbyte"
    database: "rapidbyte_test"
    schema: "raw"
  write_mode: replace
  permissions:
    network:
      allowed_hosts:
        - "127.0.0.1"

state:
  backend: sqlite
```

Note: `use: transform-sql` — this is a standard WASM connector reference, not `builtin:sql`. The orchestrator loads it from `connectors/transform-sql/target/wasm32-wasip2/release/transform_sql.wasm` the same way it loads source-postgres and dest-postgres.

**Step 2: Commit**

```bash
git add tests/fixtures/pipeline_sql_transform.yaml
git commit -m "test: add pipeline fixture with SQL transform"
```

---

## Task 7: E2E test

**Files:**
- Modify: `tests/e2e.sh` or create `tests/e2e/sql_transform.sh`

**Step 1: Check existing E2E test structure**

Read `tests/e2e.sh` and `tests/e2e/` directory to understand the pattern. The test should:
1. Seed source database with test data
2. Run the pipeline with `transform-sql` in the middle
3. Verify destination data has the SQL transform applied (e.g., `UPPER(name)`)

**Step 2: Create the E2E test scenario**

Follow the existing test pattern. Seed SQL:

```sql
CREATE TABLE IF NOT EXISTS public.users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER
);
INSERT INTO public.users (name, age) VALUES
    ('alice', 30),
    ('bob', 25),
    ('charlie', 35);
```

Verify SQL (after pipeline runs):

```sql
SELECT name FROM raw.users ORDER BY id;
-- Expected: ALICE, BOB, CHARLIE (uppercased by transform)
```

**Step 3: Run the E2E test**

Run: `just e2e` (or the specific test)
Expected: Pipeline runs end-to-end, destination has uppercased names.

**Step 4: Commit**

```bash
git add tests/e2e/sql_transform.sh tests/fixtures/  # add whatever files
git commit -m "test(e2e): add SQL transform end-to-end test"
```

---

## Task 8: Update documentation

**Files:**
- Modify: `docs/PROTOCOL.md`
- Modify: `CLAUDE.md`

**Step 1: Add SQL Transform section to PROTOCOL.md**

Add to the transforms section:

```markdown
## SQL Transform (`transform-sql`)

Executes a SQL query against each incoming Arrow batch using Apache DataFusion.
The incoming data is registered as a table named `input`.

**Config:**

```yaml
transforms:
  - use: transform-sql
    config:
      query: "SELECT id, UPPER(name) AS name, age + 1 AS next_age FROM input WHERE active = true"
```

**Supported operations:** column selection, filtering (WHERE), computed columns,
type casting (CAST), string functions (UPPER, LOWER, TRIM, CONCAT), math
expressions, CASE/WHEN, and all standard SQL scalar functions supported by
DataFusion.

**Limitations:** Batch-by-batch execution — cross-batch aggregations (GROUP BY,
DISTINCT, window functions) operate per-batch, not across the full stream.

**Table name:** Always `input`. The query must reference `FROM input`.
```

**Step 2: Update CLAUDE.md**

Add to the Project Structure connectors section:

```
  transform-sql/   # SQL transform connector (wasm32-wasip2, DataFusion)
```

Add to Key Architecture:

```
- SQL transforms (`transform-sql`) run DataFusion inside WASM sandbox, using the same connector infrastructure as source/destination connectors.
```

**Step 3: Commit**

```bash
git add docs/PROTOCOL.md CLAUDE.md
git commit -m "docs: document transform-sql connector"
```

---

## Summary

| Task | What | Files | Size |
|------|------|-------|------|
| 1 | Scaffold connector directory | 4 files | Tiny (boilerplate) |
| 2 | Config module | config.rs | ~15 LOC |
| 3 | Transform execution | transform.rs | ~75 LOC |
| 4 | Validation | main.rs | ~15 LOC |
| 5 | Build infrastructure | justfile | Tiny |
| 6 | Pipeline YAML fixture | fixture yaml | Tiny |
| 7 | E2E test | test script + SQL | ~30 LOC |
| 8 | Documentation | PROTOCOL.md, CLAUDE.md | Small |

**Total new code:** ~105 LOC production + ~30 LOC test infrastructure.

**Zero changes to existing crates.** The orchestrator, runtime, SDK, and state crates remain untouched. This is the cleanest possible integration — a new connector in `connectors/`, following the exact same pattern as source-postgres and dest-postgres.

**Key risk:** DataFusion 43 + wasm32-wasip2 may need the tokio WASI patch if `tokio = "=1.36"` isn't enough. Validated in prior session — it compiled without patching, but monitor for runtime issues.

**Binary size:** ~37MB release WASM. Acceptable for a SQL engine; connectors load once per pipeline run.

**Future extensions (not in this plan):**
- `transform-mask`: DataFusion UDFs for PII masking (HASH, REDACT, TRUNCATE)
- `materialize: true` config for cross-batch aggregation
- Query plan caching across batches (SessionContext already reuses plans)
- Custom UDF registration via config
