# Refactoring Guide: Connectors (`source-postgres` & `dest-postgres`)

> Organized around [`REFACTORING_PRINCIPLES.md`](../../REFACTORING_PRINCIPLES.md).
> Each item is tagged with a **priority tier** and **effort estimate**.

## Legend

| Tag | Meaning |
|-----|---------|
| **P0** | Do first — blocks other refactors or has outsized safety/quality impact |
| **P1** | Important — clear value, moderate risk |
| **P2** | Polish — nice-to-have, low risk, do when touching nearby code |
| **S** | Small — < 1 hour |
| **M** | Medium — 1-4 hours |
| **L** | Large — half day or more |

---

## File Inventory

### `source-postgres` (7 files, ~1,921 lines)

| File | Lines | Domain | Tests? |
|------|------:|--------|--------|
| `cdc.rs` | 815 | CDC via logical replication slots, test_decoding parser | Yes (283 lines, 17 tests) |
| `reader.rs` | 805 | Full-refresh & incremental streaming reads | No |
| `schema.rs` | 101 | PG type mapping, catalog discovery | No |
| `main.rs` | 79 | `Source` trait impl, dispatch to reader/cdc | No |
| `client.rs` | 59 | PG connection, validation | No |
| `identifier.rs` | 32 | PG identifier validation | No |
| `config.rs` | 30 | Connection config struct, serde defaults | No |

### `dest-postgres` (7 files, ~2,231 lines)

| File | Lines | Domain | Tests? |
|------|------:|--------|--------|
| `batch.rs` | 831 | Arrow decode, INSERT/COPY/fallback write paths | No |
| `writer.rs` | 614 | WriteSession lifecycle, watermark, checkpoint | No |
| `ddl.rs` | 566 | Table creation, schema drift detection, staging swap | No |
| `client.rs` | 76 | PG connection, validation (+ schema check) | No |
| `main.rs` | 59 | `Destination` trait impl | No |
| `config.rs` | 53 | Connection config, load_method validation | No |
| `identifier.rs` | 32 | PG identifier validation | No |

**Totals:** 14 files, ~4,152 lines, 17 tests (all in `cdc.rs`).

---

## 1. Structural & Type-Level Principles

> *REFACTORING_PRINCIPLES.md* &sect;2 — Make invalid states unrepresentable.

### 1.1 Replace `validate_pg_identifier` with `pg_escape::quote_identifier` &mdash; P0 / S

**Files:** `source-postgres/src/identifier.rs` (32 lines), `dest-postgres/src/identifier.rs` (32 lines), plus all call sites.

**Problem:** Both connectors contain a hand-rolled 30-line `validate_pg_identifier()` function that checks identifier length and characters, then the calling code still wraps identifiers in `"..."` manually. This validate-then-quote two-step pattern is fragile — it rejects identifiers with special characters that PostgreSQL actually supports when properly quoted (e.g., reserved words, mixed case, Unicode):

```rust
// Current pattern (both connectors):
validate_pg_identifier(&ctx.stream_name)?;
let sql = format!("SELECT {} FROM \"{}\"", col_list, ctx.stream_name);
```

**After:** Replace with [`pg_escape::quote_identifier()`](https://docs.rs/pg_escape) which implements PostgreSQL's `quote_ident()` behavior — handles double-quoting, escapes embedded quotes, handles reserved words correctly. Each connector adds `pg_escape` as its own dependency (connectors are self-contained):

```rust
// After (both connectors):
use pg_escape::quote_identifier;
let sql = format!("SELECT {} FROM {}", col_list, quote_identifier(&ctx.stream_name));
```

Delete both `identifier.rs` files and `mod identifier;` from both `main.rs`.

**Call sites to update:**
- `source-postgres`: `reader.rs` (3 sites), `cdc.rs` (2 sites)
- `dest-postgres`: `writer.rs` (2 sites), `ddl.rs` (5 sites), `batch.rs` (1 site)

**Why P0:** Eliminates 64 lines of duplicated hand-rolled validation, replaces a validate-then-quote two-step with a single correct quoting call, and handles edge cases (reserved words, Unicode) that the hand-rolled version rejects. Each connector independently owns the `pg_escape` dependency — no shared crate needed.

**Migration strategy:**
1. Add `pg_escape = "0.2"` to both connector `Cargo.toml` files.
2. Replace all `validate_pg_identifier(&name)?; format!("\"{}\"", name)` patterns with `quote_identifier(&name)`.
3. Remove `mod identifier;` from both `main.rs`.
4. Delete both `identifier.rs` files.
5. `cd connectors/source-postgres && cargo build && cd ../dest-postgres && cargo build`

---

### 1.2 Type `load_method` as enum &mdash; P1 / S

**File:** `dest-postgres/src/config.rs:17` (`load_method: String`), `config.rs:34`, `batch.rs:270`, `writer.rs:149`

**Problem:** `load_method` is a `String` compared at 3 sites. The validation in `Config::validate()` catches invalid values, but the runtime code still uses string comparison:

```rust
// config.rs:34 — validation
if self.load_method != "insert" && self.load_method != "copy" {
    return Err(ConnectorError::config("INVALID_CONFIG", ...));
}

// batch.rs:269-270 — dispatch
let use_copy =
    ctx.load_method == "copy" && !matches!(ctx.write_mode, Some(WriteMode::Upsert { .. }));

// writer.rs:486 — logging
"dest-postgres: flushed {} rows in {} batches via {}", ..., self.load_method, ...
```

**After:**

```rust
// config.rs
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoadMethod {
    Insert,
    Copy,
}

impl std::fmt::Display for LoadMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert => write!(f, "insert"),
            Self::Copy => write!(f, "copy"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    // ...
    #[serde(default)]
    pub load_method: LoadMethod,  // was: String
}
```

Delete `Config::validate()` — serde now rejects invalid values at parse time. Update `batch.rs:270` and `writer.rs:486` to use `LoadMethod::Copy` instead of string comparison.

**Why P1:** Eliminates a string comparison on every batch dispatch and prevents invalid values from reaching runtime. The serde error message is better than the manual validation.

**Migration strategy:**
1. Define the `LoadMethod` enum in `config.rs` with `#[serde(rename_all = "lowercase")]`.
2. Change `load_method: String` → `load_method: LoadMethod`.
3. Update `batch.rs:270` to `ctx.load_method == LoadMethod::Copy`.
4. Update `WriteContext` and `WriteSession` to carry `LoadMethod` instead of `&str`.
5. Remove `Config::validate()` (the `validate()` call in `main.rs:23` can be deleted).
6. `cd connectors/dest-postgres && cargo build`

---

> **Design note — intentional duplication:**
>
> The following cross-connector duplications are **intentionally preserved** to keep connectors self-contained (each connector may be extracted to its own repo in future):
>
> - **`client.rs` connection logic** (`connect()` / `connect_inner()`): ~25 shared lines across both connectors. Each connector owns its connection setup independently. If TLS support is added, each connector adds it independently.
> - **`Config` struct fields** (`host`, `port`, `user`, `password`, `database`, `connection_string()`): 5 shared fields + 1 method. Config structs are intentionally connector-local — a `PgConnectionConfig` trait in the SDK would couple connectors to the SDK for minimal dedup benefit.
>
> These are acceptable costs of self-contained connectors. Do **not** extract PG-specific helpers (connection logic, config traits) into `rapidbyte-sdk` — the SDK must remain vendor-agnostic.

---

## 2. God Function & Module Decomposition

> *REFACTORING_PRINCIPLES.md* &sect;4 — Avoid god-modules; keep functions focused.

### 2.1 Decompose `reader.rs` (805 lines) &mdash; P0 / L

**File:** `source-postgres/src/reader.rs`

**Problem:** `reader.rs` is the second-largest file in the connector layer. It mixes 4 distinct responsibilities:

1. **Query building** (lines 509-607): `build_base_query()`, `cursor_bind_param()`, `effective_cursor_type()` — cursor type resolution, SQL generation, bind parameter construction.
2. **Cursor management** (lines 170-433): DECLARE, FETCH loop, CLOSE — server-side cursor lifecycle management with batched fetching.
3. **Arrow encoding** (lines 707-804): `build_arrow_schema()`, `rows_to_record_batch()`, `batch_to_ipc()` — PG rows to Arrow IPC conversion.
4. **Emit orchestration** (lines 286-427): Batch accumulation, metrics emission, error handling — duplicated in two code paths (mid-loop at line 290 and post-loop at line 385).

The emit orchestration block is duplicated verbatim (~40 lines each occurrence):

```rust
// reader.rs:290-332 (first occurrence)
let encode_start = Instant::now();
let encode_result =
    rows_to_record_batch(&accumulated_rows, &columns, &arrow_schema)
        .and_then(|batch| batch_to_ipc(&batch));
arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
match encode_result {
    Ok(ipc_bytes) => {
        total_records += accumulated_rows.len() as u64;
        total_bytes += ipc_bytes.len() as u64;
        batches_emitted += 1;
        if let Err(e) = host_ffi::emit_batch(&ipc_bytes) { ... }
        let _ = host_ffi::metric(...);  // records_read
        let _ = host_ffi::metric(...);  // bytes_read
        accumulated_rows.clear();
        estimated_bytes = 256;
    }
    Err(e) => { loop_error = Some(e); break; }
}

// reader.rs:385-427 (second occurrence — structurally identical)
```

**After:** Split into 3 focused modules + a shared helper:

```
source-postgres/src/
  reader/
    mod.rs           # read_stream() — cursor lifecycle, calls helpers
    query.rs         # build_base_query(), cursor_bind_param(), effective_cursor_type()
    arrow_encode.rs  # build_arrow_schema(), rows_to_record_batch(), batch_to_ipc()
```

Extract the duplicated emit block into a helper:

```rust
// reader/mod.rs
struct EmitState {
    total_records: u64,
    total_bytes: u64,
    batches_emitted: u64,
    arrow_encode_nanos: u64,
}

fn emit_accumulated_rows(
    rows: &mut Vec<tokio_postgres::Row>,
    columns: &[ColumnSchema],
    schema: &Arc<Schema>,
    stream_name: &str,
    state: &mut EmitState,
    estimated_bytes: &mut usize,
) -> anyhow::Result<()> {
    let encode_start = Instant::now();
    let ipc_bytes = rows_to_record_batch(rows, columns, schema)
        .and_then(|batch| batch_to_ipc(&batch))?;
    state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;

    state.total_records += rows.len() as u64;
    state.total_bytes += ipc_bytes.len() as u64;
    state.batches_emitted += 1;

    host_ffi::emit_batch(&ipc_bytes)
        .map_err(|e| anyhow!("emit_batch failed: {}", e))?;
    emit_metrics(stream_name, state.total_records, state.total_bytes);

    rows.clear();
    *estimated_bytes = 256;
    Ok(())
}
```

**Why P0:** The duplicated emit block (80 lines total) is a known bug vector — a fix applied to one copy but not the other. The query-building logic (`cursor_bind_param` alone is 80 lines) is independently testable once extracted.

**Migration strategy:**
1. Create `reader/` directory with `mod.rs`, `query.rs`, `arrow_encode.rs`.
2. Move `build_base_query()`, `cursor_bind_param()`, `effective_cursor_type()`, `timestamp_*_to_rfc3339()`, `CursorBindParam`, `CursorQuery` to `query.rs`.
3. Move `build_arrow_schema()`, `rows_to_record_batch()`, `batch_to_ipc()` to `arrow_encode.rs`.
4. Extract `emit_accumulated_rows()` helper in `mod.rs`.
5. Replace both emit blocks in `read_stream_inner()` with calls to the helper.
6. Update `main.rs` module declaration from `mod reader;` to the directory form.
7. `cd connectors/source-postgres && cargo build`

---

### 2.2 Decompose `batch.rs` (831 lines) &mdash; P0 / L

**File:** `dest-postgres/src/batch.rs`

**Problem:** `batch.rs` is the largest file in the connector layer. It contains 5 distinct concerns:

1. **Typed column downcasting** (lines 33-65): `TypedCol` enum, `downcast_columns()` — Arrow column pre-downcast for performance.
2. **COPY text formatting** (lines 74-162): `format_copy_typed_value()` — COPY protocol text encoding with escape handling.
3. **INSERT path** (lines 328-445): `insert_batch()` — multi-value INSERT with chunking and upsert clause generation.
4. **COPY path** (lines 451-555): `copy_batch()` — COPY FROM STDIN streaming with flush management.
5. **Per-row fallback** (lines 560-668): `write_rows_individually()` — single-row INSERT with DLQ emission.

Plus shared helpers: `decode_ipc()`, `active_column_indices()`, `build_upsert_clause()`, `sql_param_value()`, `serialize_row_to_json()`, `SqlParamValue`.

**After:** Split into focused modules:

```
dest-postgres/src/
  batch/
    mod.rs            # write_batch(), decode_ipc(), WriteResult, WriteContext — dispatch logic
    typed_col.rs      # TypedCol, downcast_columns(), sql_param_value(), SqlParamValue
    copy_format.rs    # format_copy_typed_value() — COPY text protocol encoding
    insert.rs         # insert_batch(), build_upsert_clause(), active_column_indices()
    copy.rs           # copy_batch()
    fallback.rs       # write_rows_individually(), serialize_row_to_json()
```

**Why P0:** At 831 lines with 5 independent write strategies, this file is the hardest to navigate when debugging write failures. Each write path has different error semantics (COPY is all-or-nothing, INSERT is chunked, fallback is per-row), so they deserve separate modules for clarity.

**Migration strategy:**
1. Create `batch/` directory.
2. Start with `typed_col.rs` (zero dependencies on other batch code).
3. Move `copy_format.rs` (depends only on `TypedCol`).
4. Move `insert.rs` and `copy.rs` (both depend on `typed_col` and shared helpers).
5. Move `fallback.rs` last (depends on `typed_col` and DLQ serialization).
6. Keep `mod.rs` as the dispatch layer with `write_batch()`.
7. `cd connectors/dest-postgres && cargo build`

---

### 2.3 Decompose `ddl.rs` (566 lines) &mdash; P1 / M

**File:** `dest-postgres/src/ddl.rs`

**Problem:** `ddl.rs` mixes 3 concerns:

1. **Type mapping** (lines 13-60): `arrow_to_pg_type()`, `pg_types_compatible()` — Arrow-to-PG type conversion and compatibility checking.
2. **Schema drift detection** (lines 188-318): `SchemaDrift`, `detect_schema_drift()`, `get_existing_columns()`, `apply_schema_policy()` — diff engine for schema evolution.
3. **Staging table management** (lines 485-565): `drop_staging_table()`, `swap_staging_table()`, `prepare_staging()` — Replace-mode table lifecycle.

**After:**

```
dest-postgres/src/
  ddl/
    mod.rs        # ensure_table_and_schema() — orchestrates the three concerns
    type_map.rs   # arrow_to_pg_type(), pg_types_compatible()
    drift.rs      # SchemaDrift, detect_schema_drift(), apply_schema_policy()
    staging.rs    # prepare_staging(), swap_staging_table(), drop_staging_table()
```

**Why P1:** The type mapping functions are reusable (and potentially testable in isolation), but they're buried in a 566-line file. Separating drift detection makes the schema evolution logic reviewable independently.

---

### 2.4 Extract shared `emit_metrics` pattern &mdash; P1 / S

**Files:** `source-postgres/src/reader.rs:306-323, 401-418`, `source-postgres/src/cdc.rs:510-529`

**Problem:** The metrics emission pattern is duplicated 3 times across the source connector — twice in `reader.rs` (inline in the emit blocks) and once in `cdc.rs` (as the extracted `emit_metrics()` function):

```rust
// cdc.rs:510-529 — already extracted
fn emit_metrics(ctx: &StreamContext, total_records: u64, total_bytes: u64) {
    let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
        name: "records_read".to_string(),
        value: MetricValue::Counter(total_records),
        labels: vec![],
    });
    let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
        name: "bytes_read".to_string(),
        value: MetricValue::Counter(total_bytes),
        labels: vec![],
    });
}

// reader.rs:306-323 — inline duplicate (twice)
let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
    name: "records_read".to_string(),
    value: MetricValue::Counter(total_records),
    labels: vec![],
});
let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
    name: "bytes_read".to_string(),
    value: MetricValue::Counter(total_bytes),
    labels: vec![],
});
```

**After:** Move the `emit_metrics` function from `cdc.rs` to a shared module (e.g., `metrics.rs`) and reuse it from both `reader.rs` and `cdc.rs`. After item 2.1 extracts the emit block, the reader code calls this shared function too.

**Why P1:** Three copies of the same 20-line block. If a new metric is added (e.g., `batches_emitted`), all three must be updated.

---

## 3. Narrative Flow & Readability

> *REFACTORING_PRINCIPLES.md* &sect;3 — Make the code easy to read top-to-bottom.

### 3.1 Comment cursor type fallback chain &mdash; P0 / S

**File:** `source-postgres/src/reader.rs:342-357`

**Problem:** The cursor value extraction uses a complex fallback chain that silently handles a known PostgreSQL type mismatch (SERIAL is INT4, not INT8):

```rust
// reader.rs:342-357
let val: Option<String> = match ctx.cursor_info.as_ref().map(|ci| &ci.cursor_type) {
    Some(CursorType::Int64) => row
        .try_get::<_, i64>(col_idx).ok()
        .map(|n| n.to_string())
        .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string())),
    _ => row
        .try_get::<_, String>(col_idx).ok()
        .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
        .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string())),
};
```

This is a critical section: the `i32` fallback was added because PG `SERIAL` columns are INT4 (i32), but the cursor type is hardcoded to `Utf8` in the host orchestrator, which triggers the catch-all arm. A future developer might remove the "unnecessary" `i32` fallback and break incremental sync on SERIAL columns.

**After:** Add a comment block explaining the bug trap:

```rust
// IMPORTANT: PostgreSQL SERIAL is INT4 (i32), not INT8 (i64).
// tokio-postgres try_get() requires an exact type match, so we must
// chain i64 → i32 fallbacks. The host orchestrator currently hardcodes
// CursorType::Utf8, so the catch-all arm below is the common path.
// DO NOT remove the i32 fallback — it prevents silent cursor tracking
// failure on SERIAL primary keys.
let val: Option<String> = match ctx.cursor_info.as_ref().map(|ci| &ci.cursor_type) {
    // ...
};
```

**Why P0:** This is a documented bug trap (see MEMORY.md: "PG `SERIAL` is INT4 (i32), NOT INT8 (i64)"). Without a comment, the fallback chain looks like dead code and is likely to be "cleaned up" by a future contributor, silently breaking incremental sync.

---

### 3.2 Log cursor CLOSE failure &mdash; P1 / S

**File:** `source-postgres/src/reader.rs:437-438`

**Problem:** Cursor close failure is silently swallowed:

```rust
// reader.rs:437-438
let close_query = format!("CLOSE {}", CURSOR_NAME);
let _ = client.execute(&close_query, &[]).await;
```

While the cursor close is non-critical (the transaction COMMIT/ROLLBACK will clean it up), silent suppression masks potential issues like connection drops mid-query.

**After:**

```rust
let close_query = format!("CLOSE {}", CURSOR_NAME);
if let Err(e) = client.execute(&close_query, &[]).await {
    host_ffi::log(1, &format!(
        "Warning: cursor CLOSE failed for stream '{}': {} (non-fatal, transaction will clean up)",
        ctx.stream_name, e
    ));
}
```

**Why P1:** The `let _ =` pattern suppresses all errors including network failures. A warning log makes connection issues visible without affecting the success path.

---

### 3.3 Doc comments for CDC parser &mdash; P1 / S

**File:** `source-postgres/src/cdc.rs:303-420`

**Problem:** The CDC parser (`parse_change_line`, `parse_columns`, `parse_value`) implements a stateful parser for PostgreSQL's `test_decoding` output format. The format spec is only documented in code comments at line 303-311:

```rust
/// Format examples:
///   BEGIN 12345
///   table public.users: INSERT: id[integer]:1 name[text]:'John' active[boolean]:t
```

But `parse_value()` (line 390-420) handles subtle edge cases — escaped quotes (`''`), multi-byte UTF-8, unterminated strings — with no documentation of the grammar.

**After:** Add a doc comment to `parse_value()` specifying the grammar:

```rust
/// Parse a single value from test_decoding output.
///
/// Grammar (informal):
///   value     = quoted_str | unquoted
///   quoted_str = "'" (char | "''" )* "'"    -- '' is escaped single quote
///   unquoted  = [^ ]+                       -- terminated by space or EOF
///
/// Edge cases:
/// - `'O''Brien'` → `O'Brien` (escaped quote)
/// - `'élève'` → `élève` (multi-byte UTF-8 handled via char_indices)
/// - `null` → literal string "null" (caller checks for null semantics)
/// - Unterminated quote → returns accumulated value, empty remainder
fn parse_value(s: &str) -> (String, &str) { ... }
```

**Why P1:** The parser is the most complex pure logic in the connector layer and handles edge cases (UTF-8, escaped quotes) that aren't obvious from the code alone. Good doc comments prevent regressions when the parser is modified.

---

### 3.4 String-typed errors &rarr; anyhow chains &mdash; P1 / M

**Files:** `source-postgres/src/reader.rs:75-79`, `dest-postgres/src/batch.rs:245-249`, `dest-postgres/src/ddl.rs` (multiple sites)

**Problem:** Many internal functions use `.map_err(|e| e.to_string())` to convert `anyhow::Error` to `String`, erasing the error chain:

```rust
// reader.rs:75-79
pub async fn read_stream(client: &Client, ctx: &StreamContext, connect_secs: f64)
    -> Result<ReadSummary, String>
{
    read_stream_inner(client, ctx, connect_secs)
        .await
        .map_err(|e| e.to_string())  // error chain lost
}

// batch.rs:245-249
pub(crate) async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8])
    -> Result<(WriteResult, u64), String>
{
    write_batch_inner(ctx, ipc_bytes)
        .await
        .map_err(|e| e.to_string())  // error chain lost
}
```

This pattern exists because the SDK connector traits return `ConnectorError` (which takes a `String` message), so internal functions use `String` error types as an intermediate. But `.to_string()` on an `anyhow::Error` only prints the outermost error, losing the `.context()` chain.

**After:** Use `format!("{:#}", e)` to preserve the full error chain:

```rust
pub async fn read_stream(...) -> Result<ReadSummary, String> {
    read_stream_inner(client, ctx, connect_secs)
        .await
        .map_err(|e| format!("{:#}", e))  // preserves: "FETCH failed: connection reset"
}
```

Or better, change internal functions to return `anyhow::Result` all the way up and only convert at the trait boundary in `main.rs`:

```rust
// main.rs — trait boundary
async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
    reader::read_stream(&client, &ctx, connect_secs)
        .await
        .map_err(|e| ConnectorError::internal("READ_FAILED", format!("{:#}", e)))
}
```

**Why P1:** Error chain erasure makes debugging production failures harder. When a FETCH fails due to a connection reset, the error currently reads "FETCH failed for users: connection reset by peer" but with `.to_string()` on the outer anyhow, it might just show "FETCH failed for users" without the root cause.

---

### 3.5 `WriteSession` field grouping &mdash; P2 / S

**File:** `dest-postgres/src/writer.rs:141-182`

**Problem:** `WriteSession` has 19 fields spanning 5 concerns. The struct definition is hard to scan:

```rust
pub struct WriteSession<'a> {
    client: &'a Client,
    target_schema: &'a str,
    stream_name: String,
    effective_stream: String,
    effective_write_mode: Option<WriteMode>,
    load_method: String,
    schema_policy: SchemaEvolutionPolicy,
    on_data_error: DataErrorPolicy,
    checkpoint_config: CheckpointConfig,
    is_replace: bool,
    watermark_records: u64,
    cumulative_records: u64,
    flush_start: Instant,
    total_rows: u64,
    total_bytes: u64,
    total_failed: u64,
    batches_written: u64,
    checkpoint_count: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
    last_checkpoint_time: Instant,
    arrow_decode_nanos: u64,
    created_tables: HashSet<String>,
    ignored_columns: HashSet<String>,
    type_null_columns: HashSet<String>,
}
```

**After:** Group stats into a sub-struct:

```rust
struct WriteStats {
    total_rows: u64,
    total_bytes: u64,
    total_failed: u64,
    batches_written: u64,
    checkpoint_count: u64,
    bytes_since_commit: u64,
    rows_since_commit: u64,
    arrow_decode_nanos: u64,
}

pub struct WriteSession<'a> {
    client: &'a Client,
    target_schema: &'a str,
    // Stream identity
    stream_name: String,
    effective_stream: String,
    effective_write_mode: Option<WriteMode>,
    // Config
    load_method: String,
    schema_policy: SchemaEvolutionPolicy,
    on_data_error: DataErrorPolicy,
    checkpoint_config: CheckpointConfig,
    // Replace mode
    is_replace: bool,
    // Watermark resume
    watermark_records: u64,
    cumulative_records: u64,
    // Accumulated stats
    stats: WriteStats,
    flush_start: Instant,
    last_checkpoint_time: Instant,
    // Schema tracking
    created_tables: HashSet<String>,
    ignored_columns: HashSet<String>,
    type_null_columns: HashSet<String>,
}
```

**Why P2:** The stats sub-struct reduces `WriteSession` from 22 fields to 14, and makes it clearer which fields are accumulated counters vs configuration.

---

## 4. Architecture & Tooling

> *REFACTORING_PRINCIPLES.md* &sect;4 — Module structure, documentation, encapsulation.

### 4.1 Module-level rustdoc &mdash; P1 / S

**Problem:** 0 of 14 source files have `//!` module documentation. When navigating the codebase, there's no quick way to understand what each file does without reading the code.

**Action:** Add `//!` doc comments to every module. Examples:

```rust
// source-postgres/src/reader.rs
//! Full-refresh and incremental stream reads using server-side cursors.
//! Builds SQL queries with typed cursor predicates, fetches rows in batches
//! via DECLARE/FETCH, encodes to Arrow IPC, and emits to the host pipeline.

// dest-postgres/src/batch.rs
//! Arrow IPC batch write operations: INSERT, COPY, and per-row fallback.
//! Dispatches to the appropriate write strategy based on `load_method` and
//! `write_mode`, with automatic fallback to single-row INSERTs on batch failure.

// dest-postgres/src/ddl.rs
//! DDL management: table creation, schema drift detection, and Replace-mode
//! staging table lifecycle (create, swap, drop).

// source-postgres/src/cdc.rs
//! Change Data Capture via PostgreSQL logical replication.
//! Uses `pg_logical_slot_get_changes()` with `test_decoding` plugin to read
//! WAL changes, parse them into structured CDC events, and emit Arrow batches.
```

**Why P1:** Module docs are the first thing a new contributor reads. Per &sect;6 of the principles, "documentation is part of the API."

---

### 4.2 Test coverage plan &mdash; P1 / L

**Problem:** Only `cdc.rs` has tests (17 tests). The other 13 files have zero test coverage. Several pure functions are immediately testable without a database:

| Function | File | Lines | Why testable |
|----------|------|------:|--------------|
| `estimate_row_bytes()` | `reader.rs:34-47` | 14 | Pure: columns → byte estimate |
| `effective_cursor_type()` | `reader.rs:499-507` | 9 | Pure: cursor type inference |
| `cursor_bind_param()` | `reader.rs:609-690` | 82 | Pure: cursor value → bind param |
| `build_base_query()` | `reader.rs:509-607` | 99 | Pure: context → SQL string |
| `build_arrow_schema()` | `reader.rs:707-724` | 18 | Pure: columns → Arrow schema |
| `rows_to_record_batch()` | `reader.rs:726-795` | 70 | Pure if given mock rows |
| `batch_to_ipc()` | `reader.rs:797-804` | 8 | Pure: RecordBatch → bytes |
| `timestamp_millis_to_rfc3339()` | `reader.rs:692-696` | 5 | Pure: millis → string |
| `timestamp_micros_to_rfc3339()` | `reader.rs:698-705` | 8 | Pure: micros → string |
| `arrow_to_pg_type()` | `ddl.rs:13-24` | 12 | Pure: DataType → pg string |
| `pg_types_compatible()` | `ddl.rs:31-60` | 30 | Pure: type comparison |
| `format_copy_typed_value()` | `batch.rs:74-162` | 89 | Pure: value formatting |
| `sql_param_value()` | `batch.rs:777-830` | 54 | Pure: column → SQL param |
| `serialize_row_to_json()` | `batch.rs:718-775` | 58 | Pure: batch row → JSON |

That's 14 pure functions totaling ~556 lines that can be unit-tested without any database dependency.

**Action:** After decompositions in 2.1 and 2.2 land, add `#[cfg(test)] mod tests` to each new module with tests for the extracted functions. Priority order:
1. `cursor_bind_param()` — complex match logic, highest bug risk
2. `pg_types_compatible()` — normalization logic with many aliases
3. `format_copy_typed_value()` — escape handling, edge cases
4. `build_base_query()` — SQL generation with cursor predicates

**Why P1:** The untested code handles SQL generation and data encoding — the highest-risk operations. Item 2.1 and 2.2 unblock this by making functions `pub(crate)` in their own modules.

---

### 4.3 Source config validation &mdash; P1 / S

**File:** `source-postgres/src/config.rs` (30 lines, no validation), `dest-postgres/src/config.rs:33-44` (has validation)

**Problem:** The destination connector validates its config (`Config::validate()` checks `load_method`), but the source connector has no validation at all. The source `Config` has a `replication_slot` field that could contain SQL-injection-friendly characters:

```rust
// source-postgres/src/config.rs — no validate() method
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub replication_slot: Option<String>,  // never validated!
}
```

While the slot name is validated at runtime (cdc.rs:81), this happens after connection — not at config validation time.

**After:** Validate the replication slot name at init time. Use `pg_escape::quote_identifier` (see item 1.1) to ensure proper quoting, or at minimum check that the slot name is non-empty and within PG's 63-byte limit:

```rust
impl Config {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Some(ref slot) = self.replication_slot {
            if slot.is_empty() {
                return Err(ConnectorError::config("INVALID_CONFIG",
                    "replication_slot must not be empty".to_string()));
            }
            if slot.len() > 63 {
                return Err(ConnectorError::config("INVALID_CONFIG",
                    format!("replication_slot '{}' exceeds 63-byte limit", slot)));
            }
        }
        Ok(())
    }
}
```

And call it from `Source::init()` in `main.rs`:

```rust
async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
    config.validate()?;  // fail fast
    // ...
}
```

**Why P1:** Config validation should happen at init time (fail fast), not deep in the CDC read path. This matches the dest connector's pattern.

---

## 5. Performance

> *REFACTORING_PRINCIPLES.md* &sect;5 — Make it correct first, then fast. Measure.

### 5.1 Compute `estimate_row_bytes()` once per stream &mdash; P1 / S

**File:** `source-postgres/src/reader.rs:34-47, 255, 287`

**Problem:** `estimate_row_bytes()` is called O(rows) times, but it returns a constant for a given column set:

```rust
// reader.rs:34-47 — pure function of columns, no row data
fn estimate_row_bytes(columns: &[ColumnSchema]) -> usize {
    let mut total = 0usize;
    for col in columns {
        total += match col.data_type.as_str() {
            "Int16" => 2,
            "Int32" | "Float32" => 4,
            "Int64" | "Float64" => 8,
            "Boolean" => 1,
            _ => 64,
        };
        total += 1;
    }
    total
}

// Called per-row:
// reader.rs:255 — inside the inner row loop
let row_bytes = estimate_row_bytes(&columns);
// reader.rs:287 — again in the accumulation loop
let row_bytes = estimate_row_bytes(&columns);
```

**After:** Compute once before the fetch loop:

```rust
let estimated_row_bytes = estimate_row_bytes(&columns);

// Then in the loop:
let row_bytes = estimated_row_bytes;
```

**Why P1:** On a table with 7 columns, this saves ~14 match evaluations per row. For a 1M row table, that's 14M unnecessary match arms evaluated. The fix is a one-line change.

---

### 5.2 Configurable COPY flush threshold &mdash; P2 / S

**File:** `dest-postgres/src/batch.rs:199`

**Problem:** The COPY flush threshold is hardcoded:

```rust
const COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB
```

This is a reasonable default, but for high-throughput workloads, a larger buffer (e.g., 16MB) reduces the number of network round-trips. For low-memory environments, a smaller buffer may be needed.

**After:** Accept the threshold from `WriteContext` (sourced from pipeline config), with the current value as default:

```rust
const DEFAULT_COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024;

// In copy_batch():
let flush_threshold = ctx.copy_flush_bytes.unwrap_or(DEFAULT_COPY_FLUSH_BYTES);
```

**Why P2:** The current default works well for most workloads. This is only worth doing if benchmarks show COPY performance is buffer-size-sensitive.

---

### 5.3 Typed cursor max tracking &mdash; P2 / S

**File:** `source-postgres/src/reader.rs:228, 341-373`

**Problem:** `max_cursor_value` is stored as `Option<String>`, requiring per-row `String` allocation and string comparison even for integer cursors:

```rust
// reader.rs:228
let mut max_cursor_value: Option<String> = None;

// reader.rs:363-368 — per-row comparison requires parsing
let is_greater = match (val.parse::<i64>(), current.parse::<i64>()) {
    (Ok(a), Ok(b)) => a > b,
    _ => val > *current,
};
```

**After:** Track as `Option<CursorValue>` and compare natively:

```rust
let mut max_cursor_value: Option<i64> = None;  // for Int64 cursors
// or
enum MaxCursor {
    Int(i64),
    Text(String),
}
```

**Why P2:** The per-row `to_string()` + `parse::<i64>()` cycle is wasteful for integer cursors, but the actual cost is small relative to the database I/O. Profile before optimizing.

---

## 6. Polish

> *REFACTORING_PRINCIPLES.md* &sect;6 — Naming, layout, robustness, documentation.

### 6.1 Extract magic numbers as constants &mdash; P1 / S

**Files:** `source-postgres/src/reader.rs:202-203, 206-208`, `source-postgres/src/cdc.rs:121`, `dest-postgres/src/batch.rs:196-199`, `dest-postgres/src/main.rs:37,40`

**Problem:** Magic numbers scattered across multiple files:

```rust
// reader.rs:202-203 — default batch size limits
let max_batch_bytes = if ctx.limits.max_batch_bytes > 0 {
    ctx.limits.max_batch_bytes as usize
} else {
    64 * 1024 * 1024  // 64MB — matches SDK DEFAULT_MAX_BATCH_BYTES
};

// reader.rs:206-208
} else {
    16 * 1024 * 1024  // 16MB — matches SDK DEFAULT_MAX_RECORD_BYTES
};

// cdc.rs:121 — undocumented limit
let max_changes = 100_000i64;

// batch.rs:196 — INSERT chunk size
const INSERT_CHUNK_SIZE: usize = 1000;

// main.rs:37,40 — ConnectorInfo defaults
default_max_batch_bytes: 64 * 1024 * 1024,
```

**After:**

```rust
// source-postgres constants (shared or per-module)
/// Default max batch size when not specified in stream limits.
const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Default max record size when not specified in stream limits.
const DEFAULT_MAX_RECORD_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum WAL changes consumed per CDC invocation to prevent OOM.
const CDC_MAX_CHANGES: i64 = 100_000;
```

For `ConnectorInfo::default_max_batch_bytes`, reference the SDK constant `DEFAULT_MAX_BATCH_BYTES` instead of repeating `64 * 1024 * 1024`.

**Why P1:** `100_000` in `cdc.rs` has no comment explaining why that limit was chosen. Named constants with doc comments make limits auditable and tunable.

---

### 6.2 Document CDC slot naming &mdash; P2 / S

**File:** `source-postgres/src/cdc.rs:76-79`

**Problem:** The default replication slot naming convention is undocumented:

```rust
// cdc.rs:76-79
let slot_name = config.replication_slot
    .clone()
    .unwrap_or_else(|| format!("rapidbyte_{}", ctx.stream_name));
```

The format `rapidbyte_{stream_name}` is an implicit convention that operators need to know about when managing PG replication slots.

**After:** Add a doc comment and a constant:

```rust
/// Default replication slot prefix. Slots are named `rapidbyte_{stream_name}`.
const SLOT_PREFIX: &str = "rapidbyte_";

let slot_name = config.replication_slot
    .clone()
    .unwrap_or_else(|| format!("{}{}", SLOT_PREFIX, ctx.stream_name));
```

**Why P2:** Operators managing PostgreSQL replication slots need to know the naming convention to identify and clean up slots. A constant + doc comment makes this discoverable.

---

### 6.3 Consolidate `batch_to_ipc()` within source-postgres &mdash; P2 / S

**Files:** `source-postgres/src/reader.rs:797-804`, `source-postgres/src/cdc.rs:483-489`

**Problem:** `batch_to_ipc()` is an identical 6-line function duplicated within the source connector — once in `reader.rs` and once in `cdc.rs`:

```rust
// reader.rs:797-804
fn batch_to_ipc(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, batch.schema().as_ref()).context("IPC writer error")?;
    writer.write(batch).context("IPC write error")?;
    writer.finish().context("IPC finish error")?;
    Ok(buf)
}

// cdc.rs:483-489 — identical
```

**After:** Move to a connector-internal shared module and use from both files within source-postgres:

```rust
// source-postgres/src/arrow_util.rs
pub(crate) fn batch_to_ipc(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> { ... }
```

This is an **internal** dedup within the source-postgres connector only — not cross-connector. Each connector remains self-contained. (The dest connector does not have this function; it uses `decode_ipc()` in the opposite direction.)

**Why P2:** Small duplication (6 lines), but it's in the data encoding path — any change to IPC serialization (e.g., adding compression) needs to happen in both places within the same connector.

---

### 6.4 Consistent error constructors &mdash; P2 / S

**Files:** Various across both connectors

**Problem:** The connectors mix `ConnectorError` constructors with plain `String` errors inconsistently:

```rust
// writer.rs:27-28 — uses ConnectorError::config()
ConnectorError::config("INVALID_IDENTIFIER", format!("Invalid stream name: {}", e))

// writer.rs:37 — uses ConnectorError::transient_network()
ConnectorError::transient_network("CONNECTION_FAILED", e)

// ddl.rs:116 — returns plain String
format!("Failed to create table {}: {}", qualified_table, e)

// batch.rs:248 — converts anyhow to String
.map_err(|e| e.to_string())
```

The DDL and batch modules return `Result<_, String>` while the writer module returns `Result<_, ConnectorError>`. This inconsistency means the writer has to convert strings at the boundary.

**After:** Gradually migrate DDL and batch to return `anyhow::Result` (or `ConnectorError` directly), and convert at the trait boundary only. This is a dependency of item 3.4.

**Why P2:** Mixed error types are a readability issue, not a correctness issue. Address opportunistically when touching these files for other reasons.

---

## Execution Order

### Phase 1: Safety & Structural Foundation (P0 items)

```
1.1  Replace validate_pg_identifier → pg_escape  ─── Do first (simplest, removes 2 files)
     │
     ▼
3.1  Comment cursor type fallback chain          ─── Quick safety comment
     │
     ▼
2.1  Decompose reader.rs  ─┐
2.2  Decompose batch.rs   ─┘── Can run in parallel (different connectors)
```

Do **1.1** first — it removes both `identifier.rs` files and simplifies all SQL quoting call sites before the decomposition spreads them across sub-modules. **3.1** is independent and should be done immediately (it's a comment, zero risk). Then **2.1** and **2.2** are the largest changes but affect different connectors so can be done in parallel.

### Phase 2: Quality & DRY (P1 items)

```
1.2  Type load_method as enum         (independent)
2.3  Decompose ddl.rs                 (independent)
2.4  Extract shared emit_metrics      (depends on 2.1 — reader is already split)
3.2  Log cursor CLOSE failure         (independent)
3.3  Doc comments for CDC parser      (independent)
3.4  String-typed errors → anyhow     (independent — do alongside any file you touch)
4.1  Module-level rustdoc             (independent — ongoing)
4.2  Test coverage plan               (depends on 2.1, 2.2 for testable modules)
4.3  Source config validation          (independent)
5.1  Compute estimate_row_bytes once  (independent)
6.1  Extract magic numbers            (independent)
```

### Phase 3: Polish (P2 items)

```
3.5  WriteSession field grouping
5.2  Configurable COPY flush threshold
5.3  Typed cursor max tracking
6.2  Document CDC slot naming
6.3  Consolidate batch_to_ipc() within source-postgres
6.4  Consistent error constructors
```

Phase 3 items can be done opportunistically whenever touching nearby code.

---

## Summary Table

| # | Item | Principle | Priority | Effort | Primary File(s) |
|---|------|-----------|----------|--------|-----------------|
| 1.1 | Replace `validate_pg_identifier` &rarr; `pg_escape::quote_identifier` | &sect;6 DRY | **P0** | S | `*/identifier.rs`, `*/reader.rs`, `*/ddl.rs`, etc. |
| 1.2 | Type `load_method` as enum | &sect;2 Types | P1 | S | `dest/config.rs`, `batch.rs`, `writer.rs` |
| 2.1 | Decompose `reader.rs` (805 lines) | &sect;4 Architecture | **P0** | L | `source/reader.rs` |
| 2.2 | Decompose `batch.rs` (831 lines) | &sect;4 Architecture | **P0** | L | `dest/batch.rs` |
| 2.3 | Decompose `ddl.rs` (566 lines) | &sect;4 Architecture | P1 | M | `dest/ddl.rs` |
| 2.4 | Extract shared `emit_metrics` pattern | &sect;6 DRY | P1 | S | `source/reader.rs`, `source/cdc.rs` |
| 3.1 | Comment cursor type fallback chain | &sect;3 Readability | **P0** | S | `source/reader.rs` |
| 3.2 | Log cursor CLOSE failure | &sect;3 Readability | P1 | S | `source/reader.rs` |
| 3.3 | Doc comments for CDC parser | &sect;6 Documentation | P1 | S | `source/cdc.rs` |
| 3.4 | String-typed errors &rarr; anyhow chains | &sect;3 Error Handling | P1 | M | `source/reader.rs`, `dest/batch.rs`, `dest/ddl.rs` |
| 3.5 | `WriteSession` field grouping | &sect;2 Types | P2 | S | `dest/writer.rs` |
| 4.1 | Module-level rustdoc | &sect;6 Documentation | P1 | S | All 14 files |
| 4.2 | Test coverage plan | &sect;4 Tooling | P1 | L | 14 pure functions across both connectors |
| 4.3 | Source config validation | &sect;3 Fail Fast | P1 | S | `source/config.rs`, `source/main.rs` |
| 5.1 | Compute `estimate_row_bytes()` once | &sect;5 Performance | P1 | S | `source/reader.rs` |
| 5.2 | Configurable COPY flush threshold | &sect;5 Performance | P2 | S | `dest/batch.rs` |
| 5.3 | Typed cursor max tracking | &sect;5 Performance | P2 | S | `source/reader.rs` |
| 6.1 | Extract magic numbers as constants | &sect;6 Polish | P1 | S | `reader.rs`, `cdc.rs`, `batch.rs`, `main.rs` |
| 6.2 | Document CDC slot naming | &sect;6 Documentation | P2 | S | `source/cdc.rs` |
| 6.3 | Consolidate `batch_to_ipc()` within source-postgres | &sect;6 DRY | P2 | S | `source/reader.rs`, `source/cdc.rs` |
| 6.4 | Consistent error constructors | &sect;6 Polish | P2 | S | `dest/ddl.rs`, `dest/batch.rs`, `dest/writer.rs` |

**Totals:** 4 P0, 11 P1, 6 P2 &mdash; 21 items.
