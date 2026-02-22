# Connector Type Correctness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix silent data loss in PG→Arrow→PG type pipeline so ALL PostgreSQL types are correctly extracted, encoded, and written.

**Architecture:** The source connector maps PG types to `ArrowDataType` enums, extracts values from `tokio_postgres::Row` into typed Arrow arrays, and serializes as Arrow IPC. The dest connector deserializes IPC, downcasts Arrow arrays to typed columns, and writes via INSERT (parameterized) or COPY (text format). Currently only 7 types work (Int16/32/64, Float32/64, Boolean, Utf8). Everything else silently becomes NULL — catastrophic data loss.

**Tech Stack:** Rust, tokio-postgres (with `with-chrono-0_4` feature), Arrow 53, chrono 0.4, pg_escape

---

## Critical Bugs Found

1. **Source `try_get::<_, String>` silent NULL** — The catch-all in `rows_to_record_batch()` uses `row.try_get::<_, String>(col_idx).ok()`. tokio-postgres `FromSql<String>` only works for text-like PG types (text, varchar, char, name). For timestamp, date, numeric, json, uuid, bytea, inet, interval — `try_get` returns `Err`, `.ok()` converts to `None`, Arrow stores NULL. **All non-text, non-numeric, non-boolean values are silently lost.**

2. **Dest `TypedCol::Null` for unknown types** — `downcast_columns()` maps any Arrow type beyond 7 basics to `TypedCol::Null`. `sql_param_value` for `Null` returns `SqlParamValue::Text(None)` — always NULL. Even if the source correctly extracted a timestamp, the dest writes NULL.

3. **Cursor extraction missing chrono fallbacks** — `mod.rs:375-383` text cursor path tries `String → i64 → i32`. For timestamp cursor columns, `try_get::<_, String>` fails (timestamp is not text), producing `None`. Cursor never advances → incremental sync re-reads everything or stalls.

## Type Strategy

### Native Arrow types (proper typed extraction from PG)

| PG Type | ArrowDataType | tokio-postgres Rust type | Arrow array | Notes |
|---------|---------------|-------------------------|-------------|-------|
| timestamp / timestamp without time zone | TimestampMicros | `NaiveDateTime` | `TimestampMicrosecondArray` (tz=None) | chrono feature required |
| timestamptz / timestamp with time zone | TimestampMicros | `DateTime<Utc>` | `TimestampMicrosecondArray` (tz=Some("UTC")) | chrono feature required |
| date | Date32 | `NaiveDate` | `Date32Array` | days since epoch |
| bytea | Binary | `Vec<u8>` | `BinaryArray` | raw bytes |
| json / jsonb | Utf8 | `serde_json::Value` | `StringArray` | serialize to string, dest writes as TEXT |

### Text-cast types (SQL `::text` in SELECT for types without native FromSql<T>)

numeric, uuid, time, timetz, interval, inet, cidr, macaddr, money, oid, bit, varbit, xml, tsvector, tsquery, point, line, lseg, box, path, polygon, circle, array types, range types, composite types, enum types.

These stay `ArrowDataType::Utf8`. The fix is adding `::text` cast in the SELECT column list so tokio-postgres can extract them as `String`.

### Dest handling for new native types

| Arrow DataType | PG column type | INSERT bind | COPY text format |
|----------------|---------------|-------------|-----------------|
| `Timestamp(Microsecond, None)` | TIMESTAMP | `NaiveDateTime` | `YYYY-MM-DD HH:MM:SS.ffffff` |
| `Timestamp(Microsecond, Some("UTC"))` | TIMESTAMPTZ | `DateTime<Utc>` | `YYYY-MM-DD HH:MM:SS.ffffff+00` |
| `Date32` | DATE | `NaiveDate` | `YYYY-MM-DD` |
| `Binary` | BYTEA | `&[u8]` | `\\x<hex>` |

---

## Task Dependency Graph

```
Task 1: Enable chrono in both Cargo.toml
   │
   ├──► Task 2: Fix source pg_type_to_arrow + add needs_text_cast (no runtime dep)
   │       │
   │       ├──► Task 3: Fix source build_arrow_schema (depends on Task 2 types)
   │       │       │
   │       │       └──► Task 4: Fix source rows_to_record_batch (depends on Task 3 schema)
   │       │               │
   │       │               └──► Task 6: Fix cursor extraction (depends on Task 1 chrono)
   │       │
   │       └──► Task 5: Fix source SQL column list with ::text casts (depends on Task 2)
   │
   ├──► Task 7: Source unit tests (depends on Tasks 2-6)
   │
   ├──► Task 8: Fix dest arrow_to_pg_type (parallel with source tasks)
   │       │
   │       └──► Task 9: Fix dest TypedCol + downcast (depends on Task 8 + Task 1)
   │               │
   │               ├──► Task 10: Fix dest COPY format (depends on Task 9)
   │               │
   │               └──► Task 11: Fix dest INSERT path (depends on Task 9)
   │
   ├──► Task 12: Fix dest pg_types_compatible (parallel)
   │
   ├──► Task 13: Dest unit tests (depends on Tasks 8-12)
   │
   └──► Task 14: E2E integration test with ALL PG types (depends on all above)
```

---

### Task 1: Enable `with-chrono-0_4` feature on tokio-postgres

**Files:**
- Modify: `connectors/source-postgres/Cargo.toml`
- Modify: `connectors/dest-postgres/Cargo.toml`

**Step 1: Add chrono feature to source-postgres tokio-postgres**

In `connectors/source-postgres/Cargo.toml`, change:
```toml
tokio-postgres = { version = "0.7.16", default-features = false, features = ["with-serde_json-1"] }
```
to:
```toml
tokio-postgres = { version = "0.7.16", default-features = false, features = ["with-serde_json-1", "with-chrono-0_4"] }
```

**Step 2: Add chrono dep + feature to dest-postgres**

In `connectors/dest-postgres/Cargo.toml`, add `chrono = "0.4"` to dependencies and change:
```toml
tokio-postgres = { version = "0.7.16", default-features = false, features = ["with-serde_json-1"] }
```
to:
```toml
tokio-postgres = { version = "0.7.16", default-features = false, features = ["with-serde_json-1", "with-chrono-0_4"] }
```

**Step 3: Verify both connectors compile**

```bash
cd connectors/source-postgres && cargo check 2>&1 | tail -5
cd ../../connectors/dest-postgres && cargo check 2>&1 | tail -5
```
Expected: compiles without errors.

**Step 4: Commit**

```bash
git add connectors/source-postgres/Cargo.toml connectors/dest-postgres/Cargo.toml
git commit -m "feat(connectors): enable with-chrono-0_4 on tokio-postgres for typed timestamp/date extraction"
```

---

### Task 2: Fix source `pg_type_to_arrow()` + add `needs_text_cast()` helper

**Files:**
- Modify: `connectors/source-postgres/src/schema.rs`

**Step 1: Write failing test for new type mappings**

Add to the `#[cfg(test)] mod tests` block at the bottom of `schema.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_to_arrow_maps_native_types() {
        // Integers
        assert_eq!(pg_type_to_arrow("integer"), ArrowDataType::Int32);
        assert_eq!(pg_type_to_arrow("bigint"), ArrowDataType::Int64);
        assert_eq!(pg_type_to_arrow("smallint"), ArrowDataType::Int16);
        assert_eq!(pg_type_to_arrow("serial"), ArrowDataType::Int32);
        assert_eq!(pg_type_to_arrow("bigserial"), ArrowDataType::Int64);

        // Floats
        assert_eq!(pg_type_to_arrow("real"), ArrowDataType::Float32);
        assert_eq!(pg_type_to_arrow("double precision"), ArrowDataType::Float64);

        // Boolean
        assert_eq!(pg_type_to_arrow("boolean"), ArrowDataType::Boolean);

        // Text
        assert_eq!(pg_type_to_arrow("text"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("varchar"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("character varying"), ArrowDataType::Utf8);

        // Timestamp (native extraction)
        assert_eq!(pg_type_to_arrow("timestamp without time zone"), ArrowDataType::TimestampMicros);
        assert_eq!(pg_type_to_arrow("timestamp with time zone"), ArrowDataType::TimestampMicros);
        assert_eq!(pg_type_to_arrow("timestamp"), ArrowDataType::TimestampMicros);
        assert_eq!(pg_type_to_arrow("timestamptz"), ArrowDataType::TimestampMicros);

        // Date (native extraction)
        assert_eq!(pg_type_to_arrow("date"), ArrowDataType::Date32);

        // Binary (native extraction)
        assert_eq!(pg_type_to_arrow("bytea"), ArrowDataType::Binary);

        // JSON (native extraction via serde_json)
        assert_eq!(pg_type_to_arrow("json"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("jsonb"), ArrowDataType::Utf8);

        // Text-cast types (stay Utf8 but need ::text in SQL)
        assert_eq!(pg_type_to_arrow("numeric"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("uuid"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("time without time zone"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("interval"), ArrowDataType::Utf8);
        assert_eq!(pg_type_to_arrow("inet"), ArrowDataType::Utf8);

        // Unknown fallback
        assert_eq!(pg_type_to_arrow("some_custom_type"), ArrowDataType::Utf8);
    }

    #[test]
    fn needs_text_cast_identifies_non_native_types() {
        // Native types: no cast needed
        assert!(!needs_text_cast("integer"));
        assert!(!needs_text_cast("bigint"));
        assert!(!needs_text_cast("real"));
        assert!(!needs_text_cast("boolean"));
        assert!(!needs_text_cast("text"));
        assert!(!needs_text_cast("varchar"));
        assert!(!needs_text_cast("timestamp without time zone"));
        assert!(!needs_text_cast("timestamptz"));
        assert!(!needs_text_cast("date"));
        assert!(!needs_text_cast("bytea"));
        // JSON: extracted natively via serde_json
        assert!(!needs_text_cast("json"));
        assert!(!needs_text_cast("jsonb"));

        // Text-cast needed: tokio-postgres can't extract these as String
        assert!(needs_text_cast("numeric"));
        assert!(needs_text_cast("uuid"));
        assert!(needs_text_cast("time without time zone"));
        assert!(needs_text_cast("time with time zone"));
        assert!(needs_text_cast("interval"));
        assert!(needs_text_cast("inet"));
        assert!(needs_text_cast("cidr"));
        assert!(needs_text_cast("macaddr"));
        assert!(needs_text_cast("money"));
        assert!(needs_text_cast("bit"));
        assert!(needs_text_cast("xml"));

        // Unknown type: needs cast to be safe
        assert!(needs_text_cast("some_custom_type"));
    }
}
```

**Step 2: Run tests to verify they fail**

```bash
cd connectors/source-postgres && cargo test --lib schema::tests -- --nocapture 2>&1 | tail -20
```
Expected: FAIL — `needs_text_cast` doesn't exist, and timestamp/date/bytea still map to Utf8.

**Step 3: Update `pg_type_to_arrow` and add `needs_text_cast`**

Replace the `pg_type_to_arrow` function and add `needs_text_cast`:

```rust
/// Map PostgreSQL data types to Arrow-compatible data types.
///
/// Types with native tokio-postgres FromSql support get proper Arrow types.
/// Types that need `::text` casting in SQL stay as Utf8.
pub(crate) fn pg_type_to_arrow(pg_type: &str) -> ArrowDataType {
    match pg_type {
        // Integer types
        "integer" | "int4" | "serial" => ArrowDataType::Int32,
        "bigint" | "int8" | "bigserial" => ArrowDataType::Int64,
        "smallint" | "int2" | "smallserial" => ArrowDataType::Int16,

        // Float types
        "real" | "float4" => ArrowDataType::Float32,
        "double precision" | "float8" => ArrowDataType::Float64,

        // Boolean
        "boolean" | "bool" => ArrowDataType::Boolean,

        // Text types (native FromSql<String>)
        "text" | "varchar" | "character varying" | "char" | "character" | "name" | "bpchar" => {
            ArrowDataType::Utf8
        }

        // Timestamp types (native via chrono)
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => ArrowDataType::TimestampMicros,

        // Date (native via chrono)
        "date" => ArrowDataType::Date32,

        // Binary (native FromSql<Vec<u8>>)
        "bytea" => ArrowDataType::Binary,

        // JSON (native via serde_json, stored as Utf8 string in Arrow)
        "json" | "jsonb" => ArrowDataType::Utf8,

        // Everything else: stays Utf8, needs ::text cast in SQL
        _ => ArrowDataType::Utf8,
    }
}

/// Returns true if a PG type needs `::text` casting in SELECT to be extractable
/// as a String by tokio-postgres. Types with native FromSql support return false.
pub(crate) fn needs_text_cast(pg_type: &str) -> bool {
    match pg_type {
        // Integer types: native FromSql<i16/i32/i64>
        "integer" | "int4" | "serial"
        | "bigint" | "int8" | "bigserial"
        | "smallint" | "int2" | "smallserial" => false,

        // Float types: native FromSql<f32/f64>
        "real" | "float4"
        | "double precision" | "float8" => false,

        // Boolean: native FromSql<bool>
        "boolean" | "bool" => false,

        // Text types: native FromSql<String>
        "text" | "varchar" | "character varying" | "char" | "character" | "name" | "bpchar" => false,

        // Timestamp: native FromSql<NaiveDateTime/DateTime<Utc>> via chrono
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => false,

        // Date: native FromSql<NaiveDate> via chrono
        "date" => false,

        // Binary: native FromSql<Vec<u8>>
        "bytea" => false,

        // JSON: native FromSql<serde_json::Value>
        "json" | "jsonb" => false,

        // Everything else needs ::text cast
        _ => true,
    }
}
```

**Step 4: Run tests to verify they pass**

```bash
cd connectors/source-postgres && cargo test --lib schema::tests -- --nocapture 2>&1 | tail -20
```
Expected: PASS

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/schema.rs
git commit -m "feat(source-postgres): map timestamp/date/bytea to native Arrow types, add needs_text_cast helper"
```

---

### Task 3: Fix source `build_arrow_schema()` — handle new Arrow types

**Files:**
- Modify: `connectors/source-postgres/src/reader/arrow_encode.rs`

**Step 1: Write failing test for new type mappings in Arrow schema**

Add to the existing `mod tests` block in `arrow_encode.rs`:

```rust
#[test]
fn build_arrow_schema_maps_timestamp_date_binary() {
    let columns = vec![
        ColumnSchema {
            name: "created_at".to_string(),
            data_type: ArrowDataType::TimestampMicros,
            nullable: true,
        },
        ColumnSchema {
            name: "birth_date".to_string(),
            data_type: ArrowDataType::Date32,
            nullable: true,
        },
        ColumnSchema {
            name: "avatar".to_string(),
            data_type: ArrowDataType::Binary,
            nullable: true,
        },
    ];
    let schema = build_arrow_schema(&columns);
    assert_eq!(
        *schema.field(0).data_type(),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
    assert_eq!(*schema.field(1).data_type(), DataType::Date32);
    assert_eq!(*schema.field(2).data_type(), DataType::Binary);
}
```

**Step 2: Run test to verify it fails**

```bash
cd connectors/source-postgres && cargo test --lib reader::arrow_encode::tests::build_arrow_schema_maps_timestamp_date_binary -- --nocapture 2>&1 | tail -10
```
Expected: FAIL — TimestampMicros/Date32/Binary all fall through to Utf8.

**Step 3: Update `build_arrow_schema` to handle new types**

Update the `build_arrow_schema` function in `arrow_encode.rs`:

```rust
pub(crate) fn build_arrow_schema(columns: &[ColumnSchema]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = match col.data_type.as_str() {
                "Int16" => DataType::Int16,
                "Int32" => DataType::Int32,
                "Int64" => DataType::Int64,
                "Float32" => DataType::Float32,
                "Float64" => DataType::Float64,
                "Boolean" => DataType::Boolean,
                "TimestampMicros" => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
                "Date32" => DataType::Date32,
                "Binary" => DataType::Binary,
                _ => DataType::Utf8,
            };
            Field::new(&col.name, dt, col.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}
```

Note: All timestamps use `tz=None` here. The source connector extracts both `timestamp` and `timestamptz` as `NaiveDateTime`/`DateTime<Utc>` respectively, but both get stored as microsecond i64 values. The distinction between with/without timezone is not tracked in the current `ArrowDataType` enum (single `TimestampMicros` variant). For now, all timestamps are written as `Timestamp(Microsecond, None)`. This is correct for the destination — it always creates `TIMESTAMP` columns, and the values round-trip correctly.

**Step 4: Run test to verify it passes**

```bash
cd connectors/source-postgres && cargo test --lib reader::arrow_encode::tests -- --nocapture 2>&1 | tail -10
```
Expected: PASS

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/reader/arrow_encode.rs
git commit -m "feat(source-postgres): handle TimestampMicros, Date32, Binary in Arrow schema builder"
```

---

### Task 4: Fix source `rows_to_record_batch()` — typed value extraction

This is the critical data correctness fix. Currently the catch-all uses `try_get::<_, String>` which silently fails for non-text PG types.

**Files:**
- Modify: `connectors/source-postgres/src/reader/arrow_encode.rs`
- Modify: `connectors/source-postgres/src/schema.rs` (to also store PG type strings)
- Modify: `connectors/source-postgres/src/reader/mod.rs` (pass PG types through)

**Step 1: Add PG type tracking to schema discovery**

In `schema.rs`, add a struct to carry both ColumnSchema and PG type:

```rust
/// Column metadata including the raw PostgreSQL type string.
/// Needed for type-aware extraction in Arrow encoding.
#[derive(Debug, Clone)]
pub(crate) struct PgColumn {
    pub(crate) schema: ColumnSchema,
    pub(crate) pg_type: String,
}
```

Update the `discover_catalog` function and `read_stream_inner` (in `reader/mod.rs`) to pass PG types alongside columns. However, since `discover_catalog` returns `Vec<Stream>` which uses `ColumnSchema`, we need a separate approach.

The simplest change: in `reader/mod.rs:read_stream_inner()`, where the schema is queried a second time (lines 145-167), we already have the `data_type` string. Store it in a parallel `Vec<String>`:

In `reader/mod.rs`, after the columns are built, add:
```rust
let pg_types: Vec<String> = schema_rows
    .iter()
    .map(|row| row.get::<_, String>(1))
    .collect();
```

Then apply the same projection filter that `columns` uses, and pass `pg_types` to `rows_to_record_batch`.

**Step 2: Update `rows_to_record_batch` signature to accept PG types**

In `arrow_encode.rs`, change the signature:

```rust
pub(crate) fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    columns: &[ColumnSchema],
    pg_types: &[String],
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
```

**Step 3: Implement typed extraction**

Replace the match body in `rows_to_record_batch`:

```rust
use arrow::array::{
    BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringBuilder,
    TimestampMicrosecondArray,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

// Inside rows_to_record_batch:
let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
    .iter()
    .enumerate()
    .map(
        |(col_idx, col)| -> anyhow::Result<Arc<dyn arrow::array::Array>> {
            let pg_type = pg_types.get(col_idx).map(|s| s.as_str()).unwrap_or("");
            match col.data_type.as_str() {
                "Int16" => {
                    let arr: Int16Array = rows
                        .iter()
                        .map(|row| row.try_get::<_, i16>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Int32" => {
                    let arr: Int32Array = rows
                        .iter()
                        .map(|row| row.try_get::<_, i32>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Int64" => {
                    let arr: Int64Array = rows
                        .iter()
                        .map(|row| row.try_get::<_, i64>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Float32" => {
                    let arr: Float32Array = rows
                        .iter()
                        .map(|row| row.try_get::<_, f32>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Float64" => {
                    let arr: Float64Array = rows
                        .iter()
                        .map(|row| row.try_get::<_, f64>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Boolean" => {
                    let arr: BooleanArray = rows
                        .iter()
                        .map(|row| row.try_get::<_, bool>(col_idx).ok())
                        .collect();
                    Ok(Arc::new(arr))
                }
                "TimestampMicros" => {
                    // Extract as NaiveDateTime for both timestamp and timestamptz.
                    // tokio-postgres with-chrono-0_4 provides FromSql for both.
                    // For timestamptz, PG converts to UTC before sending.
                    let arr: TimestampMicrosecondArray = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<_, NaiveDateTime>(col_idx)
                                .ok()
                                .map(|dt| dt.and_utc().timestamp_micros())
                        })
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Date32" => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let arr: Date32Array = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<_, NaiveDate>(col_idx)
                                .ok()
                                .map(|d| (d - epoch).num_days() as i32)
                        })
                        .collect();
                    Ok(Arc::new(arr))
                }
                "Binary" => {
                    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
                    for row in rows {
                        match row.try_get::<_, Vec<u8>>(col_idx).ok() {
                            Some(bytes) => builder.append_value(&bytes),
                            None => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => {
                    // Utf8 path: handles text, varchar, json/jsonb, and all ::text-cast types.
                    let mut builder =
                        StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                    for row in rows {
                        // For json/jsonb: extract as serde_json::Value then serialize.
                        // For text-cast types: the SQL already has ::text, so String works.
                        let val = if pg_type == "json" || pg_type == "jsonb" {
                            row.try_get::<_, serde_json::Value>(col_idx)
                                .ok()
                                .map(|v| v.to_string())
                        } else {
                            row.try_get::<_, String>(col_idx).ok()
                        };
                        match val {
                            Some(s) => builder.append_value(&s),
                            None => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
            }
        },
    )
    .collect::<anyhow::Result<Vec<_>>>()?;
```

**Step 4: Update call sites in `reader/mod.rs`**

In `reader/mod.rs`, update `read_stream_inner()`:

After building `columns` (line ~167) and before `build_arrow_schema`, add:
```rust
// Build parallel PG type vector for type-aware Arrow encoding.
let pg_types: Vec<String> = schema_rows
    .iter()
    .map(|row| row.get::<_, String>(1))
    .collect();

// Apply same projection filter as columns.
let pg_types: Vec<String> = match &ctx.selected_columns {
    Some(selected) if !selected.is_empty() => {
        let all_names: Vec<String> = schema_rows.iter().map(|row| row.get::<_, String>(0)).collect();
        all_names.iter().zip(pg_types.into_iter())
            .filter(|(name, _)| selected.iter().any(|s| s == *name))
            .map(|(_, pg_type)| pg_type)
            .collect()
    }
    _ => pg_types,
};
```

Update `emit_accumulated_rows` to accept and pass `pg_types`:
```rust
fn emit_accumulated_rows(
    rows: &mut Vec<tokio_postgres::Row>,
    columns: &[ColumnSchema],
    pg_types: &[String],
    schema: &Arc<Schema>,
    // ... rest same
) -> anyhow::Result<()> {
    let encode_start = Instant::now();
    let ipc_bytes = rows_to_record_batch(rows, columns, pg_types, schema)
        .and_then(|batch| batch_to_ipc(&batch))?;
    // ... rest same
}
```

Update both call sites of `emit_accumulated_rows` to pass `&pg_types`.

**Step 5: Verify compilation**

```bash
cd connectors/source-postgres && cargo check 2>&1 | tail -10
```
Expected: compiles.

**Step 6: Run existing tests**

```bash
cd connectors/source-postgres && cargo test --lib 2>&1 | tail -10
```
Expected: PASS (existing tests don't exercise new paths but shouldn't break).

**Step 7: Commit**

```bash
git add connectors/source-postgres/src/reader/arrow_encode.rs connectors/source-postgres/src/reader/mod.rs
git commit -m "fix(source-postgres): extract timestamp/date/bytea/json with proper typed FromSql, preventing silent NULL data loss"
```

---

### Task 5: Fix source SQL column list — add `::text` casts

**Files:**
- Modify: `connectors/source-postgres/src/reader/query.rs`
- Modify: `connectors/source-postgres/src/reader/mod.rs` (pass PG types to query builder)

**Step 1: Update `build_base_query` to accept PG types and apply `::text` casts**

Change the signature:
```rust
pub(crate) fn build_base_query(
    ctx: &StreamContext,
    columns: &[ColumnSchema],
    pg_types: &[String],
) -> anyhow::Result<CursorQuery> {
```

Change the column list construction:
```rust
use crate::schema::needs_text_cast;

let col_list = columns
    .iter()
    .zip(pg_types.iter())
    .map(|(c, pg_type)| {
        let ident = quote_identifier(&c.name);
        if needs_text_cast(pg_type) {
            format!("{}::text AS {}", ident, ident)
        } else {
            ident.to_string()
        }
    })
    .collect::<Vec<_>>()
    .join(", ");
```

**Step 2: Update call sites**

In `reader/mod.rs`, update the call to `build_base_query`:
```rust
let cursor_query = build_base_query(ctx, &columns, &pg_types)?;
```

**Step 3: Update tests in query.rs**

Update existing tests to pass an empty or matching `pg_types` vector. For example:
```rust
fn build_base_query_full_refresh_quotes_identifiers() {
    // ...
    let pg_types = vec!["text".to_string()];
    let query = build_base_query(&ctx, &columns, &pg_types).expect("query should build");
    // ...
}
```

Add a new test:
```rust
#[test]
fn build_base_query_applies_text_cast_for_uuid() {
    let mut ctx = base_context();
    let columns = vec![
        ColumnSchema {
            name: "id".to_string(),
            data_type: ArrowDataType::Int64,
            nullable: false,
        },
        ColumnSchema {
            name: "external_id".to_string(),
            data_type: ArrowDataType::Utf8,
            nullable: true,
        },
    ];
    let pg_types = vec!["bigint".to_string(), "uuid".to_string()];
    let query = build_base_query(&ctx, &columns, &pg_types).expect("query should build");
    assert!(query.sql.contains("\"external_id\"::text AS \"external_id\""));
    assert!(!query.sql.contains("\"id\"::text")); // int doesn't need cast
}
```

**Step 4: Run tests**

```bash
cd connectors/source-postgres && cargo test --lib reader::query::tests -- --nocapture 2>&1 | tail -15
```
Expected: PASS

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/reader/query.rs connectors/source-postgres/src/reader/mod.rs
git commit -m "fix(source-postgres): add ::text SQL cast for non-native PG types (numeric, uuid, interval, etc.)"
```

---

### Task 6: Fix cursor value extraction — add chrono fallbacks

**Files:**
- Modify: `connectors/source-postgres/src/reader/mod.rs`

**Step 1: Add chrono fallbacks to text cursor extraction**

In `reader/mod.rs`, around lines 375-384, the text cursor path is:
```rust
let val = row
    .try_get::<_, String>(col_idx)
    .ok()
    .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
    .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string()));
```

Add chrono fallbacks after the i32 fallback:
```rust
use chrono::NaiveDateTime;

let val = row
    .try_get::<_, String>(col_idx)
    .ok()
    .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
    .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string()))
    .or_else(|| {
        row.try_get::<_, NaiveDateTime>(col_idx)
            .ok()
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
    })
    .or_else(|| {
        row.try_get::<_, chrono::NaiveDate>(col_idx)
            .ok()
            .map(|d| d.to_string())
    })
    .or_else(|| {
        row.try_get::<_, serde_json::Value>(col_idx)
            .ok()
            .map(|v| v.to_string())
    });
```

Also add the chrono import at the top of the file:
```rust
use chrono::NaiveDateTime;
```

**Step 2: Verify compilation**

```bash
cd connectors/source-postgres && cargo check 2>&1 | tail -5
```
Expected: compiles.

**Step 3: Commit**

```bash
git add connectors/source-postgres/src/reader/mod.rs
git commit -m "fix(source-postgres): add chrono/json fallbacks in cursor extraction to prevent stalled incremental syncs"
```

---

### Task 7: Source unit tests for type mapping

**Files:**
- Modify: `connectors/source-postgres/src/reader/arrow_encode.rs` (already has tests module)

**Step 1: Add unit tests for the updated `build_arrow_schema`**

These tests verify the schema builder without needing a running database:

```rust
#[test]
fn build_arrow_schema_handles_all_supported_types() {
    let columns = vec![
        ColumnSchema { name: "a".into(), data_type: ArrowDataType::Int16, nullable: false },
        ColumnSchema { name: "b".into(), data_type: ArrowDataType::Int32, nullable: false },
        ColumnSchema { name: "c".into(), data_type: ArrowDataType::Int64, nullable: false },
        ColumnSchema { name: "d".into(), data_type: ArrowDataType::Float32, nullable: false },
        ColumnSchema { name: "e".into(), data_type: ArrowDataType::Float64, nullable: false },
        ColumnSchema { name: "f".into(), data_type: ArrowDataType::Boolean, nullable: false },
        ColumnSchema { name: "g".into(), data_type: ArrowDataType::Utf8, nullable: true },
        ColumnSchema { name: "h".into(), data_type: ArrowDataType::TimestampMicros, nullable: true },
        ColumnSchema { name: "i".into(), data_type: ArrowDataType::Date32, nullable: true },
        ColumnSchema { name: "j".into(), data_type: ArrowDataType::Binary, nullable: true },
    ];
    let schema = build_arrow_schema(&columns);
    assert_eq!(schema.fields().len(), 10);
    assert_eq!(*schema.field(0).data_type(), DataType::Int16);
    assert_eq!(*schema.field(7).data_type(), DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None));
    assert_eq!(*schema.field(8).data_type(), DataType::Date32);
    assert_eq!(*schema.field(9).data_type(), DataType::Binary);
}
```

**Step 2: Run all source tests**

```bash
cd connectors/source-postgres && cargo test --lib 2>&1 | tail -15
```
Expected: PASS

**Step 3: Commit**

```bash
git add connectors/source-postgres/src/reader/arrow_encode.rs
git commit -m "test(source-postgres): add comprehensive Arrow schema builder tests for all supported types"
```

---

### Task 8: Fix dest `arrow_to_pg_type()` — map new Arrow types to PG DDL types

**Files:**
- Modify: `connectors/dest-postgres/src/ddl/type_map.rs`

**Step 1: Write failing test for new type mappings**

Add to the existing `mod tests` block:

```rust
#[test]
fn arrow_to_pg_type_maps_timestamp_date_binary() {
    assert_eq!(
        arrow_to_pg_type(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None
        )),
        "TIMESTAMP"
    );
    assert_eq!(
        arrow_to_pg_type(&DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into())
        )),
        "TIMESTAMPTZ"
    );
    assert_eq!(arrow_to_pg_type(&DataType::Date32), "DATE");
    assert_eq!(arrow_to_pg_type(&DataType::Binary), "BYTEA");
}
```

**Step 2: Run test to verify it fails**

```bash
cd connectors/dest-postgres && cargo test --lib ddl::type_map::tests::arrow_to_pg_type_maps_timestamp_date_binary -- --nocapture 2>&1 | tail -10
```
Expected: FAIL — all map to "TEXT".

**Step 3: Update `arrow_to_pg_type`**

```rust
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        DataType::Date32 => "DATE",
        DataType::Binary => "BYTEA",
        _ => "TEXT",
    }
}
```

**Step 4: Also update `pg_types_compatible` with missing normalizations**

Add these entries to BOTH `norm_a` and `norm_b` match blocks:

```rust
"numeric" | "decimal" => "numeric",
"date" => "date",
"time without time zone" | "time" => "time",
"time with time zone" | "timetz" => "timetz",
"interval" => "interval",
"json" => "json",
"jsonb" => "jsonb",
"uuid" => "uuid",
"bytea" => "bytea",
"serial" | "int4" | "integer" => "integer",
"bigserial" | "int8" | "bigint" => "bigint",
"smallserial" | "int2" | "smallint" => "smallint",
```

Add test:
```rust
#[test]
fn pg_types_compatible_normalizes_timestamp_date_bytea() {
    assert!(pg_types_compatible("timestamp without time zone", "TIMESTAMP"));
    assert!(pg_types_compatible("timestamp", "TIMESTAMP"));
    assert!(pg_types_compatible("date", "DATE"));
    assert!(pg_types_compatible("bytea", "BYTEA"));
    assert!(pg_types_compatible("numeric", "NUMERIC"));
    assert!(!pg_types_compatible("timestamp", "TEXT"));
}
```

**Step 5: Run tests**

```bash
cd connectors/dest-postgres && cargo test --lib ddl::type_map::tests -- --nocapture 2>&1 | tail -15
```
Expected: PASS

**Step 6: Commit**

```bash
git add connectors/dest-postgres/src/ddl/type_map.rs
git commit -m "feat(dest-postgres): map Timestamp/Date32/Binary Arrow types to TIMESTAMP/DATE/BYTEA, fix pg_types_compatible normalizations"
```

---

### Task 9: Fix dest `TypedCol` + `downcast_columns()` — handle new types

**Files:**
- Modify: `connectors/dest-postgres/src/batch/typed_col.rs`

**Step 1: Add new TypedCol variants**

Add imports:
```rust
use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
```

Add new variants to `TypedCol`:
```rust
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a arrow::array::StringArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Date32(&'a Date32Array),
    Binary(&'a BinaryArray),
    Null,
}
```

**Step 2: Update `downcast_columns`**

Add timestamp/date/binary arms:
```rust
DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
    TypedCol::TimestampMicros(col.as_any().downcast_ref().unwrap())
}
DataType::Date32 => TypedCol::Date32(col.as_any().downcast_ref().unwrap()),
DataType::Binary => TypedCol::Binary(col.as_any().downcast_ref().unwrap()),
```

**Step 3: Add new SqlParamValue variants**

```rust
use chrono::{NaiveDate, NaiveDateTime};

pub(crate) enum SqlParamValue<'a> {
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Boolean(Option<bool>),
    Text(Option<&'a str>),
    Timestamp(Option<NaiveDateTime>),
    Date(Option<NaiveDate>),
    Bytes(Option<&'a [u8]>),
}
```

Update `as_tosql`:
```rust
impl<'a> SqlParamValue<'a> {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int16(v) => v,
            Self::Int32(v) => v,
            Self::Int64(v) => v,
            Self::Float32(v) => v,
            Self::Float64(v) => v,
            Self::Boolean(v) => v,
            Self::Text(v) => v,
            Self::Timestamp(v) => v,
            Self::Date(v) => v,
            Self::Bytes(v) => v,
        }
    }
}
```

**Step 4: Update `sql_param_value`**

Add arms for new types:
```rust
TypedCol::TimestampMicros(arr) => {
    if arr.is_null(row_idx) {
        SqlParamValue::Timestamp(None)
    } else {
        let micros = arr.value(row_idx);
        let secs = micros.div_euclid(1_000_000);
        let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
        let dt = DateTime::from_timestamp(secs, nsecs)
            .map(|dt| dt.naive_utc());
        SqlParamValue::Timestamp(dt)
    }
}
TypedCol::Date32(arr) => {
    if arr.is_null(row_idx) {
        SqlParamValue::Date(None)
    } else {
        let days = arr.value(row_idx);
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date = epoch.checked_add_signed(chrono::Duration::days(days as i64));
        SqlParamValue::Date(date)
    }
}
TypedCol::Binary(arr) => {
    if arr.is_null(row_idx) {
        SqlParamValue::Bytes(None)
    } else {
        SqlParamValue::Bytes(Some(arr.value(row_idx)))
    }
}
```

Add chrono imports:
```rust
use chrono::{DateTime, NaiveDate, NaiveDateTime};
```

**Step 5: Add tests**

```rust
#[test]
fn sql_param_value_handles_timestamp() {
    use arrow::array::TimestampMicrosecondArray;
    // 2024-01-15 10:30:00 UTC = 1705312200000000 micros
    let arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64), None]);
    let col = TypedCol::TimestampMicros(&arr);

    match sql_param_value(&col, 0) {
        SqlParamValue::Timestamp(Some(dt)) => {
            assert_eq!(dt.format("%Y-%m-%d %H:%M:%S").to_string(), "2024-01-15 10:30:00");
        }
        other => panic!("expected Timestamp(Some), got {:?}", std::mem::discriminant(&other)),
    }
    match sql_param_value(&col, 1) {
        SqlParamValue::Timestamp(None) => {}
        _ => panic!("expected Timestamp(None)"),
    }
}

#[test]
fn sql_param_value_handles_date32() {
    use arrow::array::Date32Array;
    // 2024-01-15 = 19737 days since epoch
    let arr = Date32Array::from(vec![Some(19737), None]);
    let col = TypedCol::Date32(&arr);

    match sql_param_value(&col, 0) {
        SqlParamValue::Date(Some(d)) => {
            assert_eq!(d.to_string(), "2024-01-15");
        }
        other => panic!("expected Date(Some), got {:?}", std::mem::discriminant(&other)),
    }
    match sql_param_value(&col, 1) {
        SqlParamValue::Date(None) => {}
        _ => panic!("expected Date(None)"),
    }
}

#[test]
fn sql_param_value_handles_binary() {
    use arrow::array::BinaryArray;
    let arr = BinaryArray::from(vec![Some(b"hello" as &[u8]), None]);
    let col = TypedCol::Binary(&arr);

    match sql_param_value(&col, 0) {
        SqlParamValue::Bytes(Some(b)) => assert_eq!(b, b"hello"),
        _ => panic!("expected Bytes(Some)"),
    }
    match sql_param_value(&col, 1) {
        SqlParamValue::Bytes(None) => {}
        _ => panic!("expected Bytes(None)"),
    }
}
```

**Step 6: Run tests**

```bash
cd connectors/dest-postgres && cargo test --lib batch::typed_col::tests -- --nocapture 2>&1 | tail -15
```
Expected: PASS

**Step 7: Commit**

```bash
git add connectors/dest-postgres/src/batch/typed_col.rs
git commit -m "feat(dest-postgres): add TimestampMicros, Date32, Binary TypedCol variants for INSERT path"
```

---

### Task 10: Fix dest COPY text format — timestamp/date/binary formatting

**Files:**
- Modify: `connectors/dest-postgres/src/batch/copy_format.rs`

**Step 1: Add new arms to `format_copy_typed_value`**

```rust
TypedCol::TimestampMicros(arr) => {
    if arr.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }
    let micros = arr.value(row_idx);
    let secs = micros.div_euclid(1_000_000);
    let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
        let _ = write!(buf, "{}", dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f"));
    } else {
        buf.extend_from_slice(b"\\N");
    }
}
TypedCol::Date32(arr) => {
    if arr.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }
    let days = arr.value(row_idx);
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
        let _ = write!(buf, "{}", date.format("%Y-%m-%d"));
    } else {
        buf.extend_from_slice(b"\\N");
    }
}
TypedCol::Binary(arr) => {
    if arr.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }
    // COPY text format for bytea: hex encoding with \\x prefix.
    buf.extend_from_slice(b"\\\\x");
    for byte in arr.value(row_idx) {
        let _ = write!(buf, "{:02x}", byte);
    }
}
```

Add import: `use chrono;`

**Step 2: Add tests**

```rust
#[test]
fn format_copy_typed_value_formats_timestamp() {
    use arrow::array::TimestampMicrosecondArray;
    // 2024-01-15 10:30:00.123456 UTC
    let micros = 1705312200123456i64;
    let arr = TimestampMicrosecondArray::from(vec![Some(micros), None]);
    let col = TypedCol::TimestampMicros(&arr);

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 0);
    assert_eq!(String::from_utf8(buf).unwrap(), "2024-01-15 10:30:00.123456");

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 1);
    assert_eq!(String::from_utf8(buf).unwrap(), "\\N");
}

#[test]
fn format_copy_typed_value_formats_date32() {
    use arrow::array::Date32Array;
    // 2024-01-15 = 19737 days since 1970-01-01
    let arr = Date32Array::from(vec![Some(19737), None]);
    let col = TypedCol::Date32(&arr);

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 0);
    assert_eq!(String::from_utf8(buf).unwrap(), "2024-01-15");

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 1);
    assert_eq!(String::from_utf8(buf).unwrap(), "\\N");
}

#[test]
fn format_copy_typed_value_formats_binary_hex() {
    use arrow::array::BinaryArray;
    let arr = BinaryArray::from(vec![Some(b"\xde\xad\xbe\xef" as &[u8]), None]);
    let col = TypedCol::Binary(&arr);

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 0);
    assert_eq!(String::from_utf8(buf).unwrap(), "\\\\xdeadbeef");

    let mut buf = Vec::new();
    format_copy_typed_value(&mut buf, &col, 1);
    assert_eq!(String::from_utf8(buf).unwrap(), "\\N");
}
```

**Step 3: Run tests**

```bash
cd connectors/dest-postgres && cargo test --lib batch::copy_format::tests -- --nocapture 2>&1 | tail -15
```
Expected: PASS

**Step 4: Commit**

```bash
git add connectors/dest-postgres/src/batch/copy_format.rs
git commit -m "feat(dest-postgres): add timestamp/date/binary COPY text format encoding"
```

---

### Task 11: Fix dest INSERT path — verify SqlParamValue works with tokio-postgres ToSql

**Files:**
- No new code changes needed if Task 9 was implemented correctly.

The `SqlParamValue` variants added in Task 9 use:
- `Option<NaiveDateTime>` — implements `ToSql` via `tokio-postgres with-chrono-0_4`
- `Option<NaiveDate>` — implements `ToSql` via `tokio-postgres with-chrono-0_4`
- `Option<&[u8]>` — implements `ToSql` natively in tokio-postgres

**Step 1: Verify the INSERT path compiles end-to-end**

```bash
cd connectors/dest-postgres && cargo check 2>&1 | tail -10
```
Expected: compiles.

**Step 2: Run all dest tests**

```bash
cd connectors/dest-postgres && cargo test --lib 2>&1 | tail -15
```
Expected: PASS

**Step 3: Commit (if any fixups needed)**

```bash
# Only if changes were needed
git add connectors/dest-postgres/
git commit -m "fix(dest-postgres): ensure INSERT path handles timestamp/date/binary via ToSql"
```

---

### Task 12: Fix dest `pg_types_compatible()` — complete normalizations

This was partially done in Task 8. Verify completeness here.

**Files:**
- Modify: `connectors/dest-postgres/src/ddl/type_map.rs`

**Step 1: Ensure all PG type aliases are normalized**

The complete normalization list (for both `norm_a` and `norm_b`):

```rust
let norm = |s: &str| -> &str {
    match s {
        "int" | "int4" | "integer" | "serial" => "integer",
        "int2" | "smallint" | "smallserial" => "smallint",
        "int8" | "bigint" | "bigserial" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" | "character" | "char" | "name" | "bpchar" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "numeric" | "decimal" => "numeric",
        "date" => "date",
        "time without time zone" | "time" => "time",
        "time with time zone" | "timetz" => "timetz",
        "bytea" => "bytea",
        "json" => "json",
        "jsonb" => "jsonb",
        "uuid" => "uuid",
        "interval" => "interval",
        "inet" => "inet",
        "cidr" => "cidr",
        "macaddr" | "macaddr8" => "macaddr",
        other => other,
    }
};
```

Refactor `pg_types_compatible` to use a shared normalizer to avoid duplicating the match:

```rust
fn normalize_pg_type(t: &str) -> &str {
    match t {
        "int" | "int4" | "integer" | "serial" => "integer",
        "int2" | "smallint" | "smallserial" => "smallint",
        "int8" | "bigint" | "bigserial" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" | "character" | "char" | "name" | "bpchar" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "numeric" | "decimal" => "numeric",
        "date" => "date",
        "time without time zone" | "time" => "time",
        "time with time zone" | "timetz" => "timetz",
        "bytea" => "bytea",
        "json" => "json",
        "jsonb" => "jsonb",
        "uuid" => "uuid",
        "interval" => "interval",
        "inet" => "inet",
        "cidr" => "cidr",
        "macaddr" | "macaddr8" => "macaddr",
        other => other,
    }
}

pub(crate) fn pg_types_compatible(info_schema_type: &str, ddl_type: &str) -> bool {
    let a = info_schema_type.to_lowercase();
    let b = ddl_type.to_lowercase();
    normalize_pg_type(&a) == normalize_pg_type(&b)
}
```

**Step 2: Add comprehensive test**

```rust
#[test]
fn pg_types_compatible_comprehensive() {
    // Integer aliases
    assert!(pg_types_compatible("integer", "INT4"));
    assert!(pg_types_compatible("serial", "INTEGER"));
    assert!(pg_types_compatible("bigserial", "BIGINT"));
    assert!(pg_types_compatible("smallserial", "SMALLINT"));

    // Text aliases
    assert!(pg_types_compatible("character varying", "TEXT"));
    assert!(pg_types_compatible("name", "TEXT"));
    assert!(pg_types_compatible("bpchar", "TEXT"));

    // Timestamp aliases
    assert!(pg_types_compatible("timestamp with time zone", "timestamptz"));
    assert!(pg_types_compatible("timestamp without time zone", "TIMESTAMP"));

    // Numeric aliases
    assert!(pg_types_compatible("numeric", "DECIMAL"));
    assert!(pg_types_compatible("decimal", "NUMERIC"));

    // Date/time types
    assert!(pg_types_compatible("date", "DATE"));
    assert!(pg_types_compatible("time without time zone", "TIME"));
    assert!(pg_types_compatible("time with time zone", "TIMETZ"));

    // Other types
    assert!(pg_types_compatible("bytea", "BYTEA"));
    assert!(pg_types_compatible("uuid", "UUID"));
    assert!(pg_types_compatible("json", "JSON"));
    assert!(pg_types_compatible("jsonb", "JSONB"));

    // Incompatible
    assert!(!pg_types_compatible("bigint", "text"));
    assert!(!pg_types_compatible("timestamp", "date"));
    assert!(!pg_types_compatible("json", "jsonb"));
}
```

**Step 3: Run tests**

```bash
cd connectors/dest-postgres && cargo test --lib ddl::type_map::tests -- --nocapture 2>&1 | tail -15
```
Expected: PASS

**Step 4: Commit**

```bash
git add connectors/dest-postgres/src/ddl/type_map.rs
git commit -m "fix(dest-postgres): comprehensive pg_types_compatible normalizations for all PG type aliases"
```

---

### Task 13: Dest unit tests — comprehensive type coverage

**Files:**
- Modify: `connectors/dest-postgres/src/batch/typed_col.rs`
- Modify: `connectors/dest-postgres/src/batch/copy_format.rs`

**Step 1: Add downcast_columns test for new types**

In `typed_col.rs` tests:

```rust
#[test]
fn downcast_columns_handles_timestamp_date_binary() {
    use arrow::array::{BinaryArray, Date32Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("d", DataType::Date32, true),
        Field::new("b", DataType::Binary, true),
    ]));

    let ts_arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64)]);
    let d_arr = Date32Array::from(vec![Some(19737)]);
    let b_arr = BinaryArray::from(vec![Some(b"test" as &[u8])]);

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ts_arr), Arc::new(d_arr), Arc::new(b_arr)],
    )
    .unwrap();

    let cols = downcast_columns(&batch, &[0, 1, 2]);
    assert!(matches!(cols[0], TypedCol::TimestampMicros(_)));
    assert!(matches!(cols[1], TypedCol::Date32(_)));
    assert!(matches!(cols[2], TypedCol::Binary(_)));
}
```

**Step 2: Run all dest tests**

```bash
cd connectors/dest-postgres && cargo test --lib 2>&1 | tail -15
```
Expected: PASS

**Step 3: Commit**

```bash
git add connectors/dest-postgres/
git commit -m "test(dest-postgres): add comprehensive downcast and format tests for timestamp/date/binary types"
```

---

### Task 14: E2E integration test fixture with ALL PG types

**Files:**
- Create: `tests/fixtures/all_types.sql`
- Modify: `tests/e2e.sh`

**Step 1: Create SQL fixture with all PG types**

```sql
-- All PG types fixture for type correctness testing.
CREATE TABLE IF NOT EXISTS all_types (
    id              SERIAL PRIMARY KEY,
    -- Integer types
    col_smallint    SMALLINT,
    col_int         INTEGER,
    col_bigint      BIGINT,
    -- Float types
    col_real        REAL,
    col_double      DOUBLE PRECISION,
    -- Boolean
    col_bool        BOOLEAN,
    -- Text types
    col_text        TEXT,
    col_varchar     VARCHAR(255),
    col_char        CHAR(10),
    -- Timestamp types
    col_timestamp   TIMESTAMP WITHOUT TIME ZONE,
    col_timestamptz TIMESTAMP WITH TIME ZONE,
    -- Date
    col_date        DATE,
    -- Binary
    col_bytea       BYTEA,
    -- JSON
    col_json        JSON,
    col_jsonb       JSONB,
    -- Text-cast types (extracted via ::text)
    col_numeric     NUMERIC(18,6),
    col_uuid        UUID,
    col_time        TIME WITHOUT TIME ZONE,
    col_timetz      TIME WITH TIME ZONE,
    col_interval    INTERVAL,
    col_inet        INET,
    col_cidr        CIDR,
    col_macaddr     MACADDR
);

INSERT INTO all_types (
    col_smallint, col_int, col_bigint,
    col_real, col_double,
    col_bool,
    col_text, col_varchar, col_char,
    col_timestamp, col_timestamptz,
    col_date,
    col_bytea,
    col_json, col_jsonb,
    col_numeric, col_uuid,
    col_time, col_timetz, col_interval,
    col_inet, col_cidr, col_macaddr
) VALUES
(
    1, 100, 1000000000000,
    3.14, 2.718281828459045,
    true,
    'hello world', 'varchar_val', 'char_val  ',
    '2024-01-15 10:30:00', '2024-01-15 10:30:00+00',
    '2024-01-15',
    E'\\xDEADBEEF',
    '{"key": "value"}', '{"nested": {"a": 1}}',
    123456.789012, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '10:30:00', '10:30:00+02', '1 year 2 months 3 days',
    '192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03'
),
(
    NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL
);
```

**Step 2: Add all_types verification to e2e.sh**

After existing e2e verification, add a section that:
1. Seeds the `all_types` table
2. Runs a pipeline that reads from `all_types`
3. Verifies each column type round-tripped correctly in the destination:

```bash
# Verify all_types round-trip
echo "=== Verifying all PG types round-trip ==="
psql "$PG_URL" -c "SELECT count(*) FROM raw.all_types" | grep -q "2" || {
    echo "FAIL: expected 2 rows in raw.all_types"
    exit 1
}

# Verify non-null row has correct values
psql "$PG_URL" -At -c "
  SELECT
    col_smallint = 1
    AND col_int = 100
    AND col_bigint = 1000000000000
    AND col_bool = true
    AND col_text = 'hello world'
    AND col_date = '2024-01-15'
    AND col_timestamp = '2024-01-15 10:30:00'
    AND col_bytea = E'\\\\xDEADBEEF'
  FROM raw.all_types WHERE id = 1
" | grep -q "t" || {
    echo "FAIL: all_types values mismatch"
    exit 1
}

# Verify null row is all nulls (except id)
psql "$PG_URL" -At -c "
  SELECT
    col_smallint IS NULL
    AND col_int IS NULL
    AND col_bigint IS NULL
    AND col_timestamp IS NULL
    AND col_date IS NULL
    AND col_bytea IS NULL
    AND col_json IS NULL
  FROM raw.all_types WHERE id = 2
" | grep -q "t" || {
    echo "FAIL: all_types NULL row has non-null values"
    exit 1
}

echo "=== All PG types round-trip PASSED ==="
```

**Step 3: Run E2E test**

```bash
just e2e
```
Expected: PASS — all types round-trip correctly.

**Step 4: Commit**

```bash
git add tests/fixtures/all_types.sql tests/e2e.sh
git commit -m "test: add comprehensive all-PG-types E2E fixture and verification"
```

---

## Out of Scope (Future Work)

1. **CDC all-Utf8 schema** — The CDC path (`cdc.rs`) hardcodes all columns as Utf8 via `build_cdc_arrow_schema`. This means CDC-produced batches have schema mismatch with reader-produced batches. This is a separate task.

2. **timestamptz vs timestamp distinction** — Currently both map to `ArrowDataType::TimestampMicros`. To properly distinguish, the `ArrowDataType` enum would need separate variants or metadata. For now, all timestamps create `TIMESTAMP` columns in dest. This is acceptable since timestamptz values are converted to UTC on read and stored as micros.

3. **ARRAY/RANGE/COMPOSITE types** — These are handled as `::text` cast to Utf8 and round-trip as TEXT columns. Proper Arrow list/struct types would require significant schema changes.

4. **Decimal128** — `ArrowDataType::Decimal128` exists in the protocol enum but is not wired through. Numeric values stay as text-cast Utf8 for now. Proper decimal support requires fixed precision/scale tracking.
