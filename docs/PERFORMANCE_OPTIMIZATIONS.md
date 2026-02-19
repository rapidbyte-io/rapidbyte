# Performance Optimizations

Benchmark-driven analysis of Rapidbyte pipeline bottlenecks with prioritized optimization recommendations.

## Current Baseline

All benchmarks run on macOS (Darwin 24.6.0), local Docker PostgreSQL, AOT-compiled WASM connectors (release mode, 3 iterations averaged).

### 10,000 Rows (0.89 MB, 93 B/row avg)

| Metric (avg)          | INSERT   | COPY     | Speedup |
|-----------------------|----------|----------|---------|
| **Total duration**    | 0.153s   | 0.121s   | 1.3x    |
| Dest duration         | 0.133s   | 0.096s   | 1.4x    |
| - Connect             | 0.011s   | 0.011s   | 1.0x    |
| - Flush               | 0.107s   | 0.060s   | 1.8x    |
| - Arrow decode        | 0.000s   | 0.000s   | -       |
| - Commit              | 0.002s   | 0.012s   | 0.1x    |
| - VM setup            | 0.002s   | 0.001s   | -       |
| - Recv loop           | 0.131s   | 0.096s   | 1.4x    |
| - WASM overhead       | 0.000s   | 0.000s   | -       |
| Source duration        | 0.047s   | 0.048s   | 1.0x    |
| - Connect             | 0.011s   | 0.011s   | 1.0x    |
| - Query               | 0.011s   | 0.011s   | 1.0x    |
| - Fetch               | 0.022s   | 0.024s   | 0.9x    |
| - Arrow encode        | 0.003s   | 0.003s   | 0.9x    |
| Source module load     | 5.7ms    | 2.0ms    | 2.8x    |
| Dest module load       | 5.7ms    | 2.0ms    | 2.8x    |
| **Throughput (rows/s)**| 65,801   | 85,098   | 1.3x    |
| **Throughput (MB/s)**  | 5.89     | 7.62     | 1.3x    |

### 100,000 Rows (9.04 MB, 94 B/row avg)

| Metric (avg)          | INSERT   | COPY     | Speedup |
|-----------------------|----------|----------|---------|
| **Total duration**    | 1.054s   | 0.507s   | 2.1x    |
| Dest duration         | 1.034s   | 0.495s   | 2.1x    |
| - Connect             | 0.012s   | 0.012s   | 1.0x    |
| - Flush               | 1.006s   | 0.399s   | 2.5x    |
| - Arrow decode        | 0.002s   | 0.003s   | 0.9x    |
| - Commit              | 0.003s   | 0.067s   | 0.0x    |
| - VM setup            | 0.001s   | 0.001s   | -       |
| - Recv loop           | 1.033s   | 0.494s   | 2.1x    |
| - WASM overhead       | 0.000s   | 0.000s   | -       |
| Source duration        | 0.297s   | 0.327s   | 0.9x    |
| - Connect             | 0.012s   | 0.013s   | 1.0x    |
| - Query               | 0.012s   | 0.013s   | 0.9x    |
| - Fetch               | 0.270s   | 0.298s   | 0.9x    |
| - Arrow encode        | 0.024s   | 0.027s   | 0.9x    |
| Source module load     | 7.0ms    | 2.3ms    | 3.0x    |
| Dest module load       | 5.7ms    | 2.3ms    | 2.4x    |
| **Throughput (rows/s)**| 94,911   | 199,719  | 2.1x    |
| **Throughput (MB/s)**  | 8.58     | 18.06    | 2.1x    |

### Key Observations

1. **Destination flush dominates**: At 100K rows, `dest_flush_secs` is 95% of INSERT total (1.006s / 1.054s) and 79% of COPY total (0.399s / 0.507s).
2. **COPY is 2-2.5x faster than INSERT** at 100K rows, up from 1.3x at 10K rows — the advantage grows with dataset size.
3. **Arrow encode/decode is negligible**: ~0.024s encode + ~0.002s decode = 2.5% of total at 100K rows.
4. **WASM overhead is near-zero**: AOT compilation eliminates runtime interpretation cost.
5. **Source is not the bottleneck**: Source duration (0.297s) is 3.5x faster than dest duration (1.034s) with INSERT at 100K.
6. **Module load with AOT is fast**: 2-7ms vs ~500ms without AOT.
7. **COPY commit is slower**: 0.067s vs 0.003s for INSERT — PostgreSQL flushes COPY data to WAL at commit time.

### Timing Hierarchy (100K rows, INSERT mode)

```
Total: 1.054s
├── Dest: 1.034s (98.1%)
│   ├── Flush (INSERT execution): 1.006s (95.4%)  ← PRIMARY BOTTLENECK
│   │   ├── SQL string building: ~200ms (est.)
│   │   │   ├── format_sql_value() per cell: 700K String allocs + 700K downcasts
│   │   │   └── SQL VALUES string reallocation (no capacity hint)
│   │   └── PG wire protocol + execution: ~800ms
│   ├── Connect: 0.012s (1.1%)
│   ├── Commit: 0.003s (0.3%)
│   ├── Arrow decode: 0.002s (0.2%)
│   └── VM setup: 0.001s (0.1%)
├── Source: 0.297s (28.2%, overlapped)
│   ├── Fetch (cursor iteration): 0.270s (25.6%)
│   │   ├── PG row fetching: ~246ms (100 round-trips @ FETCH_CHUNK=1000)
│   │   └── Row accumulation + bookkeeping: ~24ms
│   ├── Arrow encode: 0.024s (2.3%)
│   ├── Query (DECLARE CURSOR): 0.012s (1.1%)
│   └── Connect: 0.012s (1.1%)
└── Module load: ~10ms (< 1%)
```

Note: Source and dest run in parallel on separate threads. Total wall time is max(source, dest), not the sum.

## Bottleneck Analysis

### P0: Destination INSERT — per-cell String allocation + per-cell downcast

**Impact**: High (affects every cell in every row for INSERT mode)
**Files**: `connectors/dest-postgres/src/sink.rs:1109-1125, 1380-1437`

Two compounding issues in the innermost loop:

**Issue 1**: `format_sql_value()` returns a new `String` for every cell:

```rust
fn format_sql_value(col: &dyn Array, row_idx: usize) -> String {
    // Returns "NULL".to_string(), arr.value(row_idx).to_string(), etc.
    // Each call allocates a fresh String on the heap
}
```

At 100K rows with 7 columns = 700K `String` allocations per pipeline run. Each allocation hits the global allocator, and each result is immediately consumed by `push_str` into the SQL buffer, then dropped.

**Issue 2**: Inside `format_sql_value`, `col.as_any().downcast_ref::<Int32Array>().unwrap()` is called per cell. While `downcast_ref` itself is cheap (a `TypeId` comparison), the repeated per-cell function call + match + downcast is unnecessary work that also hurts branch prediction. Downcasting once per column per batch eliminates 700K redundant type checks.

**Fix**: Pre-downcast columns outside the row loop and write directly into the SQL buffer:

```rust
enum TypedCol<'a> {
    Int32(&'a Int32Array),
    Utf8(&'a StringArray),
    // ...
}

// Downcast once per batch, before the row loop
let typed_cols: Vec<TypedCol> = active_cols.iter().map(|&i| {
    let col = batch.column(i);
    match col.data_type() {
        DataType::Int32 => TypedCol::Int32(col.as_any().downcast_ref().unwrap()),
        DataType::Utf8 => TypedCol::Utf8(col.as_string::<i32>()),
        // ...
    }
}).collect();

// Inner loop: no allocations, no downcasts
for row_idx in chunk_start..chunk_end {
    for col in &typed_cols {
        match col {
            TypedCol::Int32(arr) if arr.is_null(row_idx) => buf.push_str("NULL"),
            TypedCol::Int32(arr) => write!(buf, "{}", arr.value(row_idx)).unwrap(),
            TypedCol::Utf8(arr) if arr.is_null(row_idx) => buf.push_str("NULL"),
            TypedCol::Utf8(arr) => {
                buf.push('\'');
                for ch in arr.value(row_idx).chars() {
                    if ch == '\'' { buf.push_str("''"); }
                    else if ch == '\0' { /* skip */ }
                    else { buf.push(ch); }
                }
                buf.push('\'');
            }
            // ...
        }
    }
}
```

**Estimated improvement**: 15-25% reduction in INSERT flush time by eliminating 700K String allocations + 700K downcasts (100K rows).
**Complexity**: Low-Medium — new enum + function restructure + callers updated.

### P0: Destination INSERT — SQL string capacity pre-allocation

**Impact**: Medium-High (avoids repeated reallocation of the SQL buffer)
**Files**: `connectors/dest-postgres/src/sink.rs:1107`

The SQL VALUES string is built without pre-reserved capacity:

```rust
let mut sql = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);
// Then pushes row-by-row without capacity hint
```

With INSERT_CHUNK_SIZE=1000 rows and ~94 bytes/row, the SQL string reaches ~100KB. The `String` starts small and grows via repeated doubling reallocations.

**Fix**: Pre-calculate and reserve capacity:

```rust
let estimated_row_width = active_cols.len() * 15; // avg value width
let estimated_capacity = header_len + chunk_size * (estimated_row_width + 3);
let mut sql = String::with_capacity(estimated_capacity);
write!(sql, "INSERT INTO {} ({}) VALUES ", qualified_table, col_list).unwrap();
```

**Estimated improvement**: 5-10% reduction in INSERT flush time.
**Complexity**: Low — add one `with_capacity` call + header formatting.

### P1: Source — unnecessary intermediate allocations in Arrow array building

**Impact**: Medium (affects all columns during Arrow encoding)
**Files**: `connectors/source-postgres/src/source.rs:446-503`

**Issue 1 (text columns)**: String columns are collected into `Vec<Option<String>>` and then mapped to `Vec<Option<&str>>` to construct the `StringArray`:

```rust
let values: Vec<Option<String>> = rows.iter()
    .map(|row| row.try_get::<_, String>(col_idx).ok())
    .collect();
Ok(Arc::new(StringArray::from(
    values.iter().map(|v| v.as_deref()).collect::<Vec<Option<&str>>>(),
)))
```

This creates two intermediate vectors: one of owned `String`s and one of `&str` references.

**Issue 2 (primitive columns)**: Primitive arrays also use an intermediate `Vec`:

```rust
let values: Vec<Option<i32>> = rows.iter()
    .map(|row| row.try_get::<_, i32>(col_idx).ok())
    .collect();
Ok(Arc::new(Int32Array::from(values)))
```

`Vec<Option<i64>>` uses 16 bytes per element (8 for value + 8 for discriminant/padding), creating unnecessary heap allocation for 10K rows. Arrow primitive arrays implement `FromIterator<Option<T>>`, so the iterator can be collected directly into the array.

**Fix**: Use `StringBuilder` for text, and `collect()` directly into arrays for primitives:

```rust
// Text columns: use StringBuilder
use arrow::array::StringBuilder;
let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
for row in rows {
    match row.try_get::<_, String>(col_idx).ok() {
        Some(s) => builder.append_value(&s),
        None => builder.append_null(),
    }
}
Ok(Arc::new(builder.finish()))

// Primitive columns: collect directly into Arrow array
let arr: Int32Array = rows.iter()
    .map(|row| row.try_get::<_, i32>(col_idx).ok())
    .collect();
Ok(Arc::new(arr))
```

**Estimated improvement**: 5-15% reduction in source Arrow encode time (0.024s at 100K).
**Complexity**: Low — replace collect pattern with builder/direct collect.

### P1: Source — FETCH_CHUNK causes 10x excess network round-trips

**Impact**: Medium-High (significant for remote PostgreSQL, modest for local Docker)
**Files**: `connectors/source-postgres/src/source.rs:20-21`

`FETCH_CHUNK` is 1,000 rows but `BATCH_SIZE` is 10,000 rows. To fill one Arrow batch, the connector makes 10 sequential `FETCH 1000 FROM rb_cursor` round-trips to PostgreSQL:

```rust
const BATCH_SIZE: usize = 10_000;
const FETCH_CHUNK: usize = 1_000;
```

At 100K rows, that's 100 round-trips instead of 10. On local Docker (RTT ~0.1ms), the extra round-trip overhead is ~9ms total. On remote PostgreSQL (RTT ~1-5ms), this becomes 90-450ms of pure network latency waste.

**Fix**: Align `FETCH_CHUNK` with `BATCH_SIZE`:

```rust
const FETCH_CHUNK: usize = 10_000;
```

Or better, make it configurable. The tradeoff is memory: `FETCH_CHUNK` rows of `tokio_postgres::Row` are held in memory before being converted to Arrow. At 94 B/row, 10K rows = ~1MB which is well within limits.

**Estimated improvement**: 10x fewer PG round-trips. Saves ~9ms locally, potentially 100ms+ on remote PG.
**Complexity**: Trivial — one constant change.

### P1: WASM Runtime — memory copies per batch

**Impact**: Medium (but near-zero measured overhead in current benchmarks)
**Files**: `crates/rapidbyte-core/src/runtime/host_functions.rs:110-180, 186-277`

Each batch crosses the guest/host boundary with this copy chain:

1. Guest emits batch → `read_from_guest()` copies WASM linear memory → `Vec<u8>` (host)
2. Optional compress → new `Vec<u8>`
3. `mpsc::sync_channel` send → moves (no copy, just ownership transfer)
4. Optional decompress → new `Vec<u8>`
5. Host writes to guest → `memory.set_data()` copies host → WASM linear memory

That's 2 WASM memory copies + 2 possible compression allocations per batch. At 100K rows with 10 batches, measured WASM overhead is ~0ms (near noise floor). This is because batch sizes are large (10K rows each), amortizing the per-batch cost.

**Fix** (future): Pool allocations for decompressed data in `host_next_batch`:

```rust
// Reuse a thread-local buffer instead of allocating new Vec each time
thread_local! {
    static DECOMPRESS_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1 << 20));
}
```

**Estimated improvement**: Negligible with current batch sizes. Becomes relevant if batch sizes decrease.
**Complexity**: Medium — need to manage buffer lifetime across calls.

### P2: Source — redundant `estimate_row_bytes()`

**Impact**: Low
**Files**: `connectors/source-postgres/src/source.rs:26-42, 193, 228`

`estimate_row_bytes()` is called on every row during fetching for two purposes:
1. `max_record_bytes` enforcement (line 193)
2. `max_batch_bytes` accumulation (line 228)

For text columns, it calls `row.try_get::<_, String>(col_idx)` which deserializes the PG wire protocol value into a `String`, only to measure its `.len()` and then drop it. The same value is later re-extracted in `rows_to_record_batch()`.

**Fix**: Calculate a per-column fixed estimate from the schema (use max column type widths) instead of deserializing per-row. For `max_record_bytes`, sample the first row.

```rust
fn estimate_fixed_row_bytes(columns: &[ColumnSchema]) -> usize {
    columns.iter().map(|col| match col.data_type.as_str() {
        "Int16" => 3, "Int32" | "Float32" => 5,
        "Int64" | "Float64" => 9, "Boolean" => 2,
        _ => 64, // conservative estimate for text
    }).sum()
}
```

**Estimated improvement**: 2-5% reduction in source fetch time.
**Complexity**: Low.

### P2: Source — cursor value extraction tries wrong types first

**Impact**: Low-Medium (only affects incremental sync mode, per-row cost)
**Files**: `connectors/source-postgres/src/source.rs:277-281`

When tracking the max cursor value for incremental sync, the code chains `try_get` calls trying `String` first, then `i64`, then `i32`:

```rust
let val: Option<String> = row.try_get::<_, String>(col_idx).ok()
    .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
    .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string()));
```

If the cursor column is `i32`, every row creates and discards two `tokio_postgres::Error` objects (for the failed `String` and `i64` attempts) before succeeding. Error creation in tokio-postgres involves formatting a descriptive string, so this is 200K unnecessary string allocations at 100K rows.

Note: This does NOT apply to our benchmark (which uses FullRefresh, not Incremental), but would be a problem in production incremental syncs.

**Fix**: The `CursorInfo` already contains `cursor_type: CursorType`. Use it to call the correct `try_get` directly:

```rust
if let Some(col_idx) = cursor_col_idx {
    let cursor_type = &ctx.cursor_info.as_ref().unwrap().cursor_type;
    let val: Option<String> = match cursor_type {
        CursorType::Int64 => row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()),
        CursorType::Utf8 => row.try_get::<_, String>(col_idx).ok(),
        // ... other types
    };
}
```

**Estimated improvement**: Eliminates 2 error allocations per row in incremental mode. ~5-10% of source fetch time for incremental syncs.
**Complexity**: Low — match on known cursor type.

### P2: Destination INSERT — upsert clause rebuilt per chunk

**Impact**: Low (only affects upsert mode, `ceil(rows/1000)` times)
**Files**: `connectors/dest-postgres/src/sink.rs:1128-1149`

The `ON CONFLICT (...) DO UPDATE SET ...` clause is rebuilt from the schema on every 1000-row chunk:

```rust
for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
    // ... build VALUES ...
    if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
        // Rebuilds pk_cols and update_cols strings every chunk
    }
}
```

At 100K rows, this rebuilds the conflict clause 100 times. Each rebuild allocates ~200 bytes of strings.

**Fix**: Pre-compute the suffix before the chunk loop:

```rust
let conflict_suffix = if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
    // Build once
    Some(format!(" ON CONFLICT ({}) DO UPDATE SET {}", pk_cols, update_cols))
} else {
    None
};

for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
    // ... build VALUES ...
    if let Some(ref suffix) = conflict_suffix {
        sql.push_str(suffix);
    }
}
```

**Estimated improvement**: ~0.5% of INSERT flush time in upsert mode.
**Complexity**: Trivial — move string building before the loop.

### P2: Destination COPY — buffer capacity discarded

**Impact**: Low
**Files**: `connectors/dest-postgres/src/sink.rs:1237-1242`

When the COPY buffer reaches `COPY_FLUSH_BYTES` (4MB), `std::mem::take()` discards the allocation and a new `Vec` is created:

```rust
if buf.len() >= COPY_FLUSH_BYTES {
    sink.send(Bytes::from(std::mem::take(&mut buf))).await?;
    buf = Vec::with_capacity(COPY_FLUSH_BYTES);  // new allocation
}
```

`Bytes::from(Vec<u8>)` takes ownership, so the old allocation can't be reused. But we can avoid the overhead by restructuring:

**Fix**: Use double-buffering — swap between two pre-allocated buffers:

```rust
let mut buf_a = Vec::with_capacity(COPY_FLUSH_BYTES);
let mut buf_b = Vec::with_capacity(COPY_FLUSH_BYTES);
let mut active = &mut buf_a;
// On flush: swap active buffer, send the full one
```

Or accept the current approach since 4MB allocations at most happen `ceil(data_size / 4MB)` times (3 times for 9MB dataset).

**Estimated improvement**: ~1% of COPY flush time.
**Complexity**: Low-Medium.

### P2: Compression — context reuse

**Impact**: Low (compression is off by default)
**Files**: `crates/rapidbyte-core/src/runtime/compression.rs:20-35`

Both `compress()` and `decompress()` create new encoder/decoder state per call:

```rust
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Zstd => zstd::encode_all(data, 1).expect("zstd compress failed"),
        // ...
    }
}
```

`zstd::encode_all()` creates a new compression context internally each time. For zstd specifically, context creation involves allocating internal hash tables (~256KB+).

**Fix**: Use `zstd::stream::Encoder`/`Decoder` with persistent context or thread-local cached contexts:

```rust
thread_local! {
    static ZSTD_COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new(zstd::bulk::Compressor::new(1).unwrap());
}
```

`lz4_flex` doesn't have persistent context — it's already stateless and fast.

**Estimated improvement**: ~50% reduction in compression overhead per batch for zstd. Negligible for lz4.
**Complexity**: Low.

### P3: Arrow IPC — schema repeated per batch

**Impact**: Very Low
**Files**: `connectors/source-postgres/src/source.rs:512-523`, `connectors/dest-postgres/src/sink.rs:998-1007`

Each batch creates a new `StreamWriter`/`StreamReader` with the full schema. Arrow IPC streaming format prepends the schema to every message. For 7 columns, the schema overhead is ~200 bytes — negligible compared to 900KB batch payloads.

**Fix**: Not worth optimizing. Arrow encode/decode is 2.5% of total at 100K rows.

**Estimated improvement**: < 0.1% of total runtime.
**Complexity**: High (would require custom IPC format).

## Implementation Guide

### Quick Wins (bundle together, ~1 hour)

| Optimization | Files | Est. Impact |
|---|---|---|
| P0: `TypedCol` pre-downcast + write to buffer | `sink.rs` | 15-25% of INSERT flush |
| P0: SQL capacity pre-alloc | `sink.rs` | 5-10% of INSERT flush |
| P1: FETCH_CHUNK = BATCH_SIZE | `source.rs` | 10x fewer PG round-trips |
| P1: StringBuilder + direct collect for arrays | `source.rs` | 5-15% of Arrow encode |

Combined estimated improvement for INSERT mode at 100K rows:
- Before: ~1.05s total
- After: ~0.80-0.85s total (20-25% faster)

### Medium-Term (individual PRs)

| Optimization | Files | Est. Impact |
|---|---|---|
| P2: Cursor type-aware extraction | `source.rs` | 5-10% of incremental fetch |
| P2: Fixed-estimate row bytes | `source.rs` | 2-5% of source fetch |
| P2: Pre-compute upsert clause | `sink.rs` | ~0.5% of upsert flush |
| P2: Zstd context reuse | `compression.rs` | 50% of zstd overhead (when enabled) |
| P2: COPY buffer swap | `sink.rs` | ~1% of COPY flush |

### Low Priority (defer)

| Optimization | Reason to defer |
|---|---|
| P1: Host memory pool | WASM overhead is near-zero with large batches |
| P3: Arrow schema dedup | Arrow encode/decode is < 3% of total |

## Measurement Methodology

### Re-running benchmarks after changes

```bash
# Quick validation (10K rows, 3 iterations)
just bench-connector-postgres

# Full benchmark (100K rows)
just bench-connector-postgres 100000

# Compare two git refs
./tests/bench_compare.sh main feature-branch 100000
```

### Key metrics to watch

| Metric | Why it matters |
|---|---|
| `dest_flush_secs` | Direct measure of INSERT/COPY write performance |
| `throughput (rows/s)` | End-to-end pipeline speed |
| `source_fetch_secs` | Source-side row extraction + accumulation |
| `source_arrow_encode_secs` | Arrow serialization overhead |
| `dest_arrow_decode_secs` | Arrow deserialization overhead |

### Regression thresholds

- Total throughput regression > 5% across 3+ iterations → investigate
- Any single sub-phase regression > 20% → investigate
- Module load time increase > 2x → check AOT compilation

### Profiling CPU hotspots

```bash
# CPU profile with samply (macOS)
just bench-profile 50000 insert

# View in Firefox Profiler at https://profiler.firefox.com
```
