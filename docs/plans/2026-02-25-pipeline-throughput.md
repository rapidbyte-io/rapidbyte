# Pipeline Throughput Optimization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate pipeline bottlenecks — checkpoint overhead, backpressure stalls, COPY session churn, and FETCH roundtrips — to maximize end-to-end throughput when PG I/O dominates.

**Architecture:** Four independent optimizations targeting different pipeline stages. Each is independently valuable, testable, and merge-safe. No backward compatibility concerns (internal config defaults + connector internals).

**Tech Stack:** Rust, tokio, tokio-postgres (binary COPY), WASI Component Model (Wasmtime 41)

**Context:** Benchmarks show PG I/O consumes 99.5% of wall time. The V3 zero-copy frame transport is already in place on the `perf` branch. These optimizations target the actual bottleneck: the PG read/write paths and the pipeline plumbing between them.

---

## Copy Landscape: Where Time Goes

```
Source (3.0s)                    Channel (16-slot)            Dest (7.6s)
┌──────────────────┐            ┌──────────┐           ┌──────────────────────┐
│ FETCH 10K rows   │──batch──>  │ bounded  │──batch──> │ Open COPY session    │
│ encode Arrow     │──batch──>  │ mpsc     │──batch──> │ Write binary rows    │
│ emit IPC frame   │            │ (16 cap) │           │ Close COPY session   │
│ FETCH 10K rows   │            │          │           │ Watermark UPSERT     │
│ ...repeat...     │            │          │           │ COMMIT               │
│                  │            │          │           │ BEGIN                │
│ [stalls here     │            │ [FULL]   │           │ Open COPY session    │
│  when channel    │            │          │           │ ...repeat...         │
│  is full]        │            │          │           │                      │
└──────────────────┘            └──────────┘           └──────────────────────┘
```

**Per-batch overhead we're eliminating:**
| Overhead | Cost per batch | Eliminated by |
|----------|---------------|---------------|
| COPY open + close | ~2ms | Task 3 (persistent COPY) |
| Checkpoint COMMIT+BEGIN (if triggered) | ~20-50ms | Task 2 (amortization) |
| Source FETCH roundtrip | ~1-10ms | Task 4 (adaptive fetch) |
| Channel backpressure stall | variable | Task 1 (larger buffer) |

---

## Task 1: Increase Channel Buffer Default (16 → 32)

**Why:** When the destination is slower than the source (always true for PG writes), the channel fills and the source blocks on `sender.blocking_send()`. A larger buffer smooths out variance from PG COMMIT latency spikes, letting the source keep producing while the dest handles slow commits.

**Memory impact:** 32 × 64MB max_batch_bytes = 2GB worst-case (rarely hit; batches are typically smaller than max). Acceptable for a host process.

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs` (line 134)
- Modify: `crates/rapidbyte-engine/src/config/types.rs` (line 16)
- Test: `crates/rapidbyte-types/src/stream.rs` (line 195)
- Test: `crates/rapidbyte-engine/src/config/types.rs` (existing defaults test)

**Step 1: Update the failing test first**

In `crates/rapidbyte-types/src/stream.rs`, update the test at line 195:

```rust
assert_eq!(limits.max_inflight_batches, 32);
```

Run: `cargo test -p rapidbyte-types stream_limits_defaults`
Expected: FAIL — still returns 16.

**Step 2: Update the constant in `rapidbyte-types`**

In `crates/rapidbyte-types/src/stream.rs` line 134:

```rust
pub const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 32;
```

Run: `cargo test -p rapidbyte-types stream_limits_defaults`
Expected: PASS.

**Step 3: Update the engine-side constant to match**

In `crates/rapidbyte-engine/src/config/types.rs` line 16:

```rust
const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 32;
```

Run: `cargo test -p rapidbyte-engine`
Expected: PASS (no test asserts the engine default directly, but the value should match types crate).

**Step 4: Verify full workspace**

Run: `cargo test --workspace`
Expected: All pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/stream.rs crates/rapidbyte-engine/src/config/types.rs
git commit -m "perf: increase default max_inflight_batches from 16 to 32"
```

---

## Task 2: Checkpoint Amortization

**Why:** With defaults (`checkpoint_interval_bytes=64MB`, `max_batch_bytes=64MB`), every batch triggers a checkpoint (watermark UPSERT + COMMIT + BEGIN = 3 roundtrips, ~20-50ms). For workloads with many batches, this adds up. We:
1. Raise the default checkpoint interval from 64MB → 256MB (fewer checkpoints per run)
2. Add a `checkpoint_interval_batches` threshold (commit every N batches regardless of bytes)

The combination lets users tune checkpoint frequency precisely: by bytes, rows, seconds, OR batch count.

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs` — add `checkpoint_interval_batches` field to `StreamLimits`
- Modify: `crates/rapidbyte-engine/src/config/types.rs` — add `checkpoint_interval_batches` to `ResourceConfig`, bump default interval
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` — pass new field to stream limits
- Modify: `connectors/dest-postgres/src/writer.rs` — add batches threshold to `CheckpointConfig` and `maybe_checkpoint()`
- Test: existing tests in types + engine + dest connector

**Step 1: Add field to `StreamLimits` (types crate)**

In `crates/rapidbyte-types/src/stream.rs`, add after `checkpoint_interval_seconds` (line 123):

```rust
    /// Checkpoint after this many batches (0 = disabled).
    #[serde(default)]
    pub checkpoint_interval_batches: u64,
```

Update the `Default` impl (line 148) to add:

```rust
            checkpoint_interval_batches: 0,
```

Update `DEFAULT_CHECKPOINT_INTERVAL_BYTES` (line 132) from `64 * 1024 * 1024` to `256 * 1024 * 1024`:

```rust
    /// 256 MiB default checkpoint interval.
    pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024 * 1024;
```

Run: `cargo test -p rapidbyte-types`
Expected: FAIL on `stream_limits_defaults` test (checkpoint_interval_bytes changed).

**Step 2: Fix the types test**

In the `stream_limits_defaults` test, no direct assertion on checkpoint_interval_bytes currently, but verify:

```rust
assert_eq!(limits.checkpoint_interval_bytes, 256 * 1024 * 1024);
assert_eq!(limits.checkpoint_interval_batches, 0);
```

Run: `cargo test -p rapidbyte-types`
Expected: PASS.

**Step 3: Add to engine `ResourceConfig`**

In `crates/rapidbyte-engine/src/config/types.rs`:

Update `DEFAULT_CHECKPOINT_INTERVAL_BYTES` (line 14):

```rust
const DEFAULT_CHECKPOINT_INTERVAL_BYTES: &str = "256mb";
```

Add field to `ResourceConfig` (after line 178):

```rust
    /// Destination commits after this many batches. 0 = disabled.
    pub checkpoint_interval_batches: u64,
```

Add default in `impl Default for ResourceConfig` (after line 198):

```rust
            checkpoint_interval_batches: 0,
```

Run: `cargo test -p rapidbyte-engine`
Expected: FAIL on `test_deserialize_minimal_pipeline` which asserts `max_batch_bytes == "64mb"` — that test is about max_batch_bytes not checkpoint_interval, so it should still pass. The `test_deserialize_full_pipeline` uses `checkpoint_interval_bytes: 32mb` explicitly, should still pass. Run and check.

**Step 4: Wire through orchestrator**

In `crates/rapidbyte-engine/src/orchestrator.rs`, around line 337 where stream limits are built, add the new field:

```rust
        checkpoint_interval_batches: config.resources.checkpoint_interval_batches,
```

Run: `cargo build`
Expected: PASS.

**Step 5: Add to dest connector `CheckpointConfig`**

In `connectors/dest-postgres/src/writer.rs`:

Add to `CheckpointConfig` (after line 110):

```rust
    pub batches: u64,
```

Update `write_stream()` where `SessionConfig` is built (after line 44):

```rust
                batches: stream.limits.checkpoint_interval_batches,
```

Add `batches_since_commit` to `WriteStats` (after line 129):

```rust
    batches_since_commit: u64,
```

Initialize in `begin()` (line 259):

```rust
                batches_since_commit: 0,
```

Increment in `process_batch()` after `batches_written += 1` (line 350):

```rust
        self.stats.batches_since_commit += 1;
```

Update `maybe_checkpoint()` condition (lines 384-388) to add batches check:

```rust
        let should_checkpoint = (cfg.bytes > 0
            && self.stats.bytes_since_commit >= cfg.bytes)
            || (cfg.rows > 0 && self.stats.rows_since_commit >= cfg.rows)
            || (cfg.seconds > 0
                && self.last_checkpoint_time.elapsed().as_secs() >= cfg.seconds)
            || (cfg.batches > 0
                && self.stats.batches_since_commit >= cfg.batches);
```

Reset in checkpoint (after line 412):

```rust
        self.stats.batches_since_commit = 0;
```

**Step 6: Build and test**

Run: `cargo test --workspace && cd connectors/dest-postgres && cargo build`
Expected: PASS.

**Step 7: Commit**

```bash
git add crates/rapidbyte-types/src/stream.rs crates/rapidbyte-engine/src/config/types.rs \
        crates/rapidbyte-engine/src/orchestrator.rs connectors/dest-postgres/src/writer.rs
git commit -m "perf: checkpoint amortization — raise default interval to 256MB, add batches threshold"
```

---

## Task 3: Persistent COPY Session Across Batches

**Why:** Currently, every `process_batch()` call opens a new COPY FROM STDIN session, writes rows, closes it (`finish()`). Each open/close costs ~2ms (2 roundtrips). More importantly, PG re-parses the COPY statement each time. Keeping the COPY session open across batches eliminates this per-batch overhead. We only close+reopen at checkpoint boundaries (when we must COMMIT the transaction).

**Architecture:**

```
Before:  batch1 → [COPY open, write, close] → batch2 → [COPY open, write, close] → checkpoint
After:   batch1 → [COPY open, write         → batch2 →               write, close] → checkpoint
```

COPY session is held in `WriteSession` as an `Option<ActiveCopy>`. The session is:
- **Opened** lazily on first COPY batch (or after a checkpoint reopens the transaction)
- **Flushed and closed** in `maybe_checkpoint()` before COMMIT (PG requires COPY to end before other queries)
- **Closed** in `commit()` for the final flush

**Files:**
- Create: `connectors/dest-postgres/src/copy_session.rs` — `ActiveCopy` struct with lifecycle methods
- Modify: `connectors/dest-postgres/src/copy.rs` — extract row-writing logic into shared helper, keep standalone `write()` for non-persistent use
- Modify: `connectors/dest-postgres/src/writer.rs` — hold `Option<ActiveCopy>`, manage lifecycle in process_batch/maybe_checkpoint/commit
- Modify: `connectors/dest-postgres/src/lib.rs` or `connectors/dest-postgres/src/main.rs` — add `mod copy_session;`

**Step 1: Create `copy_session.rs` with `ActiveCopy`**

Create `connectors/dest-postgres/src/copy_session.rs`:

```rust
//! Persistent COPY session that stays open across multiple batches.
//!
//! Opened lazily on the first COPY batch. Closed at checkpoint boundaries
//! (when we must COMMIT) or at session end. This eliminates per-batch
//! COPY open/close roundtrips.

use bytes::Bytes;
use futures_util::SinkExt;
use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::decode::{downcast_columns, write_binary_field, WriteTarget};

/// Default COPY flush buffer size (4 MB).
const DEFAULT_FLUSH_BYTES: usize = 4 * 1024 * 1024;

/// `PostgreSQL` binary COPY signature (11 bytes).
const PGCOPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\x00";
/// Flags field (4 bytes): no OIDs.
const PGCOPY_FLAGS: [u8; 4] = 0_i32.to_be_bytes();
/// Header extension area length (4 bytes): none.
const PGCOPY_EXT_LEN: [u8; 4] = 0_i32.to_be_bytes();
/// File trailer: field count = -1 signals end of data.
const PGCOPY_TRAILER: [u8; 2] = (-1_i16).to_be_bytes();

/// An active COPY FROM STDIN session that can span multiple batches.
pub(crate) struct ActiveCopy {
    sink: std::pin::Pin<Box<tokio_postgres::CopyInSink<Bytes>>>,
    buf: Vec<u8>,
    flush_threshold: usize,
    total_rows: u64,
}

impl ActiveCopy {
    /// Open a new COPY session on the given client.
    pub async fn open(
        client: &Client,
        target: &WriteTarget<'_>,
        flush_bytes: Option<usize>,
    ) -> Result<Self, String> {
        let col_list = target
            .active_cols
            .iter()
            .map(|&i| quote_identifier(target.schema.field(i).name()))
            .collect::<Vec<_>>()
            .join(", ");
        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            target.table, col_list
        );

        let sink = client
            .copy_in(&copy_stmt)
            .await
            .map_err(|e| format!("COPY start failed: {e}"))?;
        let sink = Box::pin(sink);

        let flush_threshold = flush_bytes.unwrap_or(DEFAULT_FLUSH_BYTES).max(1);
        let mut buf = Vec::with_capacity(flush_threshold);

        // Binary header (19 bytes)
        buf.extend_from_slice(&PGCOPY_SIGNATURE);
        buf.extend_from_slice(&PGCOPY_FLAGS);
        buf.extend_from_slice(&PGCOPY_EXT_LEN);

        Ok(Self {
            sink,
            buf,
            flush_threshold,
            total_rows: 0,
        })
    }

    /// Write rows from record batches into the open COPY session.
    /// Returns number of rows written in this call.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    pub async fn write_batches(
        &mut self,
        target: &WriteTarget<'_>,
        batches: &[RecordBatch],
    ) -> Result<u64, String> {
        let num_fields = target.active_cols.len() as i16;
        let mut rows_written: u64 = 0;

        for batch in batches {
            let typed_cols = downcast_columns(batch, target.active_cols)?;

            for row_idx in 0..batch.num_rows() {
                self.buf.extend_from_slice(&num_fields.to_be_bytes());

                for (pos, typed_col) in typed_cols.iter().enumerate() {
                    if target.type_null_flags[pos] {
                        self.buf.extend_from_slice(&(-1_i32).to_be_bytes());
                    } else {
                        write_binary_field(&mut self.buf, typed_col, row_idx);
                    }
                }

                rows_written += 1;

                if self.buf.len() >= self.flush_threshold {
                    self.sink
                        .send(Bytes::from(std::mem::take(&mut self.buf)))
                        .await
                        .map_err(|e| format!("COPY send failed: {e}"))?;
                    self.buf = Vec::with_capacity(self.flush_threshold);
                }
            }
        }

        self.total_rows += rows_written;
        Ok(rows_written)
    }

    /// Close the COPY session: write trailer, send final buffer, finish.
    /// Returns total rows written across all batches in this session.
    pub async fn close(mut self, ctx: &Context) -> Result<u64, String> {
        // Trailer
        self.buf.extend_from_slice(&PGCOPY_TRAILER);

        // Send final buffer
        self.sink
            .send(Bytes::from(self.buf))
            .await
            .map_err(|e| format!("COPY send failed: {e}"))?;

        // Finish COPY protocol
        let _pg_rows = self
            .sink
            .as_mut()
            .finish()
            .await
            .map_err(|e| format!("COPY finish failed: {e}"))?;

        ctx.log(
            LogLevel::Debug,
            &format!("COPY session closed after {} rows", self.total_rows),
        );

        Ok(self.total_rows)
    }
}
```

Run: `cd connectors/dest-postgres && cargo build`
Expected: PASS (no references yet).

**Step 2: Register the module**

In `connectors/dest-postgres/src/main.rs` (or `lib.rs`, wherever modules are declared), add:

```rust
mod copy_session;
```

Run: `cd connectors/dest-postgres && cargo build`
Expected: PASS.

**Step 3: Integrate `ActiveCopy` into `WriteSession`**

In `connectors/dest-postgres/src/writer.rs`:

Add to `WriteSession` struct (after `copy_flush_bytes` field, line 148):

```rust
    // Persistent COPY session (stays open across batches, closed at checkpoint)
    active_copy: Option<crate::copy_session::ActiveCopy>,
```

Initialize in `begin()` (line 262 area):

```rust
            active_copy: None,
```

**Step 4: Update `process_batch()` for persistent COPY**

Replace the COPY dispatch block in `process_batch()` (lines 329-344). The new logic:

```rust
        // Dispatch to write path
        let use_copy = self.load_method == LoadMethod::Copy
            && !matches!(self.effective_write_mode, Some(WriteMode::Upsert { .. }));

        let rows_written = if use_copy {
            // Lazy-open persistent COPY session
            if self.active_copy.is_none() {
                self.active_copy = Some(
                    crate::copy_session::ActiveCopy::open(
                        self.client,
                        &target,
                        self.copy_flush_bytes,
                    )
                    .await?,
                );
            }

            self.active_copy
                .as_mut()
                .expect("just opened")
                .write_batches(&target, batches)
                .await?
        } else {
            let upsert_clause = decode::build_upsert_clause(
                self.effective_write_mode.as_ref(),
                schema,
                &active_cols,
            );
            crate::insert::write(self.ctx, self.client, &target, batches, upsert_clause.as_deref())
                .await?
        };
```

**Step 5: Close COPY before checkpoint**

In `maybe_checkpoint()`, add COPY closure BEFORE the watermark UPSERT (before line 394):

```rust
        // Close persistent COPY session before COMMIT (PG can't run
        // other queries while COPY is active).
        if let Some(copy) = self.active_copy.take() {
            copy.close(self.ctx).await.map_err(|e| format!("COPY close at checkpoint: {e}"))?;
        }
```

**Step 6: Close COPY in `commit()`**

In `commit()`, add before the watermark UPSERT (before line 435):

```rust
        // Close persistent COPY session before final COMMIT.
        if let Some(copy) = self.active_copy.take() {
            copy.close(self.ctx).await.map_err(|e| format!("COPY close at commit: {e}"))?;
        }
```

**Step 7: Close COPY in `rollback()`**

In `rollback()`, add before the ROLLBACK:

```rust
    pub async fn rollback(mut self) {
        // Attempt to close COPY session gracefully before rollback.
        if let Some(copy) = self.active_copy.take() {
            let _ = copy.close(self.ctx).await;
        }
        let _ = self.client.execute("ROLLBACK", &[]).await;
    }
```

Note: `rollback` must become `&mut self` or take `mut self` — it already takes `self`. The `active_copy.take()` works on `mut self`.

**Step 8: Build and test**

Run: `cd connectors/dest-postgres && cargo build`
Expected: PASS.

Run: `cargo test --workspace`
Expected: PASS (host tests don't directly test connector internals).

**Step 9: Commit**

```bash
git add connectors/dest-postgres/src/copy_session.rs connectors/dest-postgres/src/writer.rs \
        connectors/dest-postgres/src/main.rs
git commit -m "perf: persistent COPY session across batches, close only at checkpoint boundaries"
```

---

## Task 4: Adaptive Source FETCH Chunk Size

**Why:** The source connector currently uses a fixed `FETCH_CHUNK = 10,000` rows per cursor iteration. For small rows (~500 bytes), this means many unnecessary FETCH roundtrips (each costs ~1-10ms). For large rows (~69KB), 10,000 rows might exceed memory. An adaptive chunk size reduces roundtrips for small rows and prevents OOM for large rows.

**Strategy:**

```
fetch_chunk = clamp(
    max_batch_bytes / estimated_row_bytes,   // fit one batch's worth
    MIN_FETCH_CHUNK,                          // floor: 1,000 rows
    MAX_FETCH_CHUNK                           // ceiling: 50,000 rows
)
```

For small rows (500 B/row, max_batch_bytes=64MB): `fetch_chunk = min(50000, 64M/500) = 50,000` → 5x fewer roundtrips.
For large rows (69KB/row, max_batch_bytes=512MB): `fetch_chunk = min(50000, 512M/69K) = 7,420` → slightly fewer rows per fetch, prevents memory bloat.

**Files:**
- Modify: `connectors/source-postgres/src/reader.rs` — replace fixed constants with adaptive calculation

**Step 1: Write the test**

Add a test to `connectors/source-postgres/src/reader.rs` at the end of the `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn adaptive_fetch_chunk_small_rows() {
        // 500 B/row, 64MB batch → ~128K rows. Capped at MAX_FETCH_CHUNK.
        let chunk = compute_fetch_chunk(500, 64 * 1024 * 1024);
        assert_eq!(chunk, MAX_FETCH_CHUNK);
    }

    #[test]
    fn adaptive_fetch_chunk_large_rows() {
        // 69KB/row, 512MB batch → ~7400 rows.
        let chunk = compute_fetch_chunk(69_000, 512 * 1024 * 1024);
        assert!(chunk >= MIN_FETCH_CHUNK);
        assert!(chunk <= 8_000);
    }

    #[test]
    fn adaptive_fetch_chunk_tiny_rows() {
        // 10 B/row, 64MB batch → millions. Capped at MAX_FETCH_CHUNK.
        let chunk = compute_fetch_chunk(10, 64 * 1024 * 1024);
        assert_eq!(chunk, MAX_FETCH_CHUNK);
    }

    #[test]
    fn adaptive_fetch_chunk_huge_rows() {
        // 1MB/row, 64MB batch → 64 rows. Floored at MIN_FETCH_CHUNK.
        let chunk = compute_fetch_chunk(1_000_000, 64 * 1024 * 1024);
        assert_eq!(chunk, MIN_FETCH_CHUNK);
    }
```

Run: `cd connectors/source-postgres && cargo test`
Expected: FAIL — `compute_fetch_chunk` doesn't exist yet.

**Step 2: Implement adaptive fetch chunk**

Replace the constants at the top of `reader.rs` (lines 23-26):

```rust
/// Maximum number of rows per Arrow `RecordBatch`.
const BATCH_SIZE: usize = 10_000;

/// Minimum rows per FETCH (floor to avoid excessive roundtrips with huge rows).
const MIN_FETCH_CHUNK: usize = 1_000;

/// Maximum rows per FETCH (ceiling to bound PG-side memory and network transfer).
const MAX_FETCH_CHUNK: usize = 50_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Initial fixed overhead estimate for an empty batch payload.
const BATCH_OVERHEAD_BYTES: usize = 256;

/// Compute adaptive fetch chunk size based on row size and batch byte limit.
///
/// Aims to fetch enough rows to fill roughly one batch, clamped between
/// `MIN_FETCH_CHUNK` and `MAX_FETCH_CHUNK`.
fn compute_fetch_chunk(estimated_row_bytes: usize, max_batch_bytes: usize) -> usize {
    if estimated_row_bytes == 0 {
        return MAX_FETCH_CHUNK;
    }
    let ideal = max_batch_bytes / estimated_row_bytes;
    ideal.clamp(MIN_FETCH_CHUNK, MAX_FETCH_CHUNK)
}
```

Run: `cd connectors/source-postgres && cargo test`
Expected: PASS.

**Step 3: Use adaptive chunk in fetch loop**

In `read_stream()`, replace the fixed fetch query (line 202) with:

```rust
    let fetch_chunk = compute_fetch_chunk(estimated_row_bytes, max_batch_bytes);
    let fetch_query = format!("FETCH {fetch_chunk} FROM {CURSOR_NAME}");
```

Run: `cd connectors/source-postgres && cargo build`
Expected: PASS.

**Step 4: Build and test workspace**

Run: `cargo test --workspace`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/reader.rs
git commit -m "perf: adaptive FETCH chunk size based on row size and batch limit"
```

---

## Expected Impact

| Optimization | Large profile (10K rows, 69KB/row) | Small profile (100K rows, 500B/row) |
|-------------|-----------------------------------|-------------------------------------|
| Task 1: Channel 16→32 | Minimal (only 2 batches) | Moderate — smooths dest COMMIT stalls |
| Task 2: Checkpoint 64→256MB | Moderate — reduces 2→1 checkpoint | Significant — reduces ~100→~4 checkpoints |
| Task 3: Persistent COPY | Minimal (2 batches) | Significant — eliminates ~100 COPY opens |
| Task 4: Adaptive FETCH | Minimal (only 1 FETCH) | Significant — reduces ~10→~2 FETCHes |
| **Combined** | **~5-10% improvement** | **~20-40% improvement** |

The large profile is dominated by PG bulk write I/O (COPY protocol streaming 658MB). These optimizations have more impact on workloads with many smaller batches. Run benchmarks with `--profile small` and `--profile medium` to see the full effect.

---

## Verification

After all tasks:

```bash
# Host tests
cargo test --workspace

# Connector builds
cd connectors/source-postgres && cargo build && cd ../..
cd connectors/dest-postgres && cargo build && cd ../..

# Benchmark (compare before/after)
just bench postgres --profile small --iters 3
just bench postgres --profile medium --iters 3
just bench postgres --profile large --iters 3
```
