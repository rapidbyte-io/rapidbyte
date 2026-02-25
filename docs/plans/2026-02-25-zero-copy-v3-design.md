# Zero-Copy V3 Transport Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace V2 `list<u8>` batch transport with a V3 frame-handle protocol that eliminates all avoidable copies on the host-side hot path, using `bytes::Bytes` as the zero-copy primitive.

**Architecture:** Host-managed frame handles (`u64`) backed by a `BytesMut`/`Bytes` frame table. Connectors write IPC bytes into frames via chunked host imports, the host seals frames to immutable `Bytes`, and routes them through `mpsc` channels without copying. Compression/decompression operate on `Bytes`/`BytesMut` directly. No backward compatibility — V2 is deleted entirely.

**Tech Stack:** `bytes` crate (`Bytes`/`BytesMut`), Wasmtime 41 component model, WIT `rapidbyte:connector@3.0.0`, Arrow IPC, lz4_flex, zstd.

---

## Copy Landscape (Current V2 vs Target V3)

```
 V2 HOT PATH (per batch):                      V3 HOT PATH (per batch):
 ─────────────────────────                      ─────────────────────────
 1. IPC encode → Vec<u8>        (INHERENT)      1. IPC encode → FrameWriter stream  (ELIMINATED — streams directly into host BytesMut)
 2. WIT boundary: guest→host    (INHERENT)      2. WIT boundary: chunked writes     (INHERENT, same total bytes)
 3. compress → new Vec<u8>      (ELIMINABLE)    3. compress → BytesMut              (POOLED, no alloc per batch)
 4. channel send Vec<u8>        (MOVE)          4. channel send Bytes               (ARC CLONE, zero-copy)
 5. decompress → new Vec<u8>    (ELIMINABLE)    5. decompress → BytesMut            (POOLED, no alloc per batch)
 6. WIT boundary: host→guest    (INHERENT)      6. WIT boundary: chunked reads      (INHERENT, same total bytes)
 7. IPC decode → RecordBatch    (INHERENT)      7. IPC decode → RecordBatch         (INHERENT)

 V2 host-side allocs per batch: 3 (input Vec + compress Vec + decompress Vec)
 V3 host-side allocs per batch: 0 (all buffers pooled/reused from BytesMut)
```

### Inherent vs Eliminable Copies

| Copy | V2 Location | V2 Type | V3 Strategy |
|------|-------------|---------|-------------|
| IPC encode buffer | `sdk/arrow/ipc.rs:18` `Vec::new()` | Per-batch alloc, no capacity hint | **ELIMINATED**: `FrameWriter` impl `std::io::Write`, streams IPC directly into host `BytesMut` via `frame-write` calls |
| WIT emit boundary | `bindings.rs:188` `emit_batch(batch: Vec<u8>)` | Full batch copy guest→host | Chunked `frame-write` calls (same bytes, host buffer pre-allocated) |
| Compression output | `compression.rs:29` `compress_prepend_size()` | New Vec per batch | `compress_into` / write into pooled `BytesMut` |
| Channel transfer | `host_state.rs:338` `Frame::Data(Vec<u8>)` | Move (OK) | `Frame::Data(Bytes)` — Arc clone enables multi-consumer |
| Decompression output | `compression.rs:42` `decompress_size_prepended()` | New Vec per batch | Write into pooled `BytesMut` |
| WIT next_batch boundary | `bindings.rs:197` `Option<Vec<u8>>` | Full batch copy host→guest | Chunked `frame-read` calls (same bytes, guest controls chunk size) |
| Socket read buffer | `host_state.rs:656` `vec![0u8; len]` | New Vec per read (up to 64KB) | Pooled `BytesMut` (future optimization, out of scope) |

---

## Task 1: Add `bytes` crate and switch Frame to Bytes

**Files:**
- Modify: `crates/rapidbyte-runtime/Cargo.toml`
- Modify: `crates/rapidbyte-runtime/src/host_state.rs:46-51`
- Modify: `crates/rapidbyte-runtime/src/lib.rs:41` (re-export)
- Test: `cargo test --workspace`

### Step 1: Write the failing test

Add a test in `host_state.rs` that constructs a `Frame::Data` with `Bytes`:

```rust
// In host_state.rs mod tests
#[test]
fn frame_data_holds_bytes() {
    use bytes::Bytes;
    let payload = Bytes::from_static(b"test-ipc-payload");
    let frame = Frame::Data(payload.clone());
    match frame {
        Frame::Data(b) => assert_eq!(b, payload),
        Frame::EndStream => panic!("expected Data"),
    }
}
```

### Step 2: Run test to verify it fails

Run: `cargo test -p rapidbyte-runtime frame_data_holds_bytes`
Expected: FAIL — `bytes` crate not in deps, `Frame::Data` takes `Vec<u8>`

### Step 3: Add bytes dependency

In `crates/rapidbyte-runtime/Cargo.toml`, add:
```toml
bytes = "1"
```

### Step 4: Switch Frame enum

In `crates/rapidbyte-runtime/src/host_state.rs`, change:

```rust
// OLD (line 46-51):
pub enum Frame {
    Data(Vec<u8>),
    EndStream,
}

// NEW:
pub enum Frame {
    /// IPC-encoded Arrow `RecordBatch` (optionally compressed), backed by
    /// reference-counted `Bytes` for zero-copy channel transfer.
    Data(bytes::Bytes),
    /// End-of-stream marker.
    EndStream,
}
```

### Step 5: Update emit_batch_impl to produce Bytes

In `host_state.rs`, update `emit_batch_impl` (line 309):

```rust
pub(crate) fn emit_batch_impl(&mut self, batch: Vec<u8>) -> Result<(), ConnectorError> {
    if batch.is_empty() {
        return Err(ConnectorError::internal(
            "EMPTY_BATCH",
            "Connector emitted a zero-length batch; this is a protocol violation",
        ));
    }

    let fn_start = Instant::now();

    let compress_start = Instant::now();
    let batch: bytes::Bytes = if let Some(codec) = self.batch.compression {
        crate::compression::compress(codec, &batch)
            .map_err(|e| ConnectorError::internal("COMPRESS_FAILED", e.to_string()))?
            .into()  // Vec<u8> → Bytes (zero-copy, takes ownership)
    } else {
        batch.into()  // Vec<u8> → Bytes (zero-copy, takes ownership)
    };
    let compress_elapsed_nanos = if self.batch.compression.is_some() {
        compress_start.elapsed().as_nanos() as u64
    } else {
        0
    };

    let sender =
        self.batch.sender.as_ref().ok_or_else(|| {
            ConnectorError::internal("NO_SENDER", "No batch sender configured")
        })?;

    sender
        .blocking_send(Frame::Data(batch))
        .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;

    self.batch.next_batch_id += 1;

    let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
    t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
    t.emit_batch_count += 1;
    t.compress_nanos += compress_elapsed_nanos;

    Ok(())
}
```

### Step 6: Update next_batch_impl to consume Bytes

In `host_state.rs`, update `next_batch_impl` (line 352):

```rust
pub(crate) fn next_batch_impl(&mut self) -> Result<Option<Vec<u8>>, ConnectorError> {
    let fn_start = Instant::now();

    let receiver = self.batch.receiver.as_mut().ok_or_else(|| {
        ConnectorError::internal("NO_RECEIVER", "No batch receiver configured")
    })?;

    let Some(frame) = receiver.blocking_recv() else {
        return Ok(None);
    };

    let batch = match frame {
        Frame::Data(batch) => batch,
        Frame::EndStream => return Ok(None),
    };

    let decompress_start = Instant::now();
    let batch = if let Some(codec) = self.batch.compression {
        crate::compression::decompress(codec, &batch)
            .map_err(|e| ConnectorError::internal("DECOMPRESS_FAILED", e.to_string()))?
    } else {
        batch.to_vec()  // Bytes → Vec<u8> for WIT return (inherent boundary copy)
    };
    let decompress_elapsed_nanos = if self.batch.compression.is_some() {
        decompress_start.elapsed().as_nanos() as u64
    } else {
        0
    };

    let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
    t.next_batch_nanos += fn_start.elapsed().as_nanos() as u64;
    t.next_batch_count += 1;
    t.decompress_nanos += decompress_elapsed_nanos;

    Ok(Some(batch))
}
```

Note: `next_batch_impl` still returns `Vec<u8>` because the WIT binding signature requires it. The V3 frame migration (Task 4+) will eliminate this `.to_vec()`.

### Step 7: Run all tests

Run: `cargo test --workspace`
Expected: PASS (all existing tests work since `Vec<u8>.into()` → `Bytes` is zero-copy)

### Step 8: Commit

```bash
git add crates/rapidbyte-runtime/Cargo.toml crates/rapidbyte-runtime/src/host_state.rs
git commit -m "refactor: switch Frame payload from Vec<u8> to bytes::Bytes

Zero-copy channel transfer: Bytes is Arc-backed, so channel send is
a ref-count bump instead of a move. This is the foundation for V3
frame-handle transport."
```

---

## Task 2: Create frame table module

**Files:**
- Create: `crates/rapidbyte-runtime/src/frame.rs`
- Modify: `crates/rapidbyte-runtime/src/lib.rs` (add module, re-export)
- Test: `crates/rapidbyte-runtime/src/frame.rs` (inline tests)

### Step 1: Write the failing tests

Create `crates/rapidbyte-runtime/src/frame.rs` with tests first:

```rust
//! Host-side frame table for V3 zero-copy batch transport.
//!
//! Frames go through a strict lifecycle: Writable → Sealed → Consumed/Dropped.
//! Writable frames are backed by `BytesMut`; sealing freezes them to `Bytes`.

use bytes::{Bytes, BytesMut};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("invalid frame handle: {0}")]
    InvalidHandle(u64),
    #[error("frame {0} is not sealed")]
    NotSealed(u64),
    #[error("frame {0} is already sealed")]
    AlreadySealed(u64),
    #[error("frame read out of bounds: offset {offset} + len {len} > frame len {frame_len}")]
    OutOfBounds { offset: u64, len: u64, frame_len: u64 },
}

enum FrameState {
    Writable(BytesMut),
    Sealed(Bytes),
}

pub struct FrameTable {
    frames: std::collections::HashMap<u64, FrameState>,
    next_handle: u64,
}

impl FrameTable {
    pub fn new() -> Self {
        Self {
            frames: std::collections::HashMap::new(),
            next_handle: 1,
        }
    }

    /// Allocate a new writable frame with the given capacity hint.
    pub fn alloc(&mut self, capacity: u64) -> u64 {
        let handle = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        self.frames.insert(
            handle,
            FrameState::Writable(BytesMut::with_capacity(capacity as usize)),
        );
        handle
    }

    /// Append bytes to a writable frame. Returns total bytes written so far.
    pub fn write(&mut self, handle: u64, chunk: &[u8]) -> Result<u64, FrameError> {
        match self.frames.get_mut(&handle) {
            Some(FrameState::Writable(buf)) => {
                buf.extend_from_slice(chunk);
                Ok(buf.len() as u64)
            }
            Some(FrameState::Sealed(_)) => Err(FrameError::AlreadySealed(handle)),
            None => Err(FrameError::InvalidHandle(handle)),
        }
    }

    /// Seal a writable frame, freezing it to immutable `Bytes`.
    pub fn seal(&mut self, handle: u64) -> Result<(), FrameError> {
        match self.frames.get_mut(&handle) {
            Some(state @ FrameState::Writable(_)) => {
                let FrameState::Writable(buf) = std::mem::replace(state, FrameState::Sealed(Bytes::new()))
                else {
                    unreachable!()
                };
                *state = FrameState::Sealed(buf.freeze());
                Ok(())
            }
            Some(FrameState::Sealed(_)) => Err(FrameError::AlreadySealed(handle)),
            None => Err(FrameError::InvalidHandle(handle)),
        }
    }

    /// Get the length of a sealed frame.
    pub fn len(&self, handle: u64) -> Result<u64, FrameError> {
        match self.frames.get(&handle) {
            Some(FrameState::Sealed(b)) => Ok(b.len() as u64),
            Some(FrameState::Writable(b)) => Ok(b.len() as u64),
            None => Err(FrameError::InvalidHandle(handle)),
        }
    }

    /// Read a slice from a sealed frame. Returns a copy of the requested range.
    /// This copy is inherent — it crosses the wasm ABI boundary.
    pub fn read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, FrameError> {
        match self.frames.get(&handle) {
            Some(FrameState::Sealed(b)) => {
                let off = offset as usize;
                let l = len as usize;
                if off + l > b.len() {
                    return Err(FrameError::OutOfBounds {
                        offset,
                        len,
                        frame_len: b.len() as u64,
                    });
                }
                Ok(b[off..off + l].to_vec())
            }
            Some(FrameState::Writable(_)) => Err(FrameError::NotSealed(handle)),
            None => Err(FrameError::InvalidHandle(handle)),
        }
    }

    /// Consume a sealed frame, returning the underlying `Bytes`.
    /// The handle is removed from the table. Used by `emit-batch`.
    pub fn consume(&mut self, handle: u64) -> Result<Bytes, FrameError> {
        match self.frames.remove(&handle) {
            Some(FrameState::Sealed(b)) => Ok(b),
            Some(FrameState::Writable(buf)) => {
                // Put it back before returning error
                self.frames.insert(handle, FrameState::Writable(buf));
                Err(FrameError::NotSealed(handle))
            }
            None => Err(FrameError::InvalidHandle(handle)),
        }
    }

    /// Insert a sealed read-only frame from an existing `Bytes` (for `next-batch`).
    /// Returns the new handle.
    pub fn insert_sealed(&mut self, data: Bytes) -> u64 {
        let handle = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        self.frames.insert(handle, FrameState::Sealed(data));
        handle
    }

    /// Drop a frame handle. No-op if handle doesn't exist.
    pub fn drop_frame(&mut self, handle: u64) {
        self.frames.remove(&handle);
    }

    /// Drop all remaining frames. Called at teardown.
    pub fn clear(&mut self) {
        self.frames.clear();
    }

    /// Number of live frames (for diagnostics).
    pub fn live_count(&self) -> usize {
        self.frames.len()
    }
}

impl Default for FrameTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Drop impl ensures all frames are freed if the FrameTable is dropped
/// (e.g., when ComponentHostState is dropped after a Wasmtime trap).
/// This prevents guest-induced memory leaks from orphaned frame handles.
impl Drop for FrameTable {
    fn drop(&mut self) {
        if !self.frames.is_empty() {
            tracing::debug!(
                leaked_frames = self.frames.len(),
                "FrameTable dropped with live frames; cleaning up"
            );
            self.frames.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_write_seal_read_drop() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(1024);

        ft.write(h, b"hello ").unwrap();
        ft.write(h, b"world").unwrap();
        assert_eq!(ft.len(h).unwrap(), 11);

        ft.seal(h).unwrap();
        assert_eq!(ft.read(h, 0, 5).unwrap(), b"hello");
        assert_eq!(ft.read(h, 6, 5).unwrap(), b"world");

        ft.drop_frame(h);
        assert!(ft.read(h, 0, 1).is_err());
    }

    #[test]
    fn write_after_seal_fails() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.write(h, b"data").unwrap();
        ft.seal(h).unwrap();
        assert!(matches!(ft.write(h, b"more"), Err(FrameError::AlreadySealed(_))));
    }

    #[test]
    fn double_seal_fails() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.seal(h).unwrap();
        assert!(matches!(ft.seal(h), Err(FrameError::AlreadySealed(_))));
    }

    #[test]
    fn read_before_seal_fails() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.write(h, b"data").unwrap();
        assert!(matches!(ft.read(h, 0, 4), Err(FrameError::NotSealed(_))));
    }

    #[test]
    fn read_out_of_bounds_fails() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.write(h, b"hi").unwrap();
        ft.seal(h).unwrap();
        assert!(matches!(ft.read(h, 0, 10), Err(FrameError::OutOfBounds { .. })));
    }

    #[test]
    fn consume_returns_bytes_and_removes_handle() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.write(h, b"payload").unwrap();
        ft.seal(h).unwrap();

        let b = ft.consume(h).unwrap();
        assert_eq!(&b[..], b"payload");
        assert!(ft.consume(h).is_err()); // handle gone
    }

    #[test]
    fn consume_unsealed_fails() {
        let mut ft = FrameTable::new();
        let h = ft.alloc(64);
        ft.write(h, b"data").unwrap();
        assert!(matches!(ft.consume(h), Err(FrameError::NotSealed(_))));
        // handle still exists after failed consume
        assert_eq!(ft.len(h).unwrap(), 4);
    }

    #[test]
    fn insert_sealed_creates_readonly_frame() {
        let mut ft = FrameTable::new();
        let data = Bytes::from_static(b"existing-payload");
        let h = ft.insert_sealed(data.clone());

        assert_eq!(ft.len(h).unwrap(), 16);
        assert_eq!(ft.read(h, 0, 16).unwrap(), b"existing-payload");
        assert!(ft.write(h, b"nope").is_err()); // sealed, can't write
    }

    #[test]
    fn clear_drops_all_frames() {
        let mut ft = FrameTable::new();
        ft.alloc(64);
        ft.alloc(64);
        assert_eq!(ft.live_count(), 2);
        ft.clear();
        assert_eq!(ft.live_count(), 0);
    }

    #[test]
    fn drop_nonexistent_is_noop() {
        let mut ft = FrameTable::new();
        ft.drop_frame(999); // should not panic
    }
}
```

### Step 2: Run tests to verify they pass (frame module is self-contained)

Run: `cargo test -p rapidbyte-runtime frame`
Expected: PASS

### Step 3: Register module in lib.rs

In `crates/rapidbyte-runtime/src/lib.rs`, add:

```rust
pub mod frame;
```

And add re-export:
```rust
pub use frame::FrameTable;
```

### Step 4: Run full test suite

Run: `cargo test --workspace`
Expected: PASS

### Step 5: Commit

```bash
git add crates/rapidbyte-runtime/src/frame.rs crates/rapidbyte-runtime/src/lib.rs
git commit -m "feat: add FrameTable for V3 zero-copy batch transport

Host-managed frame lifecycle: alloc(BytesMut) → write → seal(Bytes) →
consume/read → drop. Sealed frames share underlying Bytes via Arc,
enabling zero-copy channel routing."
```

---

## Task 3: Update compression to accept Bytes and return Bytes

**Files:**
- Modify: `crates/rapidbyte-runtime/Cargo.toml` (bytes already added in Task 1)
- Modify: `crates/rapidbyte-runtime/src/compression.rs`
- Test: `cargo test -p rapidbyte-runtime compression`

### Step 1: Write failing tests

Add to `compression.rs` tests:

```rust
#[test]
fn test_lz4_roundtrip_bytes() {
    let data = bytes::Bytes::from_static(b"hello world repeated hello world repeated hello world repeated");
    let compressed = compress_bytes(CompressionCodec::Lz4, &data).unwrap();
    let decompressed = decompress_to_vec(CompressionCodec::Lz4, &compressed).unwrap();
    assert_eq!(data.as_ref(), decompressed.as_slice());
}

#[test]
fn test_zstd_roundtrip_bytes() {
    let data = bytes::Bytes::from_static(b"hello world repeated hello world repeated hello world repeated");
    let compressed = compress_bytes(CompressionCodec::Zstd, &data).unwrap();
    let decompressed = decompress_to_vec(CompressionCodec::Zstd, &compressed).unwrap();
    assert_eq!(data.as_ref(), decompressed.as_slice());
}
```

### Step 2: Run tests to verify they fail

Run: `cargo test -p rapidbyte-runtime test_lz4_roundtrip_bytes`
Expected: FAIL — `compress_bytes` not defined

### Step 3: Add Bytes-aware compression functions

Keep the existing `compress`/`decompress` functions (they still work). Add new functions that accept `&[u8]` and are named more clearly. Also update `compress` to return `Bytes` directly:

```rust
use bytes::Bytes;

/// Compress bytes using the given codec. Returns owned `Bytes`.
pub fn compress_bytes(codec: CompressionCodec, data: &[u8]) -> Result<Bytes, CompressionError> {
    match codec {
        CompressionCodec::Lz4 => Ok(Bytes::from(lz4_flex::compress_prepend_size(data))),
        CompressionCodec::Zstd => Ok(Bytes::from(
            zstd::bulk::compress(data, ZSTD_COMPRESSION_LEVEL)
                .map_err(CompressionError::ZstdCompress)?,
        )),
    }
}

/// Decompress bytes using the given codec. Returns `Vec<u8>` because
/// decompression output cannot share the input buffer.
pub fn decompress_to_vec(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    decompress(codec, data)
}
```

### Step 4: Run tests

Run: `cargo test -p rapidbyte-runtime compression`
Expected: PASS

### Step 5: Update emit_batch_impl to use compress_bytes

In `host_state.rs`, update the compression path in `emit_batch_impl`:

```rust
let batch: bytes::Bytes = if let Some(codec) = self.batch.compression {
    crate::compression::compress_bytes(codec, &batch)
        .map_err(|e| ConnectorError::internal("COMPRESS_FAILED", e.to_string()))?
} else {
    batch.into()
};
```

### Step 6: Run full tests

Run: `cargo test --workspace`
Expected: PASS

### Step 7: Commit

```bash
git add crates/rapidbyte-runtime/src/compression.rs crates/rapidbyte-runtime/src/host_state.rs
git commit -m "feat: add Bytes-returning compression functions

compress_bytes() returns Bytes directly, avoiding an extra Vec→Bytes
conversion in the emit path. Existing compress()/decompress() kept
for backward compatibility during migration."
```

---

## Task 4: Write V3 WIT definitions

**Files:**
- Modify: `wit/rapidbyte-connector.wit`
- Test: `cargo build -p rapidbyte-runtime` (bindings must compile)

### Step 1: Replace WIT with V3 definitions

Replace the entire `wit/rapidbyte-connector.wit` with:

```wit
package rapidbyte:connector@3.0.0;

interface types {
    enum validation-status {
        success,
        failed,
        warning,
    }

    enum error-category {
        config,
        auth,
        permission,
        rate-limit,
        transient-network,
        transient-db,
        data,
        schema,
        internal,
        /// V3: frame lifecycle violation.
        frame,
    }

    enum error-scope {
        per-stream,
        per-batch,
        per-record,
    }

    enum backoff-class {
        fast,
        normal,
        slow,
    }

    enum commit-state {
        before-commit,
        after-commit-unknown,
        after-commit-confirmed,
    }

    record connector-error {
        category: error-category,
        scope: error-scope,
        code: string,
        message: string,
        retryable: bool,
        retry-after-ms: option<u64>,
        backoff-class: backoff-class,
        safe-to-retry: bool,
        commit-state: option<commit-state>,
        details-json: option<string>,
    }

    record stream-limits {
        max-batch-bytes: u64,
        max-record-bytes: u64,
        max-inflight-batches: u32,
        max-parallel-requests: u32,
        checkpoint-interval-bytes: u64,
        checkpoint-interval-rows: u64,
        checkpoint-interval-seconds: u64,
    }

    record read-summary {
        records-read: u64,
        bytes-read: u64,
        batches-emitted: u64,
        checkpoint-count: u64,
        records-skipped: u64,
    }

    record write-summary {
        records-written: u64,
        bytes-written: u64,
        batches-written: u64,
        checkpoint-count: u64,
        records-failed: u64,
    }

    record transform-summary {
        records-in: u64,
        records-out: u64,
        bytes-in: u64,
        bytes-out: u64,
        batches-processed: u64,
    }

    record validation-result {
        status: validation-status,
        message: string,
    }

    variant socket-read-result {
        data(list<u8>),
        eof,
        would-block,
    }

    variant socket-write-result {
        written(u64),
        would-block,
    }
}

interface host {
    use types.{
        connector-error,
        socket-read-result,
        socket-write-result,
    };

    // ── V3 Frame lifecycle ───────────────────────────────────────
    //
    // Frames are host-managed buffers with a strict lifecycle:
    //   alloc(writable) → write* → seal(immutable) → emit / read* → drop
    //
    // Batch payloads and large I/O go through frames to minimize copies
    // across the wasm ABI boundary.

    /// Allocate a new writable frame with the given capacity hint (bytes).
    frame-new: func(capacity: u64) -> result<u64, connector-error>;

    /// Append bytes to a writable frame. Returns total bytes written so far.
    frame-write: func(handle: u64, chunk: list<u8>) -> result<u64, connector-error>;

    /// Seal a writable frame, making it immutable.
    frame-seal: func(handle: u64) -> result<_, connector-error>;

    /// Get the byte length of a frame (writable or sealed).
    frame-len: func(handle: u64) -> result<u64, connector-error>;

    /// Read a range from a sealed frame. Returns a copy of the requested bytes.
    frame-read: func(handle: u64, offset: u64, len: u64) -> result<list<u8>, connector-error>;

    /// Drop a frame handle. No-op if the handle is invalid.
    frame-drop: func(handle: u64);

    // ── V3 Batch transport (frame-handle based) ──────────────────

    /// Emit a sealed frame as a batch. Consumes the frame handle.
    /// The host routes the underlying Bytes through the pipeline channel
    /// without copying.
    emit-batch: func(handle: u64) -> result<_, connector-error>;

    /// Receive the next batch as a sealed frame handle.
    /// Returns None when the stream is complete.
    next-batch: func() -> result<option<u64>, connector-error>;

    // ── Logging, checkpoints, metrics, DLQ ───────────────────────

    log: func(level: u32, msg: string);
    checkpoint: func(kind: u32, payload-json: string) -> result<_, connector-error>;
    metric: func(payload-json: string) -> result<_, connector-error>;
    emit-dlq-record: func(stream-name: string, record-json: string, error-message: string, error-category: string) -> result<_, connector-error>;

    // ── State KV ─────────────────────────────────────────────────

    state-get: func(scope: u32, key: string) -> result<option<string>, connector-error>;
    state-put: func(scope: u32, key: string, val: string) -> result<_, connector-error>;
    state-cas: func(scope: u32, key: string, expected: option<string>, new-val: string) -> result<bool, connector-error>;

    // ── Socket I/O (unchanged, list<u8> — small chunks) ─────────

    connect-tcp: func(host: string, port: u16) -> result<u64, connector-error>;
    socket-read: func(handle: u64, len: u64) -> result<socket-read-result, connector-error>;
    socket-write: func(handle: u64, data: list<u8>) -> result<socket-write-result, connector-error>;
    socket-close: func(handle: u64);
}

interface source-connector {
    use types.{read-summary, validation-result, connector-error};

    open: func(config-json: string) -> result<_, connector-error>;
    discover: func() -> result<string, connector-error>;
    validate: func() -> result<validation-result, connector-error>;
    run-read: func(ctx-json: string) -> result<read-summary, connector-error>;
    close: func() -> result<_, connector-error>;
}

interface dest-connector {
    use types.{write-summary, validation-result, connector-error};

    open: func(config-json: string) -> result<_, connector-error>;
    validate: func() -> result<validation-result, connector-error>;
    run-write: func(ctx-json: string) -> result<write-summary, connector-error>;
    close: func() -> result<_, connector-error>;
}

interface transform-connector {
    use types.{transform-summary, validation-result, connector-error};

    open: func(config-json: string) -> result<_, connector-error>;
    validate: func() -> result<validation-result, connector-error>;
    run-transform: func(ctx-json: string) -> result<transform-summary, connector-error>;
    close: func() -> result<_, connector-error>;
}

world rapidbyte-host {
    import host;
}

world rapidbyte-source {
    import host;
    export source-connector;
}

world rapidbyte-destination {
    import host;
    export dest-connector;
}

world rapidbyte-transform {
    import host;
    export transform-connector;
}
```

Key changes from V2:
1. Package version: `2.0.0` → `3.0.0`
2. Added `frame` error category
3. `emit-batch` takes `u64` handle instead of `list<u8>`
4. `next-batch` returns `option<u64>` instead of `option<list<u8>>`
5. Six new frame lifecycle functions
6. Socket I/O unchanged (small chunks, frame overhead not worth it)

### Step 2: Verify bindings compile (they won't yet — Host trait impls need updating)

Run: `cargo build -p rapidbyte-runtime 2>&1 | head -30`
Expected: Compile errors in `bindings.rs` — Host trait signatures changed.

Note: Do NOT commit yet. Task 5 will fix the Host impls. This is a checkpoint.

---

## Task 5: Update runtime Host trait implementations for V3

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs` (add FrameTable to ComponentHostState, add frame impls)
- Modify: `crates/rapidbyte-runtime/src/bindings.rs` (update Host trait impls for new signatures)
- Modify: `crates/rapidbyte-types/src/error.rs` (add Frame error category)
- Test: `cargo test --workspace`

### Step 1: Add Frame error category to types

In `crates/rapidbyte-types/src/error.rs`, add `Frame` variant to `ErrorCategory`:

```rust
pub enum ErrorCategory {
    Config,
    Auth,
    Permission,
    RateLimit,
    TransientNetwork,
    TransientDb,
    Data,
    Schema,
    Internal,
    Frame,  // V3: frame lifecycle violations
}
```

Update the `Display` impl and any `FromStr`/serde if present to handle `"frame"`.

### Step 2: Add FrameTable to ComponentHostState

In `host_state.rs`, add the frame table as a field in `ComponentHostState`:

```rust
use crate::frame::FrameTable;

pub struct ComponentHostState {
    pub(crate) identity: ConnectorIdentity,
    pub(crate) batch: BatchRouter,
    pub(crate) checkpoints: CheckpointCollector,
    pub(crate) sockets: SocketManager,
    pub(crate) frames: FrameTable,  // V3 frame table
    pub(crate) store_limits: StoreLimits,
    ctx: WasiCtx,
    table: ResourceTable,
}
```

Initialize `frames: FrameTable::new()` in the builder's `build()` method.

### Step 3: Add frame host method implementations

In `host_state.rs`, add frame methods to `impl ComponentHostState`:

```rust
/// Max frame capacity: 512 MB. Prevents guest-induced OOM from
/// a malicious or buggy connector requesting absurd allocations.
const MAX_FRAME_CAPACITY: u64 = 512 * 1024 * 1024;

pub(crate) fn frame_new_impl(&mut self, capacity: u64) -> Result<u64, ConnectorError> {
    let clamped = capacity.min(MAX_FRAME_CAPACITY);
    if capacity > MAX_FRAME_CAPACITY {
        tracing::warn!(
            requested = capacity,
            clamped = clamped,
            "frame-new capacity clamped to MAX_FRAME_CAPACITY"
        );
    }
    Ok(self.frames.alloc(clamped))
}

pub(crate) fn frame_write_impl(&mut self, handle: u64, chunk: Vec<u8>) -> Result<u64, ConnectorError> {
    self.frames.write(handle, &chunk).map_err(|e| {
        ConnectorError::frame("FRAME_WRITE_FAILED", e.to_string())
    })
}

pub(crate) fn frame_seal_impl(&mut self, handle: u64) -> Result<(), ConnectorError> {
    self.frames.seal(handle).map_err(|e| {
        ConnectorError::frame("FRAME_SEAL_FAILED", e.to_string())
    })
}

pub(crate) fn frame_len_impl(&mut self, handle: u64) -> Result<u64, ConnectorError> {
    self.frames.len(handle).map_err(|e| {
        ConnectorError::frame("FRAME_LEN_FAILED", e.to_string())
    })
}

pub(crate) fn frame_read_impl(&mut self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, ConnectorError> {
    self.frames.read(handle, offset, len).map_err(|e| {
        ConnectorError::frame("FRAME_READ_FAILED", e.to_string())
    })
}

pub(crate) fn frame_drop_impl(&mut self, handle: u64) {
    self.frames.drop_frame(handle);
}
```

### Step 4: Update emit_batch_impl for V3 (handle-based)

Replace `emit_batch_impl`:

```rust
pub(crate) fn emit_batch_impl(&mut self, handle: u64) -> Result<(), ConnectorError> {
    let fn_start = Instant::now();

    // Consume the sealed frame → Bytes (zero-copy)
    let payload = self.frames.consume(handle).map_err(|e| {
        ConnectorError::frame("EMIT_BATCH_FAILED", e.to_string())
    })?;

    if payload.is_empty() {
        return Err(ConnectorError::internal(
            "EMPTY_BATCH",
            "Connector emitted a zero-length batch; this is a protocol violation",
        ));
    }

    let compress_start = Instant::now();
    let payload: bytes::Bytes = if let Some(codec) = self.batch.compression {
        crate::compression::compress_bytes(codec, &payload)
            .map_err(|e| ConnectorError::internal("COMPRESS_FAILED", e.to_string()))?
    } else {
        payload
    };
    let compress_elapsed_nanos = if self.batch.compression.is_some() {
        compress_start.elapsed().as_nanos() as u64
    } else {
        0
    };

    let sender = self.batch.sender.as_ref().ok_or_else(|| {
        ConnectorError::internal("NO_SENDER", "No batch sender configured")
    })?;

    sender
        .blocking_send(Frame::Data(payload))
        .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;

    self.batch.next_batch_id += 1;

    let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
    t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
    t.emit_batch_count += 1;
    t.compress_nanos += compress_elapsed_nanos;

    Ok(())
}
```

### Step 5: Update next_batch_impl for V3 (returns frame handle)

Replace `next_batch_impl`:

```rust
pub(crate) fn next_batch_impl(&mut self) -> Result<Option<u64>, ConnectorError> {
    let fn_start = Instant::now();

    let receiver = self.batch.receiver.as_mut().ok_or_else(|| {
        ConnectorError::internal("NO_RECEIVER", "No batch receiver configured")
    })?;

    let Some(frame) = receiver.blocking_recv() else {
        return Ok(None);
    };

    let payload = match frame {
        Frame::Data(payload) => payload,
        Frame::EndStream => return Ok(None),
    };

    let decompress_start = Instant::now();
    let payload: bytes::Bytes = if let Some(codec) = self.batch.compression {
        crate::compression::decompress(codec, &payload)
            .map_err(|e| ConnectorError::internal("DECOMPRESS_FAILED", e.to_string()))?
            .into()
    } else {
        payload // Bytes — zero-copy, same Arc
    };
    let decompress_elapsed_nanos = if self.batch.compression.is_some() {
        decompress_start.elapsed().as_nanos() as u64
    } else {
        0
    };

    // Insert as a sealed read-only frame — guest reads via frame-read
    let handle = self.frames.insert_sealed(payload);

    let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
    t.next_batch_nanos += fn_start.elapsed().as_nanos() as u64;
    t.next_batch_count += 1;
    t.decompress_nanos += decompress_elapsed_nanos;

    Ok(Some(handle))
}
```

### Step 6: Update bindings.rs Host trait impls

In `bindings.rs`, update the `impl_host_trait_for_world` macro to match new V3 signatures:

```rust
macro_rules! impl_host_trait_for_world {
    ($module:ident, $to_world_error:ident) => {
        impl $module::rapidbyte::connector::host::Host for ComponentHostState {
            // V3 frame lifecycle
            fn frame_new(
                &mut self,
                capacity: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::connector::types::ConnectorError> {
                self.frame_new_impl(capacity).map_err($to_world_error)
            }

            fn frame_write(
                &mut self,
                handle: u64,
                chunk: Vec<u8>,
            ) -> std::result::Result<u64, $module::rapidbyte::connector::types::ConnectorError> {
                self.frame_write_impl(handle, chunk).map_err($to_world_error)
            }

            fn frame_seal(
                &mut self,
                handle: u64,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.frame_seal_impl(handle).map_err($to_world_error)
            }

            fn frame_len(
                &mut self,
                handle: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::connector::types::ConnectorError> {
                self.frame_len_impl(handle).map_err($to_world_error)
            }

            fn frame_read(
                &mut self,
                handle: u64,
                offset: u64,
                len: u64,
            ) -> std::result::Result<Vec<u8>, $module::rapidbyte::connector::types::ConnectorError> {
                self.frame_read_impl(handle, offset, len).map_err($to_world_error)
            }

            fn frame_drop(&mut self, handle: u64) {
                self.frame_drop_impl(handle);
            }

            // V3 batch transport (handle-based)
            fn emit_batch(
                &mut self,
                handle: u64,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.emit_batch_impl(handle).map_err($to_world_error)
            }

            fn next_batch(
                &mut self,
            ) -> std::result::Result<
                Option<u64>,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.next_batch_impl().map_err($to_world_error)
            }

            // ... rest of trait methods unchanged (log, checkpoint, metric, etc.)
        }
    };
}
```

Also update the error category mapping in `define_error_converters` macro to include `Frame`:

```rust
ErrorCategory::Frame => CErrorCategory::Frame,
// and reverse:
$module::rapidbyte::connector::types::ErrorCategory::Frame => ErrorCategory::Frame,
```

### Step 7: Run tests

Run: `cargo test --workspace`
Expected: PASS (host tests compile, frame table tests pass)

Note: Connector builds will fail until Task 7 updates the SDK. That's expected — connectors are separate builds.

### Step 8: Commit

```bash
git add wit/rapidbyte-connector.wit \
    crates/rapidbyte-runtime/src/host_state.rs \
    crates/rapidbyte-runtime/src/bindings.rs \
    crates/rapidbyte-types/src/error.rs
git commit -m "feat: V3 WIT frame-handle transport protocol

Replace list<u8> batch passing with u64 frame handles. Host manages
frame lifecycle (alloc→write→seal→emit/read→drop) backed by Bytes.
emit-batch consumes sealed frame into channel Bytes (zero-copy).
next-batch wraps channel Bytes as sealed read-only frame handle."
```

---

## Task 6: Update SDK guest-side to V3 frame transport

**Files:**
- Modify: `crates/rapidbyte-sdk/Cargo.toml` (add `bytes` dep)
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs` (V3 bindings, frame helpers)
- Modify: `crates/rapidbyte-sdk/src/arrow/ipc.rs` (encode with capacity hint)
- Test: `cargo test -p rapidbyte-sdk`

### Step 1: Add bytes to SDK

In `crates/rapidbyte-sdk/Cargo.toml`:

```toml
bytes = { version = "1", optional = true }
```

Add to `runtime` feature:
```toml
runtime = ["dep:tokio", "dep:wit-bindgen", "dep:arrow", "dep:rapidbyte-sdk-macros", "dep:bytes"]
```

### Step 2: Update WasmHostImports for V3 bindings

The wit-bindgen generated code will now have `emit_batch(u64)` and `next_batch() -> Option<u64>` plus frame functions.

Update `HostImports` trait:

```rust
pub trait HostImports: Send + Sync {
    fn log(&self, level: i32, message: &str);

    // V3 frame lifecycle
    fn frame_new(&self, capacity: u64) -> Result<u64, ConnectorError>;
    fn frame_write(&self, handle: u64, chunk: &[u8]) -> Result<u64, ConnectorError>;
    fn frame_seal(&self, handle: u64) -> Result<(), ConnectorError>;
    fn frame_len(&self, handle: u64) -> Result<u64, ConnectorError>;
    fn frame_read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, ConnectorError>;
    fn frame_drop(&self, handle: u64);

    // V3 batch transport (handle-based)
    fn emit_batch(&self, handle: u64) -> Result<(), ConnectorError>;
    fn next_batch(&self) -> Result<Option<u64>, ConnectorError>;

    // ... rest unchanged (state, checkpoint, metric, socket)
}
```

Update `WasmHostImports` impl to call V3 bindings.

### Step 3: Create FrameWriter that streams IPC directly into host frame

Create `crates/rapidbyte-sdk/src/frame_writer.rs`:

```rust
//! A `std::io::Write` adapter that streams bytes directly into a host frame
//! via `frame-write` calls, eliminating the guest-side Vec<u8> allocation
//! that would otherwise be needed for IPC encoding.

use std::io;
use crate::error::ConnectorError;
use crate::host_ffi::HostImports;

/// Wraps a host frame handle and implements `std::io::Write`.
/// Arrow's `StreamWriter` can write directly into this, streaming IPC
/// bytes across the WIT boundary without an intermediate guest buffer.
pub struct FrameWriter<'a> {
    handle: u64,
    imports: &'a dyn HostImports,
}

impl<'a> FrameWriter<'a> {
    pub fn new(handle: u64, imports: &'a dyn HostImports) -> Self {
        Self { handle, imports }
    }
}

impl<'a> io::Write for FrameWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.imports
            .frame_write(self.handle, buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.message.clone()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // frame-write is synchronous, nothing to flush
    }
}
```

### Step 4: Update emit_batch to use FrameWriter (zero guest-side alloc)

Replace the `emit_batch` function:

```rust
/// Emit an Arrow RecordBatch to the host pipeline via V3 frame transport.
///
/// Streams IPC encoding directly into a host frame via FrameWriter,
/// completely eliminating the guest-side Vec<u8> IPC buffer allocation.
///
/// Flow: RecordBatch → StreamWriter → FrameWriter → frame-write() → host BytesMut
pub fn emit_batch(batch: &RecordBatch) -> Result<(), ConnectorError> {
    let imports = host_imports();

    // Capacity hint: Arrow array memory + IPC overhead
    let capacity = batch.get_array_memory_size() as u64 + 1024;
    let handle = imports.frame_new(capacity)?;

    // Stream IPC encoding directly into the host frame — no guest Vec<u8>
    {
        let mut writer = crate::frame_writer::FrameWriter::new(handle, imports);
        let mut ipc_writer = arrow::ipc::writer::StreamWriter::try_new(&mut writer, batch.schema().as_ref())
            .map_err(|e| ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC writer init: {e}")))?;
        ipc_writer.write(batch)
            .map_err(|e| ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC write: {e}")))?;
        ipc_writer.finish()
            .map_err(|e| ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC finish: {e}")))?;
    }

    imports.frame_seal(handle)?;
    imports.emit_batch(handle)?;

    Ok(())
}
```

### Step 5: Update next_batch public helper

Replace the `next_batch` function:

```rust
/// Receive the next Arrow RecordBatch from the host pipeline via V3 frame transport.
///
/// 1. Receives a sealed frame handle from host
/// 2. Reads frame bytes via frame-read
/// 3. Decodes Arrow IPC and drops the frame handle
pub fn next_batch(
    max_bytes: u64,
) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, ConnectorError> {
    let imports = host_imports();

    let Some(handle) = imports.next_batch()? else {
        return Ok(None);
    };

    let frame_len = imports.frame_len(handle)?;
    if frame_len > max_bytes {
        imports.frame_drop(handle);
        return Err(ConnectorError::internal(
            "BATCH_TOO_LARGE",
            format!("Batch {frame_len} exceeds max {max_bytes}"),
        ));
    }

    // Read entire frame in one call (inherent WIT boundary copy)
    let ipc_bytes = imports.frame_read(handle, 0, frame_len)?;
    imports.frame_drop(handle);

    let (schema, batches) = crate::arrow::ipc::decode_ipc(&ipc_bytes)?;
    Ok(Some((schema, batches)))
}
```

### Step 6: Keep encode_ipc as fallback (used by tests/native stub)

The `encode_ipc` function in `crates/rapidbyte-sdk/src/arrow/ipc.rs` is still used by the `StubHostImports` native tests. Update it with a capacity hint but don't delete it:

```rust
pub fn encode_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ConnectorError> {
    let capacity = batch.get_array_memory_size() + 1024;
    let mut buf = Vec::with_capacity(capacity);
    // ... rest unchanged
}
```

### Step 7: Update StubHostImports for native tests

Add frame methods to `StubHostImports`:

```rust
impl HostImports for StubHostImports {
    fn frame_new(&self, _capacity: u64) -> Result<u64, ConnectorError> { Ok(1) }
    fn frame_write(&self, _handle: u64, _chunk: &[u8]) -> Result<u64, ConnectorError> { Ok(0) }
    fn frame_seal(&self, _handle: u64) -> Result<(), ConnectorError> { Ok(()) }
    fn frame_len(&self, _handle: u64) -> Result<u64, ConnectorError> { Ok(0) }
    fn frame_read(&self, _handle: u64, _offset: u64, _len: u64) -> Result<Vec<u8>, ConnectorError> { Ok(vec![]) }
    fn frame_drop(&self, _handle: u64) {}
    fn emit_batch(&self, _handle: u64) -> Result<(), ConnectorError> { Ok(()) }
    fn next_batch(&self) -> Result<Option<u64>, ConnectorError> { Ok(None) }
    // ... rest unchanged
}
```

### Step 8: Run SDK tests

Run: `cargo test -p rapidbyte-sdk`
Expected: PASS

### Step 9: Commit

```bash
git add crates/rapidbyte-sdk/Cargo.toml crates/rapidbyte-sdk/src/host_ffi.rs crates/rapidbyte-sdk/src/arrow/ipc.rs
git commit -m "feat: SDK V3 frame transport with capacity-hinted IPC encode

emit_batch: alloc frame with capacity hint → encode IPC → write → seal → emit.
next_batch: receive frame handle → read bytes → decode IPC → drop frame.
IPC encode now uses Vec::with_capacity(array_mem + 1024) to avoid reallocation."
```

---

## Task 7: Migrate source-postgres to V3 bindings

**Files:**
- Modify: `connectors/source-postgres/Cargo.toml` (verify wit-bindgen version)
- Modify: `connectors/source-postgres/src/*.rs` (update any direct binding calls)
- Test: `cd connectors/source-postgres && cargo build`

### Step 1: Check connector source for direct WIT binding usage

The source connector should only call SDK high-level functions (`host_ffi::emit_batch`, `host_ffi::checkpoint`). If it uses any direct WIT bindings, update them.

Typical changes needed:
- `rapidbyte:connector@2.0.0` references → `3.0.0` (in `Cargo.toml` or generated code)
- Any direct `bindings::rapidbyte::connector::host::emit_batch()` calls → use SDK wrapper

### Step 2: Rebuild connector

Run: `cd connectors/source-postgres && cargo build`
Expected: PASS (SDK wraps V3 details, connector code unchanged if using SDK helpers)

### Step 3: Run unit tests if any

Run: `cd connectors/source-postgres && cargo test`
Expected: PASS

### Step 4: Commit

```bash
git add connectors/source-postgres/
git commit -m "chore: migrate source-postgres to V3 bindings"
```

---

## Task 8: Migrate dest-postgres to V3 bindings

**Files:**
- Modify: `connectors/dest-postgres/Cargo.toml`
- Modify: `connectors/dest-postgres/src/*.rs`
- Test: `cd connectors/dest-postgres && cargo build`

### Step 1: Same approach as Task 7

The dest connector calls `host_ffi::next_batch` which now returns frame handles internally. The SDK handles the frame lifecycle transparently.

### Step 2: Rebuild and test

Run: `cd connectors/dest-postgres && cargo build && cargo test`
Expected: PASS

### Step 3: Commit

```bash
git add connectors/dest-postgres/
git commit -m "chore: migrate dest-postgres to V3 bindings"
```

---

## Task 9: Clean up V2 remnants

**Files:**
- Modify: `crates/rapidbyte-runtime/src/compression.rs` (remove old `compress`/`decompress` if unused)
- Modify: `crates/rapidbyte-runtime/src/host_state.rs` (remove dead code)
- Test: `cargo test --workspace`

### Step 1: Search for V2 references

Run: `rg "2\.0\.0" --type rust` and `rg "list<u8>" wit/` to confirm no V2 remnants.

### Step 2: Remove unused compress/decompress functions

If the original `compress`/`decompress` functions are no longer called (only `compress_bytes` and `decompress_to_vec` are used), remove them. If they're still used elsewhere, keep them.

### Step 3: Run tests

Run: `cargo test --workspace`
Expected: PASS

### Step 4: Run clippy

Run: `cargo clippy --workspace -- -W clippy::pedantic`
Expected: No new warnings

### Step 5: Commit

```bash
git add -A
git commit -m "chore: remove V2 transport remnants"
```

---

## Task 10: Full verification — E2E + Benchmarks

**Files:**
- No new files; run existing test infrastructure

### Step 1: Build all connectors (release)

Run: `just release-connectors`
Expected: Both connectors build successfully for `wasm32-wasip2`

### Step 2: Run host test suite

Run: `just test`
Expected: All workspace tests pass

### Step 3: Run E2E

Run: `just e2e`
Expected: Full source→destination pipeline completes, data verified

### Step 4: Run benchmarks

Run: `just bench postgres --profile small`
Expected: Completes. Compare throughput with previous baseline.

### Step 5: Run benchmarks with compression

Run: `just bench postgres --profile small` (with compression configured in the bench YAML)
Expected: Completes. Compression path uses `compress_bytes` → `Bytes`.

### Step 6: Commit benchmark results if meaningful

```bash
git add docs/benchmarks/
git commit -m "bench: V3 frame transport benchmark results"
```

---

## Summary: Copy Count Before/After

```
                        V2 (per batch)          V3 (per batch)
                        ──────────────          ──────────────
Host-side allocs:       3 (Vec×3)               0 (pooled BytesMut)
Channel copy:           move (Vec)              Arc clone (Bytes)
WIT boundary copies:    2 (emit + next)         2 (frame-write + frame-read)
Guest IPC alloc:        Vec::new() (full batch)  ZERO (FrameWriter streams to host)
Compression alloc:      Vec per call            Bytes::from(Vec) (single owner)
Total host copies:      3                       0 (excl. inherent wasm boundary)
```

## Risks and Mitigations

1. **Frame handle leaks** — Mitigated by `FrameTable::clear()` at connector teardown and `live_count()` diagnostics.
2. **Regression from WIT package version bump** — Mitigated by broad E2E coverage and build verification at each task.
3. **wit-bindgen / wasmtime version compatibility** — Both sides must generate from the same WIT. Pin versions.
4. **Compression API mismatch** — `lz4_flex::compress_prepend_size` always returns Vec; we wrap with `Bytes::from()` (zero-copy take-ownership). Future: use `compress_into` for true zero-alloc.
