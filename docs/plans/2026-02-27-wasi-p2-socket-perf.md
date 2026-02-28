# WASI P2 Socket Performance: Buffered I/O Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the ~1.3x performance gap between native WASI P2 sockets and the old host-proxied approach by reducing WASI boundary crossings via buffered I/O.

**Architecture:** Wrap `HostTcpStream` in `tokio::io::BufStream` with 64 KiB read/write buffers before passing to `tokio-postgres`. This batches the 8 KiB reads/writes that `tokio_util::codec::Framed` (used internally by tokio-postgres) makes into fewer, larger WASI syscalls. For the COPY write path (4 MB flushes): reduces ~500 WASI calls per batch to ~62.

**Tech Stack:** `tokio::io::BufStream`, `HostTcpStream` (SDK), `tokio-postgres` 0.7

---

## Root Cause Analysis

The 1.3x slowdown is NOT from the busy-poll pattern (identical in old and new code). It's from **per-call overhead of WASI P2 boundary crossings** (~1-5 μs per syscall).

The call chain today:
```
tokio-postgres Connection
  → Framed<HostTcpStream, PostgresCodec>   (8 KiB read/write buffers)
    → HostTcpStream::poll_read/poll_write   (raw AsyncRead/AsyncWrite)
      → std::net::TcpStream::read/write     (WASI P2 syscall, ~1-5 μs)
```

Each 8 KiB chunk Framed reads/writes triggers one WASI boundary crossing. With 64 KiB BufStream:
```
tokio-postgres Connection
  → Framed<BufStream<HostTcpStream>, PostgresCodec>   (8 KiB internal)
    → BufStream                                         (64 KiB buffers)
      → HostTcpStream::poll_read/poll_write
        → std::net::TcpStream::read/write               (WASI P2 syscall)
```

Multiple Framed 8 KiB reads/writes hit the BufStream buffer. WASI calls only happen when BufStream refills (reads) or flushes (writes) — ~8x fewer crossings.

## Why NOT wasi:io/poll

- `std::net::TcpStream` does not expose WASI `Pollable` handles
- Using `wasi` crate directly would mean rewriting the TCP adapter with low-level WASI socket APIs — no tokio async integration exists
- The busy-poll overhead is the same as the old code (not the regression cause)
- WASIp3 will bring native `future`/`stream` types with proper async integration — wait for that

## Why NOT blocking I/O

- `tokio-postgres` needs concurrent read/write on the `Connection` task
- Blocking one direction stalls the single-threaded WASM tokio runtime → deadlock
- Confirmed by test: blocking adapter hangs on full_refresh e2e test

---

## Task 1: Add `into_buffered()` to HostTcpStream in SDK

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_tcp.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn into_buffered_returns_buf_stream() {
    // We can't connect in unit tests, but we can verify the method exists
    // and the type compiles. The real test is e2e.
    // This test just ensures the API compiles.
    fn _assert_types() {
        fn takes_async_rw<T: tokio::io::AsyncRead + tokio::io::AsyncWrite>(_: T) {}
        // If this compiles, BufStream<HostTcpStream> implements both traits.
        fn check(s: super::HostTcpStream) {
            takes_async_rw(s.into_buffered());
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-sdk host_tcp -- --nocapture`
Expected: FAIL — `into_buffered` method doesn't exist.

**Step 3: Write the implementation**

Add to `crates/rapidbyte-sdk/src/host_tcp.rs`:

```rust
/// Default buffer capacity for TCP I/O (64 KiB).
///
/// Reduces WASI boundary crossings by batching tokio-postgres' 8 KiB
/// Framed reads/writes into fewer, larger syscalls.
pub const TCP_BUFFER_SIZE: usize = 64 * 1024;
```

Add method to `impl HostTcpStream`:

```rust
/// Wrap this stream in a buffered adapter with 64 KiB read/write buffers.
///
/// This reduces the number of WASI boundary crossings when used with
/// tokio-postgres (which internally reads/writes in 8 KiB chunks).
pub fn into_buffered(self) -> tokio::io::BufStream<Self> {
    tokio::io::BufStream::with_capacity(TCP_BUFFER_SIZE, TCP_BUFFER_SIZE, self)
}
```

**Step 4: Run test to verify it passes**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-sdk host_tcp -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_tcp.rs
git commit -m "sdk: add into_buffered() for reduced WASI boundary crossings"
```

---

## Task 2: Update source-postgres to use buffered stream

**Files:**
- Modify: `connectors/source-postgres/src/client.rs`

**Step 1: Read current code**

The current connection code in `client.rs` looks like:

```rust
let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
    .map_err(|e| format!("Connection failed: {e}"))?;
let (client, connection) = pg
    .connect_raw(stream, NoTls)
    .await
    .map_err(|e| format!("Connection failed: {e}"))?;
```

**Step 2: Update to use buffered stream**

Change to:

```rust
let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
    .map_err(|e| format!("Connection failed: {e}"))?;
let (client, connection) = pg
    .connect_raw(stream.into_buffered(), NoTls)
    .await
    .map_err(|e| format!("Connection failed: {e}"))?;
```

One-line change: `stream` → `stream.into_buffered()`.

**Step 3: Build connector**

Run: `cd /home/netf/rapidbyte/connectors/source-postgres && cargo build`
Expected: Build succeeds.

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/client.rs
git commit -m "source-postgres: use buffered TCP for reduced WASI overhead"
```

---

## Task 3: Update dest-postgres to use buffered stream

**Files:**
- Modify: `connectors/dest-postgres/src/client.rs`

**Step 1: Read current code**

Same pattern as source-postgres — `HostTcpStream::connect()` passed directly to `connect_raw()`.

**Step 2: Update to use buffered stream**

Same change: `stream` → `stream.into_buffered()`.

**Step 3: Build connector**

Run: `cd /home/netf/rapidbyte/connectors/dest-postgres && cargo build`
Expected: Build succeeds.

**Step 4: Commit**

```bash
git add connectors/dest-postgres/src/client.rs
git commit -m "dest-postgres: use buffered TCP for reduced WASI overhead"
```

---

## Task 4: Verify all tests pass

**Step 1: Run workspace tests**

Run: `cd /home/netf/rapidbyte && cargo test --workspace`
Expected: All tests pass.

**Step 2: Run e2e tests**

Run: `cd /home/netf/rapidbyte/tests/e2e && rm -rf /home/netf/rapidbyte/target/connectors/ && cargo test -- --test-threads=1 --nocapture`
Expected: All 15 e2e tests pass (connectors rebuild with buffered streams).

**Step 3: Run clippy + fmt**

Run: `cargo clippy --workspace && cargo fmt --check`
Expected: Clean.

---

## Task 5: Benchmark comparison

**Step 1: Run benchmark on arch1 branch (with buffering)**

Run: `cd /home/netf/rapidbyte && just bench postgres --profile medium`
Record results.

**Step 2: Compare against main (old host-proxied)**

Run: `just bench-compare main arch1`
Or manually compare the results tables.

**Expected improvements:**
- Dest Flush: ~1.4x → ~1.1x (biggest impact — COPY writes batched)
- Source Fetch: ~1.1x → ~1.05x (reads batched)
- Overall: ~1.3x → ~1.1x or better

If results are still significantly slower than main, investigate further:
- Try 128 KiB or 256 KiB buffer sizes
- Profile with `strace -e trace=read,write` to count actual syscall reduction

---

## Future Considerations (NOT tasks for now)

### wasi:io/poll integration (WASIp3)
When WASIp3 ships with native `future`/`stream` types:
- Replace busy-poll adapter with proper readiness-driven I/O
- Eliminate CPU spin on WouldBlock
- May require new async runtime support for wasm32-wasip2

### Configurable buffer sizes
If different workloads need different buffer sizes:
- Add `buffer_size` to connector config
- Pass through to `BufStream::with_capacity`
- Larger buffers for bulk (COPY), smaller for interactive queries
