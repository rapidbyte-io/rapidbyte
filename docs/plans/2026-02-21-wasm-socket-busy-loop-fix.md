# WASM Socket Busy-Loop Fix — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate the busy-loop caused by immediate waker self-wake on `WouldBlock` in the WASM guest's `HostTcpStream`, without blocking and without sacrificing throughput.

**Architecture:** The fix lives entirely on the host side. When a non-blocking socket read/write returns `WouldBlock`, the host uses `libc::poll()` with a short timeout (default 1ms) to wait for readiness before returning. If the socket becomes ready within that window, the host retries and returns data. If not, it returns `WouldBlock` — but now the guest's re-poll cycle is rate-limited to ~1000/sec instead of millions/sec. Zero overhead on the hot path (data already available). No WIT, SDK, or connector changes required.

**Tech Stack:** Rust, `libc::poll()`, `std::os::unix::io::AsRawFd` (gated behind `#[cfg(unix)]`)

---

## Problem Verification

The busy-loop is **structurally guaranteed** by the current code. Here is the exact causal chain:

1. **Host** sets `stream.set_nonblocking(true)` on every socket (`component_runtime.rs:607`)
2. **Host** `socket_read_impl` returns `WouldBlock` variant immediately when socket has no data (`component_runtime.rs:638-639`)
3. **Guest** `HostTcpStream::poll_read` receives `WouldBlock`, calls `cx.waker().wake_by_ref()`, returns `Poll::Pending` (`host_tcp.rs:75-77`)
4. **Guest** tokio runtime sees the waker fired → immediately re-polls the task
5. Go to step 2. Repeat at CPU speed (~millions of iterations/sec)

The same loop exists for writes (`host_tcp.rs:104-106`).

### Why it matters even during bulk transfer

During active data streaming, `WouldBlock` is infrequent because Postgres pushes data fast. But:
- **Connection setup** (TLS negotiation, auth handshake): multiple round-trips with idle waits
- **Query execution before first row**: server-side planning time
- **Between FETCH chunks**: server-side cursor seek time
- **Dest writes when network congested**: OS send buffer full
- **Small/slow queries**: any latency-bound workload

During any of these windows, the busy-loop burns 100% of the host thread's CPU doing nothing useful.

### Why we can't just block

The guest runs a single-threaded tokio runtime with two tasks:
1. The main task (query execution, batch processing)
2. The `connection` task (tokio-postgres protocol handler)

If `socket_read` blocks the OS thread, the entire tokio runtime stalls. Task 2 can't make progress while task 1 is blocked and vice versa. This causes hangs/deadlocks.

---

## Solution: Host-Side Poll-Then-Retry

Modify `socket_read_impl` and `socket_write_impl` to use `libc::poll()` with a short timeout on `WouldBlock`, then retry once. This is transparent to the guest.

**Why 1ms default?**
- Postgres LAN RTT: 0.1-1ms → 1ms catches most data arrivals on first wait
- Worst case: adds 1ms latency per WouldBlock event (negligible vs network RTT)
- Spin rate: bounded to ~1000/sec (from millions/sec)
- Zero overhead when data is immediately available (poll not called)
- Tunable via `RAPIDBYTE_SOCKET_POLL_MS` env var for perf profiling

**Why host-side, not guest-side?**
- Single-file change (`component_runtime.rs`)
- No WIT interface changes
- No SDK changes
- No connector rebuilds
- Transparent to all existing and future connectors

### Architectural note

This is a **tactical fix** that caps spin rate. It does not eliminate the guest's self-wake pattern — `host_tcp.rs:75` and `host_tcp.rs:104` still call `cx.waker().wake_by_ref()` on every `WouldBlock`, so the guest will still poll at ~1000/sec per socket while idle. For short-lived ingestion jobs with 1-2 sockets this is acceptable. The long-term ideal is a WIT-level `poll-socket` function (or migrating to WASI P2 `wasi:io/poll` pollable resources), which would let the guest park the waker until the host signals readiness. That's a separate, larger change.

---

### Task 1: Add `libc` dependency to `rapidbyte-core`

**Files:**
- Modify: `crates/rapidbyte-core/Cargo.toml`

**Step 1: Add the dependency**

Add `libc` to `[dependencies]` in `crates/rapidbyte-core/Cargo.toml`, gated to unix targets:

```toml
[target.'cfg(unix)'.dependencies]
libc = "0.2"
```

(`libc` is already in the transitive dependency tree via `getrandom`, so this adds no new compiled code.)

**Step 2: Verify it compiles**

Run: `cargo check -p rapidbyte-core`
Expected: compiles with no errors

**Step 3: Commit**

```bash
git add crates/rapidbyte-core/Cargo.toml
git commit -m "chore: add libc dependency to rapidbyte-core for socket polling (unix only)"
```

---

### Task 2: Add `wait_socket_ready` helper to `component_runtime.rs`

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`

**Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block at the end of `component_runtime.rs`:

```rust
#[cfg(unix)]
mod socket_poll_tests {
    use super::super::{wait_socket_ready, SocketInterest};

    #[test]
    fn wait_ready_returns_true_when_data_available() {
        use std::io::Write;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();

        server.write_all(b"hello").unwrap();
        // Generous sleep to ensure data arrives in kernel buffer (CI-safe)
        std::thread::sleep(std::time::Duration::from_millis(50));

        assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
    }

    #[test]
    fn wait_ready_returns_false_on_timeout() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let _server = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();

        // No data written — poll should timeout
        assert!(!wait_socket_ready(&client, SocketInterest::Read, 1).unwrap());
    }

    #[test]
    fn wait_ready_writable_for_connected_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let _server = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();

        // Fresh connected socket with empty send buffer should be writable
        assert!(wait_socket_ready(&client, SocketInterest::Write, 500).unwrap());
    }

    #[test]
    fn wait_ready_detects_peer_close_as_ready() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_nonblocking(true).unwrap();

        // Close server side — client should see HUP/readability for EOF
        drop(server);
        std::thread::sleep(std::time::Duration::from_millis(50));

        assert!(wait_socket_ready(&client, SocketInterest::Read, 500).unwrap());
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-core -- socket_poll_tests`
Expected: FAIL — `wait_socket_ready` and `SocketInterest` are not defined

**Step 3: Implement `wait_socket_ready`**

Add the gated import at the top of the file:
```rust
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
```

Add the constant, enum, and function above `impl ComponentHostState` (around line 260):

```rust
/// Default milliseconds to wait for socket readiness before returning WouldBlock.
/// Override with RAPIDBYTE_SOCKET_POLL_MS env var for performance tuning.
const SOCKET_READY_POLL_MS: i32 = 1;

/// Interest direction for socket readiness polling.
#[cfg(unix)]
enum SocketInterest {
    Read,
    Write,
}

/// Wait up to `timeout_ms` for a socket to become ready for the given interest.
///
/// Returns:
/// - `Ok(true)` if the socket is ready (including error/HUP conditions — the
///   caller should discover the specifics via the next I/O call).
/// - `Ok(false)` on a clean timeout (no events).
/// - `Err(io::Error)` on a non-EINTR poll failure (logged by caller for
///   observability, then treated as "ready" to let I/O surface the real error).
///
/// Handles EINTR by retrying. POLLERR/POLLHUP/POLLNVAL are returned as
/// `Ok(true)` so that the subsequent read()/write() surfaces the real error
/// through normal error handling.
#[cfg(unix)]
fn wait_socket_ready(
    stream: &TcpStream,
    interest: SocketInterest,
    timeout_ms: i32,
) -> std::io::Result<bool> {
    let events = match interest {
        SocketInterest::Read => libc::POLLIN,
        SocketInterest::Write => libc::POLLOUT,
    };
    let mut pfd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events,
        revents: 0,
    };
    loop {
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue; // EINTR: retry
            }
            return Err(err);
        }
        // ret == 0: clean timeout, socket not ready.
        // ret > 0: socket has events. This includes POLLERR, POLLHUP, POLLNVAL
        // — all of which mean the next I/O call will return the real error/EOF.
        return Ok(ret > 0);
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-core -- socket_poll_tests`
Expected: all 4 tests PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/component_runtime.rs
git commit -m "feat: add wait_socket_ready helper with EINTR handling and Result return

- Gated behind #[cfg(unix)] with libc::poll()
- Returns Result<bool, io::Error> for observability
- POLLERR/POLLHUP/POLLNVAL treated as ready (let I/O surface real error)
- EINTR retried automatically
- Default timeout defined as SOCKET_READY_POLL_MS const"
```

---

### Task 3: Integrate poll-then-retry into `socket_read_impl`

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs:620-646`

**Step 1: Add env-override helper for poll timeout**

Add a helper function near the `SOCKET_READY_POLL_MS` constant:

```rust
/// Read the socket poll timeout, allowing env override for perf tuning.
fn socket_poll_timeout_ms() -> i32 {
    static CACHED: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("RAPIDBYTE_SOCKET_POLL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(SOCKET_READY_POLL_MS)
    })
}
```

**Step 2: Modify `socket_read_impl` to poll-then-retry**

Replace `socket_read_impl` (lines 620-646) with:

```rust
fn socket_read_impl(
    &mut self,
    handle: u64,
    len: u64,
) -> Result<SocketReadResultInternal, ConnectorError> {
    let stream = self
        .sockets
        .get_mut(&handle)
        .ok_or_else(|| ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

    let read_len = len.min(64 * 1024).max(1) as usize;
    let mut buf = vec![0u8; read_len];
    match stream.read(&mut buf) {
        Ok(0) => Ok(SocketReadResultInternal::Eof),
        Ok(n) => {
            buf.truncate(n);
            Ok(SocketReadResultInternal::Data(buf))
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Poll for readiness before returning WouldBlock.
            // This prevents the guest from busy-looping at CPU speed.
            #[cfg(unix)]
            let ready = match wait_socket_ready(stream, SocketInterest::Read, socket_poll_timeout_ms()) {
                Ok(ready) => ready,
                Err(poll_err) => {
                    tracing::warn!(handle, error = %poll_err, "poll() failed in socket_read");
                    true // Let the next read() surface the real error
                }
            };
            #[cfg(not(unix))]
            let ready = false;

            if ready {
                match stream.read(&mut buf) {
                    Ok(0) => Ok(SocketReadResultInternal::Eof),
                    Ok(n) => {
                        buf.truncate(n);
                        Ok(SocketReadResultInternal::Data(buf))
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        Ok(SocketReadResultInternal::WouldBlock)
                    }
                    Err(e) => Err(ConnectorError::transient_network(
                        "SOCKET_READ_FAILED",
                        e.to_string(),
                    )),
                }
            } else {
                tracing::trace!(handle, "socket_read: WouldBlock after poll timeout");
                Ok(SocketReadResultInternal::WouldBlock)
            }
        }
        Err(e) => Err(ConnectorError::transient_network(
            "SOCKET_READ_FAILED",
            e.to_string(),
        )),
    }
}
```

**Step 3: Run all host tests**

Run: `cargo test -p rapidbyte-core`
Expected: all PASS (existing tests + new poll helper tests)

**Step 4: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/component_runtime.rs
git commit -m "fix: poll before returning WouldBlock in socket_read_impl to prevent busy-loop

- Uses wait_socket_ready with Result return for observability
- Propagates real errors (ConnectionReset, BrokenPipe) from retry path
- Logs poll() failures at warn level before falling through to I/O
- Falls back to original behavior on non-unix platforms
- Timeout tunable via RAPIDBYTE_SOCKET_POLL_MS env var"
```

---

### Task 4: Integrate poll-then-retry into `socket_write_impl`

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs:648-668`

**Step 1: Modify `socket_write_impl` to poll-then-retry**

Replace `socket_write_impl` (lines 648-668) with:

```rust
fn socket_write_impl(
    &mut self,
    handle: u64,
    data: Vec<u8>,
) -> Result<SocketWriteResultInternal, ConnectorError> {
    let stream = self
        .sockets
        .get_mut(&handle)
        .ok_or_else(|| ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

    match stream.write(&data) {
        Ok(n) => Ok(SocketWriteResultInternal::Written(n as u64)),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Poll for writability before returning WouldBlock.
            // This prevents the guest from busy-looping at CPU speed.
            #[cfg(unix)]
            let ready = match wait_socket_ready(stream, SocketInterest::Write, socket_poll_timeout_ms()) {
                Ok(ready) => ready,
                Err(poll_err) => {
                    tracing::warn!(handle, error = %poll_err, "poll() failed in socket_write");
                    true // Let the next write() surface the real error
                }
            };
            #[cfg(not(unix))]
            let ready = false;

            if ready {
                match stream.write(&data) {
                    Ok(n) => Ok(SocketWriteResultInternal::Written(n as u64)),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        Ok(SocketWriteResultInternal::WouldBlock)
                    }
                    Err(e) => Err(ConnectorError::transient_network(
                        "SOCKET_WRITE_FAILED",
                        e.to_string(),
                    )),
                }
            } else {
                tracing::trace!(handle, "socket_write: WouldBlock after poll timeout");
                Ok(SocketWriteResultInternal::WouldBlock)
            }
        }
        Err(e) => Err(ConnectorError::transient_network(
            "SOCKET_WRITE_FAILED",
            e.to_string(),
        )),
    }
}
```

**Step 2: Run all host tests**

Run: `cargo test -p rapidbyte-core`
Expected: all PASS

**Step 3: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/component_runtime.rs
git commit -m "fix: poll before returning WouldBlock in socket_write_impl to prevent busy-loop

- Same pattern as socket_read_impl: poll, log errors, propagate real failures
- Falls back to original behavior on non-unix platforms"
```

---

### Task 5: Build and run E2E test

**Files:**
- No file changes — validation only

**Step 1: Build host binary**

Run: `just build`
Expected: compiles successfully

**Step 2: Build connectors**

Run: `just build-connectors`
Expected: compiles successfully (no connector changes needed)

**Step 3: Run E2E test**

Run: `just e2e`
Expected: E2E test passes — data flows correctly, no regressions. This validates that:
- `socket_read_impl` poll-then-retry works end-to-end with real Postgres
- `socket_write_impl` poll-then-retry works end-to-end with real Postgres
- Error propagation is correct (no masked errors, no hangs)
- Incremental cursor tracking still works

**Step 4: Run benchmark**

Run: `just bench-connector-postgres`
Expected: Performance should be equal or better than before. CPU usage during idle/wait periods should be dramatically reduced.

---

## Summary of Changes

| File | Change | Lines |
|------|--------|-------|
| `crates/rapidbyte-core/Cargo.toml` | Add `libc = "0.2"` (unix-only via `[target.'cfg(unix)'.dependencies]`) | 2 lines |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Add `#[cfg(unix)] use std::os::unix::io::AsRawFd;` | 1 line |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Add `SOCKET_READY_POLL_MS` const + `socket_poll_timeout_ms()` env helper | ~12 lines |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Add `SocketInterest` enum + `wait_socket_ready() -> Result<bool>` with EINTR/POLLERR/POLLHUP | ~35 lines |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Modify `socket_read_impl()` with poll-then-retry, `#[cfg]` gating, warn logging | ~15 lines net |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Modify `socket_write_impl()` with poll-then-retry, `#[cfg]` gating, warn logging | ~13 lines net |
| `crates/rapidbyte-core/src/runtime/component_runtime.rs` | Add 4 unit tests for `wait_socket_ready` in `#[cfg(unix)]` submodule | ~55 lines |

**Total: ~130 lines changed, 1 file + 1 Cargo.toml**

No WIT changes. No SDK changes. No connector rebuilds. Fully backward compatible.

## Design decisions and tradeoffs

### What we did

- **`#[cfg(unix)]` gating**: All `libc::poll()` code, `AsRawFd` import, `SocketInterest`, `wait_socket_ready`, and tests are gated behind `#[cfg(unix)]`. Non-unix builds compile and run with original WouldBlock-immediate behavior (no poll). This is acceptable because Rapidbyte currently targets Linux/macOS.
- **`Result<bool, io::Error>` return**: `wait_socket_ready` returns `Result` so callers can log poll failures at warn level before falling through to I/O. Non-EINTR poll errors are observable in host logs without masking.
- **Named constant + env override**: `SOCKET_READY_POLL_MS` (default 1) is the single source of truth. `socket_poll_timeout_ms()` reads `RAPIDBYTE_SOCKET_POLL_MS` env var once (via `OnceLock`) for runtime tuning during perf profiling — no rebuild required.
- **Retry path propagates real errors**: The retry after `poll()` matches `WouldBlock` explicitly and sends all other errors (`ConnectionReset`, `BrokenPipe`, etc.) through the `ConnectorError::transient_network` path. No error masking.
- **CI-safe test timeouts**: "Should succeed" tests use 500ms poll timeout and 50ms sleep before assertion. "Should timeout" test uses 1ms. No sub-millisecond timing assertions.
- **Tests are isolated**: Unit tests exercise `wait_socket_ready` directly with raw `TcpStream` pairs in a `#[cfg(unix)]` submodule — no `ComponentHostState` construction. Integration coverage comes from the E2E test (`just e2e`) which exercises the full host-guest stack with real Postgres.

### What we did NOT do (and why)

- **Did not add a WIT `poll-socket` function**: Would require WIT change + SDK change + connector rebuild. Unnecessary complexity for the same result. If we later need to support many concurrent idle sockets, this is the right next step.
- **Did not modify `host_tcp.rs` (guest side)**: The `wake_by_ref()` call is still correct — it's the guest's only way to re-poll. The fix is making each re-poll take ~1ms instead of 0ns on the host side.
- **Did not make sockets blocking**: Would deadlock the guest's single-threaded tokio runtime (connection task vs. main task).
- **Did not add backoff in the guest**: Adding `thread::sleep` in WASM is unreliable and leaks platform details into the SDK.
- **Did not use `AsFd`/`BorrowedFd`**: While `AsFd` is the modern Rust idiom, `libc::poll()` requires a raw `c_int` fd. Using `AsRawFd` directly avoids an unnecessary `as_fd().as_raw_fd()` chain and is the standard pattern for `libc` interop.

### Known limitation

The 1ms host-side wait is a **tactical fix**, not the cleanest possible architecture. The guest still self-wakes immediately (`host_tcp.rs:75`, `host_tcp.rs:104`), so idle sockets will poll at ~1000/sec. For short-lived ingestion jobs with 1-2 sockets this is acceptable. For many concurrent idle sockets, the proper long-term solution is either:
1. A WIT-level `poll-socket(handle, interest, timeout-ms) -> bool` function that lets the guest park the waker
2. Migration to WASI P2 `wasi:io/poll` pollable resources with proper event subscription

Both are larger changes that should be tracked separately.
