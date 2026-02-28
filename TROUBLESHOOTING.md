# Async Wasmtime Migration: Pipeline Hang After SCRAM Auth

## Status: FIXED

**Date**: 2026-02-27
**Branch**: `main` (commits `8d0acdc..aca29b0`)
**Context**: Migrated from sync Wasmtime + `libc::poll` to async Wasmtime + Tokio

---

## 1. Symptom

After the async Wasmtime migration, running any pipeline with a Postgres connector (source or destination) hangs indefinitely after successful SCRAM-SHA-256 authentication. The process uses 0% CPU — it's a deadlock, not a spin loop.

**All 289 workspace tests pass.** All 3 WASM connectors build. The host binary builds. The hang only manifests at E2E runtime when connectors actually talk to Postgres.

## 2. Reproduction

```bash
# Requires a running Postgres container
docker run -d --name rbpg -e POSTGRES_PASSWORD=password -e POSTGRES_DB=rapidbyte_test -p 5433:5432 postgres:17

# Pipeline config at /tmp/debug_pipeline.yaml:
cat > /tmp/debug_pipeline.yaml <<'EOF'
version: "1.0"
pipeline: debug_async
source:
  use: source-postgres
  config:
    host: 127.0.0.1
    port: 5433
    user: postgres
    password: password
    database: rapidbyte_test
  streams:
    - name: debug_test
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: 127.0.0.1
    port: 5433
    user: postgres
    password: password
    database: rapidbyte_test
    schema: raw
  write_mode: append
state:
  backend: sqlite
  connection: /tmp/debug_state.db
EOF

# Symlink connectors for manual testing
mkdir -p /tmp/rb_connectors
ln -sf /home/netf/rapidbyte/target/wasm32-wasip2/debug/source_postgres.wasm /tmp/rb_connectors/
ln -sf /home/netf/rapidbyte/target/wasm32-wasip2/debug/dest_postgres.wasm /tmp/rb_connectors/

# Run with trace logging — hangs after ~2 seconds
rm -f /tmp/debug_state.db
RAPIDBYTE_CONNECTOR_DIR=/tmp/rb_connectors \
RUST_LOG=rapidbyte_runtime=trace,rapidbyte_engine=debug \
timeout 20 cargo run -- run /tmp/debug_pipeline.yaml
```

## 3. Diagnostic Evidence

### 3.1 Trace Log (host-side socket operations)

```
TRACE Attempting TCP connect host=127.0.0.1 port=5433 addr=127.0.0.1:5433
TRACE TCP connection established host=127.0.0.1 port=5433 handle=1
TRACE socket_write: written handle=1 bytes=68        ← PG StartupMessage
TRACE socket_read: waiting handle=1 read_len=8192
TRACE socket_read: data handle=1 bytes=24 first_byte=82 msg_types=['R']       ← AuthenticationSASL
TRACE socket_write: written handle=1 bytes=55        ← SASLInitialResponse
TRACE socket_read: waiting handle=1 read_len=8168
TRACE socket_read: data handle=1 bytes=93 first_byte=82 msg_types=['R']       ← SASLContinue
TRACE socket_write: written handle=1 bytes=109       ← SASLResponse
TRACE socket_read: waiting handle=1 read_len=8075
TRACE socket_read: data handle=1 bytes=491 first_byte=82 msg_types=['R', 'R', 'S'×14, 'K', 'Z']  ← AuthOK + Params + BackendKeyData + ReadyForQuery
TRACE socket_read: waiting handle=1 read_len=7584    ← *** HANGS HERE FOREVER ***
```

Key observations:
- SCRAM-SHA-256 authentication **completes successfully** (491-byte response includes `ReadyForQuery` 'Z' byte)
- After the 491-byte read, the guest calls `socket_read` **again** with `read_len=7584`
- This read never returns — Postgres is idle, waiting for queries
- **Zero host imports called after this point** — no `log`, `emit_batch`, `next_batch`, nothing

### 3.2 Strace Evidence

- Thread doing WASM execution: stuck in `futex_wait` (Wasmtime fiber suspended)
- Thread doing Tokio I/O: stuck in `epoll_wait` (no pending I/O to drive)
- 0% CPU usage

### 3.3 Postgres Message Parse

The 491-byte response contains these PG protocol messages:
```
['R', 'R', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'K', 'Z']
  R = AuthenticationSASLFinal
  R = AuthenticationOk
  S = ParameterStatus (×14: server_version, client_encoding, etc.)
  K = BackendKeyData
  Z = ReadyForQuery (idle)
```

ReadyForQuery IS present. The `connect_raw` call in `tokio-postgres` has all the data it needs. **The extra `socket_read` comes from the spawned `Connection` task, NOT from `connect_raw`.**

## 4. Root Cause

### Architecture of the Problem

The Postgres connectors use `tokio-postgres` which splits a connection into two halves:

```rust
// connectors/source-postgres/src/client.rs (and dest-postgres equivalent)
let (client, connection) = pg.connect_raw(stream, NoTls).await?;
tokio::spawn(async move {
    if let Err(e) = connection.await { /* log error */ }
});
// Now use `client` for queries...
```

- **`Client`**: sends SQL queries through an internal `mpsc` channel
- **`Connection`**: drives the actual socket I/O — reads server messages and routes responses back to `Client`
- These must run **concurrently**: `Connection` reads/writes the socket while `Client` sends requests

### How the Guest Tokio Runtime Works

The guest creates a `current_thread` Tokio runtime (single-threaded, cooperative):

```rust
// From SDK connector macro
tokio::runtime::Builder::new_current_thread()
    .build()
    .expect("Failed to create guest tokio runtime")
```

Each WIT export (open, validate, run_read, run_write) calls `rt.block_on(async_fn)`. Inside `block_on`, spawned tasks are polled cooperatively — but only when the main future yields `Poll::Pending`.

### The Deadlock Mechanism

After `connect_raw` completes:

1. `tokio::spawn(connection)` spawns the Connection driver as a background task
2. The Connection task immediately tries to read the next server message by calling `poll_read` on `HostTcpStream`
3. `HostTcpStream::poll_read` calls `host_ffi::socket_read()` — a synchronous WIT import
4. On the host side, `socket_read_impl` does `stream.read(&mut buf).await`
5. No data available from Postgres (server is idle, waiting for queries)
6. **Wasmtime async suspends the entire WASM fiber** — the guest execution is frozen

At this point:
- The Connection task is blocked waiting for server data (that will never come)
- `Client.query("SELECT 1")` can never be polled (the entire WASM fiber is suspended)
- The query is never sent to Postgres
- Postgres never sends a response
- **Classic deadlock: Connection waits for data ← data requires query ← query requires Connection**

### Why It Worked Before (Pre-Async Migration)

The old implementation (commit `55dc084` and earlier) used **non-blocking sockets + `libc::poll`**:

```rust
// OLD host_state.rs socket_read_impl (sync Wasmtime):
match entry.stream.read(&mut buf) {
    Ok(n) => Ok(SocketReadResult::Data(buf)),
    Err(e) if e.kind() == WouldBlock => {
        entry.read_would_block_streak += 1;
        if entry.read_would_block_streak < THRESHOLD {
            return Ok(SocketReadResult::WouldBlock);  // ← RETURNS TO GUEST
        }
        // After threshold: use libc::poll() to wait, then retry
        ...
    }
}
```

The old WIT contract included a `would-block` variant:
```wit
// OLD wit/rapidbyte-connector.wit:
variant socket-read-result {
    data(list<u8>),
    eof,
    would-block,   // ← THIS WAS REMOVED IN THE ASYNC MIGRATION
}
```

And the SDK's `HostTcpStream::poll_read` returned `Poll::Pending` on `WouldBlock`:
```rust
// OLD sdk/src/host_tcp.rs:
Ok(SocketReadResult::WouldBlock) => {
    cx.waker().wake_by_ref();  // immediate self-wake for retry
    Poll::Pending              // yields to guest Tokio runtime
}
```

This allowed the guest Tokio runtime to poll other tasks (like Client.query) while the Connection was waiting for data. The `wake_by_ref()` ensured the Connection task would be re-polled on the next `block_on` iteration.

### Why Async Wasmtime Breaks This

With async Wasmtime (`async_support(true)`):
- Host imports are `async fn` — they can `.await` internal I/O
- When a host import yields `Pending`, Wasmtime **suspends the WASM fiber**
- The guest execution is frozen until the host future completes
- **From the guest's perspective, host import calls are synchronous** — they block until completion
- The guest Tokio runtime NEVER sees `Poll::Pending` from socket operations
- Therefore, `tokio::spawn()` + concurrent I/O is fundamentally broken

The async migration (commits `8d0acdc..aca29b0`) removed `would-block` from WIT and from the SDK because we assumed async host imports would handle everything. But async host imports solve a different problem (allowing the HOST to do concurrent work), not the guest concurrency problem.

## 5. Files Involved

### Currently Modified (diagnostic traces — to be removed)
- `crates/rapidbyte-runtime/src/host_state.rs` — trace logging in `connect_tcp_impl`, `socket_read_impl`, `socket_write_impl`
- `crates/rapidbyte-runtime/src/bindings.rs` — trace logging in `emit_batch`, `next_batch`, `log` host traits

### Root Cause Files
- `wit/rapidbyte-connector.wit` — WIT socket contract (removed `would-block` variants)
- `crates/rapidbyte-sdk/src/host_tcp.rs` — `HostTcpStream::poll_read` (removed `Poll::Pending` path)
- `crates/rapidbyte-sdk/src/host_ffi.rs` — `SocketReadResult` enum (removed `WouldBlock` variant)
- `crates/rapidbyte-runtime/src/host_state.rs` — `socket_read_impl` (changed from non-blocking + libc::poll to async blocking read)

### Connector Code (uses tokio::spawn pattern)
- `connectors/source-postgres/src/client.rs` — `connect()` function
- `connectors/dest-postgres/src/client.rs` — `connect()` function

### Connector Macro (guest Tokio runtime)
- `crates/rapidbyte-sdk/macros/src/connector.rs` — `Builder::new_current_thread().build()`, `rt.block_on()`

## 6. Proposed Fix Options

### Option A: Re-introduce `would-block` (Recommended — Minimal Change)

Restore the pre-migration non-blocking behavior but keep async Wasmtime:

1. **WIT**: Add back `would-block` variant to `socket-read-result` and `socket-write-result`
2. **Host `socket_read_impl`**: Try non-blocking read first (via `tokio::io::AsyncReadExt::try_read` or `poll_read` with `noop_waker`). If no data, return `WouldBlock` instead of awaiting
3. **SDK `HostTcpStream::poll_read`**: Restore `Poll::Pending` + `cx.waker().wake_by_ref()` on `WouldBlock`
4. **SDK `host_ffi.rs`**: Add `WouldBlock` back to `SocketReadResult`

Pros: Minimal change, known working pattern, keeps async Wasmtime for other host imports
Cons: Busy-polling (self-wake) is inefficient — but it's what worked before, and for short-lived connector operations it's acceptable

### Option B: Add `socket-poll-readable` Host Import

Add a new non-blocking host import that checks socket readiness without reading:

1. **WIT**: Add `socket-poll-readable: func(handle: u64) -> bool`
2. **Host**: Implement as `stream.readable().now_or_never().is_some()` or similar
3. **SDK `HostTcpStream::poll_read`**: Call `socket_poll_readable` first; if false, return `Poll::Pending` + self-wake

Pros: Cleaner API separation, no `would-block` variant pollution
Cons: Extra WIT boundary crossing per read, still busy-polling

### Option C: Change Connector Architecture (Don't spawn Connection)

Modify connector code to not use `tokio::spawn(connection)`:

1. Replace `tokio::spawn(connection)` with a manual approach where Connection is driven inline with client operations
2. Would need a wrapper that runs Connection and Client operations as a single future

Pros: No WIT/SDK changes needed
Cons: Complex connector-side changes, every connector needs updating, fights tokio-postgres's API design

### Option D: Use `epoch_interruption` + Cooperative Yielding

Use Wasmtime's epoch-based interruption to periodically yield the WASM fiber:

1. Host periodically bumps the engine epoch
2. WASM execution yields at epoch boundaries
3. Host checks if any socket wakers need firing

Pros: No WIT changes needed
Cons: Complex, adds latency, doesn't directly solve the poll_read blocking issue

## 7. Recommendation

**Option A** is the clear winner: it's the smallest change, it restores a known-working pattern, and it keeps all the benefits of async Wasmtime for non-socket host imports (batch transport, state backend, etc.).

The key insight is: **async Wasmtime + `would-block` are complementary, not contradictory.** Async Wasmtime solves host-side concurrency (multiple WASM instances sharing the Tokio runtime). `would-block` solves guest-side concurrency (multiple tasks within one WASM instance's Tokio runtime).

## 8. Applied Fix

The fix uses a hybrid approach: **async Wasmtime for TCP connect**, **non-blocking `std::net::TcpStream` for socket I/O**, and **restored `would-block` WIT variants**.

### Changes Made

1. **WIT** (`wit/rapidbyte-connector.wit`): Re-added `would-block` to `socket-read-result` and `socket-write-result`

2. **Host socket storage** (`crates/rapidbyte-runtime/src/host_state.rs`):
   - Changed `SocketManager.sockets` from `HashMap<u64, tokio::net::TcpStream>` to `HashMap<u64, std::net::TcpStream>`
   - `connect_tcp_impl`: Uses `tokio::net::TcpStream::connect().await` for async connect, then `.into_std()` + `set_nonblocking(true)` for storage
   - `socket_read_impl`: Uses `std::io::Read::read()` — returns `WouldBlock` naturally from the OS
   - `socket_write_impl`: Uses `std::io::Write::write()` — returns `WouldBlock` naturally from the OS

3. **SDK** (`crates/rapidbyte-sdk/src/host_ffi.rs`, `src/host_tcp.rs`):
   - Re-added `WouldBlock` variant to `SocketReadResult` and `SocketWriteResult`
   - `HostTcpStream::poll_read`: Returns `Poll::Pending` + `cx.waker().wake_by_ref()` on `WouldBlock`
   - `HostTcpStream::poll_write`: Same pattern

4. **Host bindings** (`crates/rapidbyte-runtime/src/bindings.rs`, `src/socket.rs`):
   - Added `WouldBlock` variant mapping in socket result conversions

### Why This Works

- `std::net::TcpStream` in non-blocking mode does direct `read(2)`/`write(2)` syscalls
- Returns `EAGAIN`/`EWOULDBLOCK` immediately when no data available (no Tokio reactor needed)
- Guest `poll_read` sees `WouldBlock`, returns `Poll::Pending` + self-wake
- Guest Tokio runtime re-polls immediately, allowing cooperative scheduling between Connection and Client tasks
- The busy-poll works because each iteration is a fast WASM boundary crossing + syscall
- TCP connect remains async (uses `tokio::net::TcpStream::connect`) for proper DNS resolution and timeout support

### Key Insight

The fix splits socket lifecycle into two phases:
1. **Connect phase**: Async (Tokio, suspends WASM fiber) — correct for DNS + TCP handshake
2. **I/O phase**: Synchronous non-blocking (std::net) — correct for guest cooperative scheduling

This avoids both the Tokio reactor dependency problem (try_read needs reactor polling) and the fiber suspension problem (readable().await freezes all guest tasks).

## 9. Git History Reference

```
55dc084 — Last commit before async migration (working state with libc::poll)
8d0acdc — refactor(wit): remove would-block variants from socket result types
35a6315 — refactor(sdk): remove WouldBlock from socket result enums
5a5ba9a — refactor(sdk): simplify HostTcpStream without WouldBlock spin loop
bfa2908 — refactor(sdk): remove Tokio I/O reactor from WASM connector runtime
4bcb1bc — refactor(runtime): update bindings glue for removed would-block WIT variants
bbfce3d — feat(runtime): enable async Wasmtime with Tokio socket and batch I/O
c20bd09 — refactor(state): make StateBackend trait async
16054a5 — feat(engine): migrate runners and orchestrator to async execution
aca29b0 — style: apply cargo fmt to async migration changes
```

The would-block removal happened in commits `8d0acdc`, `35a6315`, `5a5ba9a`, `4bcb1bc`.
The async Wasmtime enablement happened in `bbfce3d`.

## 9. Key Realization

The async migration design made a wrong assumption:

> "With async host imports, socket operations will naturally suspend and resume through Wasmtime's fiber mechanism, eliminating the need for WouldBlock."

This is true for **sequential** I/O (SCRAM handshake works perfectly). But it breaks **concurrent** guest I/O where the guest Tokio runtime needs `Poll::Pending` to cooperatively schedule multiple tasks that share one socket. The WASM fiber model gives us one suspension point — when it suspends, ALL guest tasks freeze, not just the one waiting for I/O.
