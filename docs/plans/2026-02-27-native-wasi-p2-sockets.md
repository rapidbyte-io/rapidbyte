# Native WASI P2 Sockets Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the custom host-proxied TCP socket system (connect-tcp/socket-read/socket-write/socket-close WIT imports) with native WASI Preview 2 sockets, using Wasmtime's `socket_addr_check` for network ACLs.

**Architecture:** Connectors will use `std::net::TcpStream` (which works natively on wasm32-wasip2) wrapped in a thin async adapter, instead of calling custom host FFI for every socket operation. The host enables WASI P2 TCP via `WasiCtxBuilder::allow_tcp(true)` and enforces network ACLs through the `socket_addr_check` callback. This eliminates ~500 lines of custom socket proxying code across host, SDK, and WIT layers.

**Tech Stack:** Wasmtime 41.0.3 (wasmtime-wasi), WASI Preview 2 sockets (wasi:sockets/tcp), std::net::TcpStream on wasm32-wasip2

---

## Summary of Changes

| Layer | What Changes | Files |
|-------|-------------|-------|
| WIT | Remove socket functions + types | `wit/rapidbyte-connector.wit` |
| Host Runtime | Enable WASI TCP, add socket_addr_check, remove socket manager | `crates/rapidbyte-runtime/src/{sandbox,host_state,bindings,acl,lib}.rs` |
| Host Runtime | Delete socket helpers module | `crates/rapidbyte-runtime/src/socket.rs` |
| SDK | Remove socket FFI, replace HostTcpStream with native TCP adapter | `crates/rapidbyte-sdk/src/{host_ffi,host_tcp,lib}.rs` |
| Connectors | Switch from HostTcpStream to new TcpStream adapter | `connectors/{source,dest}-postgres/src/client.rs` |
| Docs | Update protocol spec | `docs/PROTOCOL.md` |

## Design Decisions

### Why `std::net::TcpStream` + async adapter (not `tokio::net::TcpStream`)

Tokio's `net` module relies on mio/epoll which doesn't support wasm32-wasip2. However, `std::net::TcpStream` works natively on wasip2 because Rust's std has WASI P2 socket support. We wrap it in a thin async adapter (implementing `AsyncRead + AsyncWrite`) using the same poll pattern the current `HostTcpStream` already uses — but without crossing the FFI boundary for every read/write.

### Network ACL: hostname → IP resolution strategy

The `socket_addr_check` callback receives `SocketAddr` (IP-based), not hostnames. Our current ACL is hostname-based. Resolution strategy:

1. **Exact hosts** in the ACL → pre-resolve to IPs at sandbox creation via DNS
2. **Wildcard patterns** (e.g. `*.example.com`) → check concrete config-derived hosts against the wildcard, resolve matched hosts to IPs
3. **allow_all** → passthrough, no check needed
4. **Intersected ACLs** → resolve both sides, intersect the IP sets

This preserves the security model for all real-world usage (connectors always connect to hosts from their config).

---

## Task 1: Remove socket functions from WIT interface

**Files:**
- Modify: `wit/rapidbyte-connector.wit`

**Step 1: Write a snapshot test of the current WIT**

This is a structural refactor — the "test" is compilation. No unit test to write. Instead, take a snapshot for reference.

Run: `cp wit/rapidbyte-connector.wit wit/rapidbyte-connector.wit.bak`

**Step 2: Remove socket types and functions from WIT**

In `wit/rapidbyte-connector.wit`:

Remove from `interface types`:
```wit
variant socket-read-result {
    data(list<u8>),
    eof,
    would-block,
}

variant socket-write-result {
    written(u64),
    would-block,
}
```

Remove from `interface host`:
```wit
// Remove these use entries from the use clause:
//   socket-read-result,
//   socket-write-result,

// Remove these functions:
connect-tcp: func(host: string, port: u16) -> result<u64, connector-error>;
socket-read: func(handle: u64, len: u64) -> result<socket-read-result, connector-error>;
socket-write: func(handle: u64, data: list<u8>) -> result<socket-write-result, connector-error>;
socket-close: func(handle: u64);
```

The `host` interface `use` clause should become:
```wit
use types.{connector-error};
```

**Step 3: Verify WIT syntax is valid**

Run: `cat wit/rapidbyte-connector.wit` (manual review)

**Step 4: Commit**

```bash
git add wit/rapidbyte-connector.wit
git commit -m "wit: remove custom socket functions from host interface

Native WASI P2 sockets will replace host-proxied TCP."
```

---

## Task 2: Update ACL module for IP-based resolution

**Files:**
- Modify: `crates/rapidbyte-runtime/src/acl.rs`

**Step 1: Write tests for the new `ResolvedAcl` type**

Add to the bottom of `acl.rs` tests module:

```rust
#[test]
fn resolved_acl_allow_all_allows_any_addr() {
    let acl = ResolvedAcl::allow_all();
    let addr: SocketAddr = "1.2.3.4:5432".parse().unwrap();
    assert!(acl.allows_addr(&addr));
}

#[test]
fn resolved_acl_specific_ip_allows_match() {
    let mut ips = HashSet::new();
    ips.insert("127.0.0.1".parse::<IpAddr>().unwrap());
    let acl = ResolvedAcl { allow_all: false, allowed_ips: ips };
    assert!(acl.allows_addr(&"127.0.0.1:5432".parse().unwrap()));
    assert!(!acl.allows_addr(&"10.0.0.1:5432".parse().unwrap()));
}

#[test]
fn resolved_acl_from_allow_all_network_acl() {
    let acl = NetworkAcl::allow_all();
    let config = serde_json::json!({});
    let resolved = acl.resolve_to_ip_acl(&config);
    assert!(resolved.allows_addr(&"1.2.3.4:80".parse().unwrap()));
}

#[test]
fn resolved_acl_from_exact_ip_host() {
    let mut acl = NetworkAcl::default();
    acl.add_host("127.0.0.1");
    let config = serde_json::json!({});
    let resolved = acl.resolve_to_ip_acl(&config);
    assert!(resolved.allows_addr(&"127.0.0.1:5432".parse().unwrap()));
    assert!(!resolved.allows_addr(&"10.0.0.1:5432".parse().unwrap()));
}

#[test]
fn resolved_acl_from_localhost_hostname() {
    let mut acl = NetworkAcl::default();
    acl.add_host("localhost");
    let config = serde_json::json!({});
    let resolved = acl.resolve_to_ip_acl(&config);
    // localhost resolves to 127.0.0.1 and/or ::1
    let lo4: SocketAddr = "127.0.0.1:5432".parse().unwrap();
    let lo6: SocketAddr = "[::1]:5432".parse().unwrap();
    assert!(resolved.allows_addr(&lo4) || resolved.allows_addr(&lo6));
}

#[test]
fn resolved_acl_wildcard_with_config_hosts() {
    let mut acl = NetworkAcl::default();
    acl.add_host("*.example.com");
    let config = serde_json::json!({"host": "localhost"});
    // "localhost" does not match *.example.com, so it won't be resolved
    let resolved = acl.resolve_to_ip_acl(&config);
    // No IPs resolved (localhost doesn't match the wildcard)
    assert!(!resolved.allows_addr(&"127.0.0.1:80".parse().unwrap()));
}
```

**Step 2: Run tests to verify they fail**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime acl::tests --no-run 2>&1 | head -30`
Expected: Compilation error — `ResolvedAcl` doesn't exist yet.

**Step 3: Implement `ResolvedAcl` and `resolve_to_ip_acl`**

Add to `crates/rapidbyte-runtime/src/acl.rs`:

```rust
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

/// IP-address-based ACL for use with Wasmtime's `socket_addr_check`.
/// Created by resolving hostname-based `NetworkAcl` entries to IP addresses.
#[derive(Debug, Clone)]
pub struct ResolvedAcl {
    allow_all: bool,
    allowed_ips: HashSet<IpAddr>,
}

impl ResolvedAcl {
    #[must_use]
    pub fn allow_all() -> Self {
        Self {
            allow_all: true,
            allowed_ips: HashSet::new(),
        }
    }

    #[must_use]
    pub fn allows_addr(&self, addr: &SocketAddr) -> bool {
        self.allow_all || self.allowed_ips.contains(&addr.ip())
    }
}

fn resolve_host_to_ips(host: &str, ips: &mut HashSet<IpAddr>) {
    if let Ok(ip) = host.parse::<IpAddr>() {
        ips.insert(ip);
    } else if let Ok(addrs) = (host, 0u16).to_socket_addrs() {
        for addr in addrs {
            ips.insert(addr.ip());
        }
    } else {
        tracing::warn!(host, "Failed to resolve allowed host to IP; connections may be denied");
    }
}

impl NetworkAcl {
    /// Resolve this ACL to an IP-based ACL for use with `socket_addr_check`.
    ///
    /// Takes the connector config to extract concrete hostnames when wildcard
    /// patterns are present (wildcards can't be pre-resolved to IPs directly).
    #[must_use]
    pub fn resolve_to_ip_acl(&self, config: &serde_json::Value) -> ResolvedAcl {
        if self.allow_all {
            return ResolvedAcl::allow_all();
        }

        if let Some(ref inner) = self.inner {
            let left = inner.left.resolve_to_ip_acl(config);
            let right = inner.right.resolve_to_ip_acl(config);
            return intersect_resolved(&left, &right);
        }

        let mut ips = HashSet::new();

        // Resolve exact hosts directly
        for host in &self.exact_hosts {
            resolve_host_to_ips(host, &mut ips);
        }

        // For wildcards, check config-derived hosts against the pattern
        if !self.suffix_wildcards.is_empty() {
            let mut config_hosts = HashSet::new();
            collect_runtime_hosts(config, &mut config_hosts);
            for host in &config_hosts {
                if self.allows(host) {
                    resolve_host_to_ips(host, &mut ips);
                }
            }
        }

        ResolvedAcl {
            allow_all: false,
            allowed_ips: ips,
        }
    }
}

fn intersect_resolved(left: &ResolvedAcl, right: &ResolvedAcl) -> ResolvedAcl {
    if left.allow_all && right.allow_all {
        return ResolvedAcl::allow_all();
    }
    if left.allow_all {
        return right.clone();
    }
    if right.allow_all {
        return left.clone();
    }
    ResolvedAcl {
        allow_all: false,
        allowed_ips: left
            .allowed_ips
            .intersection(&right.allowed_ips)
            .copied()
            .collect(),
    }
}
```

Also add `#[must_use]` and a public accessor:
```rust
impl NetworkAcl {
    #[must_use]
    pub fn is_allow_all(&self) -> bool {
        self.allow_all
    }
}
```

Note: `add_host` needs to become `pub(crate)` or `pub` since it was previously used only internally. Check visibility — it's currently private, keep it `pub(crate)`.

**Step 4: Run tests to verify they pass**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime acl::tests -v`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-runtime/src/acl.rs
git commit -m "acl: add ResolvedAcl for IP-based socket_addr_check"
```

---

## Task 3: Update sandbox to enable WASI P2 sockets with ACL

**Files:**
- Modify: `crates/rapidbyte-runtime/src/sandbox.rs`

**Step 1: Write test for the new build_wasi_ctx signature**

Add to `sandbox.rs` tests:

```rust
#[test]
fn build_wasi_ctx_with_acl_succeeds() {
    use crate::acl::{NetworkAcl, ResolvedAcl};
    let acl = ResolvedAcl::allow_all();
    let ctx = build_wasi_ctx(None, None, &acl);
    assert!(ctx.is_ok());
}
```

**Step 2: Run test to verify it fails**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime sandbox::tests --no-run 2>&1 | head -20`
Expected: Compilation error — `build_wasi_ctx` doesn't accept `ResolvedAcl` yet.

**Step 3: Update `build_wasi_ctx` to enable TCP and use socket_addr_check**

Modify `crates/rapidbyte-runtime/src/sandbox.rs`:

```rust
use std::future::ready;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use crate::acl::ResolvedAcl;

/// Build a WASI context with native WASI P2 TCP sockets enabled.
/// Network ACLs are enforced via `socket_addr_check`.
pub fn build_wasi_ctx(
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
    resolved_acl: &ResolvedAcl,
) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.allow_blocking_current_thread(true);

    // Enable native WASI P2 sockets — connectors use std::net::TcpStream directly.
    builder.allow_tcp(true);
    builder.allow_ip_name_lookup(true);

    // Network ACL enforcement via socket_addr_check.
    let acl = Arc::new(resolved_acl.clone());
    builder.socket_addr_check(move |addr: SocketAddr, _use_type| -> Pin<Box<dyn std::future::Future<Output = bool> + Send + Sync>> {
        let allowed = acl.allows_addr(&addr);
        Box::pin(ready(allowed))
    });

    if let Some(perms) = permissions {
        // ... (env vars and preopens logic stays the same)
    }

    Ok(builder.build())
}
```

**Step 4: Fix all callers of `build_wasi_ctx`**

The only caller is in `host_state.rs` (`HostStateBuilder::build`). This will be fixed in Task 5 but for now, update the call to pass a `ResolvedAcl`. Temporarily construct one from the existing `NetworkAcl`:

In `host_state.rs` `build()`:
```rust
let acl = derive_network_acl(
    self.permissions.as_ref(),
    &self.config,
    self.overrides.as_ref().and_then(|o| o.allowed_hosts.as_deref()),
);
let resolved_acl = acl.resolve_to_ip_acl(&self.config);

// ... later:
ctx: build_wasi_ctx(self.permissions.as_ref(), self.overrides.as_ref(), &resolved_acl)?,
```

**Step 5: Run tests to verify they pass**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime sandbox::tests -v`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add crates/rapidbyte-runtime/src/sandbox.rs
git commit -m "sandbox: enable native WASI P2 TCP with socket_addr_check ACL"
```

---

## Task 4: Remove socket manager from host state

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`

**Step 1: Remove socket-related code from ComponentHostState**

In `crates/rapidbyte-runtime/src/host_state.rs`:

1. Remove imports:
   - `use std::io::{Read, Write};`
   - `use std::net::{SocketAddr, TcpStream};`
   - `use std::time::Duration;`
   - `use crate::socket::*` imports

2. Remove the `SocketManager` struct:
   ```rust
   // DELETE this entire struct
   pub(crate) struct SocketManager { ... }
   ```

3. Remove the `sockets` field from `ComponentHostState`:
   ```rust
   // DELETE: pub(crate) sockets: SocketManager,
   ```

4. Remove the `MAX_SOCKET_READ_BYTES` constant.

5. Remove all socket `_impl` methods from `impl ComponentHostState`:
   - `connect_tcp_impl`
   - `socket_read_impl`
   - `socket_write_impl`
   - `socket_close_impl`

6. Update `HostStateBuilder::build()`:
   - Remove the `sockets: SocketManager { ... }` block
   - The `NetworkAcl` derivation now feeds into `ResolvedAcl` for `build_wasi_ctx`

The `build()` method becomes:
```rust
pub fn build(self) -> Result<ComponentHostState> {
    let pipeline = self.pipeline.expect("pipeline is required");
    let connector_id = self.connector_id.expect("connector_id is required");
    let stream = self.stream.expect("stream is required");
    let state_backend = self.state_backend.expect("state_backend is required");

    let acl = derive_network_acl(
        self.permissions.as_ref(),
        &self.config,
        self.overrides.as_ref().and_then(|o| o.allowed_hosts.as_deref()),
    );
    let resolved_acl = acl.resolve_to_ip_acl(&self.config);

    Ok(ComponentHostState {
        identity: ConnectorIdentity {
            pipeline: PipelineId::new(pipeline),
            connector_id,
            stream: StreamName::new(stream),
            state_backend,
        },
        batch: BatchRouter {
            sender: self.sender,
            receiver: self.receiver,
            next_batch_id: 1,
            compression: self.compression,
        },
        checkpoints: CheckpointCollector {
            source: self.source_checkpoints.unwrap_or_default(),
            dest: self.dest_checkpoints.unwrap_or_default(),
            dlq_records: self.dlq_records.unwrap_or_default(),
            timings: self.timings
                .unwrap_or_else(|| Arc::new(Mutex::new(HostTimings::default()))),
            dlq_limit: self.dlq_limit,
        },
        frames: FrameTable::new(),
        store_limits: build_store_limits(self.overrides.as_ref()),
        ctx: build_wasi_ctx(self.permissions.as_ref(), self.overrides.as_ref(), &resolved_acl)?,
        table: ResourceTable::new(),
    })
}
```

**Step 2: Run tests**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime host_state::tests -v`
Expected: All existing host_state tests pass (they don't test sockets directly).

**Step 3: Commit**

```bash
git add crates/rapidbyte-runtime/src/host_state.rs
git commit -m "host_state: remove SocketManager and socket_*_impl methods

Network I/O now handled natively by WASI P2 sockets."
```

---

## Task 5: Remove socket functions from host bindings

**Files:**
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`

**Step 1: Remove socket-related Host trait methods and converters**

In `crates/rapidbyte-runtime/src/bindings.rs`:

1. Remove the import:
   ```rust
   // DELETE:
   use crate::socket::{SocketReadResult, SocketWriteResult};
   ```

2. In the `impl_host_trait_for_world` macro, remove these methods:
   - `connect_tcp`
   - `socket_read`
   - `socket_write`
   - `socket_close`

The macro should only contain: `emit_batch`, `next_batch`, `log`, `checkpoint`, `metric`, `emit_dlq_record`, `state_get`, `state_put`, `state_cas`, `frame_new`, `frame_write`, `frame_seal`, `frame_len`, `frame_read`, `frame_drop`.

**Step 2: Verify compilation**

Run: `cd /home/netf/rapidbyte && cargo check -p rapidbyte-runtime`
Expected: Compiles successfully.

**Step 3: Commit**

```bash
git add crates/rapidbyte-runtime/src/bindings.rs
git commit -m "bindings: remove socket Host trait impls (now via WASI P2)"
```

---

## Task 6: Delete socket.rs and update runtime lib.rs

**Files:**
- Delete: `crates/rapidbyte-runtime/src/socket.rs`
- Modify: `crates/rapidbyte-runtime/src/lib.rs`

**Step 1: Delete socket.rs**

Run: `rm crates/rapidbyte-runtime/src/socket.rs`

**Step 2: Remove socket module from lib.rs**

In `crates/rapidbyte-runtime/src/lib.rs`, remove:
```rust
pub mod socket;
```

**Step 3: Verify compilation**

Run: `cd /home/netf/rapidbyte && cargo check -p rapidbyte-runtime`
Expected: Compiles successfully.

**Step 4: Run all runtime tests**

Run: `cd /home/netf/rapidbyte && cargo test -p rapidbyte-runtime -v`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add -A crates/rapidbyte-runtime/
git commit -m "runtime: delete socket.rs module (replaced by WASI P2 sockets)"
```

---

## Task 7: Update SDK host_ffi — remove socket FFI

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`

**Step 1: Remove socket types and trait methods**

In `crates/rapidbyte-sdk/src/host_ffi.rs`:

1. Remove the `SocketReadResult` and `SocketWriteResult` enums.

2. Remove from `HostImports` trait:
   ```rust
   fn connect_tcp(&self, host: &str, port: u16) -> Result<u64, ConnectorError>;
   fn socket_read(&self, handle: u64, len: u64) -> Result<SocketReadResult, ConnectorError>;
   fn socket_write(&self, handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError>;
   fn socket_close(&self, handle: u64);
   ```

3. Remove from `WasmHostImports` impl (the `#[cfg(target_arch = "wasm32")]` block):
   - `connect_tcp`, `socket_read`, `socket_write`, `socket_close` implementations

4. Remove from `StubHostImports` impl (the `#[cfg(not(target_arch = "wasm32"))]` block):
   - `connect_tcp`, `socket_read`, `socket_write`, `socket_close` implementations

5. Remove the free functions at the bottom:
   ```rust
   pub fn connect_tcp(...) { ... }
   pub fn socket_read(...) { ... }
   pub fn socket_write(...) { ... }
   pub fn socket_close(...) { ... }
   ```

6. Remove the socket-related test:
   ```rust
   fn test_stub_socket_variants_construct() { ... }
   ```

**Step 2: Verify SDK compiles for native target**

Run: `cd /home/netf/rapidbyte && cargo check -p rapidbyte-sdk`
Expected: Compiles (native target).

**Step 3: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_ffi.rs
git commit -m "sdk: remove socket FFI from host_ffi (now via native WASI sockets)"
```

---

## Task 8: Replace HostTcpStream with native WASI TCP adapter

**Files:**
- Rewrite: `crates/rapidbyte-sdk/src/host_tcp.rs`

**Step 1: Write tests for the new TcpStream adapter**

Add tests at the bottom of the rewritten file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_stream_to_io_error_formats_correctly() {
        let err = ConnectorError::transient_network("TEST", "test error");
        let io_err = to_io_error(err);
        assert_eq!(io_err.kind(), io::ErrorKind::Other);
    }
}
```

(Integration testing of actual TCP will happen in the pipeline smoke test — Task 11.)

**Step 2: Rewrite host_tcp.rs with native std::net::TcpStream**

Replace the entire contents of `crates/rapidbyte-sdk/src/host_tcp.rs`:

```rust
//! Async TCP stream adapter for WASI P2 native sockets.
//!
//! Wraps `std::net::TcpStream` with `AsyncRead`/`AsyncWrite` for use with
//! async libraries like `tokio-postgres`. On wasm32-wasip2, `std::net::TcpStream`
//! uses native WASI P2 sockets. On native targets, it uses OS sockets.

use std::io::{self, Read, Write};
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::ConnectorError;

/// TCP stream backed by native WASI P2 sockets (or OS sockets on native).
pub struct HostTcpStream {
    inner: std::net::TcpStream,
}

impl HostTcpStream {
    /// Connect to `host:port` using native TCP.
    ///
    /// On wasm32-wasip2, this uses WASI P2 sockets (host enforces ACL via
    /// `socket_addr_check`). On native targets, this uses OS sockets directly.
    pub fn connect(host: &str, port: u16) -> Result<Self, ConnectorError> {
        let addr = format!("{host}:{port}");
        let addrs: Vec<_> = addr
            .to_socket_addrs()
            .map_err(|e| {
                ConnectorError::transient_network(
                    "DNS_RESOLUTION_FAILED",
                    format!("{host}:{port}: {e}"),
                )
            })?
            .collect();

        if addrs.is_empty() {
            return Err(ConnectorError::transient_network(
                "DNS_RESOLUTION_FAILED",
                format!("{host}:{port}: no addresses resolved"),
            ));
        }

        let mut last_err = None;
        for a in &addrs {
            match std::net::TcpStream::connect_timeout(a, std::time::Duration::from_secs(10)) {
                Ok(stream) => {
                    stream.set_nonblocking(true).map_err(|e| {
                        ConnectorError::internal("SOCKET_CONFIG", e.to_string())
                    })?;
                    stream.set_nodelay(true).map_err(|e| {
                        ConnectorError::internal("SOCKET_CONFIG", e.to_string())
                    })?;
                    return Ok(Self { inner: stream });
                }
                Err(e) => last_err = Some((*a, e)),
            }
        }

        let details = last_err.map_or_else(
            || "no addresses available".to_string(),
            |(a, e)| format!("last attempt {a}: {e}"),
        );
        Err(ConnectorError::transient_network(
            "TCP_CONNECT_FAILED",
            format!("{host}:{port} ({details})"),
        ))
    }
}

fn to_io_error(err: ConnectorError) -> io::Error {
    io::Error::other(err.to_string())
}

impl AsyncRead for HostTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.inner.read(buf.initialize_unfilled()) {
            Ok(0) => Poll::Ready(Ok(())),
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for HostTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match this.inner.write(data) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.inner.flush().map_or_else(
            |e| if e.kind() == io::ErrorKind::WouldBlock {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(e))
            },
            |()| Poll::Ready(Ok(())),
        )
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let _ = this.inner.shutdown(std::net::Shutdown::Both);
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_bad_host_returns_error() {
        let result = HostTcpStream::connect("256.256.256.256", 1);
        assert!(result.is_err());
    }
}
```

**Key differences from old implementation:**
- No FFI calls — uses `std::net::TcpStream` directly
- Same `AsyncRead`/`AsyncWrite` interface — connectors don't change their usage pattern
- Retains the name `HostTcpStream` to minimize connector changes (only internal impl changes)
- Proper multi-address fallback (same as old `connect_tcp_impl`)
- `TCP_NODELAY` set (same as before)
- No `Drop`-based cleanup needed — `std::net::TcpStream::drop` handles socket cleanup

**Step 3: Verify SDK compiles**

Run: `cd /home/netf/rapidbyte && cargo check -p rapidbyte-sdk`
Expected: Compiles.

**Step 4: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_tcp.rs
git commit -m "sdk: replace FFI-based HostTcpStream with native std::net adapter

HostTcpStream now wraps std::net::TcpStream directly. On wasm32-wasip2,
this uses native WASI P2 sockets. ACL enforcement moves to the host's
socket_addr_check callback."
```

---

## Task 9: Verify host crate compiles and passes tests

**Files:** None (verification only)

**Step 1: Check host compilation**

Run: `cd /home/netf/rapidbyte && cargo check --workspace`
Expected: All workspace crates compile. Fix any remaining compilation errors from the socket removal.

**Step 2: Run all workspace tests**

Run: `cd /home/netf/rapidbyte && cargo test --workspace`
Expected: All tests pass.

**Step 3: Fix any issues found**

If compilation errors remain, they will be in code that references the removed socket types/functions. Trace and fix each one.

**Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix: resolve remaining compilation issues from socket removal"
```

---

## Task 10: Build connectors for wasm32-wasip2

**Files:** None modified (connectors use the updated SDK, which changed under them)

The connectors' `client.rs` files already use `rapidbyte_sdk::host_tcp::HostTcpStream::connect()` — the API is unchanged, only the internal implementation changed. The WIT bindings regenerated by `wit_bindgen::generate!()` will no longer contain socket functions, but connectors don't call those directly (they go through `host_tcp`).

**Step 1: Build source-postgres**

Run: `cd /home/netf/rapidbyte/connectors/source-postgres && cargo build`
Expected: Compiles for wasm32-wasip2.

If there are errors related to missing WIT bindings for socket types, they'll be in the SDK's `host_ffi.rs` WIT bindings block. These were already removed in Task 7.

**Step 2: Build dest-postgres**

Run: `cd /home/netf/rapidbyte/connectors/dest-postgres && cargo build`
Expected: Compiles for wasm32-wasip2.

**Step 3: Build transform-sql**

Run: `cd /home/netf/rapidbyte/connectors/transform-sql && cargo build`
Expected: Compiles for wasm32-wasip2 (no socket usage).

**Step 4: Commit (if any connector-side changes were needed)**

```bash
git add -A connectors/
git commit -m "connectors: rebuild with native WASI P2 socket support"
```

---

## Task 11: Simple pipeline smoke test

**Files:**
- Create: `tests/fixtures/pipelines/smoke_native_sockets.yaml`

**Step 1: Build everything**

Run: `cd /home/netf/rapidbyte && just build && just build-connectors`
Expected: All binaries built successfully.

**Step 2: Start a test Postgres**

Run: `docker run -d --name rb-smoke-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=smoke_test -p 15432:5432 postgres:16-alpine`
Wait for ready: `until docker exec rb-smoke-pg pg_isready -U postgres; do sleep 1; done`

**Step 3: Seed test data**

Run:
```bash
docker exec rb-smoke-pg psql -U postgres -d smoke_test -c "
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO test_users (name) VALUES ('alice'), ('bob'), ('charlie');
"
```

**Step 4: Create a smoke test pipeline**

Create `tests/fixtures/pipelines/smoke_native_sockets.yaml`:
```yaml
version: "1.0"
pipeline: smoke_native_sockets

source:
  use: source-postgres
  config:
    host: "127.0.0.1"
    port: 15432
    user: postgres
    password: postgres
    database: smoke_test
  streams:
    - name: test_users
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: "127.0.0.1"
    port: 15432
    user: postgres
    password: postgres
    database: smoke_test
    schema: dest_raw
  write_mode: append

state:
  backend: sqlite
```

**Step 5: Run the pipeline**

Run: `cd /home/netf/rapidbyte && cargo run -- run tests/fixtures/pipelines/smoke_native_sockets.yaml`
Expected: Pipeline completes, 3 records read and written.

**Step 6: Verify data arrived**

Run:
```bash
docker exec rb-smoke-pg psql -U postgres -d smoke_test -c "SELECT count(*) FROM dest_raw.test_users;"
```
Expected: count = 3

**Step 7: Cleanup**

Run: `docker rm -f rb-smoke-pg`

**Step 8: Commit**

```bash
git add tests/fixtures/pipelines/smoke_native_sockets.yaml
git commit -m "test: add smoke test pipeline for native WASI P2 sockets"
```

---

## Task 12: Update protocol documentation

**Files:**
- Modify: `docs/PROTOCOL.md`

**Step 1: Update the security and permissions section**

Find the section about `connect-tcp` and host-proxied sockets in `docs/PROTOCOL.md` and update it to reflect the new architecture:

Replace references to:
- "Direct WASI network usage is disabled; connectors use host-proxied TCP via HostTcpStream"
- "Host enforces ACL on connect-tcp"

With:
- "Connectors use native WASI P2 sockets (std::net::TcpStream) for network I/O"
- "Host enforces network ACL via Wasmtime's socket_addr_check callback"
- "The socket_addr_check validates destination IP addresses against the resolved ACL derived from manifest permissions and pipeline-level allowed_hosts"

**Step 2: Remove references to connect-tcp, socket-read, socket-write from the host imports section**

If the protocol doc lists the host interface functions, remove the socket functions.

**Step 3: Commit**

```bash
git add docs/PROTOCOL.md
git commit -m "docs: update PROTOCOL.md for native WASI P2 socket architecture"
```

---

## Task 13: Run full test suite

**Files:** None (verification only)

**Step 1: Run workspace unit tests**

Run: `cd /home/netf/rapidbyte && cargo test --workspace`
Expected: All pass.

**Step 2: Run clippy**

Run: `cd /home/netf/rapidbyte && cargo clippy --workspace -- -D warnings`
Expected: No warnings.

**Step 3: Run e2e tests**

Run: `cd /home/netf/rapidbyte && just e2e`
Expected: All e2e scenarios pass.

**Step 4: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "fix: resolve issues found during full test suite"
```

---

## Task 14: Cleanup — remove backup and dead code

**Files:**
- Delete: `wit/rapidbyte-connector.wit.bak` (if created in Task 1)

**Step 1: Remove any remaining dead code**

Search for any remaining references to the old socket system:

Run: `grep -r "connect_tcp\|socket_read\|socket_write\|socket_close\|SocketManager\|SocketEntry\|SocketReadResult\|SocketWriteResult\|SOCKET_POLL" --include="*.rs" crates/ connectors/`

If any references remain, remove them.

**Step 2: Remove backup file**

Run: `rm -f wit/rapidbyte-connector.wit.bak`

**Step 3: Final commit**

```bash
git add -A
git commit -m "chore: remove dead socket proxy code and backup files"
```

---

## Files Changed Summary

### Deleted
- `crates/rapidbyte-runtime/src/socket.rs` — entire module (186 lines)

### Modified
- `wit/rapidbyte-connector.wit` — remove 4 functions + 2 variant types (~15 lines removed)
- `crates/rapidbyte-runtime/src/acl.rs` — add `ResolvedAcl` type + `resolve_to_ip_acl` method (~80 lines added)
- `crates/rapidbyte-runtime/src/sandbox.rs` — enable TCP, add `socket_addr_check`, new signature (~15 lines changed)
- `crates/rapidbyte-runtime/src/host_state.rs` — remove `SocketManager` + 4 `_impl` methods (~200 lines removed)
- `crates/rapidbyte-runtime/src/bindings.rs` — remove socket Host trait methods (~50 lines removed)
- `crates/rapidbyte-runtime/src/lib.rs` — remove `pub mod socket;` (1 line)
- `crates/rapidbyte-sdk/src/host_ffi.rs` — remove socket FFI types/functions (~120 lines removed)
- `crates/rapidbyte-sdk/src/host_tcp.rs` — rewrite: FFI-based → native std::net (~90 lines, net reduction)
- `docs/PROTOCOL.md` — update security/permissions section

### Created
- `tests/fixtures/pipelines/smoke_native_sockets.yaml` — smoke test pipeline

### Net impact: ~400 lines of custom socket proxying code removed.
