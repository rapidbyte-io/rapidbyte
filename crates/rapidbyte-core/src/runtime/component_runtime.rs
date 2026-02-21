use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use anyhow::{Context, Result};
use chrono::Utc;
use rapidbyte_sdk::errors::{
    BackoffClass, CommitState, ConnectorError, ErrorCategory, ErrorScope, ValidationResult,
    ValidationStatus,
};
use rapidbyte_sdk::manifest::{ConnectorManifest, Permissions};
use rapidbyte_sdk::protocol::{Checkpoint, StateScope};
use sha2::{Digest, Sha256};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::state::backend::{CursorState, RunStats, StateBackend};

use super::compression::CompressionCodec;

const RAPIDBYTE_WASMTIME_AOT_ENV: &str = "RAPIDBYTE_WASMTIME_AOT";
const RAPIDBYTE_WASMTIME_AOT_DIR_ENV: &str = "RAPIDBYTE_WASMTIME_AOT_DIR";

pub mod source_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-source",
    });
}

pub mod dest_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-destination",
    });
}

pub mod transform_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-transform",
    });
}

/// Channel frame type for batch routing between connector stages.
pub enum Frame {
    /// IPC-encoded Arrow RecordBatch.
    Data(Vec<u8>),
    /// End-of-stream marker for per-stream boundaries.
    EndStream,
}

/// Cumulative timing counters for host function calls.
#[derive(Debug, Clone, Default)]
pub struct HostTimings {
    pub emit_batch_nanos: u64,
    pub next_batch_nanos: u64,
    pub compress_nanos: u64,
    pub decompress_nanos: u64,
    pub emit_batch_count: u64,
    pub next_batch_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct NetworkAcl {
    allow_all: bool,
    exact_hosts: HashSet<String>,
    suffix_wildcards: Vec<String>,
}

impl NetworkAcl {
    fn allow_all() -> Self {
        Self {
            allow_all: true,
            ..Self::default()
        }
    }

    fn add_host(&mut self, raw: &str) {
        let host = normalize_host(raw);
        if host.is_empty() {
            return;
        }
        if host == "*" {
            self.allow_all = true;
            return;
        }
        if let Some(suffix) = host.strip_prefix("*.") {
            self.suffix_wildcards.push(format!(".{}", suffix));
            return;
        }
        self.exact_hosts.insert(host);
    }

    fn allows(&self, host: &str) -> bool {
        if self.allow_all {
            return true;
        }

        let normalized = normalize_host(host);
        if normalized.is_empty() {
            return false;
        }

        if self.exact_hosts.contains(&normalized) {
            return true;
        }

        self.suffix_wildcards
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
    }
}

fn normalize_host(host: &str) -> String {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Bracketed IPv6 literals: [::1] or [::1]:5432
    if let Some(stripped) = trimmed.strip_prefix('[') {
        if let Some((inside, _)) = stripped.split_once(']') {
            return inside.trim().to_ascii_lowercase();
        }
    }

    // Preserve raw IPv4/IPv6 literals as-is (normalized case), including IPv6.
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return ip.to_string().to_ascii_lowercase();
    }

    // Only treat `host:port` as having a port separator when there is exactly one colon.
    // Multi-colon forms are likely IPv6 literals and must not be truncated.
    if trimmed.matches(':').count() == 1 {
        if let Some((left, right)) = trimmed.rsplit_once(':') {
            if right.chars().all(|c| c.is_ascii_digit()) {
                return left.trim().to_ascii_lowercase();
            }
        }
    }

    trimmed.to_ascii_lowercase()
}

fn extract_host_from_url(raw: &str) -> Option<String> {
    let (_, rest) = raw.split_once("://")?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let authority = authority.rsplit('@').next().unwrap_or(authority);

    if authority.starts_with('[') {
        let end = authority.find(']')?;
        return Some(normalize_host(&authority[1..end]));
    }

    Some(normalize_host(
        authority.split(':').next().unwrap_or(authority),
    ))
}

fn collect_runtime_hosts(value: &serde_json::Value, hosts: &mut HashSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map {
                let key_lower = key.to_ascii_lowercase();
                if let serde_json::Value::String(s) = v {
                    if matches!(
                        key_lower.as_str(),
                        "host" | "hostname" | "server" | "address"
                    ) {
                        hosts.insert(normalize_host(s));
                    } else if key_lower.contains("url") {
                        if let Some(host) = extract_host_from_url(s) {
                            hosts.insert(host);
                        }
                    }
                }
                collect_runtime_hosts(v, hosts);
            }
        }
        serde_json::Value::Array(list) => {
            for item in list {
                collect_runtime_hosts(item, hosts);
            }
        }
        _ => {}
    }
}

pub fn derive_network_acl(
    permissions: Option<&Permissions>,
    config: &serde_json::Value,
) -> NetworkAcl {
    let Some(perms) = permissions else {
        // Backwards compatibility for connectors without manifests.
        return NetworkAcl::allow_all();
    };

    let mut acl = NetworkAcl::default();

    if let Some(domains) = &perms.network.allowed_domains {
        for domain in domains {
            acl.add_host(domain);
        }
    }

    if perms.network.allow_runtime_config_domains {
        let mut dynamic_hosts = HashSet::new();
        collect_runtime_hosts(config, &mut dynamic_hosts);
        for host in dynamic_hosts {
            acl.add_host(&host);
        }
    }

    acl
}

fn build_wasi_ctx(permissions: Option<&Permissions>) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.allow_blocking_current_thread(true);

    // WASI-level network is disabled; connectors must use host `connect-tcp`.
    builder.allow_tcp(false);
    builder.allow_udp(false);
    builder.allow_ip_name_lookup(false);

    if let Some(perms) = permissions {
        for var in &perms.env.allowed_vars {
            if let Ok(value) = std::env::var(var) {
                builder.env(var, &value);
            }
        }

        for dir in &perms.fs.preopens {
            let path = Path::new(dir);
            if !path.exists() {
                tracing::warn!(
                    path = dir,
                    "Declared preopen path does not exist on host, skipping"
                );
                continue;
            }

            builder
                .preopened_dir(path, dir, DirPerms::all(), FilePerms::all())
                .with_context(|| format!("failed to preopen directory '{}'", dir))?;
        }
    }

    Ok(builder.build())
}

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

/// Shared state passed to component host imports.
pub struct ComponentHostState {
    pub pipeline_name: String,
    pub connector_id: String,
    pub current_stream: Arc<Mutex<String>>,
    pub state_backend: Arc<dyn StateBackend>,
    pub stats: Arc<Mutex<RunStats>>,

    pub batch_sender: Option<mpsc::SyncSender<Frame>>,
    pub next_batch_id: u64,

    pub batch_receiver: Option<mpsc::Receiver<Frame>>,

    pub compression: Option<CompressionCodec>,

    pub source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub timings: Arc<Mutex<HostTimings>>,

    network_acl: NetworkAcl,
    sockets: HashMap<u64, TcpStream>,
    next_socket_handle: u64,

    ctx: WasiCtx,
    table: ResourceTable,
}

impl ComponentHostState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_name: String,
        connector_id: String,
        current_stream: Arc<Mutex<String>>,
        state_backend: Arc<dyn StateBackend>,
        stats: Arc<Mutex<RunStats>>,
        batch_sender: Option<mpsc::SyncSender<Frame>>,
        batch_receiver: Option<mpsc::Receiver<Frame>>,
        compression: Option<CompressionCodec>,
        source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        timings: Arc<Mutex<HostTimings>>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
    ) -> Result<Self> {
        Ok(Self {
            pipeline_name,
            connector_id,
            current_stream,
            state_backend,
            stats,
            batch_sender,
            next_batch_id: 1,
            batch_receiver,
            compression,
            source_checkpoints,
            dest_checkpoints,
            timings,
            network_acl: derive_network_acl(permissions, config),
            sockets: HashMap::new(),
            next_socket_handle: 1,
            ctx: build_wasi_ctx(permissions)?,
            table: ResourceTable::new(),
        })
    }

    fn current_stream(&self) -> String {
        self.current_stream.lock().unwrap().clone()
    }

    fn emit_batch_impl(&mut self, batch: Vec<u8>) -> Result<(), ConnectorError> {
        if batch.is_empty() {
            return Err(ConnectorError::internal(
                "EMPTY_BATCH",
                "Connector emitted a zero-length batch; this is a protocol violation",
            ));
        }

        let fn_start = Instant::now();

        let compress_start = Instant::now();
        let batch = if let Some(codec) = self.compression {
            super::compression::compress(codec, &batch)
        } else {
            batch
        };
        let compress_elapsed_nanos = if self.compression.is_some() {
            compress_start.elapsed().as_nanos() as u64
        } else {
            0
        };

        let sender = self
            .batch_sender
            .as_ref()
            .ok_or_else(|| ConnectorError::internal("NO_SENDER", "No batch sender configured"))?;

        sender
            .send(Frame::Data(batch))
            .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;

        self.next_batch_id += 1;

        let mut t = self.timings.lock().unwrap();
        t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
        t.emit_batch_count += 1;
        t.compress_nanos += compress_elapsed_nanos;

        Ok(())
    }

    fn next_batch_impl(&mut self) -> Result<Option<Vec<u8>>, ConnectorError> {
        let fn_start = Instant::now();

        let receiver = self.batch_receiver.as_ref().ok_or_else(|| {
            ConnectorError::internal("NO_RECEIVER", "No batch receiver configured")
        })?;

        let frame = match receiver.recv() {
            Ok(frame) => frame,
            Err(_) => return Ok(None),
        };

        let batch = match frame {
            Frame::Data(batch) => batch,
            Frame::EndStream => return Ok(None),
        };

        let decompress_start = Instant::now();
        let batch = if let Some(codec) = self.compression {
            super::compression::decompress(codec, &batch)
        } else {
            batch
        };
        let decompress_elapsed_nanos = if self.compression.is_some() {
            decompress_start.elapsed().as_nanos() as u64
        } else {
            0
        };

        let mut t = self.timings.lock().unwrap();
        t.next_batch_nanos += fn_start.elapsed().as_nanos() as u64;
        t.next_batch_count += 1;
        t.decompress_nanos += decompress_elapsed_nanos;

        Ok(Some(batch))
    }

    fn scoped_state_key(&self, scope: StateScope, key: &str) -> String {
        match scope {
            StateScope::Pipeline => key.to_string(),
            StateScope::Stream => format!("{}:{}", self.current_stream(), key),
            StateScope::ConnectorInstance => format!("{}:{}", self.connector_id, key),
        }
    }

    fn state_get_impl(
        &mut self,
        scope: u32,
        key: String,
    ) -> Result<Option<String>, ConnectorError> {
        if key.len() > 1024 {
            return Err(ConnectorError::config(
                "KEY_TOO_LONG",
                format!("Key length {} exceeds 1024", key.len()),
            ));
        }

        let scope = StateScope::from_i32(scope as i32).ok_or_else(|| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);

        self.state_backend
            .get_cursor(&self.pipeline_name, &scoped_key)
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
            .map(|opt| opt.and_then(|cursor| cursor.cursor_value))
    }

    fn state_put_impl(
        &mut self,
        scope: u32,
        key: String,
        value: String,
    ) -> Result<(), ConnectorError> {
        if key.len() > 1024 {
            return Err(ConnectorError::config(
                "KEY_TOO_LONG",
                format!("Key length {} exceeds 1024", key.len()),
            ));
        }

        let scope = StateScope::from_i32(scope as i32).ok_or_else(|| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);
        let cursor = CursorState {
            cursor_field: Some(key),
            cursor_value: Some(value),
            updated_at: Utc::now(),
        };

        self.state_backend
            .set_cursor(&self.pipeline_name, &scoped_key, &cursor)
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
    }

    fn state_cas_impl(
        &mut self,
        scope: u32,
        key: String,
        expected: Option<String>,
        new_value: String,
    ) -> Result<bool, ConnectorError> {
        if key.len() > 1024 {
            return Err(ConnectorError::config(
                "KEY_TOO_LONG",
                format!("Key length {} exceeds 1024", key.len()),
            ));
        }

        let scope = StateScope::from_i32(scope as i32).ok_or_else(|| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);

        self.state_backend
            .compare_and_set(
                &self.pipeline_name,
                &scoped_key,
                expected.as_deref(),
                &new_value,
            )
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
    }

    fn checkpoint_impl(&mut self, kind: u32, payload_json: String) -> Result<(), ConnectorError> {
        let envelope: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| ConnectorError::internal("PARSE_CHECKPOINT", e.to_string()))?;

        let current_stream = self.current_stream();

        tracing::debug!(
            pipeline = self.pipeline_name,
            stream = %current_stream,
            "Received checkpoint: {}",
            serde_json::to_string(&envelope).unwrap_or_default()
        );

        match kind {
            0 => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(
                    envelope.get("payload").cloned().unwrap_or(envelope.clone()),
                ) {
                    self.source_checkpoints.lock().unwrap().push(cp);
                }
            }
            1 => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(
                    envelope.get("payload").cloned().unwrap_or(envelope.clone()),
                ) {
                    self.dest_checkpoints.lock().unwrap().push(cp);
                }
            }
            2 => {
                tracing::debug!(
                    pipeline = self.pipeline_name,
                    stream = %current_stream,
                    "Received transform checkpoint"
                );
            }
            _ => {
                return Err(ConnectorError::config(
                    "INVALID_CHECKPOINT_KIND",
                    format!("Invalid checkpoint kind: {}", kind),
                ));
            }
        }

        Ok(())
    }

    fn metric_impl(&mut self, payload_json: String) -> Result<(), ConnectorError> {
        let metric: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| ConnectorError::internal("PARSE_METRIC", e.to_string()))?;

        tracing::debug!(
            pipeline = self.pipeline_name,
            stream = %self.current_stream(),
            "Received metric: {}",
            serde_json::to_string(&metric).unwrap_or_default()
        );

        Ok(())
    }

    fn log_impl(&mut self, level: u32, msg: String) {
        let pipeline = &self.pipeline_name;
        let stream = self.current_stream();

        match level {
            0 => tracing::error!(pipeline, stream = %stream, "[connector] {}", msg),
            1 => tracing::warn!(pipeline, stream = %stream, "[connector] {}", msg),
            2 => tracing::info!(pipeline, stream = %stream, "[connector] {}", msg),
            3 => tracing::debug!(pipeline, stream = %stream, "[connector] {}", msg),
            _ => tracing::trace!(pipeline, stream = %stream, "[connector] {}", msg),
        }
    }

    fn connect_tcp_impl(&mut self, host: String, port: u16) -> Result<u64, ConnectorError> {
        if !self.network_acl.allows(&host) {
            return Err(ConnectorError::permission(
                "NETWORK_DENIED",
                format!("Host '{}' is not allowed by connector permissions", host),
            ));
        }

        let addrs = resolve_socket_addrs(&host, port).map_err(|e| {
            ConnectorError::transient_network("DNS_RESOLUTION_FAILED", e.to_string())
        })?;

        let mut last_error: Option<(SocketAddr, std::io::Error)> = None;
        let mut connected: Option<TcpStream> = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, Duration::from_secs(10)) {
                Ok(stream) => {
                    connected = Some(stream);
                    break;
                }
                Err(err) => {
                    last_error = Some((addr, err));
                }
            }
        }

        let stream = connected.ok_or_else(|| {
            let details = last_error
                .map(|(addr, err)| format!("last attempt {} failed: {}", addr, err))
                .unwrap_or_else(|| "no resolved addresses available".to_string());
            ConnectorError::transient_network(
                "TCP_CONNECT_FAILED",
                format!("{}:{} ({})", host, port, details),
            )
        })?;

        stream
            .set_nonblocking(true)
            .map_err(|e| ConnectorError::internal("SOCKET_CONFIG", e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| ConnectorError::internal("SOCKET_CONFIG", e.to_string()))?;

        let handle = self.next_socket_handle;
        self.next_socket_handle = self.next_socket_handle.saturating_add(1);
        self.sockets.insert(handle, stream);

        Ok(handle)
    }

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

    fn socket_close_impl(&mut self, handle: u64) {
        self.sockets.remove(&handle);
    }
}

impl WasiView for ComponentHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

fn resolve_socket_addrs(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addrs.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "No address",
        ))
    } else {
        Ok(addrs)
    }
}

enum SocketReadResultInternal {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

enum SocketWriteResultInternal {
    Written(u64),
    WouldBlock,
}

macro_rules! define_error_converters {
    ($to_world_fn:ident, $from_world_fn:ident, $module:ident) => {
        fn $to_world_fn(
            error: ConnectorError,
        ) -> $module::rapidbyte::connector::types::ConnectorError {
            use $module::rapidbyte::connector::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                ConnectorError as CConnectorError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };

            CConnectorError {
                category: match error.category {
                    ErrorCategory::Config => CErrorCategory::Config,
                    ErrorCategory::Auth => CErrorCategory::Auth,
                    ErrorCategory::Permission => CErrorCategory::Permission,
                    ErrorCategory::RateLimit => CErrorCategory::RateLimit,
                    ErrorCategory::TransientNetwork => CErrorCategory::TransientNetwork,
                    ErrorCategory::TransientDb => CErrorCategory::TransientDb,
                    ErrorCategory::Data => CErrorCategory::Data,
                    ErrorCategory::Schema => CErrorCategory::Schema,
                    ErrorCategory::Internal => CErrorCategory::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => CErrorScope::PerStream,
                    ErrorScope::Batch => CErrorScope::PerBatch,
                    ErrorScope::Record => CErrorScope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBackoffClass::Fast,
                    BackoffClass::Normal => CBackoffClass::Normal,
                    BackoffClass::Slow => CBackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    CommitState::BeforeCommit => CCommitState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CCommitState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CCommitState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        pub fn $from_world_fn(
            error: $module::rapidbyte::connector::types::ConnectorError,
        ) -> ConnectorError {
            ConnectorError {
                category: match error.category {
                    $module::rapidbyte::connector::types::ErrorCategory::Config => {
                        ErrorCategory::Config
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Auth => {
                        ErrorCategory::Auth
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Permission => {
                        ErrorCategory::Permission
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::RateLimit => {
                        ErrorCategory::RateLimit
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::TransientNetwork => {
                        ErrorCategory::TransientNetwork
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::TransientDb => {
                        ErrorCategory::TransientDb
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Data => {
                        ErrorCategory::Data
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Schema => {
                        ErrorCategory::Schema
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Internal => {
                        ErrorCategory::Internal
                    }
                },
                scope: match error.scope {
                    $module::rapidbyte::connector::types::ErrorScope::PerStream => {
                        ErrorScope::Stream
                    }
                    $module::rapidbyte::connector::types::ErrorScope::PerBatch => ErrorScope::Batch,
                    $module::rapidbyte::connector::types::ErrorScope::PerRecord => {
                        ErrorScope::Record
                    }
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    $module::rapidbyte::connector::types::BackoffClass::Fast => BackoffClass::Fast,
                    $module::rapidbyte::connector::types::BackoffClass::Normal => {
                        BackoffClass::Normal
                    }
                    $module::rapidbyte::connector::types::BackoffClass::Slow => BackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    $module::rapidbyte::connector::types::CommitState::BeforeCommit => {
                        CommitState::BeforeCommit
                    }
                    $module::rapidbyte::connector::types::CommitState::AfterCommitUnknown => {
                        CommitState::AfterCommitUnknown
                    }
                    $module::rapidbyte::connector::types::CommitState::AfterCommitConfirmed => {
                        CommitState::AfterCommitConfirmed
                    }
                }),
                details: error
                    .details_json
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
            }
        }
    };
}

define_error_converters!(to_source_error, source_error_to_sdk, source_bindings);
define_error_converters!(to_dest_error, dest_error_to_sdk, dest_bindings);
define_error_converters!(
    to_transform_error,
    transform_error_to_sdk,
    transform_bindings
);

macro_rules! impl_host_trait_for_world {
    ($module:ident, $to_world_error:ident) => {
        impl $module::rapidbyte::connector::host::Host for ComponentHostState {
            fn emit_batch(
                &mut self,
                batch: Vec<u8>,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.emit_batch_impl(batch).map_err($to_world_error)
            }

            fn next_batch(
                &mut self,
            ) -> std::result::Result<
                Option<Vec<u8>>,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.next_batch_impl().map_err($to_world_error)
            }

            fn log(&mut self, level: u32, msg: String) {
                self.log_impl(level, msg);
            }

            fn checkpoint(
                &mut self,
                kind: u32,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.checkpoint_impl(kind, payload_json)
                    .map_err($to_world_error)
            }

            fn metric(
                &mut self,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.metric_impl(payload_json).map_err($to_world_error)
            }

            fn state_get(
                &mut self,
                scope: u32,
                key: String,
            ) -> std::result::Result<
                Option<String>,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.state_get_impl(scope, key).map_err($to_world_error)
            }

            fn state_put(
                &mut self,
                scope: u32,
                key: String,
                val: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.state_put_impl(scope, key, val)
                    .map_err($to_world_error)
            }

            fn state_cas(
                &mut self,
                scope: u32,
                key: String,
                expected: Option<String>,
                new_val: String,
            ) -> std::result::Result<bool, $module::rapidbyte::connector::types::ConnectorError>
            {
                self.state_cas_impl(scope, key, expected, new_val)
                    .map_err($to_world_error)
            }

            fn connect_tcp(
                &mut self,
                host: String,
                port: u16,
            ) -> std::result::Result<u64, $module::rapidbyte::connector::types::ConnectorError>
            {
                self.connect_tcp_impl(host, port).map_err($to_world_error)
            }

            fn socket_read(
                &mut self,
                handle: u64,
                len: u64,
            ) -> std::result::Result<
                $module::rapidbyte::connector::types::SocketReadResult,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.socket_read_impl(handle, len)
                    .map(|result| match result {
                        SocketReadResultInternal::Data(data) => {
                            $module::rapidbyte::connector::types::SocketReadResult::Data(data)
                        }
                        SocketReadResultInternal::Eof => {
                            $module::rapidbyte::connector::types::SocketReadResult::Eof
                        }
                        SocketReadResultInternal::WouldBlock => {
                            $module::rapidbyte::connector::types::SocketReadResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_write(
                &mut self,
                handle: u64,
                data: Vec<u8>,
            ) -> std::result::Result<
                $module::rapidbyte::connector::types::SocketWriteResult,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.socket_write_impl(handle, data)
                    .map(|result| match result {
                        SocketWriteResultInternal::Written(n) => {
                            $module::rapidbyte::connector::types::SocketWriteResult::Written(n)
                        }
                        SocketWriteResultInternal::WouldBlock => {
                            $module::rapidbyte::connector::types::SocketWriteResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_close(&mut self, handle: u64) {
                self.socket_close_impl(handle)
            }
        }
    };
}

impl_host_trait_for_world!(source_bindings, to_source_error);
impl_host_trait_for_world!(dest_bindings, to_dest_error);
impl_host_trait_for_world!(transform_bindings, to_transform_error);

impl source_bindings::rapidbyte::connector::types::Host for ComponentHostState {}
impl dest_bindings::rapidbyte::connector::types::Host for ComponentHostState {}
impl transform_bindings::rapidbyte::connector::types::Host for ComponentHostState {}

pub fn source_validation_to_sdk(
    value: source_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            source_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            source_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            source_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}

pub fn dest_validation_to_sdk(
    value: dest_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}

pub fn transform_validation_to_sdk(
    value: transform_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}

#[derive(Clone)]
pub struct LoadedComponent {
    pub engine: Arc<Engine>,
    pub component: Arc<Component>,
}

/// Manages loading connector components.
pub struct WasmRuntime {
    engine: Arc<Engine>,
    aot_cache_dir: Option<PathBuf>,
    aot_compat_hash: u64,
}

impl WasmRuntime {
    pub fn new() -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);

        let engine = Engine::new(&config).context("Failed to initialize Wasmtime engine")?;
        let mut hasher = DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        let aot_compat_hash = hasher.finish();

        let aot_cache_dir = if env_flag_enabled(RAPIDBYTE_WASMTIME_AOT_ENV, true) {
            let dir = resolve_aot_cache_dir();
            match std::fs::create_dir_all(&dir) {
                Ok(()) => {
                    tracing::debug!(dir = %dir.display(), "Wasmtime AOT cache enabled");
                    Some(dir)
                }
                Err(err) => {
                    tracing::warn!(
                        dir = %dir.display(),
                        error = %err,
                        "Failed to create Wasmtime AOT cache directory; falling back to JIT"
                    );
                    None
                }
            }
        } else {
            tracing::debug!("Wasmtime AOT disabled via RAPIDBYTE_WASMTIME_AOT=0");
            None
        };

        Ok(Self {
            engine: Arc::new(engine),
            aot_cache_dir,
            aot_compat_hash,
        })
    }

    /// Load a component from a file on disk.
    pub fn load_module(&self, wasm_path: &Path) -> Result<LoadedComponent> {
        let component = if self.aot_cache_dir.is_some() {
            match self.load_module_aot(wasm_path) {
                Ok(component) => component,
                Err(err) => {
                    tracing::warn!(
                        path = %wasm_path.display(),
                        error = %err,
                        "AOT load failed; falling back to direct Wasm load"
                    );
                    Component::from_file(&self.engine, wasm_path).with_context(|| {
                        format!("Failed to load Wasm component: {}", wasm_path.display())
                    })?
                }
            }
        } else {
            Component::from_file(&self.engine, wasm_path).with_context(|| {
                format!("Failed to load Wasm component: {}", wasm_path.display())
            })?
        };

        Ok(LoadedComponent {
            engine: self.engine.clone(),
            component: Arc::new(component),
        })
    }

    pub fn source_linker(&self) -> Result<Linker<ComponentHostState>> {
        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .context("Failed to add WASI imports to linker")?;
        source_bindings::RapidbyteSource::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
            &mut linker,
            |state| state,
        )
        .context("Failed to add rapidbyte source imports to linker")?;
        Ok(linker)
    }

    pub fn dest_linker(&self) -> Result<Linker<ComponentHostState>> {
        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .context("Failed to add WASI imports to linker")?;
        dest_bindings::RapidbyteDestination::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
            &mut linker,
            |state| state,
        )
        .context("Failed to add rapidbyte destination imports to linker")?;
        Ok(linker)
    }

    pub fn transform_linker(&self) -> Result<Linker<ComponentHostState>> {
        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .context("Failed to add WASI imports to linker")?;
        transform_bindings::RapidbyteTransform::add_to_linker::<
            _,
            wasmtime::component::HasSelf<_>,
        >(&mut linker, |state| state)
        .context("Failed to add rapidbyte transform imports to linker")?;
        Ok(linker)
    }

    pub fn new_store(&self, host_state: ComponentHostState) -> Store<ComponentHostState> {
        Store::new(&self.engine, host_state)
    }

    fn load_module_aot(&self, wasm_path: &Path) -> Result<Component> {
        let cache_dir = self
            .aot_cache_dir
            .as_ref()
            .context("AOT cache directory is not configured")?;

        let wasm_bytes = std::fs::read(wasm_path)
            .with_context(|| format!("Failed to read Wasm component: {}", wasm_path.display()))?;
        let wasm_hash = sha256_hex(&wasm_bytes);
        let artifact_path = self.aot_artifact_path(cache_dir, wasm_path, &wasm_hash);

        if artifact_path.exists() {
            // SAFETY: this artifact is generated by Wasmtime's `precompile_component` and
            // scoped to the current engine compatibility hash in its filename. If loading
            // fails (e.g. corruption), we remove it and rebuild.
            match unsafe { Component::deserialize_file(&self.engine, &artifact_path) } {
                Ok(component) => {
                    tracing::debug!(
                        path = %wasm_path.display(),
                        artifact = %artifact_path.display(),
                        "Loaded connector from Wasmtime AOT artifact"
                    );
                    return Ok(component);
                }
                Err(err) => {
                    tracing::warn!(
                        artifact = %artifact_path.display(),
                        error = %err,
                        "Failed to load cached Wasmtime AOT artifact; recompiling"
                    );
                    let _ = std::fs::remove_file(&artifact_path);
                }
            }
        }

        let precompiled = self
            .engine
            .precompile_component(&wasm_bytes)
            .with_context(|| format!("Failed to AOT-precompile {}", wasm_path.display()))?;

        if let Err(err) = write_file_atomic(&artifact_path, &precompiled) {
            tracing::warn!(
                artifact = %artifact_path.display(),
                error = %err,
                "Failed to persist Wasmtime AOT artifact; continuing with in-memory artifact"
            );
        }

        // SAFETY: these bytes were produced by `Engine::precompile_component` on this exact
        // engine, so deserializing them is trusted and sound.
        let component = unsafe { Component::deserialize(&self.engine, &precompiled) }
            .with_context(|| {
                format!(
                    "Failed to deserialize in-memory Wasmtime AOT artifact for {}",
                    wasm_path.display()
                )
            })?;

        tracing::debug!(
            path = %wasm_path.display(),
            artifact = %artifact_path.display(),
            "Compiled connector into Wasmtime AOT artifact"
        );
        Ok(component)
    }

    fn aot_artifact_path(&self, cache_dir: &Path, wasm_path: &Path, wasm_hash: &str) -> PathBuf {
        let stem = wasm_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("component");
        let stem = sanitize_cache_component_name(stem);
        let short_hash = &wasm_hash[..wasm_hash.len().min(16)];
        cache_dir.join(format!(
            "{}-{}-{:016x}.cwasm",
            stem, short_hash, self.aot_compat_hash
        ))
    }
}

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            other => {
                tracing::warn!(
                    env = name,
                    value = other,
                    "Invalid boolean env value, using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}

fn resolve_aot_cache_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(RAPIDBYTE_WASMTIME_AOT_DIR_ENV) {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".rapidbyte")
            .join("cache")
            .join("wasmtime-aot");
    }

    std::env::temp_dir().join("rapidbyte").join("wasmtime-aot")
}

fn sanitize_cache_component_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "component".to_string()
    } else {
        sanitized
    }
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("Invalid artifact path (no parent): {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create directory: {}", parent.display()))?;

    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("artifact");
    let tmp = parent.join(format!(
        ".{}.{}.{}.tmp",
        filename,
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));

    std::fs::write(&tmp, bytes)
        .with_context(|| format!("Failed to write temp artifact: {}", tmp.display()))?;

    match std::fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(err) => {
            let _ = std::fs::remove_file(&tmp);
            if path.exists() {
                Ok(())
            } else {
                Err(err).with_context(|| {
                    format!("Failed to move artifact into place: {}", path.display())
                })
            }
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Parse a connector reference into (connector_id, connector_version).
///
/// Examples:
/// - "rapidbyte/source-postgres@v0.1.0" -> ("source-postgres", "0.1.0")
/// - "source-postgres@v0.1.0"           -> ("source-postgres", "0.1.0")
/// - "source-postgres"                  -> ("source-postgres", "unknown")
pub fn parse_connector_ref(connector_ref: &str) -> (String, String) {
    let after_slash = connector_ref
        .split('/')
        .next_back()
        .unwrap_or(connector_ref);

    let (name, version) = match after_slash.split_once('@') {
        Some((n, v)) => (n.to_string(), v.strip_prefix('v').unwrap_or(v).to_string()),
        None => (after_slash.to_string(), "unknown".to_string()),
    };

    (name, version)
}

/// Resolve a connector reference (e.g. "rapidbyte/source-postgres@v0.1.0")
/// to a .wasm file path on disk.
///
/// Resolution order:
/// 1. RAPIDBYTE_CONNECTOR_DIR env var
/// 2. ~/.rapidbyte/plugins/
///
/// The connector ref is mapped to a filename by extracting the connector name
/// and replacing hyphens with underscores: "source-postgres" -> "source_postgres.wasm"
pub fn resolve_connector_path(connector_ref: &str) -> Result<std::path::PathBuf> {
    let name = connector_ref
        .split('/')
        .next_back()
        .unwrap_or(connector_ref)
        .split('@')
        .next()
        .unwrap_or(connector_ref);

    let filename = format!("{}.wasm", name.replace('-', "_"));

    // Check RAPIDBYTE_CONNECTOR_DIR first
    if let Ok(dir) = std::env::var("RAPIDBYTE_CONNECTOR_DIR") {
        let path = Path::new(&dir).join(&filename);
        if path.exists() {
            return Ok(path);
        }
    }

    // Fall back to ~/.rapidbyte/plugins/
    if let Ok(home) = std::env::var("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".rapidbyte")
            .join("plugins")
            .join(&filename);
        if path.exists() {
            return Ok(path);
        }
    }

    anyhow::bail!(
        "Connector '{}' not found. Searched for '{}' in RAPIDBYTE_CONNECTOR_DIR and ~/.rapidbyte/plugins/",
        connector_ref,
        filename
    )
}

/// Derive the manifest file path from a WASM binary path.
/// `source_postgres.wasm` -> `source_postgres.manifest.json`
pub fn manifest_path_from_wasm(wasm_path: &Path) -> std::path::PathBuf {
    let stem = wasm_path.file_stem().unwrap_or_default().to_string_lossy();
    wasm_path.with_file_name(format!("{}.manifest.json", stem))
}

/// Verify WASM binary checksum against the manifest-declared value.
pub fn verify_wasm_checksum(wasm_path: &Path, expected: &str) -> Result<bool> {
    // Strip algorithm prefix (e.g., "sha256:abcd..." -> "abcd...")
    let expected_hex = expected.strip_prefix("sha256:").unwrap_or(expected);

    let bytes = std::fs::read(wasm_path)
        .with_context(|| format!("Failed to read WASM binary: {}", wasm_path.display()))?;
    let actual = sha256_hex(&bytes);

    Ok(actual == expected_hex)
}

/// Load a connector manifest from disk. Returns None if the manifest file
/// doesn't exist (backwards-compatible with connectors that don't have one yet).
pub fn load_connector_manifest(wasm_path: &Path) -> Result<Option<ConnectorManifest>> {
    let manifest_path = manifest_path_from_wasm(wasm_path);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;
    let manifest: ConnectorManifest = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse manifest: {}", manifest_path.display()))?;

    if let Some(ref expected) = manifest.artifact.checksum {
        let valid = verify_wasm_checksum(wasm_path, expected)?;
        if !valid {
            anyhow::bail!(
                "WASM checksum mismatch for {}. Expected: {}, file may be corrupted or tampered.",
                wasm_path.display(),
                expected,
            );
        }
        tracing::debug!(path = %wasm_path.display(), "WASM checksum verified");
    }

    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::normalize_host;

    #[test]
    fn normalize_host_preserves_ipv6_literal() {
        assert_eq!(normalize_host("2001:db8::1"), "2001:db8::1");
        assert_eq!(normalize_host("::1"), "::1");
    }

    #[test]
    fn normalize_host_handles_bracketed_ipv6_with_port() {
        assert_eq!(normalize_host("[2001:db8::1]:5432"), "2001:db8::1");
        assert_eq!(normalize_host("[::1]"), "::1");
    }

    #[test]
    fn normalize_host_strips_port_for_single_colon_hosts() {
        assert_eq!(normalize_host("example.com:5432"), "example.com");
        assert_eq!(normalize_host("127.0.0.1:5432"), "127.0.0.1");
    }

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
}
