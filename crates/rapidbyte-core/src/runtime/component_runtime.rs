use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rapidbyte_sdk::errors::{
    BackoffClass, CommitState, ConnectorError, ErrorCategory, ErrorScope, ValidationResult,
    ValidationStatus,
};
use rapidbyte_sdk::manifest::Permissions;
use rapidbyte_sdk::protocol::{Checkpoint, StateScope};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::state::backend::{CursorState, RunStats, StateBackend};

use super::compression::CompressionCodec;
use super::host_socket::{
    resolve_socket_addrs, SocketEntry, SocketReadResultInternal, SocketWriteResultInternal,
    SOCKET_POLL_ACTIVATION_THRESHOLD,
};
#[cfg(unix)]
use super::host_socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};
use super::connector_resolve::sha256_hex;
use super::network_acl::{derive_network_acl, NetworkAcl};

pub use super::connector_resolve::{
    load_connector_manifest, manifest_path_from_wasm, parse_connector_ref,
    resolve_connector_path, verify_wasm_checksum,
};

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
    sockets: HashMap<u64, SocketEntry>,
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
        self.sockets.insert(handle, SocketEntry {
            stream,
            read_would_block_streak: 0,
            write_would_block_streak: 0,
        });

        Ok(handle)
    }

    fn socket_read_impl(
        &mut self,
        handle: u64,
        len: u64,
    ) -> Result<SocketReadResultInternal, ConnectorError> {
        let entry = self
            .sockets
            .get_mut(&handle)
            .ok_or_else(|| ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

        let read_len = len.min(64 * 1024).max(1) as usize;
        let mut buf = vec![0u8; read_len];
        match entry.stream.read(&mut buf) {
            Ok(0) => {
                entry.read_would_block_streak = 0;
                Ok(SocketReadResultInternal::Eof)
            }
            Ok(n) => {
                entry.read_would_block_streak = 0;
                buf.truncate(n);
                Ok(SocketReadResultInternal::Data(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                entry.read_would_block_streak += 1;

                // Below threshold: return WouldBlock immediately (near-zero overhead).
                // This avoids adding 1ms poll latency to transient WouldBlocks during
                // active streaming where the next packet arrives in microseconds.
                if entry.read_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketReadResultInternal::WouldBlock);
                }

                // Above threshold: socket appears idle. Poll to avoid busy-looping.
                #[cfg(unix)]
                let ready = match wait_socket_ready(&entry.stream, SocketInterest::Read, socket_poll_timeout_ms()) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_read");
                        true // Let the next read() surface the real error
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.read(&mut buf) {
                        Ok(0) => {
                            entry.read_would_block_streak = 0;
                            Ok(SocketReadResultInternal::Eof)
                        }
                        Ok(n) => {
                            entry.read_would_block_streak = 0;
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
        let entry = self
            .sockets
            .get_mut(&handle)
            .ok_or_else(|| ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

        match entry.stream.write(&data) {
            Ok(n) => {
                entry.write_would_block_streak = 0;
                Ok(SocketWriteResultInternal::Written(n as u64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                entry.write_would_block_streak += 1;

                // Below threshold: return WouldBlock immediately (near-zero overhead).
                if entry.write_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketWriteResultInternal::WouldBlock);
                }

                // Above threshold: socket appears idle. Poll to avoid busy-looping.
                #[cfg(unix)]
                let ready = match wait_socket_ready(&entry.stream, SocketInterest::Write, socket_poll_timeout_ms()) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_write");
                        true // Let the next write() surface the real error
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.write(&data) {
                        Ok(n) => {
                            entry.write_would_block_streak = 0;
                            Ok(SocketWriteResultInternal::Written(n as u64))
                        }
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




