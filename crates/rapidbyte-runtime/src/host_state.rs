//! Host-side runtime state and host import implementations for connector components.
//!
//! The `_impl` methods mirror the WIT-generated calling convention (owned `String` / `Vec<u8>`),
//! and inner types are consumed by the not-yet-wired bindings module, so many items appear
//! unused at this point in the extraction.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use rapidbyte_types::checkpoint::{Checkpoint, StateScope};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::{ConnectorError, ErrorCategory};
use rapidbyte_types::manifest::Permissions;
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use rapidbyte_state::StateBackend;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};

use crate::acl::{derive_network_acl, NetworkAcl};
use crate::compression::CompressionCodec;
use crate::engine::HasStoreLimits;
use crate::sandbox::{build_store_limits, build_wasi_ctx, SandboxOverrides};
use crate::socket::{
    resolve_socket_addrs, SocketEntry, SocketReadResult, SocketWriteResult,
    SOCKET_POLL_ACTIVATION_THRESHOLD,
};
#[cfg(unix)]
use crate::socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};

const MAX_SOCKET_READ_BYTES: u64 = 64 * 1024;
const CHECKPOINT_KIND_SOURCE: u32 = 0;
const CHECKPOINT_KIND_DEST: u32 = 1;
const CHECKPOINT_KIND_TRANSFORM: u32 = 2;

/// Default maximum DLQ records kept in memory per run.
pub const DEFAULT_DLQ_LIMIT: usize = 10_000;

/// Channel frame type for batch routing between connector stages.
pub enum Frame {
    /// IPC-encoded Arrow `RecordBatch` (optionally compressed).
    Data(Vec<u8>),
    /// End-of-stream marker.
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

// --- Inner types ---

pub(crate) struct ConnectorIdentity {
    pub pipeline: PipelineId,
    pub connector_id: String,
    pub stream: StreamName,
    pub state_backend: Arc<dyn StateBackend>,
}

pub(crate) struct BatchRouter {
    pub sender: Option<mpsc::Sender<Frame>>,
    pub receiver: Option<mpsc::Receiver<Frame>>,
    pub next_batch_id: u64,
    pub compression: Option<CompressionCodec>,
}

pub(crate) struct CheckpointCollector {
    pub source: Arc<Mutex<Vec<Checkpoint>>>,
    pub dest: Arc<Mutex<Vec<Checkpoint>>>,
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub timings: Arc<Mutex<HostTimings>>,
    pub dlq_limit: usize,
}

pub(crate) struct SocketManager {
    pub acl: NetworkAcl,
    pub sockets: HashMap<u64, SocketEntry>,
    pub next_handle: u64,
}

/// Shared state passed to Wasmtime component host imports.
pub struct ComponentHostState {
    pub(crate) identity: ConnectorIdentity,
    pub(crate) batch: BatchRouter,
    pub(crate) checkpoints: CheckpointCollector,
    pub(crate) sockets: SocketManager,
    pub(crate) store_limits: StoreLimits,
    ctx: WasiCtx,
    table: ResourceTable,
}

// --- Builder ---

/// Builder for [`ComponentHostState`].
pub struct HostStateBuilder {
    pipeline: Option<String>,
    connector_id: Option<String>,
    stream: Option<String>,
    state_backend: Option<Arc<dyn StateBackend>>,
    sender: Option<mpsc::Sender<Frame>>,
    receiver: Option<mpsc::Receiver<Frame>>,
    source_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dest_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dlq_records: Option<Arc<Mutex<Vec<DlqRecord>>>>,
    timings: Option<Arc<Mutex<HostTimings>>>,
    permissions: Option<Permissions>,
    config: serde_json::Value,
    compression: Option<CompressionCodec>,
    overrides: Option<SandboxOverrides>,
    dlq_limit: usize,
}

impl HostStateBuilder {
    fn new() -> Self {
        Self {
            pipeline: None,
            connector_id: None,
            stream: None,
            state_backend: None,
            sender: None,
            receiver: None,
            source_checkpoints: None,
            dest_checkpoints: None,
            dlq_records: None,
            timings: None,
            permissions: None,
            config: serde_json::Value::Null,
            compression: None,
            overrides: None,
            dlq_limit: DEFAULT_DLQ_LIMIT,
        }
    }

    #[must_use]
    pub fn pipeline(mut self, name: impl Into<String>) -> Self {
        self.pipeline = Some(name.into());
        self
    }

    #[must_use]
    pub fn connector_id(mut self, id: impl Into<String>) -> Self {
        self.connector_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn stream(mut self, name: impl Into<String>) -> Self {
        self.stream = Some(name.into());
        self
    }

    #[must_use]
    pub fn state_backend(mut self, backend: Arc<dyn StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    #[must_use]
    pub fn sender(mut self, tx: mpsc::Sender<Frame>) -> Self {
        self.sender = Some(tx);
        self
    }

    #[must_use]
    pub fn receiver(mut self, rx: mpsc::Receiver<Frame>) -> Self {
        self.receiver = Some(rx);
        self
    }

    #[must_use]
    pub fn source_checkpoints(mut self, cp: Arc<Mutex<Vec<Checkpoint>>>) -> Self {
        self.source_checkpoints = Some(cp);
        self
    }

    #[must_use]
    pub fn dest_checkpoints(mut self, cp: Arc<Mutex<Vec<Checkpoint>>>) -> Self {
        self.dest_checkpoints = Some(cp);
        self
    }

    #[must_use]
    pub fn dlq_records(mut self, records: Arc<Mutex<Vec<DlqRecord>>>) -> Self {
        self.dlq_records = Some(records);
        self
    }

    #[must_use]
    pub fn timings(mut self, t: Arc<Mutex<HostTimings>>) -> Self {
        self.timings = Some(t);
        self
    }

    #[must_use]
    pub fn permissions(mut self, perms: &Permissions) -> Self {
        self.permissions = Some(perms.clone());
        self
    }

    #[must_use]
    pub fn config(mut self, cfg: &serde_json::Value) -> Self {
        self.config = cfg.clone();
        self
    }

    #[must_use]
    pub fn compression(mut self, codec: Option<CompressionCodec>) -> Self {
        self.compression = codec;
        self
    }

    #[must_use]
    pub fn overrides(mut self, ovr: &SandboxOverrides) -> Self {
        self.overrides = Some(ovr.clone());
        self
    }

    #[must_use]
    pub fn dlq_limit(mut self, limit: usize) -> Self {
        self.dlq_limit = limit;
        self
    }

    /// Build the host state. Fails if required fields are missing or WASI context fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the WASI context cannot be constructed (e.g. invalid preopens).
    ///
    /// # Panics
    ///
    /// Panics if `pipeline`, `connector_id`, `stream`, or `state_backend` were not set.
    pub fn build(self) -> Result<ComponentHostState> {
        let pipeline = self.pipeline.expect("pipeline is required");
        let connector_id = self.connector_id.expect("connector_id is required");
        let stream = self.stream.expect("stream is required");
        let state_backend = self.state_backend.expect("state_backend is required");

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
                timings: self
                    .timings
                    .unwrap_or_else(|| Arc::new(Mutex::new(HostTimings::default()))),
                dlq_limit: self.dlq_limit,
            },
            sockets: SocketManager {
                acl: derive_network_acl(
                    self.permissions.as_ref(),
                    &self.config,
                    self.overrides
                        .as_ref()
                        .and_then(|o| o.allowed_hosts.as_deref()),
                ),
                sockets: HashMap::new(),
                next_handle: 1,
            },
            store_limits: build_store_limits(self.overrides.as_ref()),
            ctx: build_wasi_ctx(self.permissions.as_ref(), self.overrides.as_ref())?,
            table: ResourceTable::new(),
        })
    }
}

impl ComponentHostState {
    /// Create a builder for `ComponentHostState`.
    #[must_use]
    pub fn builder() -> HostStateBuilder {
        HostStateBuilder::new()
    }

    fn current_stream(&self) -> &str {
        self.identity.stream.as_str()
    }

    // ── Host import implementations ─────────────────────────────────

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn emit_batch_impl(&mut self, batch: Vec<u8>) -> Result<(), ConnectorError> {
        if batch.is_empty() {
            return Err(ConnectorError::internal(
                "EMPTY_BATCH",
                "Connector emitted a zero-length batch; this is a protocol violation",
            ));
        }

        let fn_start = Instant::now();

        let compress_start = Instant::now();
        let batch = if let Some(codec) = self.batch.compression {
            crate::compression::compress(codec, &batch)
                .map_err(|e| ConnectorError::internal("COMPRESS_FAILED", e.to_string()))?
        } else {
            batch
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

    #[allow(clippy::cast_possible_truncation)]
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
            batch
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

    fn scoped_state_key(&self, scope: StateScope, key: &str) -> String {
        match scope {
            StateScope::Pipeline => key.to_string(),
            StateScope::Stream => format!("{}:{}", self.current_stream(), key),
            StateScope::ConnectorInstance => format!("{}:{}", self.identity.connector_id, key),
        }
    }

    pub(crate) fn state_get_impl(
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

        let scope = match scope {
            0 => StateScope::Pipeline,
            1 => StateScope::Stream,
            2 => StateScope::ConnectorInstance,
            _ => {
                return Err(ConnectorError::config(
                    "INVALID_SCOPE",
                    format!("Invalid scope: {scope}"),
                ))
            }
        };

        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .get_cursor(&self.identity.pipeline, &StreamName::new(scoped_key))
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
            .map(|opt| opt.and_then(|cursor| cursor.cursor_value))
    }

    pub(crate) fn state_put_impl(
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

        let scope = match scope {
            0 => StateScope::Pipeline,
            1 => StateScope::Stream,
            2 => StateScope::ConnectorInstance,
            _ => {
                return Err(ConnectorError::config(
                    "INVALID_SCOPE",
                    format!("Invalid scope: {scope}"),
                ))
            }
        };

        let scoped_key = self.scoped_state_key(scope, &key);
        let cursor = CursorState {
            cursor_field: Some(key),
            cursor_value: Some(value),
            updated_at: Utc::now().to_rfc3339(),
        };

        self.identity
            .state_backend
            .set_cursor(
                &self.identity.pipeline,
                &StreamName::new(scoped_key),
                &cursor,
            )
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn state_cas_impl(
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

        let scope = match scope {
            0 => StateScope::Pipeline,
            1 => StateScope::Stream,
            2 => StateScope::ConnectorInstance,
            _ => {
                return Err(ConnectorError::config(
                    "INVALID_SCOPE",
                    format!("Invalid scope: {scope}"),
                ))
            }
        };

        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .compare_and_set(
                &self.identity.pipeline,
                &StreamName::new(scoped_key),
                expected.as_deref(),
                &new_value,
            )
            .map_err(|e| ConnectorError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn checkpoint_impl(
        &mut self,
        kind: u32,
        payload_json: String,
    ) -> Result<(), ConnectorError> {
        let envelope: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| ConnectorError::internal("PARSE_CHECKPOINT", e.to_string()))?;

        tracing::debug!(
            pipeline = self.identity.pipeline.as_str(),
            stream = %self.current_stream(),
            "Received checkpoint: {}",
            serde_json::to_string(&envelope).unwrap_or_default()
        );

        let payload = envelope.get("payload").cloned().unwrap_or(envelope.clone());
        match kind {
            CHECKPOINT_KIND_SOURCE => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(payload) {
                    lock_mutex(&self.checkpoints.source, "source_checkpoints")?.push(cp);
                }
            }
            CHECKPOINT_KIND_DEST => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(payload) {
                    lock_mutex(&self.checkpoints.dest, "dest_checkpoints")?.push(cp);
                }
            }
            CHECKPOINT_KIND_TRANSFORM => {
                tracing::debug!(
                    pipeline = self.identity.pipeline.as_str(),
                    stream = %self.current_stream(),
                    "Received transform checkpoint"
                );
            }
            _ => {
                return Err(ConnectorError::config(
                    "INVALID_CHECKPOINT_KIND",
                    format!("Invalid checkpoint kind: {kind}"),
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn metric_impl(&mut self, payload_json: String) -> Result<(), ConnectorError> {
        let metric: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| ConnectorError::internal("PARSE_METRIC", e.to_string()))?;

        tracing::debug!(
            pipeline = self.identity.pipeline.as_str(),
            stream = %self.current_stream(),
            "Received metric: {}",
            serde_json::to_string(&metric).unwrap_or_default()
        );

        Ok(())
    }

    pub(crate) fn log_impl(&mut self, level: u32, msg: String) {
        let pipeline = self.identity.pipeline.as_str();
        let stream = self.current_stream();

        match level {
            0 => tracing::error!(pipeline, stream = %stream, "[connector] {}", msg),
            1 => tracing::warn!(pipeline, stream = %stream, "[connector] {}", msg),
            2 => tracing::info!(pipeline, stream = %stream, "[connector] {}", msg),
            3 => tracing::debug!(pipeline, stream = %stream, "[connector] {}", msg),
            _ => tracing::trace!(pipeline, stream = %stream, "[connector] {}", msg),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn connect_tcp_impl(
        &mut self,
        host: String,
        port: u16,
    ) -> Result<u64, ConnectorError> {
        if !self.sockets.acl.allows(&host) {
            return Err(ConnectorError::permission(
                "NETWORK_DENIED",
                format!("Host '{host}' is not allowed by connector permissions"),
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
            let details = last_error.map_or_else(
                || "no resolved addresses available".to_string(),
                |(addr, err)| format!("last attempt {addr} failed: {err}"),
            );
            ConnectorError::transient_network(
                "TCP_CONNECT_FAILED",
                format!("{host}:{port} ({details})"),
            )
        })?;

        stream
            .set_nonblocking(true)
            .map_err(|e| ConnectorError::internal("SOCKET_CONFIG", e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| ConnectorError::internal("SOCKET_CONFIG", e.to_string()))?;

        let handle = self.sockets.next_handle;
        self.sockets.next_handle = self.sockets.next_handle.saturating_add(1);
        self.sockets.sockets.insert(
            handle,
            SocketEntry {
                stream,
                read_would_block_streak: 0,
                write_would_block_streak: 0,
            },
        );

        Ok(handle)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn socket_read_impl(
        &mut self,
        handle: u64,
        len: u64,
    ) -> Result<SocketReadResult, ConnectorError> {
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        let read_len = len.clamp(1, MAX_SOCKET_READ_BYTES) as usize;
        let mut buf = vec![0u8; read_len];
        match entry.stream.read(&mut buf) {
            Ok(0) => {
                entry.read_would_block_streak = 0;
                Ok(SocketReadResult::Eof)
            }
            Ok(n) => {
                entry.read_would_block_streak = 0;
                buf.truncate(n);
                Ok(SocketReadResult::Data(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                entry.read_would_block_streak += 1;

                if entry.read_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketReadResult::WouldBlock);
                }

                #[cfg(unix)]
                let ready = match wait_socket_ready(
                    &entry.stream,
                    SocketInterest::Read,
                    socket_poll_timeout_ms(),
                ) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_read");
                        true
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.read(&mut buf) {
                        Ok(0) => {
                            entry.read_would_block_streak = 0;
                            Ok(SocketReadResult::Eof)
                        }
                        Ok(n) => {
                            entry.read_would_block_streak = 0;
                            buf.truncate(n);
                            Ok(SocketReadResult::Data(buf))
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            Ok(SocketReadResult::WouldBlock)
                        }
                        Err(e) => Err(ConnectorError::transient_network(
                            "SOCKET_READ_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_read: WouldBlock after poll timeout");
                    Ok(SocketReadResult::WouldBlock)
                }
            }
            Err(e) => Err(ConnectorError::transient_network(
                "SOCKET_READ_FAILED",
                e.to_string(),
            )),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn socket_write_impl(
        &mut self,
        handle: u64,
        data: Vec<u8>,
    ) -> Result<SocketWriteResult, ConnectorError> {
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        match entry.stream.write(&data) {
            Ok(n) => {
                entry.write_would_block_streak = 0;
                Ok(SocketWriteResult::Written(n as u64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                entry.write_would_block_streak += 1;

                if entry.write_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketWriteResult::WouldBlock);
                }

                #[cfg(unix)]
                let ready = match wait_socket_ready(
                    &entry.stream,
                    SocketInterest::Write,
                    socket_poll_timeout_ms(),
                ) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_write");
                        true
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.write(&data) {
                        Ok(n) => {
                            entry.write_would_block_streak = 0;
                            Ok(SocketWriteResult::Written(n as u64))
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            Ok(SocketWriteResult::WouldBlock)
                        }
                        Err(e) => Err(ConnectorError::transient_network(
                            "SOCKET_WRITE_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_write: WouldBlock after poll timeout");
                    Ok(SocketWriteResult::WouldBlock)
                }
            }
            Err(e) => Err(ConnectorError::transient_network(
                "SOCKET_WRITE_FAILED",
                e.to_string(),
            )),
        }
    }

    pub(crate) fn socket_close_impl(&mut self, handle: u64) {
        self.sockets.sockets.remove(&handle);
    }

    pub(crate) fn emit_dlq_record_impl(
        &mut self,
        stream_name: String,
        record_json: String,
        error_message: String,
        error_category: String,
    ) -> Result<(), ConnectorError> {
        let mut dlq = lock_mutex(&self.checkpoints.dlq_records, "dlq_records")?;
        if dlq.len() >= self.checkpoints.dlq_limit {
            tracing::warn!(
                max = self.checkpoints.dlq_limit,
                "DLQ record cap reached; dropping further records"
            );
            return Ok(());
        }
        dlq.push(DlqRecord {
            stream_name,
            record_json,
            error_message,
            error_category: parse_error_category(&error_category),
            failed_at: Timestamp::new(Utc::now().to_rfc3339()),
        });
        Ok(())
    }
}

// --- HasStoreLimits ---

impl HasStoreLimits for ComponentHostState {
    fn store_limits(&mut self) -> &mut StoreLimits {
        &mut self.store_limits
    }
}

// --- WasiView ---

impl WasiView for ComponentHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

// --- Helpers ---

fn lock_mutex<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> std::result::Result<MutexGuard<'a, T>, ConnectorError> {
    mutex
        .lock()
        .map_err(|_| ConnectorError::internal("MUTEX_POISONED", format!("{name} mutex poisoned")))
}

fn parse_error_category(raw: &str) -> ErrorCategory {
    match raw {
        "config" => ErrorCategory::Config,
        "auth" => ErrorCategory::Auth,
        "permission" => ErrorCategory::Permission,
        "rate_limit" => ErrorCategory::RateLimit,
        "transient_network" => ErrorCategory::TransientNetwork,
        "transient_db" => ErrorCategory::TransientDb,
        "data" => ErrorCategory::Data,
        "schema" => ErrorCategory::Schema,
        _ => ErrorCategory::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_state::SqliteStateBackend;

    fn test_host_state() -> ComponentHostState {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        ComponentHostState::builder()
            .pipeline("test-pipeline")
            .connector_id("source-postgres")
            .stream("users")
            .state_backend(state)
            .build()
            .unwrap()
    }

    #[test]
    fn builder_creates_valid_state() {
        let host = test_host_state();
        assert_eq!(host.identity.pipeline.as_str(), "test-pipeline");
        assert_eq!(host.identity.connector_id, "source-postgres");
        assert_eq!(host.current_stream(), "users");
    }

    #[test]
    fn builder_defaults_dlq_limit() {
        let host = test_host_state();
        assert_eq!(host.checkpoints.dlq_limit, DEFAULT_DLQ_LIMIT);
    }

    #[test]
    fn builder_custom_dlq_limit() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("p")
            .connector_id("c")
            .stream("s")
            .state_backend(state)
            .dlq_limit(500)
            .build()
            .unwrap();
        assert_eq!(host.checkpoints.dlq_limit, 500);
    }

    #[test]
    fn scoped_state_key_pipeline_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::Pipeline, "offset"),
            "offset"
        );
    }

    #[test]
    fn scoped_state_key_stream_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::Stream, "offset"),
            "users:offset"
        );
    }

    #[test]
    fn scoped_state_key_connector_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::ConnectorInstance, "offset"),
            "source-postgres:offset"
        );
    }

    #[test]
    fn parse_error_category_known() {
        assert_eq!(parse_error_category("config"), ErrorCategory::Config);
        assert_eq!(parse_error_category("schema"), ErrorCategory::Schema);
    }

    #[test]
    fn parse_error_category_unknown() {
        assert_eq!(parse_error_category("bogus"), ErrorCategory::Internal);
    }
}
