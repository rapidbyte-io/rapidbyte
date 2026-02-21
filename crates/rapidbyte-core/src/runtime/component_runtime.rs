use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rapidbyte_sdk::errors::ConnectorError;
use rapidbyte_sdk::manifest::Permissions;
use rapidbyte_sdk::protocol::{Checkpoint, DlqRecord, StateScope};
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::state::backend::{CursorState, StateBackend};

use crate::engine::compression::CompressionCodec;
use super::host_socket::{
    resolve_socket_addrs, SocketEntry, SocketReadResultInternal, SocketWriteResultInternal,
    SOCKET_POLL_ACTIVATION_THRESHOLD,
};
#[cfg(unix)]
use super::host_socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};
use super::network_acl::{derive_network_acl, NetworkAcl};

pub use super::connector_resolve::{
    load_connector_manifest, manifest_path_from_wasm, parse_connector_ref,
    resolve_connector_path, verify_wasm_checksum,
};
pub use super::wasm_runtime::{LoadedComponent, WasmRuntime};
pub use super::wit_bindings::{
    dest_error_to_sdk, dest_validation_to_sdk, source_error_to_sdk, source_validation_to_sdk,
    transform_error_to_sdk, transform_validation_to_sdk,
};

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
    pub(crate) pipeline_name: String,
    pub(crate) connector_id: String,
    pub(crate) current_stream: String,
    pub(crate) state_backend: Arc<dyn StateBackend>,

    pub(crate) batch_sender: Option<mpsc::Sender<Frame>>,
    pub(crate) next_batch_id: u64,

    pub(crate) batch_receiver: Option<mpsc::Receiver<Frame>>,

    pub(crate) compression: Option<CompressionCodec>,

    pub(crate) source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub(crate) dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub(crate) dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub(crate) timings: Arc<Mutex<HostTimings>>,

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
        current_stream: String,
        state_backend: Arc<dyn StateBackend>,
        batch_sender: Option<mpsc::Sender<Frame>>,
        batch_receiver: Option<mpsc::Receiver<Frame>>,
        compression: Option<CompressionCodec>,
        source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
        timings: Arc<Mutex<HostTimings>>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
    ) -> Result<Self> {
        Ok(Self {
            pipeline_name,
            connector_id,
            current_stream,
            state_backend,
            batch_sender,
            next_batch_id: 1,
            batch_receiver,
            compression,
            source_checkpoints,
            dest_checkpoints,
            dlq_records,
            timings,
            network_acl: derive_network_acl(permissions, config),
            sockets: HashMap::new(),
            next_socket_handle: 1,
            ctx: build_wasi_ctx(permissions)?,
            table: ResourceTable::new(),
        })
    }

    fn current_stream(&self) -> &str {
        &self.current_stream
    }

    pub(crate) fn emit_batch_impl(&mut self, batch: Vec<u8>) -> Result<(), ConnectorError> {
        if batch.is_empty() {
            return Err(ConnectorError::internal(
                "EMPTY_BATCH",
                "Connector emitted a zero-length batch; this is a protocol violation",
            ));
        }

        let fn_start = Instant::now();

        let compress_start = Instant::now();
        let batch = if let Some(codec) = self.compression {
            crate::engine::compression::compress(codec, &batch)
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
            .blocking_send(Frame::Data(batch))
            .map_err(|e| ConnectorError::internal("CHANNEL_SEND", e.to_string()))?;

        self.next_batch_id += 1;

        let mut t = self.timings.lock().unwrap();
        t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
        t.emit_batch_count += 1;
        t.compress_nanos += compress_elapsed_nanos;

        Ok(())
    }

    pub(crate) fn next_batch_impl(&mut self) -> Result<Option<Vec<u8>>, ConnectorError> {
        let fn_start = Instant::now();

        let receiver = self.batch_receiver.as_mut().ok_or_else(|| {
            ConnectorError::internal("NO_RECEIVER", "No batch receiver configured")
        })?;

        let frame = match receiver.blocking_recv() {
            Some(frame) => frame,
            None => return Ok(None),
        };

        let batch = match frame {
            Frame::Data(batch) => batch,
            Frame::EndStream => return Ok(None),
        };

        let decompress_start = Instant::now();
        let batch = if let Some(codec) = self.compression {
            crate::engine::compression::decompress(codec, &batch)
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

        let scope = StateScope::from_i32(scope as i32).ok_or_else(|| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);

        self.state_backend
            .get_cursor(&self.pipeline_name, &scoped_key)
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

    pub(crate) fn checkpoint_impl(&mut self, kind: u32, payload_json: String) -> Result<(), ConnectorError> {
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

    pub(crate) fn metric_impl(&mut self, payload_json: String) -> Result<(), ConnectorError> {
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

    pub(crate) fn log_impl(&mut self, level: u32, msg: String) {
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

    pub(crate) fn connect_tcp_impl(&mut self, host: String, port: u16) -> Result<u64, ConnectorError> {
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

    pub(crate) fn socket_read_impl(
        &mut self,
        handle: u64,
        len: u64,
    ) -> Result<SocketReadResultInternal, ConnectorError> {
        let entry = self
            .sockets
            .get_mut(&handle)
            .ok_or_else(|| ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

        let read_len = len.clamp(1, 64 * 1024) as usize;
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

    pub(crate) fn socket_write_impl(
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

    pub(crate) fn socket_close_impl(&mut self, handle: u64) {
        self.sockets.remove(&handle);
    }

    pub(crate) fn emit_dlq_record_impl(
        &mut self,
        stream_name: String,
        record_json: String,
        error_message: String,
        error_category: String,
    ) -> Result<(), ConnectorError> {
        let mut dlq = self.dlq_records.lock().unwrap();
        if dlq.len() >= crate::engine::dlq::MAX_DLQ_RECORDS {
            tracing::warn!(
                max = crate::engine::dlq::MAX_DLQ_RECORDS,
                "DLQ record cap reached; dropping further records"
            );
            return Ok(());
        }
        dlq.push(DlqRecord {
            stream_name,
            record_json,
            error_message,
            error_category,
            failed_at: chrono::Utc::now().to_rfc3339(),
        });
        Ok(())
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
