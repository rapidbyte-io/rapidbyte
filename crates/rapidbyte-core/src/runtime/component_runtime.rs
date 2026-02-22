//! Host-side runtime state and host import implementations for connector components.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use rapidbyte_types::errors::{ConnectorError, ErrorCategory};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::protocol::{Checkpoint, DlqRecord, Iso8601Timestamp, StateScope};
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime::{StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::state::backend::{CursorState, PipelineId, StateBackend, StreamName};

use super::host_socket::{
    resolve_socket_addrs, SocketEntry, SocketReadResultInternal, SocketWriteResultInternal,
    SOCKET_POLL_ACTIVATION_THRESHOLD,
};
#[cfg(unix)]
use super::host_socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};
use super::network_acl::{derive_network_acl, NetworkAcl};
use crate::engine::compression::CompressionCodec;

pub use super::connector_resolve::{
    load_connector_manifest, manifest_path_from_wasm, parse_connector_ref, resolve_connector_path,
    verify_wasm_checksum,
};
pub use super::wasm_runtime::{LoadedComponent, WasmRuntime};
pub use super::wit_bindings::{
    dest_error_to_sdk, dest_validation_to_sdk, source_error_to_sdk, source_validation_to_sdk,
    transform_error_to_sdk, transform_validation_to_sdk,
};

const MAX_SOCKET_READ_BYTES: u64 = 64 * 1024;
const CHECKPOINT_KIND_SOURCE: u32 = 0;
const CHECKPOINT_KIND_DEST: u32 = 1;
const CHECKPOINT_KIND_TRANSFORM: u32 = 2;

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

fn lock_mutex<'a, T>(mutex: &'a Mutex<T>, name: &str) -> Result<MutexGuard<'a, T>, ConnectorError> {
    mutex
        .lock()
        .map_err(|_| ConnectorError::internal("MUTEX_POISONED", format!("{} mutex poisoned", name)))
}

/// Pipeline-level sandbox overrides passed alongside manifest permissions.
#[derive(Debug, Clone, Default)]
pub(crate) struct SandboxOverrides {
    pub allowed_hosts: Option<Vec<String>>,
    pub allowed_vars: Option<Vec<String>>,
    pub allowed_preopens: Option<Vec<String>>,
    pub max_memory_bytes: Option<u64>,
    pub timeout_seconds: Option<u64>,
}

fn intersect_env_vars(manifest_vars: &[String], pipeline_vars: Option<&[String]>) -> Vec<String> {
    match pipeline_vars {
        None => manifest_vars.to_vec(),
        Some(allowed) => {
            let allowed_set: HashSet<&str> = allowed.iter().map(|s| s.as_str()).collect();
            manifest_vars
                .iter()
                .filter(|v| allowed_set.contains(v.as_str()))
                .cloned()
                .collect()
        }
    }
}

fn intersect_preopens(
    manifest_preopens: &[String],
    pipeline_preopens: Option<&[String]>,
) -> Vec<String> {
    match pipeline_preopens {
        None => manifest_preopens.to_vec(),
        Some(allowed) => {
            let allowed_set: HashSet<&str> = allowed.iter().map(|s| s.as_str()).collect();
            manifest_preopens
                .iter()
                .filter(|p| allowed_set.contains(p.as_str()))
                .cloned()
                .collect()
        }
    }
}

/// Resolve a limit by taking the minimum of manifest and pipeline values.
/// When only one side provides a value, that value is used.
/// When neither provides a value, returns None.
pub(crate) fn resolve_min_limit(manifest: Option<u64>, pipeline: Option<u64>) -> Option<u64> {
    match (manifest, pipeline) {
        (Some(m), Some(p)) => Some(m.min(p)),
        (Some(m), None) => Some(m),
        (None, Some(p)) => Some(p),
        (None, None) => None,
    }
}

fn build_wasi_ctx(
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.allow_blocking_current_thread(true);

    // WASI-level network is disabled; connectors must use host `connect-tcp`.
    builder.allow_tcp(false);
    builder.allow_udp(false);
    builder.allow_ip_name_lookup(false);

    if let Some(perms) = permissions {
        let effective_vars = intersect_env_vars(
            &perms.env.allowed_vars,
            overrides.and_then(|o| o.allowed_vars.as_deref()),
        );
        for var in &effective_vars {
            if let Ok(value) = std::env::var(var) {
                builder.env(var, &value);
            }
        }

        let effective_preopens = intersect_preopens(
            &perms.fs.preopens,
            overrides.and_then(|o| o.allowed_preopens.as_deref()),
        );
        for dir in &effective_preopens {
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
}

pub(crate) struct SocketManager {
    pub acl: NetworkAcl,
    pub sockets: HashMap<u64, SocketEntry>,
    pub next_handle: u64,
}

/// Shared state passed to component host imports.
pub struct ComponentHostState {
    pub(crate) identity: ConnectorIdentity,
    pub(crate) batch: BatchRouter,
    pub(crate) checkpoints: CheckpointCollector,
    pub(crate) sockets: SocketManager,
    pub(crate) store_limits: StoreLimits,
    ctx: WasiCtx,
    table: ResourceTable,
}

fn build_store_limits(overrides: Option<&SandboxOverrides>) -> StoreLimits {
    let mut builder = StoreLimitsBuilder::new();
    if let Some(max_bytes) = overrides.and_then(|o| o.max_memory_bytes) {
        builder = builder.memory_size(max_bytes as usize);
    }
    builder = builder.trap_on_grow_failure(true);
    builder.build()
}

impl ComponentHostState {
    fn from_parts(
        identity: ConnectorIdentity,
        batch: BatchRouter,
        checkpoints: CheckpointCollector,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
        overrides: Option<&SandboxOverrides>,
    ) -> Result<Self> {
        Ok(Self {
            identity,
            batch,
            checkpoints,
            sockets: SocketManager {
                acl: derive_network_acl(
                    permissions,
                    config,
                    overrides.and_then(|o| o.allowed_hosts.as_deref()),
                ),
                sockets: HashMap::new(),
                next_handle: 1,
            },
            store_limits: build_store_limits(overrides),
            ctx: build_wasi_ctx(permissions, overrides)?,
            table: ResourceTable::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn for_source(
        pipeline_name: String,
        connector_id: String,
        stream_name: String,
        state_backend: Arc<dyn StateBackend>,
        sender: mpsc::Sender<Frame>,
        source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        timings: Arc<Mutex<HostTimings>>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
        compression: Option<CompressionCodec>,
        overrides: Option<&SandboxOverrides>,
    ) -> Result<Self> {
        Self::from_parts(
            ConnectorIdentity {
                pipeline: PipelineId(pipeline_name),
                connector_id,
                stream: StreamName(stream_name),
                state_backend,
            },
            BatchRouter {
                sender: Some(sender),
                receiver: None,
                next_batch_id: 1,
                compression,
            },
            CheckpointCollector {
                source: source_checkpoints,
                dest: Arc::new(Mutex::new(Vec::new())),
                dlq_records: Arc::new(Mutex::new(Vec::new())),
                timings,
            },
            permissions,
            config,
            overrides,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn for_destination(
        pipeline_name: String,
        connector_id: String,
        stream_name: String,
        state_backend: Arc<dyn StateBackend>,
        receiver: mpsc::Receiver<Frame>,
        dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
        timings: Arc<Mutex<HostTimings>>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
        compression: Option<CompressionCodec>,
        overrides: Option<&SandboxOverrides>,
    ) -> Result<Self> {
        Self::from_parts(
            ConnectorIdentity {
                pipeline: PipelineId(pipeline_name),
                connector_id,
                stream: StreamName(stream_name),
                state_backend,
            },
            BatchRouter {
                sender: None,
                receiver: Some(receiver),
                next_batch_id: 1,
                compression,
            },
            CheckpointCollector {
                source: Arc::new(Mutex::new(Vec::new())),
                dest: dest_checkpoints,
                dlq_records,
                timings,
            },
            permissions,
            config,
            overrides,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn for_transform(
        pipeline_name: String,
        connector_id: String,
        stream_name: String,
        state_backend: Arc<dyn StateBackend>,
        sender: mpsc::Sender<Frame>,
        receiver: mpsc::Receiver<Frame>,
        source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
        timings: Arc<Mutex<HostTimings>>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
        compression: Option<CompressionCodec>,
        overrides: Option<&SandboxOverrides>,
    ) -> Result<Self> {
        Self::from_parts(
            ConnectorIdentity {
                pipeline: PipelineId(pipeline_name),
                connector_id,
                stream: StreamName(stream_name),
                state_backend,
            },
            BatchRouter {
                sender: Some(sender),
                receiver: Some(receiver),
                next_batch_id: 1,
                compression,
            },
            CheckpointCollector {
                source: source_checkpoints,
                dest: dest_checkpoints,
                dlq_records: Arc::new(Mutex::new(Vec::new())),
                timings,
            },
            permissions,
            config,
            overrides,
        )
    }

    pub fn for_validation(
        pipeline_name: String,
        connector_id: String,
        stream_name: String,
        state_backend: Arc<dyn StateBackend>,
        permissions: Option<&Permissions>,
        config: &serde_json::Value,
        overrides: Option<&SandboxOverrides>,
    ) -> Result<Self> {
        Self::from_parts(
            ConnectorIdentity {
                pipeline: PipelineId(pipeline_name),
                connector_id,
                stream: StreamName(stream_name),
                state_backend,
            },
            BatchRouter {
                sender: None,
                receiver: None,
                next_batch_id: 1,
                compression: None,
            },
            CheckpointCollector {
                source: Arc::new(Mutex::new(Vec::new())),
                dest: Arc::new(Mutex::new(Vec::new())),
                dlq_records: Arc::new(Mutex::new(Vec::new())),
                timings: Arc::new(Mutex::new(HostTimings::default())),
            },
            permissions,
            config,
            overrides,
        )
    }

    fn current_stream(&self) -> &str {
        self.identity.stream.as_str()
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
        let batch = if let Some(codec) = self.batch.compression {
            crate::engine::compression::compress(codec, &batch)
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

    pub(crate) fn next_batch_impl(&mut self) -> Result<Option<Vec<u8>>, ConnectorError> {
        let fn_start = Instant::now();

        let receiver = self.batch.receiver.as_mut().ok_or_else(|| {
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
        let batch = if let Some(codec) = self.batch.compression {
            crate::engine::compression::decompress(codec, &batch)
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

        let scope = StateScope::try_from(scope as i32).map_err(|_| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .get_cursor(&self.identity.pipeline, &StreamName(scoped_key))
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

        let scope = StateScope::try_from(scope as i32).map_err(|_| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);
        let cursor = CursorState {
            cursor_field: Some(key),
            cursor_value: Some(value),
            updated_at: Utc::now(),
        };

        self.identity
            .state_backend
            .set_cursor(&self.identity.pipeline, &StreamName(scoped_key), &cursor)
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

        let scope = StateScope::try_from(scope as i32).map_err(|_| {
            ConnectorError::config("INVALID_SCOPE", format!("Invalid scope: {}", scope))
        })?;

        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .compare_and_set(
                &self.identity.pipeline,
                &StreamName(scoped_key),
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

    pub(crate) fn connect_tcp_impl(
        &mut self,
        host: String,
        port: u16,
    ) -> Result<u64, ConnectorError> {
        if !self.sockets.acl.allows(&host) {
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

    pub(crate) fn socket_read_impl(
        &mut self,
        handle: u64,
        len: u64,
    ) -> Result<SocketReadResultInternal, ConnectorError> {
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        let read_len = len.clamp(1, MAX_SOCKET_READ_BYTES) as usize;
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

                if entry.read_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketReadResultInternal::WouldBlock);
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
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                ConnectorError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        match entry.stream.write(&data) {
            Ok(n) => {
                entry.write_would_block_streak = 0;
                Ok(SocketWriteResultInternal::Written(n as u64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                entry.write_would_block_streak += 1;

                if entry.write_would_block_streak < SOCKET_POLL_ACTIVATION_THRESHOLD {
                    return Ok(SocketWriteResultInternal::WouldBlock);
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
            error_category: parse_error_category(&error_category),
            failed_at: Iso8601Timestamp(chrono::Utc::now().to_rfc3339()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::sqlite::SqliteStateBackend;

    #[test]
    fn test_parse_error_category() {
        assert_eq!(parse_error_category("config"), ErrorCategory::Config);
        assert_eq!(parse_error_category("schema"), ErrorCategory::Schema);
        assert_eq!(parse_error_category("unknown"), ErrorCategory::Internal);
    }

    #[test]
    fn test_scoped_state_key_pipeline_scope() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::for_validation(
            "pipe".to_string(),
            "source-postgres".to_string(),
            "users".to_string(),
            state,
            None,
            &serde_json::json!({}),
            None,
        )
        .unwrap();
        assert_eq!(
            host.scoped_state_key(StateScope::Pipeline, "offset"),
            "offset".to_string()
        );
    }

    // ── intersect_env_vars tests ─────────────────────────────────────

    #[test]
    fn intersect_env_vars_filters() {
        let manifest = vec!["A".into(), "B".into(), "C".into()];
        let pipeline = vec!["A".into(), "C".into()];
        let result = intersect_env_vars(&manifest, Some(&pipeline));
        assert_eq!(result, vec!["A".to_string(), "C".to_string()]);
    }

    #[test]
    fn intersect_env_vars_none_preserves_all() {
        let manifest = vec!["A".into(), "B".into()];
        let result = intersect_env_vars(&manifest, None);
        assert_eq!(result, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn intersect_env_vars_empty_blocks_all() {
        let manifest = vec!["A".into(), "B".into()];
        let result = intersect_env_vars(&manifest, Some(&vec![]));
        assert!(result.is_empty());
    }

    #[test]
    fn intersect_env_vars_pipeline_cannot_widen() {
        let manifest = vec!["A".into()];
        let pipeline = vec!["A".into(), "SECRET".into()];
        let result = intersect_env_vars(&manifest, Some(&pipeline));
        assert_eq!(result, vec!["A".to_string()]);
    }

    // ── intersect_preopens tests ─────────────────────────────────────

    #[test]
    fn intersect_preopens_filters() {
        let manifest = vec!["/data".into(), "/tmp".into()];
        let pipeline = vec!["/data".into()];
        let result = intersect_preopens(&manifest, Some(&pipeline));
        assert_eq!(result, vec!["/data".to_string()]);
    }

    #[test]
    fn intersect_preopens_none_preserves_all() {
        let manifest = vec!["/data".into(), "/tmp".into()];
        let result = intersect_preopens(&manifest, None);
        assert_eq!(result, vec!["/data".to_string(), "/tmp".to_string()]);
    }

    #[test]
    fn intersect_preopens_empty_blocks_all() {
        let manifest = vec!["/data".into()];
        let result = intersect_preopens(&manifest, Some(&vec![]));
        assert!(result.is_empty());
    }

    // ── resolve_min_limit tests ────────────────────────────────────────

    #[test]
    fn resolve_limit_min_of_both() {
        assert_eq!(resolve_min_limit(Some(100), Some(50)), Some(50));
        assert_eq!(resolve_min_limit(Some(50), Some(100)), Some(50));
    }

    #[test]
    fn resolve_limit_one_side_only() {
        assert_eq!(resolve_min_limit(Some(100), None), Some(100));
        assert_eq!(resolve_min_limit(None, Some(50)), Some(50));
    }

    #[test]
    fn resolve_limit_neither() {
        assert_eq!(resolve_min_limit(None, None), None);
    }
}
