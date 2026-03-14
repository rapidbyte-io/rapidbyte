//! Host-side runtime state and host import implementations for plugin components.
//!
//! The `*_impl` methods mirror the WIT-generated calling convention (owned `String` / `Vec<u8>`),
//! and inner types are consumed by the bindings module.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use rapidbyte_state::StateBackend;
use rapidbyte_types::checkpoint::{Checkpoint, CheckpointKind, StateScope};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::{ErrorCategory, PluginError};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};

use crate::acl::{derive_network_acl, NetworkAcl};
use crate::compression::CompressionCodec;
use crate::engine::HasStoreLimits;
use crate::frame::FrameTable;
use crate::sandbox::{build_store_limits, build_wasi_ctx, SandboxOverrides};
use crate::socket::{
    resolve_socket_addrs, should_activate_socket_poll, SocketEntry, SocketReadResult,
    SocketWriteResult,
};
#[cfg(unix)]
use crate::socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};

const MAX_SOCKET_READ_BYTES: u64 = 64 * 1024;
const MAX_STATE_KEY_LEN: usize = 1024;

/// Default maximum DLQ records kept in memory per run.
pub const DEFAULT_DLQ_LIMIT: usize = 10_000;

/// Channel frame type for batch routing between plugin stages.
pub enum Frame {
    /// IPC-encoded Arrow `RecordBatch` (optionally compressed).
    Data {
        payload: bytes::Bytes,
        checkpoint_id: u64,
    },
    /// End-of-stream marker.
    EndStream,
}

/// Records host-side operation timings directly into OpenTelemetry instruments.
#[derive(Debug, Clone, Default)]
pub struct HostTimings {
    labels: Vec<opentelemetry::KeyValue>,
    raw_snapshot: Option<
        std::sync::Arc<std::sync::Mutex<rapidbyte_metrics::snapshot::PipelineMetricsSnapshot>>,
    >,
}

impl HostTimings {
    #[must_use]
    pub fn new(pipeline: &str, stream: &str, shard: usize) -> Self {
        use opentelemetry::KeyValue;
        use rapidbyte_metrics::labels;
        Self {
            labels: vec![
                KeyValue::new(labels::PIPELINE, pipeline.to_owned()),
                KeyValue::new(labels::STREAM, stream.to_owned()),
                KeyValue::new(labels::SHARD, shard.to_string()),
            ],
            raw_snapshot: None,
        }
    }

    #[must_use]
    pub fn with_run_label(mut self, run: &str) -> Self {
        self.labels.push(opentelemetry::KeyValue::new(
            rapidbyte_metrics::labels::RUN,
            run.to_owned(),
        ));
        self
    }

    #[must_use]
    pub fn with_raw_snapshot(
        mut self,
        snapshot: std::sync::Arc<
            std::sync::Mutex<rapidbyte_metrics::snapshot::PipelineMetricsSnapshot>,
        >,
    ) -> Self {
        self.raw_snapshot = Some(snapshot);
        self
    }

    fn update_raw_snapshot(
        &self,
        update: impl FnOnce(&mut rapidbyte_metrics::snapshot::PipelineMetricsSnapshot),
    ) {
        if let Some(snapshot) = &self.raw_snapshot {
            if let Ok(mut snapshot) = snapshot.lock() {
                update(&mut snapshot);
            }
        }
    }

    pub fn record_emit_batch(&self, duration: std::time::Duration) {
        self.update_raw_snapshot(|snapshot| snapshot.record_emit_batch(duration));
        rapidbyte_metrics::instruments::host::emit_batch_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_next_batch(
        &self,
        total: std::time::Duration,
        wait: std::time::Duration,
        process: std::time::Duration,
    ) {
        self.update_raw_snapshot(|snapshot| snapshot.record_next_batch(total, wait, process));
        rapidbyte_metrics::instruments::host::next_batch_duration()
            .record(total.as_secs_f64(), &self.labels);
        rapidbyte_metrics::instruments::host::next_batch_wait_duration()
            .record(wait.as_secs_f64(), &self.labels);
        rapidbyte_metrics::instruments::host::next_batch_process_duration()
            .record(process.as_secs_f64(), &self.labels);
    }

    pub fn record_compress(&self, duration: std::time::Duration) {
        self.update_raw_snapshot(|snapshot| snapshot.record_compress(duration));
        rapidbyte_metrics::instruments::host::compress_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_decompress(&self, duration: std::time::Duration) {
        self.update_raw_snapshot(|snapshot| snapshot.record_decompress(duration));
        rapidbyte_metrics::instruments::host::decompress_duration()
            .record(duration.as_secs_f64(), &self.labels);
    }

    pub fn record_plugin_duration(&self, name: &str, value_secs: f64) {
        let stream = self
            .labels
            .iter()
            .find(|label| label.key.as_str() == rapidbyte_metrics::labels::STREAM)
            .map(|label| label.value.as_str().into_owned())
            .unwrap_or_default();
        let shard = self
            .labels
            .iter()
            .find(|label| label.key.as_str() == rapidbyte_metrics::labels::SHARD)
            .and_then(|label| label.value.as_str().parse::<usize>().ok())
            .unwrap_or_default();
        self.update_raw_snapshot(|snapshot| {
            snapshot.record_plugin_duration_for_series(name, &stream, shard, value_secs);
        });
    }
}

fn map_custom_metric_error(
    name: &str,
    err: &rapidbyte_metrics::cache::InstrumentCacheError,
) -> PluginError {
    match err {
        rapidbyte_metrics::cache::InstrumentCacheError::MetricNameTooLong { max, .. } => {
            PluginError::permission(
                "INVALID_METRIC_NAME",
                format!("metric name '{name}' exceeds max length {max}"),
            )
        }
        rapidbyte_metrics::cache::InstrumentCacheError::MetricLimitExceeded { kind, max } => {
            PluginError::permission(
                "METRIC_NAME_LIMIT_EXCEEDED",
                format!("custom {kind} metric limit exceeded (max {max})"),
            )
        }
    }
}

// --- Inner types ---

pub(crate) struct PluginIdentity {
    pub pipeline: PipelineId,
    pub plugin_id: String,
    pub plugin_instance_key: String,
    pub stream: StreamName,
    pub metric_run_label: Option<String>,
    pub state_backend: Arc<dyn StateBackend>,
}

pub(crate) struct BatchRouter {
    pub sender: Option<mpsc::SyncSender<Frame>>,
    pub receiver: Option<mpsc::Receiver<Frame>>,
    pub next_batch_id: u64,
    pub current_checkpoint_id: Option<u64>,
    pub last_emitted_checkpoint_id: Option<u64>,
    pub compression: Option<CompressionCodec>,
    /// Optional callback invoked after each `emit-batch` with the payload byte size.
    pub on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

pub(crate) struct CheckpointCollector {
    pub source: Arc<Mutex<Vec<Checkpoint>>>,
    pub dest: Arc<Mutex<Vec<Checkpoint>>>,
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub timings: HostTimings,
    pub dlq_limit: usize,
}

pub(crate) struct SocketManager {
    pub acl: NetworkAcl,
    pub sockets: HashMap<u64, SocketEntry>,
    pub next_handle: u64,
}

/// Shared state passed to Wasmtime component host imports.
pub struct ComponentHostState {
    pub(crate) identity: PluginIdentity,
    pub(crate) batch: BatchRouter,
    pub(crate) checkpoints: CheckpointCollector,
    pub(crate) sockets: SocketManager,
    pub(crate) frames: FrameTable,
    pub(crate) store_limits: StoreLimits,
    ctx: WasiCtx,
    table: ResourceTable,
}

// --- Builder ---

/// Builder for [`ComponentHostState`].
pub struct HostStateBuilder {
    pipeline: Option<String>,
    plugin_id: Option<String>,
    plugin_instance_key: Option<String>,
    stream: Option<String>,
    metric_run_label: Option<String>,
    state_backend: Option<Arc<dyn StateBackend>>,
    sender: Option<mpsc::SyncSender<Frame>>,
    receiver: Option<mpsc::Receiver<Frame>>,
    source_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dest_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dlq_records: Option<Arc<Mutex<Vec<DlqRecord>>>>,
    timings: Option<HostTimings>,
    permissions: Option<Permissions>,
    config: serde_json::Value,
    compression: Option<CompressionCodec>,
    overrides: Option<SandboxOverrides>,
    dlq_limit: usize,
    on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl HostStateBuilder {
    fn new() -> Self {
        Self {
            pipeline: None,
            plugin_id: None,
            plugin_instance_key: None,
            stream: None,
            metric_run_label: None,
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
            on_emit: None,
        }
    }

    #[must_use]
    pub fn pipeline(mut self, name: impl Into<String>) -> Self {
        self.pipeline = Some(name.into());
        self
    }

    #[must_use]
    pub fn plugin_id(mut self, id: impl Into<String>) -> Self {
        self.plugin_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn plugin_instance_key(mut self, key: impl Into<String>) -> Self {
        self.plugin_instance_key = Some(key.into());
        self
    }

    #[must_use]
    pub fn stream(mut self, name: impl Into<String>) -> Self {
        self.stream = Some(name.into());
        self
    }

    #[must_use]
    pub fn metric_run_label(mut self, label: impl Into<String>) -> Self {
        self.metric_run_label = Some(label.into());
        self
    }

    #[must_use]
    pub fn state_backend(mut self, backend: Arc<dyn StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    #[must_use]
    pub fn sender(mut self, tx: mpsc::SyncSender<Frame>) -> Self {
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
    pub fn timings(mut self, t: HostTimings) -> Self {
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

    #[must_use]
    pub fn on_emit(mut self, cb: Arc<dyn Fn(u64) + Send + Sync>) -> Self {
        self.on_emit = Some(cb);
        self
    }

    /// Build the host state. Fails if required fields are missing or WASI context fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the WASI context cannot be constructed (e.g. invalid preopens).
    ///
    pub fn build(self) -> Result<ComponentHostState> {
        let pipeline = self
            .pipeline
            .ok_or_else(|| anyhow::anyhow!("pipeline is required"))?;
        let plugin_id = self
            .plugin_id
            .ok_or_else(|| anyhow::anyhow!("plugin_id is required"))?;
        let plugin_instance_key = self
            .plugin_instance_key
            .unwrap_or_else(|| plugin_id.clone());
        let stream = self
            .stream
            .ok_or_else(|| anyhow::anyhow!("stream is required"))?;
        let state_backend = self
            .state_backend
            .ok_or_else(|| anyhow::anyhow!("state_backend is required"))?;

        Ok(ComponentHostState {
            identity: PluginIdentity {
                pipeline: PipelineId::new(pipeline),
                plugin_id,
                plugin_instance_key,
                stream: StreamName::new(stream),
                metric_run_label: self.metric_run_label,
                state_backend,
            },
            batch: BatchRouter {
                sender: self.sender,
                receiver: self.receiver,
                next_batch_id: 1,
                current_checkpoint_id: None,
                last_emitted_checkpoint_id: None,
                compression: self.compression,
                on_emit: self.on_emit,
            },
            checkpoints: CheckpointCollector {
                source: self.source_checkpoints.unwrap_or_default(),
                dest: self.dest_checkpoints.unwrap_or_default(),
                dlq_records: self.dlq_records.unwrap_or_default(),
                timings: self.timings.unwrap_or_default(),
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
            frames: FrameTable::new(),
            store_limits: build_store_limits(self.overrides.as_ref()),
            ctx: build_wasi_ctx(self.permissions.as_ref(), self.overrides.as_ref())?,
            table: ResourceTable::new(),
        })
    }
}

// The `_impl` methods intentionally take owned `String`/`Vec<u8>` to match the
// WIT-generated calling convention used by the bindings module.
#[allow(clippy::needless_pass_by_value)]
impl ComponentHostState {
    /// Create a builder for `ComponentHostState`.
    #[must_use]
    pub fn builder() -> HostStateBuilder {
        HostStateBuilder::new()
    }

    /// Max frame capacity: 512 MB. Prevents guest-induced OOM.
    const MAX_FRAME_CAPACITY: u64 = 512 * 1024 * 1024;

    fn current_stream(&self) -> &str {
        self.identity.stream.as_str()
    }

    /// Rewrite reserved scope labels to the host-authoritative values.
    fn ensure_metric_scope_labels(&self, labels: &mut Vec<opentelemetry::KeyValue>) {
        labels.retain(|kv| {
            !matches!(
                kv.key.as_str(),
                rapidbyte_metrics::labels::PIPELINE
                    | rapidbyte_metrics::labels::RUN
                    | rapidbyte_metrics::labels::PLUGIN
                    | rapidbyte_metrics::labels::STREAM
                    | rapidbyte_metrics::labels::SHARD
            )
        });
        labels.push(opentelemetry::KeyValue::new(
            rapidbyte_metrics::labels::PIPELINE,
            self.identity.pipeline.as_str().to_owned(),
        ));
        labels.push(opentelemetry::KeyValue::new(
            rapidbyte_metrics::labels::PLUGIN,
            self.identity.plugin_id.clone(),
        ));
        labels.push(opentelemetry::KeyValue::new(
            rapidbyte_metrics::labels::STREAM,
            self.identity.stream.as_str().to_owned(),
        ));
        if let Some(run_label) = self.identity.metric_run_label.as_deref() {
            labels.push(opentelemetry::KeyValue::new(
                rapidbyte_metrics::labels::RUN,
                run_label.to_owned(),
            ));
        }
        if let Some(shard) = self
            .checkpoints
            .timings
            .labels
            .iter()
            .find(|label| label.key.as_str() == rapidbyte_metrics::labels::SHARD)
            .map(|label| label.value.as_str().into_owned())
        {
            labels.push(opentelemetry::KeyValue::new(
                rapidbyte_metrics::labels::SHARD,
                shard,
            ));
        }
    }

    fn next_checkpoint_frontier(&mut self) -> u64 {
        if let Some(checkpoint_id) = self.batch.current_checkpoint_id {
            return checkpoint_id;
        }

        let checkpoint_id = self.batch.next_batch_id;
        self.batch.next_batch_id += 1;
        checkpoint_id
    }

    fn checkpoint_frontier_for(&self, kind: CheckpointKind) -> u64 {
        match kind {
            CheckpointKind::Source => self.batch.last_emitted_checkpoint_id.unwrap_or(0),
            CheckpointKind::Dest | CheckpointKind::Transform => {
                self.batch.current_checkpoint_id.unwrap_or(0)
            }
        }
    }

    // ── Frame lifecycle host imports ────────────────────────────────

    pub(crate) fn frame_new_impl(&mut self, capacity: u64) -> u64 {
        let clamped = capacity.min(Self::MAX_FRAME_CAPACITY);
        if capacity > Self::MAX_FRAME_CAPACITY {
            tracing::warn!(
                requested = capacity,
                clamped = clamped,
                "frame-new capacity clamped to MAX_FRAME_CAPACITY"
            );
        }
        self.frames.alloc(clamped)
    }

    pub(crate) fn frame_write_impl(
        &mut self,
        handle: u64,
        chunk: Vec<u8>,
    ) -> Result<u64, PluginError> {
        self.frames
            .write(handle, &chunk)
            .map_err(|e| PluginError::frame("FRAME_WRITE_FAILED", e.to_string()))
    }

    pub(crate) fn frame_seal_impl(&mut self, handle: u64) -> Result<(), PluginError> {
        self.frames
            .seal(handle)
            .map_err(|e| PluginError::frame("FRAME_SEAL_FAILED", e.to_string()))
    }

    pub(crate) fn frame_len_impl(&mut self, handle: u64) -> Result<u64, PluginError> {
        self.frames
            .len(handle)
            .map_err(|e| PluginError::frame("FRAME_LEN_FAILED", e.to_string()))
    }

    pub(crate) fn frame_read_impl(
        &mut self,
        handle: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, PluginError> {
        self.frames
            .read(handle, offset, len)
            .map_err(|e| PluginError::frame("FRAME_READ_FAILED", e.to_string()))
    }

    pub(crate) fn frame_drop_impl(&mut self, handle: u64) {
        self.frames.drop_frame(handle);
    }

    // ── Host import implementations ─────────────────────────────────

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn emit_batch_impl(&mut self, handle: u64) -> Result<(), PluginError> {
        let fn_start = Instant::now();

        // Consume the sealed frame -> Bytes (zero-copy)
        let payload = self
            .frames
            .consume(handle)
            .map_err(|e| PluginError::frame("EMIT_BATCH_FAILED", e.to_string()))?;

        if payload.is_empty() {
            return Err(PluginError::internal(
                "EMPTY_BATCH",
                "Plugin emitted a zero-length batch; this is a protocol violation",
            ));
        }

        let (payload, compress_elapsed_nanos): (bytes::Bytes, u64) =
            if let Some(codec) = self.batch.compression {
                let start = Instant::now();
                let compressed = crate::compression::compress_bytes(codec, &payload)
                    .map_err(|e| PluginError::internal("COMPRESS_FAILED", e.to_string()))?;
                (compressed, start.elapsed().as_nanos() as u64)
            } else {
                (payload, 0)
            };

        let payload_len = payload.len() as u64;
        let checkpoint_id = self.next_checkpoint_frontier();
        let sender = self
            .batch
            .sender
            .as_ref()
            .ok_or_else(|| PluginError::internal("NO_SENDER", "No batch sender configured"))?;

        sender
            .send(Frame::Data {
                payload,
                checkpoint_id,
            })
            .map_err(|e| PluginError::internal("CHANNEL_SEND", e.to_string()))?;

        if let Some(cb) = &self.batch.on_emit {
            cb(payload_len);
        }

        self.batch.last_emitted_checkpoint_id = Some(checkpoint_id);

        self.checkpoints
            .timings
            .record_emit_batch(fn_start.elapsed());
        let compress_duration = Duration::from_nanos(compress_elapsed_nanos);
        if compress_duration > Duration::ZERO {
            self.checkpoints.timings.record_compress(compress_duration);
        }

        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn next_batch_impl(&mut self) -> Result<Option<u64>, PluginError> {
        let fn_start = Instant::now();

        let receiver =
            self.batch.receiver.as_mut().ok_or_else(|| {
                PluginError::internal("NO_RECEIVER", "No batch receiver configured")
            })?;

        let wait_start = Instant::now();
        let Ok(frame) = receiver.recv() else {
            return Ok(None);
        };
        let wait_elapsed_nanos = wait_start.elapsed().as_nanos() as u64;

        let payload = match frame {
            Frame::Data {
                payload,
                checkpoint_id,
            } => {
                self.batch.current_checkpoint_id = Some(checkpoint_id);
                payload
            }
            Frame::EndStream => return Ok(None),
        };

        let (payload, decompress_elapsed_nanos): (bytes::Bytes, u64) =
            if let Some(codec) = self.batch.compression {
                let start = Instant::now();
                let decompressed = crate::compression::decompress(codec, &payload)
                    .map_err(|e| PluginError::internal("DECOMPRESS_FAILED", e.to_string()))?
                    .into();
                (decompressed, start.elapsed().as_nanos() as u64)
            } else {
                (payload, 0) // Bytes, zero-copy
            };

        // Insert as sealed read-only frame
        let handle = self.frames.insert_sealed(payload);

        let total_elapsed = fn_start.elapsed();
        let wait_elapsed = Duration::from_nanos(wait_elapsed_nanos);
        let process_elapsed = total_elapsed.saturating_sub(wait_elapsed);
        self.checkpoints
            .timings
            .record_next_batch(total_elapsed, wait_elapsed, process_elapsed);
        let decompress_duration = Duration::from_nanos(decompress_elapsed_nanos);
        if decompress_duration > Duration::ZERO {
            self.checkpoints
                .timings
                .record_decompress(decompress_duration);
        }

        Ok(Some(handle))
    }

    fn scoped_state_key(&self, scope: StateScope, key: &str) -> String {
        match scope {
            StateScope::Pipeline => key.to_string(),
            StateScope::Stream => format!("{}:{}", self.current_stream(), key),
            StateScope::PluginInstance => {
                format!("{}:{}", self.identity.plugin_instance_key, key)
            }
        }
    }

    pub(crate) fn state_get_impl(
        &mut self,
        scope: u32,
        key: String,
    ) -> Result<Option<String>, PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .get_cursor(&self.identity.pipeline, &StreamName::new(scoped_key))
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
            .map(|opt| opt.and_then(|cursor| cursor.cursor_value))
    }

    pub(crate) fn state_put_impl(
        &mut self,
        scope: u32,
        key: String,
        value: String,
    ) -> Result<(), PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
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
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn state_cas_impl(
        &mut self,
        scope: u32,
        key: String,
        expected: Option<String>,
        new_value: String,
    ) -> Result<bool, PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .compare_and_set(
                &self.identity.pipeline,
                &StreamName::new(scoped_key),
                expected.as_deref(),
                &new_value,
            )
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn checkpoint_impl(
        &mut self,
        kind: u32,
        payload_json: String,
    ) -> Result<(), PluginError> {
        let envelope: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| PluginError::internal("PARSE_CHECKPOINT", e.to_string()))?;

        tracing::debug!(
            pipeline = self.identity.pipeline.as_str(),
            stream = %self.current_stream(),
            "Received checkpoint: {}",
            payload_json
        );

        let checkpoint_kind = CheckpointKind::try_from(kind).map_err(|k| {
            PluginError::config(
                "INVALID_CHECKPOINT_KIND",
                format!("Invalid checkpoint kind: {k}"),
            )
        })?;

        let payload = match envelope {
            serde_json::Value::Object(mut map) => map
                .remove("payload")
                .unwrap_or(serde_json::Value::Object(map)),
            other => other,
        };

        let mut cp: Checkpoint = serde_json::from_value(payload)
            .map_err(|e| PluginError::internal("PARSE_CHECKPOINT", e.to_string()))?;
        cp.id = self.checkpoint_frontier_for(checkpoint_kind);

        match checkpoint_kind {
            CheckpointKind::Source => {
                lock_mutex(&self.checkpoints.source, "source_checkpoints")?.push(cp);
            }
            CheckpointKind::Dest => {
                lock_mutex(&self.checkpoints.dest, "dest_checkpoints")?.push(cp);
            }
            CheckpointKind::Transform => {
                tracing::debug!(
                    pipeline = self.identity.pipeline.as_str(),
                    stream = %self.current_stream(),
                    "Received transform checkpoint"
                );
            }
        }

        Ok(())
    }

    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(crate) fn counter_add_impl(
        &self,
        name: String,
        value: u64,
        labels_json: String,
    ) -> Result<(), PluginError> {
        let mut labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
        self.ensure_metric_scope_labels(&mut labels);
        match name.as_str() {
            "records_read" => {
                rapidbyte_metrics::instruments::pipeline::records_read().add(value, &labels);
            }
            "records_written" => {
                rapidbyte_metrics::instruments::pipeline::records_written().add(value, &labels);
            }
            "bytes_read" => {
                rapidbyte_metrics::instruments::pipeline::bytes_read().add(value, &labels);
            }
            "bytes_written" => {
                rapidbyte_metrics::instruments::pipeline::bytes_written().add(value, &labels);
            }
            _ => {
                rapidbyte_metrics::instruments::plugin::custom_counter(&name)
                    .map_err(|err| map_custom_metric_error(&name, &err))?
                    .add(value, &labels);
            }
        }
        Ok(())
    }

    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(crate) fn gauge_set_impl(
        &self,
        name: String,
        value: f64,
        labels_json: String,
    ) -> Result<(), PluginError> {
        let mut labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
        self.ensure_metric_scope_labels(&mut labels);
        rapidbyte_metrics::instruments::plugin::custom_gauge(&name)
            .map_err(|err| map_custom_metric_error(&name, &err))?
            .record(value, &labels);
        Ok(())
    }

    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub(crate) fn histogram_record_impl(
        &self,
        name: String,
        value: f64,
        labels_json: String,
    ) -> Result<(), PluginError> {
        let mut labels = rapidbyte_metrics::labels::parse_bounded_labels(&labels_json);
        self.ensure_metric_scope_labels(&mut labels);
        match name.as_str() {
            "source_connect_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::source_connect_duration()
                    .record(value, &labels);
            }
            "source_query_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::source_query_duration()
                    .record(value, &labels);
            }
            "source_fetch_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::source_fetch_duration()
                    .record(value, &labels);
            }
            "source_arrow_encode_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::source_encode_duration()
                    .record(value, &labels);
            }
            "dest_connect_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::dest_connect_duration()
                    .record(value, &labels);
            }
            "dest_flush_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::dest_flush_duration()
                    .record(value, &labels);
            }
            "dest_commit_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::dest_commit_duration()
                    .record(value, &labels);
            }
            "dest_arrow_decode_secs" => {
                self.checkpoints
                    .timings
                    .record_plugin_duration(&name, value);
                rapidbyte_metrics::instruments::plugin::dest_decode_duration()
                    .record(value, &labels);
            }
            _ => {
                rapidbyte_metrics::instruments::plugin::custom_histogram(&name)
                    .map_err(|err| map_custom_metric_error(&name, &err))?
                    .record(value, &labels);
            }
        }
        Ok(())
    }

    pub(crate) fn log_impl(&mut self, level: u32, msg: String) {
        let pipeline = self.identity.pipeline.as_str();
        let stream = self.current_stream();

        match level {
            0 => tracing::error!(pipeline, stream = %stream, "[plugin] {}", msg),
            1 => tracing::warn!(pipeline, stream = %stream, "[plugin] {}", msg),
            2 => tracing::info!(pipeline, stream = %stream, "[plugin] {}", msg),
            3 => tracing::debug!(pipeline, stream = %stream, "[plugin] {}", msg),
            _ => tracing::trace!(pipeline, stream = %stream, "[plugin] {}", msg),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn connect_tcp_impl(&mut self, host: String, port: u16) -> Result<u64, PluginError> {
        if !self.sockets.acl.allows(&host) {
            return Err(PluginError::permission(
                "NETWORK_DENIED",
                format!("Host '{host}' is not allowed by plugin permissions"),
            ));
        }

        let addrs = resolve_socket_addrs(&host, port)
            .map_err(|e| PluginError::transient_network("DNS_RESOLUTION_FAILED", e.to_string()))?;

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
            PluginError::transient_network(
                "TCP_CONNECT_FAILED",
                format!("{host}:{port} ({details})"),
            )
        })?;

        stream
            .set_nonblocking(true)
            .map_err(|e| PluginError::internal("SOCKET_CONFIG", e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| PluginError::internal("SOCKET_CONFIG", e.to_string()))?;

        let handle = self.sockets.next_handle;
        self.sockets.next_handle = self.sockets.next_handle.wrapping_add(1);
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
    ) -> Result<SocketReadResult, PluginError> {
        let entry = self
            .sockets
            .sockets
            .get_mut(&handle)
            .ok_or_else(|| PluginError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

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
                if !should_activate_socket_poll(&mut entry.read_would_block_streak) {
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
                        Err(e) => Err(PluginError::transient_network(
                            "SOCKET_READ_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_read: WouldBlock after poll timeout");
                    Ok(SocketReadResult::WouldBlock)
                }
            }
            Err(e) => Err(PluginError::transient_network(
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
    ) -> Result<SocketWriteResult, PluginError> {
        let entry = self
            .sockets
            .sockets
            .get_mut(&handle)
            .ok_or_else(|| PluginError::internal("INVALID_SOCKET", "Invalid socket handle"))?;

        match entry.stream.write(&data) {
            Ok(n) => {
                entry.write_would_block_streak = 0;
                Ok(SocketWriteResult::Written(n as u64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !should_activate_socket_poll(&mut entry.write_would_block_streak) {
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
                        Err(e) => Err(PluginError::transient_network(
                            "SOCKET_WRITE_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_write: WouldBlock after poll timeout");
                    Ok(SocketWriteResult::WouldBlock)
                }
            }
            Err(e) => Err(PluginError::transient_network(
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
    ) -> Result<(), PluginError> {
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
            error_category: error_category
                .parse::<ErrorCategory>()
                .unwrap_or(ErrorCategory::Internal),
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
) -> std::result::Result<MutexGuard<'a, T>, PluginError> {
    mutex
        .lock()
        .map_err(|_| PluginError::internal("MUTEX_POISONED", format!("{name} mutex poisoned")))
}

fn validate_state_key(key: &str) -> Result<(), PluginError> {
    if key.len() > MAX_STATE_KEY_LEN {
        return Err(PluginError::config(
            "KEY_TOO_LONG",
            format!("Key length {} exceeds {MAX_STATE_KEY_LEN}", key.len()),
        ));
    }
    Ok(())
}

fn parse_state_scope(scope: u32) -> Result<StateScope, PluginError> {
    match scope {
        0 => Ok(StateScope::Pipeline),
        1 => Ok(StateScope::Stream),
        2 => Ok(StateScope::PluginInstance),
        _ => Err(PluginError::config(
            "INVALID_SCOPE",
            format!("Invalid scope: {scope}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use opentelemetry_sdk::metrics::data::{Histogram, Sum};
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    };
    use rapidbyte_state::SqliteStateBackend;
    use std::sync::LazyLock;

    static METRIC_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn metric_test_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporterBuilder::new().build();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        (provider, exporter)
    }

    fn metric_labels(exporter: &InMemoryMetricExporter, metric_name: &str) -> serde_json::Value {
        let metrics = exporter.get_finished_metrics().unwrap_or_default();
        for resource_metrics in &metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    if metric.name != metric_name {
                        continue;
                    }
                    if let Some(sum) = metric.data.as_any().downcast_ref::<Sum<u64>>() {
                        if let Some(dp) = sum.data_points.first() {
                            return serde_json::Value::Object(
                                dp.attributes
                                    .iter()
                                    .map(|kv| {
                                        (
                                            kv.key.as_str().to_owned(),
                                            serde_json::Value::String(kv.value.to_string()),
                                        )
                                    })
                                    .collect(),
                            );
                        }
                    }
                    if let Some(hist) = metric.data.as_any().downcast_ref::<Histogram<f64>>() {
                        if let Some(dp) = hist.data_points.first() {
                            return serde_json::Value::Object(
                                dp.attributes
                                    .iter()
                                    .map(|kv| {
                                        (
                                            kv.key.as_str().to_owned(),
                                            serde_json::Value::String(kv.value.to_string()),
                                        )
                                    })
                                    .collect(),
                            );
                        }
                    }
                }
            }
        }
        panic!("metric {metric_name} not found");
    }

    fn test_host_state() -> ComponentHostState {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        ComponentHostState::builder()
            .pipeline("test-pipeline")
            .plugin_id("postgres")
            .stream("users")
            .state_backend(state)
            .build()
            .unwrap()
    }

    #[test]
    fn builder_creates_valid_state() {
        let host = test_host_state();
        assert_eq!(host.identity.pipeline.as_str(), "test-pipeline");
        assert_eq!(host.identity.plugin_instance_key, "postgres");
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
            .plugin_id("c")
            .stream("s")
            .state_backend(state)
            .dlq_limit(500)
            .build()
            .unwrap();
        assert_eq!(host.checkpoints.dlq_limit, 500);
    }

    #[test]
    fn builder_missing_required_field_returns_error() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let result = ComponentHostState::builder()
            .plugin_id("c")
            .stream("s")
            .state_backend(state)
            .build();

        assert!(result.is_err());
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
    fn scoped_state_key_plugin_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::PluginInstance, "offset"),
            "postgres:offset"
        );
    }

    #[test]
    fn scoped_state_key_plugin_scope_uses_instance_key() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("p")
            .plugin_id("postgres")
            .plugin_instance_key("destination:postgres:users:p0")
            .stream("users")
            .state_backend(state)
            .build()
            .unwrap();
        assert_eq!(
            host.scoped_state_key(StateScope::PluginInstance, "offset"),
            "destination:postgres:users:p0:offset"
        );
    }

    #[test]
    fn error_category_from_str_known() {
        assert_eq!("config".parse::<ErrorCategory>(), Ok(ErrorCategory::Config));
        assert_eq!("schema".parse::<ErrorCategory>(), Ok(ErrorCategory::Schema));
    }

    #[test]
    fn error_category_from_str_unknown() {
        assert!("bogus".parse::<ErrorCategory>().is_err());
    }

    #[test]
    fn frame_data_holds_bytes() {
        use bytes::Bytes;
        let payload = Bytes::from_static(b"test-ipc-payload");
        let frame = Frame::Data {
            payload: payload.clone(),
            checkpoint_id: 7,
        };
        match frame {
            Frame::Data {
                payload: b,
                checkpoint_id,
            } => {
                assert_eq!(b, payload);
                assert_eq!(checkpoint_id, 7);
            }
            Frame::EndStream => panic!("expected Data"),
        }
    }

    #[test]
    fn checkpoint_impl_rewrites_source_checkpoint_id_from_frontier() {
        let mut host = test_host_state();
        host.batch.last_emitted_checkpoint_id = Some(42);

        let cp = Checkpoint {
            id: 999,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: Some("id".to_string()),
            cursor_value: Some(rapidbyte_types::cursor::CursorValue::Utf8 {
                value: "42".to_string(),
            }),
            records_processed: 10,
            bytes_processed: 100,
        };

        host.checkpoint_impl(0, serde_json::to_string(&cp).unwrap())
            .unwrap();

        let checkpoints = host.checkpoints.source.lock().unwrap();
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].id, 42);
    }

    #[test]
    fn checkpoint_impl_rejects_malformed_payload() {
        let mut host = test_host_state();
        let result = host.checkpoint_impl(0, r#"{"payload":{"stream":1}}"#.to_string());
        assert!(result.is_err());
    }

    #[test]
    fn host_timings_new_sets_labels() {
        let ht = HostTimings::new("my-pipeline", "users", 0);
        assert_eq!(ht.labels.len(), 3);
    }

    #[test]
    fn host_timings_default_has_no_labels() {
        let ht = HostTimings::default();
        assert!(ht.labels.is_empty());
    }

    #[test]
    fn record_methods_do_not_panic() {
        let ht = HostTimings::new("test", "stream", 0);
        ht.record_emit_batch(Duration::from_millis(5));
        ht.record_next_batch(
            Duration::from_millis(10),
            Duration::from_millis(7),
            Duration::from_millis(3),
        );
        ht.record_compress(Duration::from_micros(500));
        ht.record_decompress(Duration::from_micros(300));
    }

    #[test]
    fn gauge_set_rejects_overlong_custom_metric_name() {
        let host = test_host_state();
        let name = "m".repeat(rapidbyte_metrics::cache::MAX_CUSTOM_METRIC_NAME_LEN + 1);

        let err = host
            .gauge_set_impl(name, 1.0, "{}".to_string())
            .expect_err("overlong metric name should be rejected");

        assert_eq!(err.code, "INVALID_METRIC_NAME");
    }

    #[test]
    fn rejected_custom_histogram_name_does_not_mutate_fallback_timing_snapshot() {
        let snapshot = Arc::new(Mutex::new(
            rapidbyte_metrics::snapshot::PipelineMetricsSnapshot::default(),
        ));
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("test-pipeline")
            .plugin_id("postgres")
            .stream("users")
            .state_backend(state)
            .timings(
                HostTimings::new("test-pipeline", "users", 0).with_raw_snapshot(snapshot.clone()),
            )
            .build()
            .unwrap();

        let err = host
            .histogram_record_impl(
                "m".repeat(rapidbyte_metrics::cache::MAX_CUSTOM_METRIC_NAME_LEN + 1),
                1.0,
                "{}".to_string(),
            )
            .expect_err("overlong histogram name should be rejected");

        assert_eq!(err.code, "INVALID_METRIC_NAME");

        let raw_snapshot = snapshot.lock().unwrap().clone();
        assert_eq!(raw_snapshot.tracked_plugin_timing_series_count(), 0);
        assert!(raw_snapshot.dest_decode_secs.abs() < f64::EPSILON);
        assert!(raw_snapshot.source_connect_secs.abs() < f64::EPSILON);
    }

    #[test]
    fn reserved_metric_labels_are_rewritten_to_host_scope() {
        let _guard = METRIC_TEST_LOCK.lock().expect("metric test lock poisoned");
        let (provider, exporter) = metric_test_provider();
        global::set_meter_provider(provider.clone());

        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("test-pipeline")
            .plugin_id("postgres")
            .stream("users")
            .metric_run_label("run-42")
            .timings(HostTimings::new("test-pipeline", "users", 0))
            .state_backend(state)
            .build()
            .unwrap();

        host.counter_add_impl(
            "records_read".to_string(),
            1,
            r#"{"pipeline":"spoofed-pipeline","run":"spoofed-run","plugin":"spoofed-plugin","stream":"spoofed-stream","rule":"not_null"}"#.to_string(),
        )
        .unwrap();

        let _ = provider.force_flush();
        let labels = metric_labels(&exporter, "pipeline.records_read");
        assert_eq!(
            labels,
            serde_json::json!({
                "pipeline": "test-pipeline",
                "run": "run-42",
                "plugin": "postgres",
                "stream": "users",
                "shard": "0",
                "rule": "not_null"
            })
        );
    }

    #[test]
    fn histogram_labels_include_host_shard_scope() {
        let _guard = METRIC_TEST_LOCK.lock().expect("metric test lock poisoned");
        let (provider, exporter) = metric_test_provider();
        global::set_meter_provider(provider.clone());

        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("test-pipeline")
            .plugin_id("postgres")
            .stream("users")
            .metric_run_label("run-42")
            .timings(HostTimings::new("test-pipeline", "users", 3))
            .state_backend(state)
            .build()
            .unwrap();

        host.histogram_record_impl("source_connect_secs".to_string(), 1.0, "{}".to_string())
            .unwrap();

        let _ = provider.force_flush();
        let labels = metric_labels(&exporter, "plugin.source_connect_duration");
        assert_eq!(labels["shard"], "3");
        assert_eq!(labels["stream"], "users");
        assert_eq!(labels["run"], "run-42");
    }
}
