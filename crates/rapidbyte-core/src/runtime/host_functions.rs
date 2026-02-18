use std::sync::{mpsc, Arc, Mutex};

use wasmedge_sdk::{CallingFrame, WasmValue};
use wasmedge_sys::Instance as SysInstance;
use wasmedge_types::error::CoreError;

use rapidbyte_sdk::errors::ConnectorError;
use rapidbyte_sdk::protocol::{Checkpoint, StateScope};

use crate::state::backend::{RunStats, StateBackend};

use super::memory_protocol;

/// Channel frame type for batch routing between source and destination.
///
/// Explicitly distinguishes data batches from stream boundary signals,
/// replacing the previous convention of "empty Vec<u8> = end-of-stream".
pub enum Frame {
    /// IPC-encoded Arrow RecordBatch. Must be non-empty;
    /// the host rejects zero-length data frames as a protocol violation.
    Data(Vec<u8>),
    /// Signals end of the current stream. The destination's pull loop
    /// sees this as EOF, returns from `run_write`, and the orchestrator
    /// can start the next stream.
    EndStream,
}

/// Shared state passed to all host functions via WasmEdge's host data mechanism.
pub struct HostState {
    /// Pipeline name for state operations.
    pub pipeline_name: String,
    /// Current stream name for state operations.
    /// Shared with the orchestrator loop so it can be updated per-stream
    /// before each `run_read`/`run_write` call.
    pub current_stream: Arc<Mutex<String>>,
    /// State backend for cursor/run persistence.
    pub state_backend: Arc<dyn StateBackend>,
    /// Accumulated stats for the current run.
    pub stats: Arc<Mutex<RunStats>>,

    // --- Source-side fields ---
    /// Channel sender for source -> orchestrator batch flow.
    pub batch_sender: Option<mpsc::SyncSender<Frame>>,
    /// Next batch ID (monotonically increasing per stream run, starts at 1).
    pub next_batch_id: u64,

    // --- Dest-side fields ---
    /// Channel receiver for orchestrator -> dest batch flow.
    pub batch_receiver: Option<mpsc::Receiver<Frame>>,
    /// Stashed batch when guest buffer was too small.
    pub pending_batch: Option<Vec<u8>>,

    // --- Error retrieval ---
    /// Last error from any host function (read+clear semantics).
    pub last_error: Option<ConnectorError>,

    // --- Checkpoint tracking ---
    /// Source checkpoints received during this stream run.
    /// Arc<Mutex<>> so the orchestrator can read checkpoints after the VM runs.
    pub source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    /// Dest checkpoints received during this stream run.
    /// Arc<Mutex<>> so the orchestrator can read checkpoints after the VM runs.
    pub dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
}

impl HostState {
    fn clear_last_error(&mut self) {
        self.last_error = None;
    }

    fn set_last_error(&mut self, err: ConnectorError) {
        self.last_error = Some(err);
    }

    /// Read the current stream name (locks briefly).
    fn current_stream(&self) -> String {
        self.current_stream.lock().unwrap().clone()
    }
}

// ============================================================
// Host functions
// ============================================================

/// Host function: emit a batch from source to host (blocking on backpressure).
///
/// Signature: (ptr: u32, len: u32) -> i32
/// Returns: 0 = success, -1 = error (call last_error).
pub fn host_emit_batch(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let ptr = args[0].to_i32() as u32;
    let len = args[1].to_i32() as u32;

    let batch_bytes = match memory_protocol::read_from_guest(frame, ptr as i32, len as i32) {
        Ok(b) => b,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Reject zero-length batches — connectors must never emit empty data.
    // Empty frames are reserved for the EndStream sentinel (sent by the host).
    if batch_bytes.is_empty() {
        let err = ConnectorError::internal(
            "EMPTY_BATCH",
            "Connector emitted a zero-length batch; this is a protocol violation",
        );
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    let sender = match &data.batch_sender {
        Some(s) => s,
        None => {
            let err = ConnectorError::internal("NO_SENDER", "No batch sender configured");
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Blocking send — applies backpressure when channel is full
    match sender.send(Frame::Data(batch_bytes)) {
        Ok(()) => {
            data.next_batch_id += 1;
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorError::internal("CHANNEL_SEND", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: dest pulls next batch. Checks pending_batch first.
///
/// Signature: (out_ptr: u32, out_cap: u32) -> i32
/// Returns: >0 = bytes written, 0 = EOF, -1 = error, -N = need N bytes.
pub fn host_next_batch(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let out_ptr = args[0].to_i32() as u32;
    let out_cap = args[1].to_i32() as u32;

    // Check pending_batch first (stashed from previous too-small-buffer call)
    let batch = if let Some(pending) = data.pending_batch.take() {
        pending
    } else {
        // Read from channel
        let receiver = match &data.batch_receiver {
            Some(r) => r,
            None => {
                let err = ConnectorError::internal("NO_RECEIVER", "No batch receiver configured");
                data.set_last_error(err);
                return Ok(vec![WasmValue::from_i32(-1)]);
            }
        };
        match receiver.recv() {
            Ok(Frame::Data(batch)) => batch,
            Ok(Frame::EndStream) => return Ok(vec![WasmValue::from_i32(0)]), // End-of-stream
            Err(_) => return Ok(vec![WasmValue::from_i32(0)]),               // Channel closed
        }
    };

    let batch_len = batch.len() as u32;

    // Guard against batches that exceed i32 range
    if batch_len > i32::MAX as u32 {
        data.pending_batch = Some(batch);
        let err = ConnectorError::internal(
            "BATCH_TOO_LARGE",
            &format!("Batch size {} exceeds i32::MAX", batch_len),
        );
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    // Check if buffer is large enough
    if batch_len > out_cap {
        // Stash the batch and tell guest to resize
        data.pending_batch = Some(batch);
        return Ok(vec![WasmValue::from_i32(-(batch_len as i32))]);
    }

    // Write batch into guest memory
    let mut memory = match frame.memory_mut(0) {
        Some(m) => m,
        None => {
            data.pending_batch = Some(batch); // Don't lose the batch
            let err = ConnectorError::internal("NO_MEMORY", "Guest has no memory export");
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    if let Err(e) = memory.set_data(&batch, out_ptr) {
        data.pending_batch = Some(batch); // Don't lose the batch
        let err = ConnectorError::internal("MEMORY_WRITE", &format!("{:?}", e));
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    Ok(vec![WasmValue::from_i32(batch_len as i32)])
}

/// Host function: retrieve and clear the last error.
///
/// Signature: (out_ptr: u32, out_cap: u32) -> i32
/// Returns: >0 = bytes written, 0 = no error, -N = need N bytes.
pub fn host_last_error(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let out_ptr = args[0].to_i32() as u32;
    let out_cap = args[1].to_i32() as u32;

    let error = match data.last_error.take() {
        Some(e) => e,
        None => return Ok(vec![WasmValue::from_i32(0)]),
    };

    let json = match serde_json::to_vec(&error) {
        Ok(j) => j,
        Err(_) => return Ok(vec![WasmValue::from_i32(0)]),
    };

    let json_len = json.len() as u32;
    if json_len > out_cap {
        // Put error back and tell guest to resize
        data.last_error = Some(error);
        return Ok(vec![WasmValue::from_i32(-(json_len as i32))]);
    }

    let mut memory = match frame.memory_mut(0) {
        Some(m) => m,
        None => return Ok(vec![WasmValue::from_i32(0)]),
    };

    if memory.set_data(&json, out_ptr).is_err() {
        return Ok(vec![WasmValue::from_i32(0)]);
    }

    Ok(vec![WasmValue::from_i32(json_len as i32)])
}

/// Host function: scoped state get.
///
/// Signature: (scope: i32, key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
/// Returns: >0 = bytes written, 0 = not found, -1 = error, -N = need N bytes.
pub fn host_state_get(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let scope_i32 = args[0].to_i32();
    let key_ptr = args[1].to_i32();
    let key_len = args[2].to_i32();
    let out_ptr = args[3].to_i32() as u32;
    let out_cap = args[4].to_i32() as u32;

    let _scope = match StateScope::from_i32(scope_i32) {
        Some(s) => s,
        None => {
            let err = ConnectorError::config(
                "INVALID_SCOPE",
                &format!("Invalid scope: {}", scope_i32),
            );
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let key = match memory_protocol::read_string_from_guest(frame, key_ptr, key_len) {
        Ok(s) => s,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    if key.len() > 1024 {
        let err = ConnectorError::config(
            "KEY_TOO_LONG",
            &format!("Key length {} exceeds 1024", key.len()),
        );
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    // Host anchors scope=Stream to current_stream
    let current_stream = data.current_stream();
    let state_key = format!("{}:{}", current_stream, key);
    match data
        .state_backend
        .get_cursor(&data.pipeline_name, &state_key)
    {
        Ok(Some(cursor)) => {
            if let Some(value) = cursor.cursor_value {
                let value_bytes = value.as_bytes();
                let write_len = value_bytes.len() as u32;

                if write_len > out_cap {
                    return Ok(vec![WasmValue::from_i32(-(write_len as i32))]);
                }

                let mut memory = match frame.memory_mut(0) {
                    Some(m) => m,
                    None => {
                        let err =
                            ConnectorError::internal("NO_MEMORY", "No memory export");
                        data.set_last_error(err);
                        return Ok(vec![WasmValue::from_i32(-1)]);
                    }
                };

                if let Err(e) = memory.set_data(value_bytes, out_ptr) {
                    let err = ConnectorError::internal(
                        "MEMORY_WRITE",
                        &format!("{:?}", e),
                    );
                    data.set_last_error(err);
                    return Ok(vec![WasmValue::from_i32(-1)]);
                }

                Ok(vec![WasmValue::from_i32(write_len as i32)])
            } else {
                Ok(vec![WasmValue::from_i32(0)])
            }
        }
        Ok(None) => Ok(vec![WasmValue::from_i32(0)]),
        Err(e) => {
            let err = ConnectorError::internal("STATE_BACKEND", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: scoped state put.
///
/// Signature: (scope: i32, key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
/// Returns: 0 = success, -1 = error.
pub fn host_state_put(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let scope_i32 = args[0].to_i32();
    let key_ptr = args[1].to_i32();
    let key_len = args[2].to_i32();
    let val_ptr = args[3].to_i32();
    let val_len = args[4].to_i32();

    let _scope = match StateScope::from_i32(scope_i32) {
        Some(s) => s,
        None => {
            let err = ConnectorError::config(
                "INVALID_SCOPE",
                &format!("Invalid scope: {}", scope_i32),
            );
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let key = match memory_protocol::read_string_from_guest(frame, key_ptr, key_len) {
        Ok(s) => s,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    if key.len() > 1024 {
        let err = ConnectorError::config(
            "KEY_TOO_LONG",
            &format!("Key length {} exceeds 1024", key.len()),
        );
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    let value = match memory_protocol::read_string_from_guest(frame, val_ptr, val_len) {
        Ok(s) => s,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let current_stream = data.current_stream();
    let state_key = format!("{}:{}", current_stream, key);
    let cursor = crate::state::backend::CursorState {
        cursor_field: Some(key),
        cursor_value: Some(value),
        updated_at: chrono::Utc::now(),
    };

    match data
        .state_backend
        .set_cursor(&data.pipeline_name, &state_key, &cursor)
    {
        Ok(()) => Ok(vec![WasmValue::from_i32(0)]),
        Err(e) => {
            let err = ConnectorError::internal("STATE_BACKEND", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: receive a checkpoint from the connector.
///
/// Signature: (kind: i32, payload_ptr: u32, payload_len: u32) -> i32
/// Returns: 0 = success, -1 = error.
pub fn host_checkpoint(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let kind = args[0].to_i32(); // 0=source, 1=dest, 2=transform
    let payload_ptr = args[1].to_i32();
    let payload_len = args[2].to_i32();

    let payload_bytes = match memory_protocol::read_from_guest(frame, payload_ptr, payload_len) {
        Ok(b) => b,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Parse the envelope to extract the checkpoint
    let current_stream = data.current_stream();
    match serde_json::from_slice::<serde_json::Value>(&payload_bytes) {
        Ok(envelope) => {
            tracing::debug!(
                pipeline = data.pipeline_name,
                stream = %current_stream,
                "Received checkpoint: {}",
                serde_json::to_string_pretty(&envelope).unwrap_or_default()
            );
            // Track checkpoint count
            if kind == 0 {
                // Source checkpoint — try to parse and store
                match serde_json::from_value::<Checkpoint>(
                    envelope
                        .get("payload")
                        .cloned()
                        .unwrap_or(envelope.clone()),
                ) {
                    Ok(cp) => data.source_checkpoints.lock().unwrap().push(cp),
                    Err(e) => {
                        tracing::warn!(
                            pipeline = data.pipeline_name,
                            stream = %current_stream,
                            "Failed to parse source checkpoint: {}",
                            e
                        );
                    }
                }
            } else if kind == 2 {
                // Transform checkpoint — log for now (no dedicated storage yet)
                tracing::debug!(
                    pipeline = data.pipeline_name,
                    stream = %current_stream,
                    "Received transform checkpoint (kind=2)"
                );
            } else {
                // Dest checkpoint (kind=1) — try to parse and store
                match serde_json::from_value::<Checkpoint>(
                    envelope
                        .get("payload")
                        .cloned()
                        .unwrap_or(envelope.clone()),
                ) {
                    Ok(cp) => data.dest_checkpoints.lock().unwrap().push(cp),
                    Err(e) => {
                        tracing::warn!(
                            pipeline = data.pipeline_name,
                            stream = %current_stream,
                            "Failed to parse dest checkpoint: {}",
                            e
                        );
                    }
                }
            }
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorError::internal("PARSE_CHECKPOINT", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: receive a metric from the connector.
///
/// Signature: (payload_ptr: u32, payload_len: u32) -> i32
/// Returns: 0 = success, -1 = error.
pub fn host_metric_fn(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    data.clear_last_error();

    let payload_ptr = args[0].to_i32();
    let payload_len = args[1].to_i32();

    let payload_bytes = match memory_protocol::read_from_guest(frame, payload_ptr, payload_len) {
        Ok(b) => b,
        Err(e) => {
            let err = ConnectorError::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let current_stream = data.current_stream();
    match serde_json::from_slice::<serde_json::Value>(&payload_bytes) {
        Ok(metric_val) => {
            tracing::debug!(
                pipeline = data.pipeline_name,
                stream = %current_stream,
                "Received metric: {}",
                serde_json::to_string(&metric_val).unwrap_or_default()
            );
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorError::internal("PARSE_METRIC", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: log a message from the connector.
///
/// Signature: (level: i32, msg_ptr: i32, msg_len: i32) -> i32
/// Returns: 0 always.
pub fn host_log(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let level = args[0].to_i32();
    let msg_ptr = args[1].to_i32();
    let msg_len = args[2].to_i32();

    let message = match memory_protocol::read_string_from_guest(frame, msg_ptr, msg_len) {
        Ok(s) => s,
        Err(_) => return Ok(vec![WasmValue::from_i32(0)]),
    };

    let pipeline = &data.pipeline_name;
    let stream = data.current_stream();

    match level {
        0 => tracing::error!(pipeline, stream = %stream, "[connector] {}", message),
        1 => tracing::warn!(pipeline, stream = %stream, "[connector] {}", message),
        2 => tracing::info!(pipeline, stream = %stream, "[connector] {}", message),
        3 => tracing::debug!(pipeline, stream = %stream, "[connector] {}", message),
        _ => tracing::trace!(pipeline, stream = %stream, "[connector] {}", message),
    }

    Ok(vec![WasmValue::from_i32(0)])
}
