use std::sync::{mpsc, Arc, Mutex};

use wasmedge_sdk::{CallingFrame, WasmValue};
use wasmedge_sys::Instance as SysInstance;
use wasmedge_types::error::CoreError;

use rapidbyte_sdk::errors::ConnectorErrorV1;
use rapidbyte_sdk::protocol::{Checkpoint, StateScope};

use crate::state::backend::{RunStats, StateBackend};

use super::memory_protocol;

/// Shared state passed to all host functions via WasmEdge's host data mechanism.
pub struct HostState {
    /// Pipeline name for state operations.
    pub pipeline_name: String,
    /// Current stream name for state operations.
    pub current_stream: String,
    /// State backend for cursor/run persistence.
    pub state_backend: Arc<dyn StateBackend>,
    /// Accumulated stats for the current run.
    pub stats: Arc<Mutex<RunStats>>,

    // --- Source-side fields ---
    /// Channel sender for source -> orchestrator batch flow (v1: Vec<u8> only).
    pub batch_sender: Option<mpsc::SyncSender<Vec<u8>>>,
    /// Next batch ID (monotonically increasing per stream run, starts at 1).
    pub next_batch_id: u64,

    // --- Dest-side fields ---
    /// Channel receiver for orchestrator -> dest batch flow.
    pub batch_receiver: Option<mpsc::Receiver<Vec<u8>>>,
    /// Stashed batch when guest buffer was too small.
    pub pending_batch: Option<Vec<u8>>,

    // --- Error retrieval ---
    /// Last error from any host function (read+clear semantics).
    pub last_error: Option<ConnectorErrorV1>,

    // --- Checkpoint tracking ---
    /// Source checkpoints received during this stream run.
    pub source_checkpoints: Vec<Checkpoint>,
    /// Number of dest checkpoints received.
    pub dest_checkpoint_count: u64,

    // --- v0 compat: sender that includes stream name ---
    pub batch_sender_v0: Option<mpsc::SyncSender<(String, Vec<u8>)>>,
}

impl HostState {
    fn clear_last_error(&mut self) {
        self.last_error = None;
    }

    fn set_last_error(&mut self, err: ConnectorErrorV1) {
        self.last_error = Some(err);
    }
}

// ============================================================
// v1 host functions
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
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let sender = match &data.batch_sender {
        Some(s) => s,
        None => {
            let err = ConnectorErrorV1::internal("NO_SENDER", "No batch sender configured");
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Blocking send — applies backpressure when channel is full
    match sender.send(batch_bytes) {
        Ok(()) => {
            data.next_batch_id += 1;
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorErrorV1::internal("CHANNEL_SEND", &e.to_string());
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
                let err = ConnectorErrorV1::internal("NO_RECEIVER", "No batch receiver configured");
                data.set_last_error(err);
                return Ok(vec![WasmValue::from_i32(-1)]);
            }
        };
        match receiver.recv() {
            Ok(batch) => batch,
            Err(_) => return Ok(vec![WasmValue::from_i32(0)]), // EOF — sender dropped
        }
    };

    let batch_len = batch.len() as u32;

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
            let err = ConnectorErrorV1::internal("NO_MEMORY", "Guest has no memory export");
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    if let Err(e) = memory.set_data(&batch, out_ptr) {
        data.pending_batch = Some(batch); // Don't lose the batch
        let err = ConnectorErrorV1::internal("MEMORY_WRITE", &format!("{:?}", e));
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
            let err = ConnectorErrorV1::config(
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
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    if key.len() > 1024 {
        let err = ConnectorErrorV1::config(
            "KEY_TOO_LONG",
            &format!("Key length {} exceeds 1024", key.len()),
        );
        data.set_last_error(err);
        return Ok(vec![WasmValue::from_i32(-1)]);
    }

    // Host anchors scope=Stream to current_stream
    let state_key = format!("{}:{}", data.current_stream, key);
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
                            ConnectorErrorV1::internal("NO_MEMORY", "No memory export");
                        data.set_last_error(err);
                        return Ok(vec![WasmValue::from_i32(-1)]);
                    }
                };

                if let Err(e) = memory.set_data(value_bytes, out_ptr) {
                    let err = ConnectorErrorV1::internal(
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
            let err = ConnectorErrorV1::internal("STATE_BACKEND", &e.to_string());
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
            let err = ConnectorErrorV1::config(
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
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let value = match memory_protocol::read_string_from_guest(frame, val_ptr, val_len) {
        Ok(s) => s,
        Err(e) => {
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let state_key = format!("{}:{}", data.current_stream, key);
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
            let err = ConnectorErrorV1::internal("STATE_BACKEND", &e.to_string());
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

    let kind = args[0].to_i32(); // 0=source, 1=dest
    let payload_ptr = args[1].to_i32();
    let payload_len = args[2].to_i32();

    let payload_bytes = match memory_protocol::read_from_guest(frame, payload_ptr, payload_len) {
        Ok(b) => b,
        Err(e) => {
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Parse the envelope to extract the checkpoint
    match serde_json::from_slice::<serde_json::Value>(&payload_bytes) {
        Ok(envelope) => {
            tracing::debug!(
                pipeline = data.pipeline_name,
                stream = data.current_stream,
                "Received checkpoint: {}",
                serde_json::to_string_pretty(&envelope).unwrap_or_default()
            );
            // Track checkpoint count
            if kind == 0 {
                // Source checkpoint — try to parse and store
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(
                    envelope
                        .get("payload")
                        .cloned()
                        .unwrap_or(envelope.clone()),
                ) {
                    data.source_checkpoints.push(cp);
                }
            } else {
                data.dest_checkpoint_count += 1;
            }
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorErrorV1::internal("PARSE_CHECKPOINT", &e.to_string());
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
            let err = ConnectorErrorV1::internal("MEMORY_READ", &e.to_string());
            data.set_last_error(err);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    match serde_json::from_slice::<serde_json::Value>(&payload_bytes) {
        Ok(metric_val) => {
            tracing::debug!(
                pipeline = data.pipeline_name,
                stream = data.current_stream,
                "Received metric: {}",
                serde_json::to_string(&metric_val).unwrap_or_default()
            );
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            let err = ConnectorErrorV1::internal("PARSE_METRIC", &e.to_string());
            data.set_last_error(err);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

// ============================================================
// v0 host functions (kept during transition)
// ============================================================

/// v0: emit record batch with stream name.
pub fn host_emit_record_batch(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let stream_ptr = args[0].to_i32();
    let stream_len = args[1].to_i32();
    let batch_ptr = args[2].to_i32();
    let batch_len = args[3].to_i32();

    let stream_name = match memory_protocol::read_string_from_guest(frame, stream_ptr, stream_len)
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("host_emit_record_batch: failed to read stream name: {}", e);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let batch_bytes = match memory_protocol::read_from_guest(frame, batch_ptr, batch_len) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("host_emit_record_batch: failed to read batch bytes: {}", e);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    // Try v0 sender first, then v1 sender
    if let Some(sender) = &data.batch_sender_v0 {
        match sender.send((stream_name, batch_bytes)) {
            Ok(()) => return Ok(vec![WasmValue::from_i32(0)]),
            Err(e) => {
                tracing::error!("host_emit_record_batch: channel send failed: {}", e);
                return Ok(vec![WasmValue::from_i32(-1)]);
            }
        }
    }

    if let Some(sender) = &data.batch_sender {
        match sender.send(batch_bytes) {
            Ok(()) => Ok(vec![WasmValue::from_i32(0)]),
            Err(e) => {
                tracing::error!("host_emit_record_batch: v1 channel send failed: {}", e);
                Ok(vec![WasmValue::from_i32(-1)])
            }
        }
    } else {
        tracing::error!("host_emit_record_batch: no sender configured");
        Ok(vec![WasmValue::from_i32(-1)])
    }
}

/// v0: get state (broken — doesn't write value to guest memory).
pub fn host_get_state(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let key_ptr = args[0].to_i32();
    let key_len = args[1].to_i32();
    let _out_ptr = args[2].to_i32();
    let _out_len = args[3].to_i32();

    let key = match memory_protocol::read_string_from_guest(frame, key_ptr, key_len) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("host_get_state: failed to read key: {}", e);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let state_key = format!("{}:{}", data.current_stream, key);
    match data
        .state_backend
        .get_cursor(&data.pipeline_name, &state_key)
    {
        Ok(Some(cursor)) => {
            if cursor.cursor_value.is_some() {
                Ok(vec![WasmValue::from_i32(1)])
            } else {
                Ok(vec![WasmValue::from_i32(0)])
            }
        }
        Ok(None) => Ok(vec![WasmValue::from_i32(0)]),
        Err(e) => {
            tracing::error!("host_get_state: state backend error: {}", e);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// v0: set state.
pub fn host_set_state(
    data: &mut HostState,
    _inst: &mut SysInstance,
    frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let key_ptr = args[0].to_i32();
    let key_len = args[1].to_i32();
    let val_ptr = args[2].to_i32();
    let val_len = args[3].to_i32();

    let key = match memory_protocol::read_string_from_guest(frame, key_ptr, key_len) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("host_set_state: failed to read key: {}", e);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let value = match memory_protocol::read_string_from_guest(frame, val_ptr, val_len) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("host_set_state: failed to read value: {}", e);
            return Ok(vec![WasmValue::from_i32(-1)]);
        }
    };

    let state_key = format!("{}:{}", data.current_stream, key);
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
            tracing::error!("host_set_state: state backend error: {}", e);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// v0: log.
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
    let stream = &data.current_stream;

    match level {
        0 => tracing::error!(pipeline, stream, "[connector] {}", message),
        1 => tracing::warn!(pipeline, stream, "[connector] {}", message),
        2 => tracing::info!(pipeline, stream, "[connector] {}", message),
        3 => tracing::debug!(pipeline, stream, "[connector] {}", message),
        _ => tracing::trace!(pipeline, stream, "[connector] {}", message),
    }

    Ok(vec![WasmValue::from_i32(0)])
}

/// v0: report progress.
pub fn host_report_progress(
    data: &mut HostState,
    _inst: &mut SysInstance,
    _frame: &mut CallingFrame,
    args: Vec<WasmValue>,
) -> Result<Vec<WasmValue>, CoreError> {
    let records = args[0].to_i64() as u64;
    let bytes = args[1].to_i64() as u64;

    if let Ok(mut stats) = data.stats.lock() {
        stats.records_read += records;
        stats.bytes_read += bytes;
    }

    Ok(vec![WasmValue::from_i32(0)])
}
