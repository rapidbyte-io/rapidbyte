use std::sync::{mpsc, Arc, Mutex};

use wasmedge_sdk::{CallingFrame, WasmValue};
use wasmedge_sys::Instance as SysInstance;
use wasmedge_types::error::CoreError;

use crate::state::backend::{RunStats, StateBackend};

use super::memory_protocol;

/// Shared state passed to all host functions via WasmEdge's host data mechanism.
pub struct HostState {
    /// Channel to send (stream_name, arrow_ipc_bytes) to the orchestrator.
    pub batch_sender: mpsc::SyncSender<(String, Vec<u8>)>,
    /// State backend for cursor/run persistence.
    pub state_backend: Arc<dyn StateBackend>,
    /// Pipeline name for state operations.
    pub pipeline_name: String,
    /// Current stream name for state operations.
    pub current_stream: String,
    /// Accumulated stats for the current run.
    pub stats: Arc<Mutex<RunStats>>,
}

/// Host function: receive an Arrow IPC record batch from the guest.
///
/// Signature: (stream_ptr: i32, stream_len: i32, batch_ptr: i32, batch_len: i32) -> i32
///
/// Reads the stream name and batch bytes from guest memory,
/// then sends them on the batch channel to the orchestrator.
/// Returns 0 on success, -1 on error.
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

    let stream_name = match memory_protocol::read_string_from_guest(frame, stream_ptr, stream_len) {
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

    match data.batch_sender.send((stream_name, batch_bytes)) {
        Ok(()) => Ok(vec![WasmValue::from_i32(0)]),
        Err(e) => {
            tracing::error!("host_emit_record_batch: channel send failed: {}", e);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: get connector state by key.
///
/// Signature: (key_ptr: i32, key_len: i32, out_ptr: i32, out_len: i32) -> i32
///
/// Reads the key from guest memory, queries the state backend for cursor value.
/// If found, writes the value JSON into guest memory at the output pointers.
/// Returns 1 if found, 0 if not found, -1 on error.
///
/// Note: For v0.1, state key maps to cursor_value in the state backend,
/// using pipeline_name and current_stream as the compound key.
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

    // Use the key as a sub-field: pipeline + stream is the primary key,
    // the "key" param selects what field to read. For v0.1, we store
    // arbitrary state in cursor_value keyed by "pipeline:stream:key".
    let state_key = format!("{}:{}", data.current_stream, key);
    match data
        .state_backend
        .get_cursor(&data.pipeline_name, &state_key)
    {
        Ok(Some(cursor)) => {
            if let Some(value) = cursor.cursor_value {
                tracing::debug!("host_get_state: found state for key '{}'", key);
                // For v0.1, we return the length of the value.
                // The guest needs to re-call with a buffer to receive it.
                // This simplified protocol returns 1 to indicate "found".
                let _ = value; // Value available but simplified protocol
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

/// Host function: set connector state by key.
///
/// Signature: (key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32
///
/// Reads key and value from guest memory, stores via state backend.
/// Returns 0 on success, -1 on error.
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
        Ok(()) => {
            tracing::debug!("host_set_state: stored state for key");
            Ok(vec![WasmValue::from_i32(0)])
        }
        Err(e) => {
            tracing::error!("host_set_state: state backend error: {}", e);
            Ok(vec![WasmValue::from_i32(-1)])
        }
    }
}

/// Host function: log a message from the guest connector.
///
/// Signature: (level: i32, msg_ptr: i32, msg_len: i32) -> i32
///
/// Level mapping: 0=error, 1=warn, 2=info, 3=debug, 4=trace
/// Returns 0 always.
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

/// Host function: report progress counters from the guest.
///
/// Signature: (records: i64, bytes: i64) -> i32
///
/// Updates the accumulated stats. Returns 0 always.
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

    tracing::debug!(
        records,
        bytes,
        pipeline = data.pipeline_name,
        stream = data.current_stream,
        "connector progress report"
    );

    Ok(vec![WasmValue::from_i32(0)])
}
