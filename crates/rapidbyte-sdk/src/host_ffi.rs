//! Guest-side host import wrappers for the component model.

use crate::errors::ConnectorError;
#[cfg(target_arch = "wasm32")]
use crate::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};
#[cfg(target_arch = "wasm32")]
use crate::protocol::CheckpointKind;
use crate::protocol::{Checkpoint, Metric, StateScope};

#[cfg(target_arch = "wasm32")]
mod bindings {
    wit_bindgen::generate!({
        path: "../../wit",
        world: "rapidbyte-host",
    });
}

#[cfg(target_arch = "wasm32")]
fn from_component_error(
    err: bindings::rapidbyte::connector::types::ConnectorError,
) -> ConnectorError {
    ConnectorError {
        category: match err.category {
            bindings::rapidbyte::connector::types::ErrorCategory::Config => ErrorCategory::Config,
            bindings::rapidbyte::connector::types::ErrorCategory::Auth => ErrorCategory::Auth,
            bindings::rapidbyte::connector::types::ErrorCategory::Permission => {
                ErrorCategory::Permission
            }
            bindings::rapidbyte::connector::types::ErrorCategory::RateLimit => {
                ErrorCategory::RateLimit
            }
            bindings::rapidbyte::connector::types::ErrorCategory::TransientNetwork => {
                ErrorCategory::TransientNetwork
            }
            bindings::rapidbyte::connector::types::ErrorCategory::TransientDb => {
                ErrorCategory::TransientDb
            }
            bindings::rapidbyte::connector::types::ErrorCategory::Data => ErrorCategory::Data,
            bindings::rapidbyte::connector::types::ErrorCategory::Schema => ErrorCategory::Schema,
            bindings::rapidbyte::connector::types::ErrorCategory::Internal => {
                ErrorCategory::Internal
            }
        },
        scope: match err.scope {
            bindings::rapidbyte::connector::types::ErrorScope::PerStream => ErrorScope::Stream,
            bindings::rapidbyte::connector::types::ErrorScope::PerBatch => ErrorScope::Batch,
            bindings::rapidbyte::connector::types::ErrorScope::PerRecord => ErrorScope::Record,
        },
        code: err.code,
        message: err.message,
        retryable: err.retryable,
        retry_after_ms: err.retry_after_ms,
        backoff_class: match err.backoff_class {
            bindings::rapidbyte::connector::types::BackoffClass::Fast => BackoffClass::Fast,
            bindings::rapidbyte::connector::types::BackoffClass::Normal => BackoffClass::Normal,
            bindings::rapidbyte::connector::types::BackoffClass::Slow => BackoffClass::Slow,
        },
        safe_to_retry: err.safe_to_retry,
        commit_state: err.commit_state.map(|s| match s {
            bindings::rapidbyte::connector::types::CommitState::BeforeCommit => {
                CommitState::BeforeCommit
            }
            bindings::rapidbyte::connector::types::CommitState::AfterCommitUnknown => {
                CommitState::AfterCommitUnknown
            }
            bindings::rapidbyte::connector::types::CommitState::AfterCommitConfirmed => {
                CommitState::AfterCommitConfirmed
            }
        }),
        details: err
            .details_json
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
    }
}

#[cfg(target_arch = "wasm32")]
pub fn log(level: i32, message: &str) {
    bindings::rapidbyte::connector::host::log(level as u32, message);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn log(_level: i32, _message: &str) {}

#[cfg(target_arch = "wasm32")]
pub fn emit_batch(ipc_bytes: &[u8]) -> Result<(), ConnectorError> {
    bindings::rapidbyte::connector::host::emit_batch(ipc_bytes).map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn emit_batch(_ipc_bytes: &[u8]) -> Result<(), ConnectorError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn next_batch(buf: &mut Vec<u8>, max_bytes: u64) -> Result<Option<usize>, ConnectorError> {
    let next = bindings::rapidbyte::connector::host::next_batch().map_err(from_component_error)?;
    match next {
        Some(batch) => {
            if batch.len() as u64 > max_bytes {
                return Err(ConnectorError::internal(
                    "BATCH_TOO_LARGE",
                    format!("Batch {} exceeds max {}", batch.len(), max_bytes),
                ));
            }
            buf.clear();
            buf.extend_from_slice(&batch);
            Ok(Some(batch.len()))
        }
        None => Ok(None),
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn next_batch(_buf: &mut Vec<u8>, _max_bytes: u64) -> Result<Option<usize>, ConnectorError> {
    Ok(None)
}

#[cfg(target_arch = "wasm32")]
pub fn state_get(scope: StateScope, key: &str) -> Result<Option<String>, ConnectorError> {
    bindings::rapidbyte::connector::host::state_get(scope.to_i32() as u32, key)
        .map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn state_get(_scope: StateScope, _key: &str) -> Result<Option<String>, ConnectorError> {
    Ok(None)
}

#[cfg(target_arch = "wasm32")]
pub fn state_put(scope: StateScope, key: &str, value: &str) -> Result<(), ConnectorError> {
    bindings::rapidbyte::connector::host::state_put(scope.to_i32() as u32, key, value)
        .map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn state_put(_scope: StateScope, _key: &str, _value: &str) -> Result<(), ConnectorError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn state_compare_and_set(
    scope: StateScope,
    key: &str,
    expected: Option<&str>,
    new_value: &str,
) -> Result<bool, ConnectorError> {
    bindings::rapidbyte::connector::host::state_cas(scope.to_i32() as u32, key, expected, new_value)
        .map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn state_compare_and_set(
    _scope: StateScope,
    _key: &str,
    _expected: Option<&str>,
    _new_value: &str,
) -> Result<bool, ConnectorError> {
    Ok(false)
}

#[cfg(target_arch = "wasm32")]
pub fn checkpoint(
    connector_id: &str,
    stream_name: &str,
    cp: &Checkpoint,
) -> Result<(), ConnectorError> {
    let kind = match cp.kind {
        CheckpointKind::Source => 0,
        CheckpointKind::Dest => 1,
        CheckpointKind::Transform => 2,
    };

    let envelope = serde_json::json!({
        "protocol_version": "2",
        "connector_id": connector_id,
        "stream_name": stream_name,
        "payload": cp,
    });
    let payload_json = serde_json::to_string(&envelope)
        .map_err(|e| ConnectorError::internal("SERIALIZE_CHECKPOINT", e.to_string()))?;

    bindings::rapidbyte::connector::host::checkpoint(kind, &payload_json)
        .map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn checkpoint(
    _connector_id: &str,
    _stream_name: &str,
    _cp: &Checkpoint,
) -> Result<(), ConnectorError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn metric(connector_id: &str, stream_name: &str, m: &Metric) -> Result<(), ConnectorError> {
    let envelope = serde_json::json!({
        "protocol_version": "2",
        "connector_id": connector_id,
        "stream_name": stream_name,
        "payload": m,
    });
    let payload_json = serde_json::to_string(&envelope)
        .map_err(|e| ConnectorError::internal("SERIALIZE_METRIC", e.to_string()))?;

    bindings::rapidbyte::connector::host::metric(&payload_json).map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn metric(_connector_id: &str, _stream_name: &str, _m: &Metric) -> Result<(), ConnectorError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
pub fn emit_dlq_record(
    stream_name: &str,
    record_json: &str,
    error_message: &str,
    error_category: &str,
) -> Result<(), ConnectorError> {
    bindings::rapidbyte::connector::host::emit_dlq_record(
        stream_name,
        record_json,
        error_message,
        error_category,
    )
    .map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn emit_dlq_record(
    _stream_name: &str,
    _record_json: &str,
    _error_message: &str,
    _error_category: &str,
) -> Result<(), ConnectorError> {
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketReadResult {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketWriteResult {
    Written(u64),
    WouldBlock,
}

#[cfg(target_arch = "wasm32")]
pub fn connect_tcp(host: &str, port: u16) -> Result<u64, ConnectorError> {
    bindings::rapidbyte::connector::host::connect_tcp(host, port).map_err(from_component_error)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn connect_tcp(_host: &str, _port: u16) -> Result<u64, ConnectorError> {
    Err(ConnectorError::internal("STUB", "No-op stub"))
}

#[cfg(target_arch = "wasm32")]
pub fn socket_read(handle: u64, len: u64) -> Result<SocketReadResult, ConnectorError> {
    let result = bindings::rapidbyte::connector::host::socket_read(handle, len)
        .map_err(from_component_error)?;
    Ok(match result {
        bindings::rapidbyte::connector::types::SocketReadResult::Data(data) => {
            SocketReadResult::Data(data)
        }
        bindings::rapidbyte::connector::types::SocketReadResult::Eof => SocketReadResult::Eof,
        bindings::rapidbyte::connector::types::SocketReadResult::WouldBlock => {
            SocketReadResult::WouldBlock
        }
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn socket_read(_handle: u64, _len: u64) -> Result<SocketReadResult, ConnectorError> {
    Ok(SocketReadResult::Eof)
}

#[cfg(target_arch = "wasm32")]
pub fn socket_write(handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError> {
    let result = bindings::rapidbyte::connector::host::socket_write(handle, data)
        .map_err(from_component_error)?;
    Ok(match result {
        bindings::rapidbyte::connector::types::SocketWriteResult::Written(n) => {
            SocketWriteResult::Written(n)
        }
        bindings::rapidbyte::connector::types::SocketWriteResult::WouldBlock => {
            SocketWriteResult::WouldBlock
        }
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub fn socket_write(_handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError> {
    Ok(SocketWriteResult::Written(data.len() as u64))
}

#[cfg(target_arch = "wasm32")]
pub fn socket_close(handle: u64) {
    bindings::rapidbyte::connector::host::socket_close(handle)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn socket_close(_handle: u64) {}
