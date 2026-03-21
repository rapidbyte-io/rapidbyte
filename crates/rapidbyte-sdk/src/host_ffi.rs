//! Guest-side host import wrappers for the component model.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

#[cfg(target_arch = "wasm32")]
use crate::checkpoint::CheckpointKind;
use crate::checkpoint::{Checkpoint, StateScope};
#[cfg(target_arch = "wasm32")]
use crate::error::{BackoffClass, CommitState, ErrorScope};
use crate::error::{ErrorCategory, PluginError};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use rapidbyte_types::batch::BatchMetadata;

#[cfg(target_arch = "wasm32")]
mod bindings {
    wit_bindgen::generate!({
        path: "../../wit",
        world: "rapidbyte-host",
    });
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

/// Host imports abstraction to make host FFI behavior testable on native targets.
pub trait HostImports: Send + Sync {
    fn log(&self, level: i32, message: &str);

    // V3 frame lifecycle
    fn frame_new(&self, capacity: u64) -> Result<u64, PluginError>;
    fn frame_write(&self, handle: u64, chunk: &[u8]) -> Result<u64, PluginError>;
    fn frame_seal(&self, handle: u64) -> Result<(), PluginError>;
    fn frame_len(&self, handle: u64) -> Result<u64, PluginError>;
    fn frame_read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, PluginError>;
    fn frame_drop(&self, handle: u64);

    // V3 batch transport (handle-based)
    fn emit_batch(&self, handle: u64) -> Result<(), PluginError>;
    fn next_batch(&self) -> Result<Option<u64>, PluginError>;

    fn state_get(&self, scope: StateScope, key: &str) -> Result<Option<String>, PluginError>;
    fn state_put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError>;
    fn checkpoint(
        &self,
        plugin_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), PluginError>;

    // Transactional checkpoints
    fn checkpoint_begin(&self, kind: u32) -> Result<u64, PluginError>;
    fn checkpoint_set_cursor(
        &self,
        txn: u64,
        stream_name: &str,
        cursor_field: &str,
        cursor_value_json: &str,
    ) -> Result<(), PluginError>;
    fn checkpoint_set_state(
        &self,
        txn: u64,
        scope: u32,
        key: &str,
        value: &str,
    ) -> Result<(), PluginError>;
    fn checkpoint_commit(&self, txn: u64, records: u64, bytes: u64) -> Result<(), PluginError>;
    fn checkpoint_abort(&self, txn: u64);

    // Cancellation
    fn is_cancelled(&self) -> bool;
    fn current_stream_name(&self) -> String;

    // Stream errors
    fn stream_error(&self, stream_index: u32, error: &PluginError) -> Result<(), PluginError>;

    // Batch with metadata
    fn emit_batch_with_metadata(
        &self,
        handle: u64,
        metadata: &BatchMetadata,
    ) -> Result<(), PluginError>;
    fn next_batch_metadata(&self, handle: u64) -> Result<BatchMetadata, PluginError>;

    fn counter_add(&self, name: &str, value: u64, labels_json: &str) -> Result<(), PluginError>;
    fn gauge_set(&self, name: &str, value: f64, labels_json: &str) -> Result<(), PluginError>;
    fn histogram_record(
        &self,
        name: &str,
        value: f64,
        labels_json: &str,
    ) -> Result<(), PluginError>;
    fn emit_dlq_record(
        &self,
        stream_name: &str,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), PluginError>;

    fn connect_tcp(&self, host: &str, port: u16) -> Result<u64, PluginError>;
    fn socket_read(&self, handle: u64, len: u64) -> Result<SocketReadResult, PluginError>;
    fn socket_write(&self, handle: u64, data: &[u8]) -> Result<SocketWriteResult, PluginError>;
    fn socket_close(&self, handle: u64);
}

/// FFI helper: convert `StateScope` to the WIT-generated `StateScope` enum.
#[cfg(target_arch = "wasm32")]
fn to_wit_state_scope(scope: StateScope) -> bindings::rapidbyte::plugin::types::StateScope {
    use bindings::rapidbyte::plugin::types::StateScope as WitStateScope;
    match scope {
        StateScope::Pipeline => WitStateScope::Pipeline,
        StateScope::Stream => WitStateScope::PerStream,
        StateScope::PluginInstance => WitStateScope::PluginInstance,
    }
}

/// FFI helper: convert `u32` kind to the WIT-generated `CheckpointKind` enum.
#[cfg(target_arch = "wasm32")]
fn to_wit_checkpoint_kind(kind: u32) -> bindings::rapidbyte::plugin::types::CheckpointKind {
    use bindings::rapidbyte::plugin::types::CheckpointKind as WitCK;
    match kind {
        0 => WitCK::Source,
        1 => WitCK::Destination,
        2 => WitCK::Transform,
        _ => WitCK::Source, // fallback
    }
}

/// FFI helper: convert SDK `PluginError` to WIT-generated `PluginError`.
#[cfg(target_arch = "wasm32")]
fn to_component_error(err: &PluginError) -> bindings::rapidbyte::plugin::types::PluginError {
    use bindings::rapidbyte::plugin::types as ct;

    ct::PluginError {
        category: match err.category {
            ErrorCategory::Config => ct::ErrorCategory::Config,
            ErrorCategory::Auth => ct::ErrorCategory::Auth,
            ErrorCategory::Permission => ct::ErrorCategory::Permission,
            ErrorCategory::RateLimit => ct::ErrorCategory::RateLimit,
            ErrorCategory::TransientNetwork => ct::ErrorCategory::TransientNetwork,
            ErrorCategory::TransientDb => ct::ErrorCategory::TransientDb,
            ErrorCategory::Data => ct::ErrorCategory::Data,
            ErrorCategory::Schema => ct::ErrorCategory::Schema,
            ErrorCategory::Internal => ct::ErrorCategory::Internal,
            ErrorCategory::Frame => ct::ErrorCategory::Frame,
            ErrorCategory::Cancelled => ct::ErrorCategory::Cancelled,
            _ => ct::ErrorCategory::Internal,
        },
        scope: match err.scope {
            ErrorScope::Stream => ct::ErrorScope::PerStream,
            ErrorScope::Batch => ct::ErrorScope::PerBatch,
            ErrorScope::Record => ct::ErrorScope::PerRecord,
        },
        code: err.code.clone(),
        message: err.message.clone(),
        retryable: err.retryable,
        retry_after_ms: err.retry_after_ms,
        backoff_class: match err.backoff_class {
            BackoffClass::Fast => ct::BackoffClass::Fast,
            BackoffClass::Normal => ct::BackoffClass::Normal,
            BackoffClass::Slow => ct::BackoffClass::Slow,
        },
        safe_to_retry: err.safe_to_retry,
        commit_state: err.commit_state.map(|s| match s {
            CommitState::BeforeCommit => ct::CommitState::BeforeCommit,
            CommitState::AfterCommitUnknown => ct::CommitState::AfterCommitUnknown,
            CommitState::AfterCommitConfirmed => ct::CommitState::AfterCommitConfirmed,
        }),
        details_json: err
            .details
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok()),
    }
}

static HOST_IMPORTS: OnceLock<Box<dyn HostImports>> = OnceLock::new();
static REPORTED_STREAM_ERRORS: OnceLock<std::sync::Mutex<BTreeSet<u32>>> = OnceLock::new();

fn reported_stream_errors() -> &'static std::sync::Mutex<BTreeSet<u32>> {
    REPORTED_STREAM_ERRORS.get_or_init(|| std::sync::Mutex::new(BTreeSet::new()))
}

#[cfg(any(all(test, not(target_arch = "wasm32")), feature = "test-support"))]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq)]
    pub enum MetricCall {
        Counter {
            name: String,
            value: u64,
            labels_json: String,
        },
        Gauge {
            name: String,
            value: f64,
            labels_json: String,
        },
        Histogram {
            name: String,
            value: f64,
            labels_json: String,
        },
    }

    type NextBatchFrame = (u64, Vec<u8>, BatchMetadata);

    static METRIC_CALLS: OnceLock<Mutex<Vec<MetricCall>>> = OnceLock::new();
    static METRIC_ERROR: OnceLock<Mutex<Option<PluginError>>> = OnceLock::new();
    static NEXT_BATCH_FRAME: OnceLock<Mutex<Option<NextBatchFrame>>> = OnceLock::new();
    static DROPPED_HANDLES: OnceLock<Mutex<Vec<u64>>> = OnceLock::new();
    static NEXT_BATCH_METADATA_CALLS: OnceLock<Mutex<Vec<u64>>> = OnceLock::new();

    fn metric_calls() -> &'static Mutex<Vec<MetricCall>> {
        METRIC_CALLS.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn metric_error() -> &'static Mutex<Option<PluginError>> {
        METRIC_ERROR.get_or_init(|| Mutex::new(None))
    }

    fn next_batch_frame() -> &'static Mutex<Option<NextBatchFrame>> {
        NEXT_BATCH_FRAME.get_or_init(|| Mutex::new(None))
    }

    fn dropped_handles() -> &'static Mutex<Vec<u64>> {
        DROPPED_HANDLES.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn next_batch_metadata_calls() -> &'static Mutex<Vec<u64>> {
        NEXT_BATCH_METADATA_CALLS.get_or_init(|| Mutex::new(Vec::new()))
    }

    pub fn reset() {
        metric_calls()
            .lock()
            .expect("metric calls lock poisoned")
            .clear();
        *metric_error().lock().expect("metric error lock poisoned") = None;
        *next_batch_frame()
            .lock()
            .expect("next batch frame lock poisoned") = None;
        dropped_handles()
            .lock()
            .expect("dropped handles lock poisoned")
            .clear();
        next_batch_metadata_calls()
            .lock()
            .expect("metadata calls lock poisoned")
            .clear();
        reported_stream_errors()
            .lock()
            .expect("reported stream errors lock poisoned")
            .clear();
    }

    pub fn set_metric_error(error: PluginError) {
        *metric_error().lock().expect("metric error lock poisoned") = Some(error);
    }

    fn take_metric_error() -> Option<PluginError> {
        metric_error()
            .lock()
            .expect("metric error lock poisoned")
            .clone()
    }

    pub fn take_metric_calls() -> Vec<MetricCall> {
        let mut calls = metric_calls().lock().expect("metric calls lock poisoned");
        std::mem::take(&mut *calls)
    }

    pub fn set_next_batch_frame(handle: u64, bytes: Vec<u8>, metadata: BatchMetadata) {
        *next_batch_frame()
            .lock()
            .expect("next batch frame lock poisoned") = Some((handle, bytes, metadata));
    }

    pub fn take_dropped_handles() -> Vec<u64> {
        let mut handles = dropped_handles()
            .lock()
            .expect("dropped handles lock poisoned");
        std::mem::take(&mut *handles)
    }

    pub fn take_next_batch_metadata_calls() -> Vec<u64> {
        let mut calls = next_batch_metadata_calls()
            .lock()
            .expect("metadata calls lock poisoned");
        std::mem::take(&mut *calls)
    }

    #[derive(Default)]
    pub struct RecordingHostImports;

    impl HostImports for RecordingHostImports {
        fn log(&self, _level: i32, _message: &str) {}

        fn frame_new(&self, _capacity: u64) -> Result<u64, PluginError> {
            Ok(1)
        }

        fn frame_write(&self, _handle: u64, _chunk: &[u8]) -> Result<u64, PluginError> {
            Ok(0)
        }

        fn frame_seal(&self, _handle: u64) -> Result<(), PluginError> {
            Ok(())
        }

        fn frame_len(&self, _handle: u64) -> Result<u64, PluginError> {
            Ok(next_batch_frame()
                .lock()
                .expect("next batch frame lock poisoned")
                .as_ref()
                .map_or(0, |(_, bytes, _)| bytes.len() as u64))
        }

        fn frame_read(&self, handle: u64, _offset: u64, _len: u64) -> Result<Vec<u8>, PluginError> {
            let guard = next_batch_frame()
                .lock()
                .expect("next batch frame lock poisoned");
            let bytes = guard
                .as_ref()
                .filter(|(configured_handle, _, _)| *configured_handle == handle)
                .map(|(_, bytes, _)| bytes.clone())
                .unwrap_or_default();
            Ok(bytes)
        }

        fn frame_drop(&self, handle: u64) {
            dropped_handles()
                .lock()
                .expect("dropped handles lock poisoned")
                .push(handle);
            let mut frame = next_batch_frame()
                .lock()
                .expect("next batch frame lock poisoned");
            if frame
                .as_ref()
                .is_some_and(|(configured_handle, _, _)| *configured_handle == handle)
            {
                *frame = None;
            }
        }

        fn emit_batch(&self, _handle: u64) -> Result<(), PluginError> {
            Ok(())
        }

        fn next_batch(&self) -> Result<Option<u64>, PluginError> {
            Ok(next_batch_frame()
                .lock()
                .expect("next batch frame lock poisoned")
                .as_ref()
                .map(|(handle, _, _)| *handle))
        }

        fn state_get(&self, _scope: StateScope, _key: &str) -> Result<Option<String>, PluginError> {
            Ok(None)
        }

        fn state_put(
            &self,
            _scope: StateScope,
            _key: &str,
            _value: &str,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn checkpoint(
            &self,
            _plugin_id: &str,
            _stream_name: &str,
            _cp: &Checkpoint,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn checkpoint_begin(&self, _kind: u32) -> Result<u64, PluginError> {
            Ok(1)
        }

        fn checkpoint_set_cursor(
            &self,
            _txn: u64,
            _stream_name: &str,
            _cursor_field: &str,
            _cursor_value_json: &str,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn checkpoint_set_state(
            &self,
            _txn: u64,
            _scope: u32,
            _key: &str,
            _value: &str,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn checkpoint_commit(
            &self,
            _txn: u64,
            _records: u64,
            _bytes: u64,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn checkpoint_abort(&self, _txn: u64) {}

        fn is_cancelled(&self) -> bool {
            false
        }

        fn current_stream_name(&self) -> String {
            "test-stream".to_string()
        }

        fn stream_error(
            &self,
            _stream_index: u32,
            _error: &PluginError,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn emit_batch_with_metadata(
            &self,
            _handle: u64,
            _metadata: &BatchMetadata,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn next_batch_metadata(&self, handle: u64) -> Result<BatchMetadata, PluginError> {
            next_batch_metadata_calls()
                .lock()
                .expect("metadata calls lock poisoned")
                .push(handle);
            let frame = next_batch_frame()
                .lock()
                .expect("next batch frame lock poisoned");
            frame
                .as_ref()
                .filter(|(configured_handle, _, _)| *configured_handle == handle)
                .map(|(_, _, metadata)| metadata.clone())
                .ok_or_else(|| PluginError::internal("TEST_METADATA", "missing batch metadata"))
        }

        fn counter_add(
            &self,
            name: &str,
            value: u64,
            labels_json: &str,
        ) -> Result<(), PluginError> {
            if let Some(error) = take_metric_error() {
                return Err(error);
            }
            metric_calls()
                .lock()
                .expect("metric calls lock poisoned")
                .push(MetricCall::Counter {
                    name: name.to_owned(),
                    value,
                    labels_json: labels_json.to_owned(),
                });
            Ok(())
        }

        fn gauge_set(&self, name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
            if let Some(error) = take_metric_error() {
                return Err(error);
            }
            metric_calls()
                .lock()
                .expect("metric calls lock poisoned")
                .push(MetricCall::Gauge {
                    name: name.to_owned(),
                    value,
                    labels_json: labels_json.to_owned(),
                });
            Ok(())
        }

        fn histogram_record(
            &self,
            name: &str,
            value: f64,
            labels_json: &str,
        ) -> Result<(), PluginError> {
            if let Some(error) = take_metric_error() {
                return Err(error);
            }
            metric_calls()
                .lock()
                .expect("metric calls lock poisoned")
                .push(MetricCall::Histogram {
                    name: name.to_owned(),
                    value,
                    labels_json: labels_json.to_owned(),
                });
            Ok(())
        }

        fn emit_dlq_record(
            &self,
            _stream_name: &str,
            _record_json: &str,
            _error_message: &str,
            _error_category: ErrorCategory,
        ) -> Result<(), PluginError> {
            Ok(())
        }

        fn connect_tcp(&self, _host: &str, _port: u16) -> Result<u64, PluginError> {
            Err(PluginError::internal("STUB", "No-op stub"))
        }

        fn socket_read(&self, _handle: u64, _len: u64) -> Result<SocketReadResult, PluginError> {
            Ok(SocketReadResult::Eof)
        }

        fn socket_write(
            &self,
            _handle: u64,
            data: &[u8],
        ) -> Result<SocketWriteResult, PluginError> {
            Ok(SocketWriteResult::Written(data.len() as u64))
        }

        fn socket_close(&self, _handle: u64) {}
    }
}

fn default_host_imports() -> Box<dyn HostImports> {
    #[cfg(target_arch = "wasm32")]
    {
        Box::new(WasmHostImports)
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    {
        Box::new(test_support::RecordingHostImports)
    }

    #[cfg(all(not(test), not(target_arch = "wasm32")))]
    {
        Box::new(StubHostImports)
    }
}

/// Returns the active host imports implementation.
///
/// This is exposed as `pub` so that `Context` can call host imports
/// directly for stream-indexed operations.
pub fn host_imports() -> &'static dyn HostImports {
    HOST_IMPORTS.get_or_init(default_host_imports).as_ref()
}

/// Installs a custom host imports implementation.
///
/// This is primarily intended for tests and native simulation.
///
/// # Errors
///
/// Returns `Err` if the host imports have already been initialized.
pub fn set_host_imports(imports: Box<dyn HostImports>) -> Result<(), Box<dyn HostImports>> {
    HOST_IMPORTS.set(imports)
}

#[cfg(target_arch = "wasm32")]
fn from_component_error(err: bindings::rapidbyte::plugin::types::PluginError) -> PluginError {
    use bindings::rapidbyte::plugin::types as ct;

    PluginError {
        category: match err.category {
            ct::ErrorCategory::Config => ErrorCategory::Config,
            ct::ErrorCategory::Auth => ErrorCategory::Auth,
            ct::ErrorCategory::Permission => ErrorCategory::Permission,
            ct::ErrorCategory::RateLimit => ErrorCategory::RateLimit,
            ct::ErrorCategory::TransientNetwork => ErrorCategory::TransientNetwork,
            ct::ErrorCategory::TransientDb => ErrorCategory::TransientDb,
            ct::ErrorCategory::Data => ErrorCategory::Data,
            ct::ErrorCategory::Schema => ErrorCategory::Schema,
            ct::ErrorCategory::Internal => ErrorCategory::Internal,
            ct::ErrorCategory::Frame => ErrorCategory::Frame,
            ct::ErrorCategory::Cancelled => ErrorCategory::Cancelled,
        },
        scope: match err.scope {
            ct::ErrorScope::PerStream => ErrorScope::Stream,
            ct::ErrorScope::PerBatch => ErrorScope::Batch,
            ct::ErrorScope::PerRecord => ErrorScope::Record,
        },
        code: err.code.into(),
        message: err.message,
        retryable: err.retryable,
        retry_after_ms: err.retry_after_ms,
        backoff_class: match err.backoff_class {
            ct::BackoffClass::Fast => BackoffClass::Fast,
            ct::BackoffClass::Normal => BackoffClass::Normal,
            ct::BackoffClass::Slow => BackoffClass::Slow,
        },
        safe_to_retry: err.safe_to_retry,
        commit_state: err.commit_state.map(|s| match s {
            ct::CommitState::BeforeCommit => CommitState::BeforeCommit,
            ct::CommitState::AfterCommitUnknown => CommitState::AfterCommitUnknown,
            ct::CommitState::AfterCommitConfirmed => CommitState::AfterCommitConfirmed,
        }),
        details: err
            .details_json
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
    }
}

#[cfg(target_arch = "wasm32")]
pub struct WasmHostImports;

#[cfg(target_arch = "wasm32")]
impl HostImports for WasmHostImports {
    fn log(&self, level: i32, message: &str) {
        bindings::rapidbyte::plugin::host::log(level as u32, message);
    }

    fn frame_new(&self, capacity: u64) -> Result<u64, PluginError> {
        bindings::rapidbyte::plugin::host::frame_new(capacity).map_err(from_component_error)
    }

    fn frame_write(&self, handle: u64, chunk: &[u8]) -> Result<u64, PluginError> {
        bindings::rapidbyte::plugin::host::frame_write(handle, chunk).map_err(from_component_error)
    }

    fn frame_seal(&self, handle: u64) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::frame_seal(handle).map_err(from_component_error)
    }

    fn frame_len(&self, handle: u64) -> Result<u64, PluginError> {
        bindings::rapidbyte::plugin::host::frame_len(handle).map_err(from_component_error)
    }

    fn frame_read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, PluginError> {
        bindings::rapidbyte::plugin::host::frame_read(handle, offset, len)
            .map_err(from_component_error)
    }

    fn frame_drop(&self, handle: u64) {
        bindings::rapidbyte::plugin::host::frame_drop(handle);
    }

    fn emit_batch(&self, handle: u64) -> Result<(), PluginError> {
        let metadata = bindings::rapidbyte::plugin::types::BatchMetadata {
            stream_index: 0,
            schema_fingerprint: None,
            sequence_number: 0,
            compression: None,
            record_count: 0,
            byte_count: 0,
        };
        bindings::rapidbyte::plugin::host::emit_batch(handle, &metadata)
            .map_err(from_component_error)
    }

    fn next_batch(&self) -> Result<Option<u64>, PluginError> {
        bindings::rapidbyte::plugin::host::next_batch().map_err(from_component_error)
    }

    fn state_get(&self, scope: StateScope, key: &str) -> Result<Option<String>, PluginError> {
        bindings::rapidbyte::plugin::host::state_get(to_wit_state_scope(scope), key)
            .map_err(from_component_error)
    }

    fn state_put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::state_put(to_wit_state_scope(scope), key, value)
            .map_err(from_component_error)
    }

    fn checkpoint(
        &self,
        _plugin_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), PluginError> {
        // v7: `checkpoint` removed from WIT; use transactional checkpoint API.
        let wit_kind = match cp.kind {
            CheckpointKind::Source => bindings::rapidbyte::plugin::types::CheckpointKind::Source,
            CheckpointKind::Dest => bindings::rapidbyte::plugin::types::CheckpointKind::Destination,
            CheckpointKind::Transform => {
                bindings::rapidbyte::plugin::types::CheckpointKind::Transform
            }
        };

        let txn = bindings::rapidbyte::plugin::host::checkpoint_begin(wit_kind)
            .map_err(from_component_error)?;

        let result = (|| {
            if let (Some(field), Some(value)) = (&cp.cursor_field, &cp.cursor_value) {
                let value_json = serde_json::to_string(value)
                    .map_err(|e| PluginError::internal("SERIALIZE_CURSOR", e.to_string()))?;
                let cursor = bindings::rapidbyte::plugin::types::CursorUpdate {
                    stream_name: stream_name.to_string(),
                    cursor_field: field.clone(),
                    cursor_value_json: value_json,
                };
                bindings::rapidbyte::plugin::host::checkpoint_set_cursor(txn, &cursor)
                    .map_err(from_component_error)?;
            }

            bindings::rapidbyte::plugin::host::checkpoint_commit(
                txn,
                cp.records_processed,
                cp.bytes_processed,
            )
            .map_err(from_component_error)
        })();

        if result.is_err() {
            bindings::rapidbyte::plugin::host::checkpoint_abort(txn);
        }

        result
    }

    fn counter_add(&self, name: &str, value: u64, labels_json: &str) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::counter_add(name, value, labels_json)
            .map_err(from_component_error)
    }

    fn gauge_set(&self, name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::gauge_set(name, value, labels_json)
            .map_err(from_component_error)
    }

    fn histogram_record(
        &self,
        name: &str,
        value: f64,
        labels_json: &str,
    ) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::histogram_record(name, value, labels_json)
            .map_err(from_component_error)
    }

    fn emit_dlq_record(
        &self,
        stream_name: &str,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::emit_dlq_record(
            stream_name,
            record_json,
            error_message,
            error_category.as_str(),
        )
        .map_err(from_component_error)
    }

    fn checkpoint_begin(&self, kind: u32) -> Result<u64, PluginError> {
        bindings::rapidbyte::plugin::host::checkpoint_begin(to_wit_checkpoint_kind(kind))
            .map_err(from_component_error)
    }

    fn checkpoint_set_cursor(
        &self,
        txn: u64,
        stream_name: &str,
        cursor_field: &str,
        cursor_value_json: &str,
    ) -> Result<(), PluginError> {
        let cursor = bindings::rapidbyte::plugin::types::CursorUpdate {
            stream_name: stream_name.to_string(),
            cursor_field: cursor_field.to_string(),
            cursor_value_json: cursor_value_json.to_string(),
        };
        bindings::rapidbyte::plugin::host::checkpoint_set_cursor(txn, &cursor)
            .map_err(from_component_error)
    }

    fn checkpoint_set_state(
        &self,
        txn: u64,
        scope: u32,
        key: &str,
        value: &str,
    ) -> Result<(), PluginError> {
        let wit_scope = match scope {
            0 => bindings::rapidbyte::plugin::types::StateScope::Pipeline,
            1 => bindings::rapidbyte::plugin::types::StateScope::PerStream,
            _ => bindings::rapidbyte::plugin::types::StateScope::PluginInstance,
        };
        let mutation = bindings::rapidbyte::plugin::types::StateMutation {
            scope: wit_scope,
            key: key.to_string(),
            value: value.to_string(),
        };
        bindings::rapidbyte::plugin::host::checkpoint_set_state(txn, &mutation)
            .map_err(from_component_error)
    }

    fn checkpoint_commit(&self, txn: u64, records: u64, bytes: u64) -> Result<(), PluginError> {
        bindings::rapidbyte::plugin::host::checkpoint_commit(txn, records, bytes)
            .map_err(from_component_error)
    }

    fn checkpoint_abort(&self, txn: u64) {
        bindings::rapidbyte::plugin::host::checkpoint_abort(txn);
    }

    fn is_cancelled(&self) -> bool {
        bindings::rapidbyte::plugin::host::is_cancelled()
    }

    fn current_stream_name(&self) -> String {
        bindings::rapidbyte::plugin::host::current_stream_name()
    }

    fn stream_error(&self, stream_index: u32, error: &PluginError) -> Result<(), PluginError> {
        let wit_error = to_component_error(error);
        bindings::rapidbyte::plugin::host::stream_error(stream_index, &wit_error)
            .map_err(from_component_error)
    }

    fn emit_batch_with_metadata(
        &self,
        handle: u64,
        metadata: &BatchMetadata,
    ) -> Result<(), PluginError> {
        let wit_metadata = bindings::rapidbyte::plugin::types::BatchMetadata {
            stream_index: metadata.stream_index,
            schema_fingerprint: metadata.schema_fingerprint.clone(),
            sequence_number: metadata.sequence_number,
            compression: metadata.compression.clone(),
            record_count: metadata.record_count,
            byte_count: metadata.byte_count,
        };
        bindings::rapidbyte::plugin::host::emit_batch(handle, &wit_metadata)
            .map_err(from_component_error)
    }

    fn next_batch_metadata(&self, handle: u64) -> Result<BatchMetadata, PluginError> {
        let result = bindings::rapidbyte::plugin::host::next_batch_metadata(handle)
            .map_err(from_component_error)?;
        Ok(BatchMetadata {
            stream_index: result.stream_index,
            schema_fingerprint: result.schema_fingerprint,
            sequence_number: result.sequence_number,
            compression: result.compression,
            record_count: result.record_count,
            byte_count: result.byte_count,
        })
    }

    fn connect_tcp(&self, host: &str, port: u16) -> Result<u64, PluginError> {
        bindings::rapidbyte::plugin::host::connect_tcp(host, port).map_err(from_component_error)
    }

    fn socket_read(&self, handle: u64, len: u64) -> Result<SocketReadResult, PluginError> {
        let result = bindings::rapidbyte::plugin::host::socket_read(handle, len)
            .map_err(from_component_error)?;
        Ok(match result {
            bindings::rapidbyte::plugin::types::SocketReadResult::Data(data) => {
                SocketReadResult::Data(data)
            }
            bindings::rapidbyte::plugin::types::SocketReadResult::Eof => SocketReadResult::Eof,
            bindings::rapidbyte::plugin::types::SocketReadResult::WouldBlock => {
                SocketReadResult::WouldBlock
            }
        })
    }

    fn socket_write(&self, handle: u64, data: &[u8]) -> Result<SocketWriteResult, PluginError> {
        let result = bindings::rapidbyte::plugin::host::socket_write(handle, data)
            .map_err(from_component_error)?;
        Ok(match result {
            bindings::rapidbyte::plugin::types::SocketWriteResult::Written(n) => {
                SocketWriteResult::Written(n)
            }
            bindings::rapidbyte::plugin::types::SocketWriteResult::WouldBlock => {
                SocketWriteResult::WouldBlock
            }
        })
    }

    fn socket_close(&self, handle: u64) {
        bindings::rapidbyte::plugin::host::socket_close(handle)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct StubHostImports;

#[cfg(not(target_arch = "wasm32"))]
impl HostImports for StubHostImports {
    fn log(&self, _level: i32, _message: &str) {}

    fn frame_new(&self, _capacity: u64) -> Result<u64, PluginError> {
        Ok(1)
    }

    fn frame_write(&self, _handle: u64, _chunk: &[u8]) -> Result<u64, PluginError> {
        Ok(0)
    }

    fn frame_seal(&self, _handle: u64) -> Result<(), PluginError> {
        Ok(())
    }

    fn frame_len(&self, _handle: u64) -> Result<u64, PluginError> {
        Ok(0)
    }

    fn frame_read(&self, _handle: u64, _offset: u64, _len: u64) -> Result<Vec<u8>, PluginError> {
        Ok(vec![])
    }

    fn frame_drop(&self, _handle: u64) {}

    fn emit_batch(&self, _handle: u64) -> Result<(), PluginError> {
        Ok(())
    }

    fn next_batch(&self) -> Result<Option<u64>, PluginError> {
        Ok(None)
    }

    fn state_get(&self, _scope: StateScope, _key: &str) -> Result<Option<String>, PluginError> {
        Ok(None)
    }

    fn state_put(&self, _scope: StateScope, _key: &str, _value: &str) -> Result<(), PluginError> {
        Ok(())
    }

    fn checkpoint(
        &self,
        _plugin_id: &str,
        _stream_name: &str,
        _cp: &Checkpoint,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn checkpoint_begin(&self, _kind: u32) -> Result<u64, PluginError> {
        Ok(1)
    }

    fn checkpoint_set_cursor(
        &self,
        _txn: u64,
        _stream_name: &str,
        _cursor_field: &str,
        _cursor_value_json: &str,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn checkpoint_set_state(
        &self,
        _txn: u64,
        _scope: u32,
        _key: &str,
        _value: &str,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn checkpoint_commit(&self, _txn: u64, _records: u64, _bytes: u64) -> Result<(), PluginError> {
        Ok(())
    }

    fn checkpoint_abort(&self, _txn: u64) {}

    fn is_cancelled(&self) -> bool {
        false
    }

    fn current_stream_name(&self) -> String {
        String::new()
    }

    fn stream_error(&self, _stream_index: u32, _error: &PluginError) -> Result<(), PluginError> {
        Ok(())
    }

    fn emit_batch_with_metadata(
        &self,
        _handle: u64,
        _metadata: &BatchMetadata,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn next_batch_metadata(&self, _handle: u64) -> Result<BatchMetadata, PluginError> {
        Ok(BatchMetadata {
            stream_index: 0,
            schema_fingerprint: None,
            sequence_number: 0,
            compression: None,
            record_count: 0,
            byte_count: 0,
        })
    }

    fn counter_add(&self, _name: &str, _value: u64, _labels_json: &str) -> Result<(), PluginError> {
        Ok(())
    }

    fn gauge_set(&self, _name: &str, _value: f64, _labels_json: &str) -> Result<(), PluginError> {
        Ok(())
    }

    fn histogram_record(
        &self,
        _name: &str,
        _value: f64,
        _labels_json: &str,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn emit_dlq_record(
        &self,
        _stream_name: &str,
        _record_json: &str,
        _error_message: &str,
        _error_category: ErrorCategory,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    fn connect_tcp(&self, _host: &str, _port: u16) -> Result<u64, PluginError> {
        Err(PluginError::internal("STUB", "No-op stub"))
    }

    fn socket_read(&self, _handle: u64, _len: u64) -> Result<SocketReadResult, PluginError> {
        Ok(SocketReadResult::Eof)
    }

    fn socket_write(&self, _handle: u64, data: &[u8]) -> Result<SocketWriteResult, PluginError> {
        Ok(SocketWriteResult::Written(data.len() as u64))
    }

    fn socket_close(&self, _handle: u64) {}
}

pub fn log(level: i32, message: &str) {
    host_imports().log(level, message)
}

/// Log an error message through the host runtime.
pub fn log_error(message: &str) {
    log(0, message);
}

/// Emit an Arrow RecordBatch to the host pipeline via V3 frame transport.
///
/// Streams IPC encoding directly into a host frame via `FrameWriter`,
/// eliminating the guest-side `Vec<u8>` IPC buffer allocation.
///
/// # Errors
///
/// Returns `Err` if frame creation, IPC encoding, or frame sealing fails.
pub fn emit_batch(batch: &RecordBatch) -> Result<(), PluginError> {
    let imports = host_imports();

    let capacity = batch.get_array_memory_size() as u64 + 1024;
    let handle = imports.frame_new(capacity)?;

    // Stream IPC directly into host frame -- no guest Vec<u8>
    {
        let mut writer = crate::frame_writer::FrameWriter::new(handle, imports);
        crate::arrow::ipc::encode_ipc_into(batch, &mut writer)?;
    }

    imports.frame_seal(handle)?;
    imports.emit_batch(handle)?;
    Ok(())
}

fn decode_next_batch_frame(
    ipc_bytes: &[u8],
    frame_len: u64,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), PluginError> {
    crate::arrow::ipc::decode_ipc(ipc_bytes).map_err(|e| {
        PluginError::internal(
            "NEXT_BATCH_DECODE",
            format!("failed to decode next_batch frame (frame_len={frame_len}): {e}"),
        )
    })
}

/// Decoded result of a single `next_batch` host frame.
#[derive(Debug)]
pub struct DecodedBatch {
    pub metadata: BatchMetadata,
    pub schema: Arc<Schema>,
    pub batches: Vec<RecordBatch>,
    pub decode_secs: f64,
}

/// Receive the next Arrow RecordBatch from the host pipeline.
///
/// Returns `None` when there are no more batches.
///
/// # Errors
///
/// Returns `Err` if frame reading or IPC decoding fails.
#[allow(clippy::type_complexity)]
pub fn next_batch(max_bytes: u64) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, PluginError> {
    next_batch_with_decode_timing(max_bytes)
        .map(|result| result.map(|decoded| (decoded.schema, decoded.batches)))
}

/// Receive the next Arrow RecordBatch from the host pipeline and report guest-side IPC decode time.
///
/// Returns `None` when there are no more batches.
///
/// # Errors
///
/// Returns `Err` if frame reading or IPC decoding fails.
#[allow(clippy::type_complexity)]
pub fn next_batch_with_decode_timing(max_bytes: u64) -> Result<Option<DecodedBatch>, PluginError> {
    let imports = host_imports();

    let Some(handle) = imports.next_batch()? else {
        return Ok(None);
    };

    let metadata = imports.next_batch_metadata(handle)?;
    let frame_len = imports.frame_len(handle)?;
    if frame_len > max_bytes {
        imports.frame_drop(handle);
        return Err(PluginError::internal(
            "BATCH_TOO_LARGE",
            format!("Batch {frame_len} exceeds max {max_bytes}"),
        ));
    }

    // Read entire frame (inherent WIT boundary copy)
    let ipc_bytes = imports.frame_read(handle, 0, frame_len)?;
    imports.frame_drop(handle);

    let decode_start = Instant::now();
    let (schema, batches) = decode_next_batch_frame(&ipc_bytes, frame_len)?;
    Ok(Some(DecodedBatch {
        metadata,
        schema,
        batches,
        decode_secs: decode_start.elapsed().as_secs_f64(),
    }))
}

/// Retrieve a value from the host state backend.
///
/// # Errors
///
/// Returns `Err` if the host state backend rejects the read.
pub fn state_get(scope: StateScope, key: &str) -> Result<Option<String>, PluginError> {
    host_imports().state_get(scope, key)
}

/// Store a value in the host state backend.
///
/// # Errors
///
/// Returns `Err` if the host state backend rejects the write.
pub fn state_put(scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> {
    host_imports().state_put(scope, key, value)
}

/// Submit a checkpoint to the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host rejects the checkpoint.
pub fn checkpoint(plugin_id: &str, stream_name: &str, cp: &Checkpoint) -> Result<(), PluginError> {
    host_imports().checkpoint(plugin_id, stream_name, cp)
}

/// Begin a transactional checkpoint.
///
/// # Errors
///
/// Returns `Err` if the host rejects the checkpoint begin.
pub fn checkpoint_begin(kind: u32) -> Result<u64, PluginError> {
    host_imports().checkpoint_begin(kind)
}

/// Set cursor position within a transactional checkpoint.
///
/// # Errors
///
/// Returns `Err` if the host rejects the cursor update.
pub fn checkpoint_set_cursor(
    txn: u64,
    stream_name: &str,
    cursor_field: &str,
    cursor_value_json: &str,
) -> Result<(), PluginError> {
    host_imports().checkpoint_set_cursor(txn, stream_name, cursor_field, cursor_value_json)
}

/// Set state within a transactional checkpoint.
///
/// # Errors
///
/// Returns `Err` if the host rejects the state mutation.
pub fn checkpoint_set_state(
    txn: u64,
    scope: u32,
    key: &str,
    value: &str,
) -> Result<(), PluginError> {
    host_imports().checkpoint_set_state(txn, scope, key, value)
}

/// Commit a transactional checkpoint.
///
/// # Errors
///
/// Returns `Err` if the host rejects the commit.
pub fn checkpoint_commit(txn: u64, records: u64, bytes: u64) -> Result<(), PluginError> {
    host_imports().checkpoint_commit(txn, records, bytes)
}

/// Abort a transactional checkpoint.
pub fn checkpoint_abort(txn: u64) {
    host_imports().checkpoint_abort(txn);
}

/// Check if the pipeline has been cancelled.
pub fn is_cancelled() -> bool {
    host_imports().is_cancelled()
}

/// Return the current stream name known to the host runtime.
pub fn current_stream_name() -> String {
    host_imports().current_stream_name()
}

/// Report an error for a specific stream.
///
/// # Errors
///
/// Returns `Err` if the host rejects the stream error report.
pub fn stream_error(stream_index: u32, error: &PluginError) -> Result<(), PluginError> {
    host_imports().stream_error(stream_index, error)?;
    reported_stream_errors()
        .lock()
        .expect("reported stream errors lock poisoned")
        .insert(stream_index);
    Ok(())
}

/// Return whether this stream reported an error since the last query.
#[doc(hidden)]
pub fn take_reported_stream_error(stream_index: u32) -> bool {
    reported_stream_errors()
        .lock()
        .expect("reported stream errors lock poisoned")
        .remove(&stream_index)
}

/// Emit a batch with metadata for a specific stream.
///
/// # Errors
///
/// Returns `Err` if the host rejects the batch emission.
pub fn emit_batch_with_metadata(handle: u64, metadata: &BatchMetadata) -> Result<(), PluginError> {
    host_imports().emit_batch_with_metadata(handle, metadata)
}

/// Log a warning message through the host runtime.
pub fn log_warn(message: &str) {
    log(1, message);
}

/// Add to a counter in the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host rejects the counter update.
pub fn counter_add(name: &str, value: u64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().counter_add(name, value, labels_json)
}

/// Set a gauge value in the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host rejects the gauge update.
pub fn gauge_set(name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().gauge_set(name, value, labels_json)
}

/// Record a histogram observation in the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host rejects the histogram observation.
pub fn histogram_record(name: &str, value: f64, labels_json: &str) -> Result<(), PluginError> {
    host_imports().histogram_record(name, value, labels_json)
}

/// Emit a dead-letter queue record to the host runtime.
///
/// # Errors
///
/// Returns `Err` if DLQ record emission fails.
pub fn emit_dlq_record(
    stream_name: &str,
    record_json: &str,
    error_message: &str,
    error_category: ErrorCategory,
) -> Result<(), PluginError> {
    host_imports().emit_dlq_record(stream_name, record_json, error_message, error_category)
}

/// Open a TCP connection through the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host denies the connection or TCP connect fails.
pub fn connect_tcp(host: &str, port: u16) -> Result<u64, PluginError> {
    host_imports().connect_tcp(host, port)
}

/// Read data from a host-managed socket.
///
/// # Errors
///
/// Returns `Err` if the socket read operation fails.
pub fn socket_read(handle: u64, len: u64) -> Result<SocketReadResult, PluginError> {
    host_imports().socket_read(handle, len)
}

/// Write data to a host-managed socket.
///
/// # Errors
///
/// Returns `Err` if the socket write operation fails.
pub fn socket_write(handle: u64, data: &[u8]) -> Result<SocketWriteResult, PluginError> {
    host_imports().socket_write(handle, data)
}

pub fn socket_close(handle: u64) {
    host_imports().socket_close(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_socket_variants_construct() {
        let _ = SocketReadResult::Eof;
        let _ = SocketReadResult::WouldBlock;
        let _ = SocketWriteResult::WouldBlock;
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_native_stub_next_batch_none() {
        let result = next_batch(1024).expect("next batch");
        assert!(result.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_batch_encodes_and_calls_host() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let result = emit_batch(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_next_batch_frame_invalid_payload_includes_context() {
        let err = decode_next_batch_frame(&[1, 2, 3], 3).expect_err("invalid ipc should fail");
        assert_eq!(err.code, "NEXT_BATCH_DECODE");
        assert!(err.message.contains("frame_len=3"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn stream_error_tracks_reported_stream_once() {
        test_support::reset();

        stream_error(7, &PluginError::internal("TEST", "boom")).expect("stream error should work");

        assert!(take_reported_stream_error(7));
        assert!(!take_reported_stream_error(7));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn next_batch_with_decode_timing_returns_metadata_and_drains_it() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        test_support::reset();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let ipc = crate::arrow::ipc::encode_ipc(&batch).expect("ipc should encode");
        let metadata = BatchMetadata {
            stream_index: 3,
            schema_fingerprint: Some("abc".into()),
            sequence_number: 9,
            compression: None,
            record_count: 2,
            byte_count: ipc.len() as u64,
        };
        test_support::set_next_batch_frame(41, ipc, metadata.clone());

        let decoded = next_batch_with_decode_timing(u64::MAX)
            .expect("next batch should succeed")
            .expect("batch should exist");

        assert_eq!(decoded.metadata, metadata);
        assert_eq!(test_support::take_next_batch_metadata_calls(), vec![41]);
        assert_eq!(test_support::take_dropped_handles(), vec![41]);
    }

    #[test]
    fn test_log_error_does_not_panic() {
        log_error("test error");
    }
}
