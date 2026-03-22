//! Thin capability wrappers used by the typed plugin inputs.
//!
//! The wrappers intentionally stay small and explicit. They forward directly to
//! the existing host FFI and do not add retry, buffering, or orchestration
//! policy.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use rapidbyte_types::batch::BatchMetadata;

use crate::checkpoint::{Checkpoint, CheckpointKind, StateScope};
use crate::context::LogLevel;
use crate::cursor::CursorValue;
use crate::error::PluginError;
use crate::frame_writer::FrameWriter;
use crate::host_ffi;
use crate::host_tcp::HostTcpStream;

/// Emit batches through the host runtime.
#[derive(Debug, Clone, Copy, Default)]
pub struct Emit;

impl Emit {
    /// Emit a batch tagged with the provided stream index.
    pub fn batch_for_stream(
        &self,
        stream_index: u32,
        batch: &RecordBatch,
    ) -> Result<(), PluginError> {
        let imports = host_ffi::host_imports();
        let capacity = batch.get_array_memory_size() as u64 + 1024;
        let handle = imports.frame_new(capacity)?;

        {
            let mut writer = FrameWriter::new(handle, imports);
            crate::arrow::ipc::encode_ipc_into(batch, &mut writer)?;
        }

        imports.frame_seal(handle)?;
        let metadata = BatchMetadata {
            stream_index,
            schema_fingerprint: None,
            sequence_number: 0,
            compression: None,
            record_count: batch.num_rows() as u32,
            byte_count: batch.get_array_memory_size() as u64,
        };
        imports.emit_batch_with_metadata(handle, &metadata)?;
        Ok(())
    }
}

/// Read the next batch from the host runtime.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reader;

impl Reader {
    /// Read the next batch using the host batch channel.
    #[allow(clippy::type_complexity)]
    pub fn next_batch(
        &self,
        max_bytes: u64,
    ) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, PluginError> {
        host_ffi::next_batch(max_bytes)
    }
}

/// Cooperative cancellation view.
#[derive(Debug, Clone, Copy, Default)]
pub struct Cancel;

impl Cancel {
    /// Returns `true` if the host has cancelled the pipeline.
    pub fn is_cancelled(&self) -> bool {
        host_ffi::is_cancelled()
    }

    /// Return a cancellation error if the pipeline has been cancelled.
    pub fn check(&self) -> Result<(), PluginError> {
        if self.is_cancelled() {
            Err(PluginError::cancelled("CANCELLED", "Pipeline cancelled"))
        } else {
            Ok(())
        }
    }
}

/// Logging capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct Log;

impl Log {
    /// Write a log entry at the given severity.
    pub fn log(&self, level: LogLevel, message: &str) {
        host_ffi::log(level as i32, message);
    }

    /// Write an error log entry.
    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }

    /// Write a warning log entry.
    pub fn warn(&self, message: &str) {
        host_ffi::log_warn(message);
    }

    /// Write an info log entry.
    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Write a debug log entry.
    pub fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message);
    }
}

/// Metrics capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics;

impl Metrics {
    /// Add to a counter metric.
    pub fn counter(&self, name: &str, value: u64) -> Result<(), PluginError> {
        self.counter_with_labels(name, value, &[])
    }

    /// Add to a counter metric with extra labels.
    pub fn counter_with_labels(
        &self,
        name: &str,
        value: u64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        host_ffi::counter_add(name, value, &labels_json(labels))
    }

    /// Set a gauge metric.
    pub fn gauge(&self, name: &str, value: f64) -> Result<(), PluginError> {
        self.gauge_with_labels(name, value, &[])
    }

    /// Set a gauge metric with extra labels.
    pub fn gauge_with_labels(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        host_ffi::gauge_set(name, value, &labels_json(labels))
    }

    /// Record a histogram observation.
    pub fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError> {
        self.histogram_with_labels(name, value, &[])
    }

    /// Record a histogram observation with extra labels.
    pub fn histogram_with_labels(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        host_ffi::histogram_record(name, value, &labels_json(labels))
    }
}

/// State-store capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct State;

impl State {
    /// Read a value from the host state store.
    pub fn get(&self, scope: StateScope, key: &str) -> Result<Option<String>, PluginError> {
        host_ffi::state_get(scope, key)
    }

    /// Write a value to the host state store.
    pub fn put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> {
        host_ffi::state_put(scope, key, value)
    }
}

/// Checkpoint capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct Checkpoints;

impl Checkpoints {
    /// Begin a transactional checkpoint.
    pub fn begin(&self, kind: CheckpointKind) -> Result<CheckpointTxn, PluginError> {
        let handle = host_ffi::checkpoint_begin(kind as u32)?;
        Ok(CheckpointTxn::new(handle))
    }

    /// Emit a checkpoint in one step using the provided identifiers.
    pub fn checkpoint(
        &self,
        plugin_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), PluginError> {
        host_ffi::checkpoint(plugin_id, stream_name, cp)
    }
}

/// Transactional checkpoint handle used by [`Checkpoints`].
#[derive(Debug)]
pub struct CheckpointTxn {
    handle: u64,
    committed: bool,
}

impl CheckpointTxn {
    fn new(handle: u64) -> Self {
        Self {
            handle,
            committed: false,
        }
    }

    /// Set a cursor position within this checkpoint transaction.
    pub fn set_cursor(
        &mut self,
        stream: &str,
        field: &str,
        value: CursorValue,
    ) -> Result<(), PluginError> {
        let value_json = serde_json::to_string(&value)
            .map_err(|e| PluginError::internal("SERIALIZE", e.to_string()))?;
        host_ffi::checkpoint_set_cursor(self.handle, stream, field, &value_json)
    }

    /// Set state within this checkpoint transaction.
    pub fn set_state(
        &mut self,
        scope: StateScope,
        key: &str,
        value: &str,
    ) -> Result<(), PluginError> {
        host_ffi::checkpoint_set_state(self.handle, scope as u32, key, value)
    }

    /// Commit the checkpoint transaction with record/byte counts.
    pub fn commit(mut self, records: u64, bytes: u64) -> Result<(), PluginError> {
        self.committed = true;
        host_ffi::checkpoint_commit(self.handle, records, bytes)
    }

    /// Explicitly abort the checkpoint transaction.
    pub fn abort(mut self) {
        self.committed = true;
        host_ffi::checkpoint_abort(self.handle);
    }
}

impl Drop for CheckpointTxn {
    fn drop(&mut self) {
        if !self.committed {
            host_ffi::log_warn("CheckpointTxn dropped without commit - aborting");
            host_ffi::checkpoint_abort(self.handle);
        }
    }
}

/// Network capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct Network;

impl Network {
    /// Open a host-proxied TCP connection.
    pub fn connect_tcp(&self, host: &str, port: u16) -> Result<HostTcpStream, PluginError> {
        HostTcpStream::connect(host, port)
    }
}

/// Build JSON labels for metrics in a deterministic order.
fn labels_json(labels: &[(&str, &str)]) -> String {
    let mut map = BTreeMap::new();
    for (key, value) in labels {
        if is_reserved_metric_label(key) {
            continue;
        }
        map.insert(
            (*key).to_owned(),
            serde_json::Value::String((*value).to_owned()),
        );
    }
    let map: serde_json::Map<String, serde_json::Value> = map.into_iter().collect();
    serde_json::Value::Object(map).to_string()
}

fn is_reserved_metric_label(label: &str) -> bool {
    matches!(label, "pipeline" | "run" | "plugin" | "stream" | "shard")
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn emit_batch_for_stream_uses_host_transport() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).expect("batch");

        let emit = Emit;
        emit.batch_for_stream(7, &batch).expect("emit");
    }

    #[test]
    fn reader_next_batch_is_thin() {
        let reader = Reader;
        let result = reader.next_batch(1024).expect("next batch");
        assert!(result.is_none());
    }

    #[test]
    fn cancel_and_log_delegate() {
        let cancel = Cancel;
        assert!(!cancel.is_cancelled());
        cancel.check().expect("not cancelled");

        let log = Log;
        log.info("hello");
        log.warn("hello");
        log.debug("hello");
        log.error("hello");
    }

    #[test]
    fn metrics_delegate_with_explicit_labels() {
        let metrics = Metrics;
        metrics.counter("reads", 1).expect("counter");
        metrics
            .gauge_with_labels(
                "rows_per_second",
                42.0,
                &[("phase", "scan"), ("plugin", "ignored")],
            )
            .expect("gauge");
        metrics.histogram("latency_secs", 0.5).expect("histogram");
    }

    #[test]
    fn state_checkpoint_and_network_delegate() {
        let state = State;
        state
            .put(StateScope::Stream, "key", "value")
            .expect("state put");
        assert!(state
            .get(StateScope::Stream, "key")
            .expect("state get")
            .is_none());

        let checkpoints = Checkpoints;
        let txn = checkpoints.begin(CheckpointKind::Source).expect("begin");
        txn.commit(0, 0).expect("commit");

        let network = Network;
        assert_eq!(std::mem::size_of_val(&network), 0);
    }
}
