//! Plugin execution context.
//!
//! `Context` bundles stream metadata with host-FFI operations so plugin
//! authors no longer need to pass `plugin_id` / `stream_name` to every call.

use arrow::record_batch::RecordBatch;

use rapidbyte_types::batch::BatchMetadata;

use crate::checkpoint::{Checkpoint, CheckpointKind, StateScope};
use crate::cursor::CursorValue;
use crate::error::{ErrorCategory, PluginError};
use crate::host_ffi;

/// Log severity levels used by [`Context::log`].
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

/// Execution context carrying stream metadata and host-FFI delegation.
///
/// Plugin authors receive a `Context` at each lifecycle entry-point.
/// Methods that require `plugin_id` or `stream_name` (checkpoints,
/// metrics, DLQ records) automatically supply the values stored here.
///
/// ```ignore
/// ctx.log(LogLevel::Info, "starting read");
/// ctx.checkpoint(&cp)?;
/// ```
#[derive(Debug, Clone)]
pub struct Context {
    plugin_id: String,
    stream_name: String,
}

impl Context {
    /// Create a new context for the given plugin and stream.
    pub fn new(plugin_id: impl Into<String>, stream_name: impl Into<String>) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            stream_name: stream_name.into(),
        }
    }

    /// Returns the plugin identifier.
    pub fn plugin_id(&self) -> &str {
        &self.plugin_id
    }

    /// Returns the current stream name.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Derive a new `Context` that targets a different stream while keeping the
    /// same plugin identity.
    pub fn with_stream(&self, stream_name: impl Into<String>) -> Self {
        Self {
            plugin_id: self.plugin_id.clone(),
            stream_name: stream_name.into(),
        }
    }

    // ------------------------------------------------------------------
    // Host-FFI delegation
    // ------------------------------------------------------------------

    /// Write a log message at the given severity level.
    pub fn log(&self, level: LogLevel, message: &str) {
        host_ffi::log(level as i32, message);
    }

    /// Emit an Arrow `RecordBatch` to the host pipeline with explicit metadata.
    pub fn emit_batch(
        &self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> Result<(), PluginError> {
        host_ffi::emit_batch(batch, metadata)
    }

    /// Receive the next Arrow `RecordBatch` from the host pipeline.
    ///
    /// Returns `None` when there are no more batches.
    pub fn next_batch(
        &self,
        max_bytes: u64,
    ) -> Result<Option<host_ffi::DecodedBatch>, PluginError> {
        host_ffi::next_batch(max_bytes)
    }

    /// Read a value from the host state store.
    pub fn state_get(&self, scope: StateScope, key: &str) -> Result<Option<String>, PluginError> {
        host_ffi::state_get(scope, key)
    }

    /// Write a value to the host state store.
    pub fn state_put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> {
        host_ffi::state_put(scope, key, value)
    }

    /// Emit a checkpoint using the context's plugin ID and stream name.
    ///
    /// Uses the transactional checkpoint API internally.
    pub fn checkpoint(&self, cp: &Checkpoint) -> Result<(), PluginError> {
        let mut txn = self.begin_checkpoint(cp.kind)?;
        if let (Some(field), Some(value)) = (&cp.cursor_field, &cp.cursor_value) {
            txn.set_cursor(&cp.stream, field, value.clone())?;
        }
        txn.commit(cp.records_processed, cp.bytes_processed)
    }

    /// Check if the pipeline has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        host_ffi::is_cancelled()
    }

    /// Check cancellation, returning Err if cancelled. Use with `?` in loops.
    pub fn check_cancelled(&self) -> Result<(), PluginError> {
        if self.is_cancelled() {
            Err(PluginError::cancelled("CANCELLED", "Pipeline cancelled"))
        } else {
            Ok(())
        }
    }

    /// Report an error for a specific stream (multi-stream sources).
    pub fn stream_error(&self, stream_index: u32, error: PluginError) -> Result<(), PluginError> {
        host_ffi::stream_error(stream_index, &error)
    }

    /// Begin a transactional checkpoint.
    pub fn begin_checkpoint(&self, kind: CheckpointKind) -> Result<CheckpointTxn, PluginError> {
        let handle = host_ffi::checkpoint_begin(kind as u32)?;
        Ok(CheckpointTxn::new(handle))
    }

    /// Add to a counter metric.
    pub fn counter(
        &self,
        name: &str,
        value: u64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        let labels_json = self.merge_default_labels(labels);
        host_ffi::counter_add(name, value, &labels_json)
    }

    /// Set a gauge metric.
    pub fn gauge(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        let labels_json = self.merge_default_labels(labels);
        host_ffi::gauge_set(name, value, &labels_json)
    }

    /// Record a histogram observation.
    pub fn histogram(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        let labels_json = self.merge_default_labels(labels);
        host_ffi::histogram_record(name, value, &labels_json)
    }

    fn merge_default_labels(&self, extra: &[(&str, &str)]) -> String {
        let mut map = serde_json::Map::new();
        map.insert(
            "plugin".to_owned(),
            serde_json::Value::String(self.plugin_id.clone()),
        );
        map.insert(
            "stream".to_owned(),
            serde_json::Value::String(self.stream_name.clone()),
        );
        for (k, v) in extra {
            if is_reserved_metric_label(k) {
                continue;
            }
            map.insert((*k).to_owned(), serde_json::Value::String((*v).to_owned()));
        }
        serde_json::Value::Object(map).to_string()
    }

    /// Emit a dead-letter-queue record using the context's stream name.
    pub fn emit_dlq_record(
        &self,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), PluginError> {
        host_ffi::emit_dlq_record(
            &self.stream_name,
            record_json,
            error_message,
            error_category,
        )
    }
}

/// A transactional checkpoint handle.
///
/// Groups cursor updates and state mutations into an atomic unit.
/// If dropped without calling [`commit`](Self::commit) or [`abort`](Self::abort),
/// the checkpoint is automatically aborted.
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
        self.committed = true; // Prevent drop from aborting again
        host_ffi::checkpoint_abort(self.handle);
    }
}

impl Drop for CheckpointTxn {
    fn drop(&mut self) {
        if !self.committed {
            host_ffi::log_warn("CheckpointTxn dropped without commit — aborting");
            host_ffi::checkpoint_abort(self.handle);
        }
    }
}

fn is_reserved_metric_label(label: &str) -> bool {
    matches!(label, "pipeline" | "run" | "plugin" | "stream" | "shard")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_ffi::test_support::{self, MetricCall};
    use std::sync::Arc;

    #[test]
    fn test_new_stores_metadata() {
        let ctx = Context::new("my-plugin", "users");
        assert_eq!(ctx.plugin_id(), "my-plugin");
        assert_eq!(ctx.stream_name(), "users");
    }

    #[test]
    fn test_with_stream_changes_stream() {
        let ctx = Context::new("my-plugin", "users");
        let ctx2 = ctx.with_stream("orders");
        assert_eq!(ctx2.plugin_id(), "my-plugin");
        assert_eq!(ctx2.stream_name(), "orders");
        // Original is unchanged.
        assert_eq!(ctx.stream_name(), "users");
    }

    #[test]
    fn test_with_stream_accepts_string() {
        let ctx = Context::new("c", "a");
        let ctx2 = ctx.with_stream(String::from("b"));
        assert_eq!(ctx2.stream_name(), "b");
    }

    #[test]
    fn test_clone_is_independent() {
        let ctx = Context::new("c1", "s1");
        let ctx2 = ctx.clone();
        assert_eq!(ctx.plugin_id(), ctx2.plugin_id());
        assert_eq!(ctx.stream_name(), ctx2.stream_name());
    }

    #[test]
    fn test_log_level_repr() {
        assert_eq!(LogLevel::Error as i32, 0);
        assert_eq!(LogLevel::Warn as i32, 1);
        assert_eq!(LogLevel::Info as i32, 2);
        assert_eq!(LogLevel::Debug as i32, 3);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_log_does_not_panic() {
        let ctx = Context::new("c", "s");
        // Should delegate to the stub without panicking.
        ctx.log(LogLevel::Info, "hello");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_batch_delegates() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let metadata = BatchMetadata {
            stream_index: 0,
            schema_fingerprint: None,
            sequence_number: 0,
            compression: None,
            record_count: batch.num_rows() as u32,
            byte_count: batch.get_array_memory_size() as u64,
        };

        let ctx = Context::new("c", "s");
        let result = ctx.emit_batch(&batch, &metadata);
        assert!(result.is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_next_batch_returns_none() {
        let ctx = Context::new("c", "s");
        let result = ctx.next_batch(1024).expect("next batch");
        assert!(result.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_state_roundtrip_with_stub() {
        let ctx = Context::new("c", "s");
        // Stub state_put succeeds.
        ctx.state_put(StateScope::Stream, "key", "value").unwrap();
        // Stub state_get always returns None.
        let val = ctx.state_get(StateScope::Stream, "key").unwrap();
        assert!(val.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_checkpoint_delegates() {
        use crate::checkpoint::CheckpointKind;

        let ctx = Context::new("my-conn", "my-stream");
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "my-stream".to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 0,
            bytes_processed: 0,
        };
        let result = ctx.checkpoint(&cp);
        assert!(result.is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_typed_metric_delegates_with_expected_labels() {
        let _guard = test_support::metric_test_lock()
            .lock()
            .expect("metric test lock poisoned");
        test_support::reset();
        let ctx = Context::new("test-plugin", "users");
        ctx.counter("records_read", 42, &[]).unwrap();
        ctx.histogram("source_connect_secs", 0.5, &[]).unwrap();
        ctx.gauge("rows_per_second", 1000.0, &[("phase", "scan")])
            .unwrap();

        let calls = test_support::take_metric_calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls[0],
            MetricCall::Counter {
                name: "records_read".to_string(),
                value: 42,
                labels_json: r#"{"plugin":"test-plugin","stream":"users"}"#.to_string(),
            }
        );
        assert_eq!(
            calls[1],
            MetricCall::Histogram {
                name: "source_connect_secs".to_string(),
                value: 0.5,
                labels_json: r#"{"plugin":"test-plugin","stream":"users"}"#.to_string(),
            }
        );
        let MetricCall::Gauge {
            name,
            value,
            labels_json,
        } = &calls[2]
        else {
            panic!("expected gauge metric call");
        };
        assert_eq!(name, "rows_per_second");
        assert!((*value - 1000.0).abs() < f64::EPSILON);
        let gauge_labels: serde_json::Value =
            serde_json::from_str(labels_json).expect("labels json should parse");
        assert_eq!(
            gauge_labels,
            serde_json::json!({
                "plugin": "test-plugin",
                "stream": "users",
                "phase": "scan"
            })
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_reserved_metric_labels_are_not_overridden() {
        let _guard = test_support::metric_test_lock()
            .lock()
            .expect("metric test lock poisoned");
        test_support::reset();
        let ctx = Context::new("test-plugin", "users");
        ctx.counter(
            "records_read",
            1,
            &[
                ("plugin", "spoofed-plugin"),
                ("stream", "spoofed-stream"),
                ("pipeline", "spoofed-pipeline"),
                ("run", "spoofed-run"),
                ("phase", "scan"),
            ],
        )
        .unwrap();

        let calls = test_support::take_metric_calls();
        let MetricCall::Counter { labels_json, .. } = &calls[0] else {
            panic!("expected counter metric call");
        };
        let labels: serde_json::Value =
            serde_json::from_str(labels_json).expect("labels json should parse");
        assert_eq!(
            labels,
            serde_json::json!({
                "plugin": "test-plugin",
                "stream": "users",
                "phase": "scan"
            })
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_typed_metric_methods_surface_host_failures() {
        let _guard = test_support::metric_test_lock()
            .lock()
            .expect("metric test lock poisoned");
        test_support::reset();
        test_support::set_metric_error(PluginError::internal(
            "METRIC_REJECTED",
            "metric rejected by host",
        ));

        let ctx = Context::new("test-plugin", "users");
        let error = ctx
            .counter("records_read", 1, &[])
            .expect_err("counter should return host error");
        assert_eq!(error.code, "METRIC_REJECTED");
        assert!(test_support::take_metric_calls().is_empty());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_dlq_record_delegates() {
        let ctx = Context::new("c", "s");
        let result = ctx.emit_dlq_record("{}", "bad record", ErrorCategory::Data);
        assert!(result.is_ok());
    }
}
