//! Test harnesses and fakes for plugin authors.
//!
//! These types are intentionally explicit and lightweight. They let authors
//! exercise plugin logic with deterministic in-memory state instead of the
//! host runtime.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::checkpoint::{Checkpoint, CheckpointKind, StateScope};
use crate::context::LogLevel;
use crate::cursor::CursorValue;
use crate::error::PluginError;
use crate::input::{ReadInput, WriteInput};
use crate::stream::StreamContext;

/// Recorded state-store value for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestStateValue {
    pub scope: StateScope,
    pub key: String,
    pub value: String,
}

/// Recorded log entry for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestLogEntry {
    pub level: LogLevel,
    pub message: String,
}

/// Recorded metric call for assertions.
#[derive(Debug, Clone, PartialEq)]
pub struct TestMetricCall {
    pub kind: TestMetricKind,
    pub name: String,
    pub value: TestMetricValue,
    pub labels: Vec<(String, String)>,
}

/// Metric call kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMetricKind {
    Counter,
    Gauge,
    Histogram,
}

/// Metric value payload.
#[derive(Debug, Clone, PartialEq)]
pub enum TestMetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

/// Recorded checkpoint transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCheckpointRecord {
    pub kind: CheckpointKind,
    pub cursor_updates: Vec<TestCursorUpdate>,
    pub state_mutations: Vec<TestStateMutation>,
    pub records_processed: u64,
    pub bytes_processed: u64,
}

/// Recorded cursor update inside a transactional checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCursorUpdate {
    pub stream_name: String,
    pub cursor_field: String,
    pub cursor_value_json: String,
}

/// Recorded state mutation inside a transactional checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestStateMutation {
    pub scope: StateScope,
    pub key: String,
    pub value: String,
}

/// A test harness that owns all fake capabilities.
#[derive(Debug, Clone, Default)]
pub struct TestHarness {
    emitted_batches: Arc<Mutex<Vec<(u32, RecordBatch)>>>,
    input_batches: Arc<Mutex<VecDeque<(Arc<Schema>, Vec<RecordBatch>)>>>,
    state_values: Arc<Mutex<HashMap<(StateScope, String), String>>>,
    checkpoints: Arc<Mutex<Vec<TestCheckpointRecord>>>,
    cancelled: Arc<AtomicBool>,
    logs: Arc<Mutex<Vec<TestLogEntry>>>,
    metrics: Arc<Mutex<Vec<TestMetricCall>>>,
}

impl TestHarness {
    /// Create a new empty harness.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return all emitted batches recorded by the fake emitter.
    pub fn emitted_batches(&self) -> Vec<(u32, RecordBatch)> {
        self.emitted_batches
            .lock()
            .expect("emitted batches")
            .clone()
    }

    /// Return all queued input batches that have not yet been consumed.
    pub fn queued_input_batches(&self) -> Vec<(Arc<Schema>, Vec<RecordBatch>)> {
        self.input_batches
            .lock()
            .expect("input batches")
            .iter()
            .cloned()
            .collect()
    }

    /// Return the stored state value for a key, if any.
    pub fn state_value(&self, scope: StateScope, key: &str) -> Option<String> {
        self.state_values
            .lock()
            .expect("state values")
            .get(&(scope, key.to_owned()))
            .cloned()
    }

    /// Return the recorded checkpoint transactions.
    pub fn checkpoint_records(&self) -> Vec<TestCheckpointRecord> {
        self.checkpoints.lock().expect("checkpoints").clone()
    }

    /// Return the recorded log entries.
    pub fn log_entries(&self) -> Vec<TestLogEntry> {
        self.logs.lock().expect("logs").clone()
    }

    /// Return the recorded metric calls.
    pub fn metric_calls(&self) -> Vec<TestMetricCall> {
        self.metrics.lock().expect("metrics").clone()
    }

    /// Mark the harness as cancelled.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Build a read input using fake capabilities.
    pub fn read_input(
        &self,
        stream: StreamContext,
    ) -> ReadInput<'static, TestEmit, TestCancel, TestState, TestCheckpoints, TestMetrics, TestLog>
    {
        ReadInput::with_capabilities(
            stream,
            TestEmit {
                emitted_batches: Arc::clone(&self.emitted_batches),
            },
            TestCancel {
                cancelled: Arc::clone(&self.cancelled),
            },
            TestState {
                values: Arc::clone(&self.state_values),
            },
            TestCheckpoints {
                records: Arc::clone(&self.checkpoints),
            },
            TestMetrics {
                metrics: Arc::clone(&self.metrics),
            },
            TestLog {
                logs: Arc::clone(&self.logs),
            },
        )
    }

    /// Build a write input using fake capabilities.
    pub fn write_input(
        &self,
        stream: StreamContext,
    ) -> WriteInput<'static, TestReader, TestCancel, TestState, TestCheckpoints> {
        WriteInput::with_capabilities(
            stream,
            TestReader {
                input_batches: Arc::clone(&self.input_batches),
            },
            TestCancel {
                cancelled: Arc::clone(&self.cancelled),
            },
            TestState {
                values: Arc::clone(&self.state_values),
            },
            TestCheckpoints {
                records: Arc::clone(&self.checkpoints),
            },
        )
    }

    /// Enqueue a batch for the fake reader to return.
    pub fn enqueue_input_batch(&self, schema: Arc<Schema>, batches: Vec<RecordBatch>) {
        self.input_batches
            .lock()
            .expect("input batches")
            .push_back((schema, batches));
    }
}

/// Fake emitter capability.
#[derive(Debug, Clone)]
pub struct TestEmit {
    emitted_batches: Arc<Mutex<Vec<(u32, RecordBatch)>>>,
}

impl TestEmit {
    /// Record an emitted batch.
    pub fn batch_for_stream(
        &self,
        stream_index: u32,
        batch: &RecordBatch,
    ) -> Result<(), PluginError> {
        self.emitted_batches
            .lock()
            .expect("emitted batches")
            .push((stream_index, batch.clone()));
        Ok(())
    }
}

/// Fake reader capability.
#[derive(Debug, Clone)]
pub struct TestReader {
    input_batches: Arc<Mutex<VecDeque<(Arc<Schema>, Vec<RecordBatch>)>>>,
}

impl TestReader {
    /// Pop the next input batch from the queue.
    #[allow(clippy::type_complexity)]
    pub fn next_batch(
        &self,
        _max_bytes: u64,
    ) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, PluginError> {
        Ok(self
            .input_batches
            .lock()
            .expect("input batches")
            .pop_front())
    }
}

/// Fake cancellation capability.
#[derive(Debug, Clone)]
pub struct TestCancel {
    cancelled: Arc<AtomicBool>,
}

impl TestCancel {
    /// Returns `true` when the harness has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Return a cancellation error if the harness has been cancelled.
    pub fn check(&self) -> Result<(), PluginError> {
        if self.is_cancelled() {
            Err(PluginError::cancelled("CANCELLED", "Pipeline cancelled"))
        } else {
            Ok(())
        }
    }
}

/// Fake logging capability.
#[derive(Debug, Clone)]
pub struct TestLog {
    logs: Arc<Mutex<Vec<TestLogEntry>>>,
}

impl TestLog {
    /// Record a log entry.
    pub fn log(&self, level: LogLevel, message: &str) {
        self.logs.lock().expect("logs").push(TestLogEntry {
            level,
            message: message.to_owned(),
        });
    }

    /// Record an info log entry.
    pub fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Record a warning log entry.
    pub fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }

    /// Record an error log entry.
    pub fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }

    /// Record a debug log entry.
    pub fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message);
    }
}

/// Fake metrics capability.
#[derive(Debug, Clone)]
pub struct TestMetrics {
    metrics: Arc<Mutex<Vec<TestMetricCall>>>,
}

impl TestMetrics {
    /// Record a counter increment.
    pub fn counter(&self, name: &str, value: u64) -> Result<(), PluginError> {
        self.counter_with_labels(name, value, &[])
    }

    /// Record a counter increment with labels.
    pub fn counter_with_labels(
        &self,
        name: &str,
        value: u64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        self.metrics.lock().expect("metrics").push(TestMetricCall {
            kind: TestMetricKind::Counter,
            name: name.to_owned(),
            value: TestMetricValue::Counter(value),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        Ok(())
    }

    /// Record a gauge value.
    pub fn gauge(&self, name: &str, value: f64) -> Result<(), PluginError> {
        self.gauge_with_labels(name, value, &[])
    }

    /// Record a gauge value with labels.
    pub fn gauge_with_labels(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        self.metrics.lock().expect("metrics").push(TestMetricCall {
            kind: TestMetricKind::Gauge,
            name: name.to_owned(),
            value: TestMetricValue::Gauge(value),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        Ok(())
    }

    /// Record a histogram value.
    pub fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError> {
        self.histogram_with_labels(name, value, &[])
    }

    /// Record a histogram value with labels.
    pub fn histogram_with_labels(
        &self,
        name: &str,
        value: f64,
        labels: &[(&str, &str)],
    ) -> Result<(), PluginError> {
        self.metrics.lock().expect("metrics").push(TestMetricCall {
            kind: TestMetricKind::Histogram,
            name: name.to_owned(),
            value: TestMetricValue::Histogram(value),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        Ok(())
    }
}

/// Fake state store capability.
#[derive(Debug, Clone)]
pub struct TestState {
    values: Arc<Mutex<HashMap<(StateScope, String), String>>>,
}

impl TestState {
    /// Read a value from the fake state store.
    pub fn get(&self, scope: StateScope, key: &str) -> Result<Option<String>, PluginError> {
        Ok(self
            .values
            .lock()
            .expect("state")
            .get(&(scope, key.to_owned()))
            .cloned())
    }

    /// Write a value to the fake state store.
    pub fn put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> {
        self.values
            .lock()
            .expect("state")
            .insert((scope, key.to_owned()), value.to_owned());
        Ok(())
    }
}

/// Fake checkpoint capability.
#[derive(Debug, Clone)]
pub struct TestCheckpoints {
    records: Arc<Mutex<Vec<TestCheckpointRecord>>>,
}

impl TestCheckpoints {
    /// Begin a fake transactional checkpoint.
    pub fn begin(&self, kind: CheckpointKind) -> Result<TestCheckpointTxn, PluginError> {
        Ok(TestCheckpointTxn {
            kind,
            cursor_updates: Vec::new(),
            state_mutations: Vec::new(),
            records: Arc::clone(&self.records),
            committed: false,
        })
    }

    /// Record a checkpoint in one step.
    pub fn checkpoint(
        &self,
        _plugin_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), PluginError> {
        let mut cursor_updates = Vec::new();
        if let (Some(field), Some(value)) = (&cp.cursor_field, &cp.cursor_value) {
            cursor_updates.push(TestCursorUpdate {
                stream_name: stream_name.to_owned(),
                cursor_field: field.clone(),
                cursor_value_json: serde_json::to_string(value)
                    .map_err(|e| PluginError::internal("SERIALIZE", e.to_string()))?,
            });
        }

        self.records
            .lock()
            .expect("checkpoints")
            .push(TestCheckpointRecord {
                kind: cp.kind,
                cursor_updates,
                state_mutations: Vec::new(),
                records_processed: cp.records_processed,
                bytes_processed: cp.bytes_processed,
            });
        Ok(())
    }
}

/// Fake transactional checkpoint handle.
#[derive(Debug)]
pub struct TestCheckpointTxn {
    kind: CheckpointKind,
    cursor_updates: Vec<TestCursorUpdate>,
    state_mutations: Vec<TestStateMutation>,
    records: Arc<Mutex<Vec<TestCheckpointRecord>>>,
    committed: bool,
}

impl TestCheckpointTxn {
    /// Record a cursor update within the fake transaction.
    pub fn set_cursor(
        &mut self,
        stream: &str,
        field: &str,
        value: CursorValue,
    ) -> Result<(), PluginError> {
        self.cursor_updates.push(TestCursorUpdate {
            stream_name: stream.to_owned(),
            cursor_field: field.to_owned(),
            cursor_value_json: serde_json::to_string(&value)
                .map_err(|e| PluginError::internal("SERIALIZE", e.to_string()))?,
        });
        Ok(())
    }

    /// Record a state mutation within the fake transaction.
    pub fn set_state(
        &mut self,
        scope: StateScope,
        key: &str,
        value: &str,
    ) -> Result<(), PluginError> {
        self.state_mutations.push(TestStateMutation {
            scope,
            key: key.to_owned(),
            value: value.to_owned(),
        });
        Ok(())
    }

    /// Commit the fake transaction and persist the recorded operations.
    pub fn commit(
        mut self,
        records_processed: u64,
        bytes_processed: u64,
    ) -> Result<(), PluginError> {
        self.committed = true;
        let cursor_updates = std::mem::take(&mut self.cursor_updates);
        let state_mutations = std::mem::take(&mut self.state_mutations);
        self.records
            .lock()
            .expect("checkpoints")
            .push(TestCheckpointRecord {
                kind: self.kind,
                cursor_updates,
                state_mutations,
                records_processed,
                bytes_processed,
            });
        Ok(())
    }

    /// Abort the fake transaction.
    pub fn abort(mut self) {
        self.committed = true;
    }
}

impl Drop for TestCheckpointTxn {
    fn drop(&mut self) {
        if !self.committed {
            self.committed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap()
    }

    #[test]
    fn testing_harness_builds_read_input_with_fakes() {
        let harness = TestHarness::new();
        let stream = StreamContext::test_default("users");
        let input: ReadInput<
            '_,
            TestEmit,
            TestCancel,
            TestState,
            TestCheckpoints,
            TestMetrics,
            TestLog,
        > = harness.read_input(stream.clone());

        input.log.info("hello");
        input.metrics.counter("reads", 1).unwrap();
        input.state.put(StateScope::Stream, "last", "42").unwrap();
        input.cancel.check().unwrap();

        let mut txn = input.checkpoints.begin(CheckpointKind::Source).unwrap();
        txn.set_cursor(&stream.stream_name, "id", CursorValue::Int64 { value: 42 })
            .unwrap();
        txn.commit(1, 2).unwrap();

        input.emit.batch_for_stream(7, &test_batch()).unwrap();

        assert_eq!(harness.emitted_batches().len(), 1);
        assert_eq!(harness.log_entries().len(), 1);
        assert_eq!(harness.metric_calls().len(), 1);
        assert_eq!(
            harness.state_value(StateScope::Stream, "last"),
            Some("42".into())
        );
        assert_eq!(harness.checkpoint_records().len(), 1);
    }

    #[test]
    fn testing_harness_builds_write_input_with_fakes_and_input_batches() {
        let harness = TestHarness::new();
        let stream = StreamContext::test_default("orders");
        let batch = test_batch();
        let schema = batch.schema();
        harness.enqueue_input_batch(Arc::clone(&schema), vec![batch.clone()]);

        let input: WriteInput<'_, TestReader, TestCancel, TestState, TestCheckpoints> =
            harness.write_input(stream.clone());

        let next = input.reader.next_batch(1024).unwrap().expect("batch");
        assert_eq!(next.0.as_ref(), schema.as_ref());
        assert_eq!(next.1.len(), 1);

        input.cancel.check().unwrap();
        input.state.put(StateScope::Stream, "offset", "7").unwrap();

        let mut txn = input.checkpoints.begin(CheckpointKind::Dest).unwrap();
        txn.set_state(StateScope::Stream, "offset", "7").unwrap();
        txn.commit(1, 2).unwrap();

        assert_eq!(
            harness.state_value(StateScope::Stream, "offset"),
            Some("7".into())
        );
        assert_eq!(harness.checkpoint_records().len(), 1);
    }

    #[test]
    fn testing_harness_cancel_flag_is_shared() {
        let harness = TestHarness::new();
        let cancel = TestCancel {
            cancelled: Arc::clone(&harness.cancelled),
        };
        harness.cancel();
        assert!(cancel.is_cancelled());
        assert!(cancel.check().is_err());
    }

    #[test]
    fn testing_checkpoint_convenience_preserves_absent_cursor_state() {
        let harness = TestHarness::new();
        let checkpoints = TestCheckpoints {
            records: Arc::clone(&harness.checkpoints),
        };

        checkpoints
            .checkpoint(
                "plugin",
                "users",
                &Checkpoint {
                    id: 1,
                    kind: CheckpointKind::Source,
                    stream: "users".into(),
                    cursor_field: None,
                    cursor_value: None,
                    records_processed: 3,
                    bytes_processed: 9,
                },
            )
            .unwrap();

        let records = harness.checkpoint_records();
        assert_eq!(records.len(), 1);
        assert!(records[0].cursor_updates.is_empty());
    }

    #[test]
    fn dropped_test_checkpoint_txn_does_not_record_checkpoint() {
        let harness = TestHarness::new();
        let checkpoints = TestCheckpoints {
            records: Arc::clone(&harness.checkpoints),
        };

        let mut txn = checkpoints.begin(CheckpointKind::Source).unwrap();
        txn.set_state(StateScope::Stream, "offset", "7").unwrap();
        drop(txn);

        assert!(harness.checkpoint_records().is_empty());
    }
}
