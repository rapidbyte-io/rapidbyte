//! In-memory fakes for all engine port traits and a [`TestContext`] factory.
//!
//! Follows the controller pattern (`crates/rapidbyte-controller/src/application/testing.rs`):
//! each fake stores data in simple `Mutex`-protected collections, and the
//! [`fake_context`] function wires everything into an [`EngineContext`].

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rapidbyte_metrics::snapshot::PipelineMetricsSnapshot;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::wire::PluginKind;

use crate::domain::error::PipelineError;
use crate::domain::ports::cursor::CursorRepository;
use crate::domain::ports::dlq::DlqRepository;
use crate::domain::ports::metrics::MetricsSnapshot;
use crate::domain::ports::resolver::{PluginResolver, ResolvedPlugin};
use crate::domain::ports::run_record::RunRecordRepository;
use crate::domain::ports::runner::{
    CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    DiscoveredStream, PluginRunner, SourceOutcome, SourceRunParams, TransformOutcome,
    TransformRunParams, ValidateParams,
};
use crate::domain::ports::RepositoryError;
use crate::domain::progress::{ProgressEvent, ProgressReporter};

use super::context::{EngineConfig, EngineContext};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Create a [`ResolvedPlugin`] with a dummy WASM path and no manifest.
///
/// Useful in unit tests that need a resolved plugin but don't care about
/// the actual WASM binary or manifest contents.
#[must_use]
pub fn test_resolved_plugin() -> ResolvedPlugin {
    ResolvedPlugin {
        wasm_path: std::path::PathBuf::from("/tmp/test.wasm"),
        manifest: None,
    }
}

// ---------------------------------------------------------------------------
// FakePluginRunner
// ---------------------------------------------------------------------------

/// Queue-based fake that returns pre-enqueued results in FIFO order.
pub struct FakePluginRunner {
    source_results: Mutex<VecDeque<Result<SourceOutcome, PipelineError>>>,
    transform_results: Mutex<VecDeque<Result<TransformOutcome, PipelineError>>>,
    dest_results: Mutex<VecDeque<Result<DestinationOutcome, PipelineError>>>,
    validate_results: Mutex<VecDeque<Result<CheckComponentStatus, PipelineError>>>,
    discover_results: Mutex<VecDeque<Result<Vec<DiscoveredStream>, PipelineError>>>,
}

impl FakePluginRunner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            source_results: Mutex::new(VecDeque::new()),
            transform_results: Mutex::new(VecDeque::new()),
            dest_results: Mutex::new(VecDeque::new()),
            validate_results: Mutex::new(VecDeque::new()),
            discover_results: Mutex::new(VecDeque::new()),
        }
    }

    /// Enqueue a source result to be returned by the next `run_source` call.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_source(&self, result: Result<SourceOutcome, PipelineError>) {
        self.source_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a transform result to be returned by the next `run_transform` call.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_transform(&self, result: Result<TransformOutcome, PipelineError>) {
        self.transform_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a destination result to be returned by the next `run_destination` call.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_destination(&self, result: Result<DestinationOutcome, PipelineError>) {
        self.dest_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a validate result to be returned by the next `validate_plugin` call.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_validate(&self, result: Result<CheckComponentStatus, PipelineError>) {
        self.validate_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a discover result to be returned by the next `discover` call.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_discover(&self, result: Result<Vec<DiscoveredStream>, PipelineError>) {
        self.discover_results.lock().unwrap().push_back(result);
    }
}

impl Default for FakePluginRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PluginRunner for FakePluginRunner {
    async fn run_source(&self, _params: SourceRunParams) -> Result<SourceOutcome, PipelineError> {
        self.source_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(PipelineError::infra(
                    "FakePluginRunner: no source result enqueued",
                ))
            })
    }

    async fn run_transform(
        &self,
        _params: TransformRunParams,
    ) -> Result<TransformOutcome, PipelineError> {
        self.transform_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(PipelineError::infra(
                    "FakePluginRunner: no transform result enqueued",
                ))
            })
    }

    async fn run_destination(
        &self,
        _params: DestinationRunParams,
    ) -> Result<DestinationOutcome, PipelineError> {
        self.dest_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(PipelineError::infra(
                    "FakePluginRunner: no destination result enqueued",
                ))
            })
    }

    async fn validate_plugin(
        &self,
        _params: &ValidateParams,
    ) -> Result<CheckComponentStatus, PipelineError> {
        self.validate_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(PipelineError::infra(
                    "FakePluginRunner: no validate result enqueued",
                ))
            })
    }

    async fn discover(
        &self,
        _params: &DiscoverParams,
    ) -> Result<Vec<DiscoveredStream>, PipelineError> {
        self.discover_results
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(PipelineError::infra(
                    "FakePluginRunner: no discover result enqueued",
                ))
            })
    }
}

// ---------------------------------------------------------------------------
// FakePluginResolver
// ---------------------------------------------------------------------------

/// Map-based fake that returns pre-registered plugins by reference string.
pub struct FakePluginResolver {
    plugins: Mutex<HashMap<String, ResolvedPlugin>>,
}

impl FakePluginResolver {
    #[must_use]
    pub fn new() -> Self {
        Self {
            plugins: Mutex::new(HashMap::new()),
        }
    }

    /// Register a plugin for a given reference string.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn register(&self, plugin_ref: &str, plugin: ResolvedPlugin) {
        self.plugins
            .lock()
            .unwrap()
            .insert(plugin_ref.to_string(), plugin);
    }
}

impl Default for FakePluginResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PluginResolver for FakePluginResolver {
    async fn resolve(
        &self,
        plugin_ref: &str,
        _expected_kind: PluginKind,
        _config_json: Option<&serde_json::Value>,
    ) -> Result<ResolvedPlugin, PipelineError> {
        self.plugins
            .lock()
            .unwrap()
            .get(plugin_ref)
            .cloned()
            .ok_or_else(|| {
                PipelineError::infra(format!(
                    "FakePluginResolver: no plugin registered for '{plugin_ref}'"
                ))
            })
    }
}

// ---------------------------------------------------------------------------
// FakeCursorRepository
// ---------------------------------------------------------------------------

/// In-memory cursor store keyed by (pipeline, stream).
pub struct FakeCursorRepository {
    cursors: Mutex<HashMap<(String, String), CursorState>>,
}

impl FakeCursorRepository {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cursors: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for FakeCursorRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CursorRepository for FakeCursorRepository {
    async fn get(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<Option<CursorState>, RepositoryError> {
        let map = self.cursors.lock().unwrap();
        Ok(map
            .get(&(pipeline.to_string(), stream.to_string()))
            .cloned())
    }

    async fn set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> Result<(), RepositoryError> {
        let mut map = self.cursors.lock().unwrap();
        map.insert((pipeline.to_string(), stream.to_string()), cursor.clone());
        Ok(())
    }

    async fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool, RepositoryError> {
        let mut map = self.cursors.lock().unwrap();
        let key = (pipeline.to_string(), stream.to_string());
        let current = map.get(&key).and_then(|c| c.cursor_value.clone());

        if current.as_deref() == expected {
            let cursor = CursorState {
                cursor_field: map.get(&key).and_then(|c| c.cursor_field.clone()),
                cursor_value: Some(new_value.to_string()),
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            map.insert(key, cursor);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// FakeRunRecordRepository
// ---------------------------------------------------------------------------

/// In-memory run record store with auto-incrementing IDs.
pub struct FakeRunRecordRepository {
    next_id: Mutex<i64>,
    runs: Mutex<HashMap<i64, (String, String)>>,
}

impl FakeRunRecordRepository {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_id: Mutex::new(1),
            runs: Mutex::new(HashMap::new()),
        }
    }
}

impl FakeRunRecordRepository {
    /// Return the number of runs that have been started.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn started_count(&self) -> usize {
        self.runs.lock().unwrap().len()
    }
}

impl Default for FakeRunRecordRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RunRecordRepository for FakeRunRecordRepository {
    async fn start(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<i64, RepositoryError> {
        let mut next = self.next_id.lock().unwrap();
        let id = *next;
        *next += 1;
        self.runs
            .lock()
            .unwrap()
            .insert(id, (pipeline.to_string(), stream.to_string()));
        Ok(id)
    }

    async fn complete(
        &self,
        _run_id: i64,
        _status: RunStatus,
        _stats: &RunStats,
    ) -> Result<(), RepositoryError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeDlqRepository
// ---------------------------------------------------------------------------

/// In-memory DLQ store for test assertions.
pub struct FakeDlqRepository {
    records: Mutex<Vec<DlqRecord>>,
}

impl FakeDlqRepository {
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    /// Return all records inserted so far for test assertions.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn inserted_records(&self) -> Vec<DlqRecord> {
        self.records.lock().unwrap().clone()
    }
}

impl Default for FakeDlqRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DlqRepository for FakeDlqRepository {
    async fn insert(
        &self,
        _pipeline: &PipelineId,
        _run_id: i64,
        records: &[DlqRecord],
    ) -> Result<u64, RepositoryError> {
        let mut store = self.records.lock().unwrap();
        let count = records.len() as u64;
        store.extend_from_slice(records);
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// FakeProgressReporter
// ---------------------------------------------------------------------------

/// Collects all progress events for test assertions.
pub struct FakeProgressReporter {
    events: Mutex<Vec<ProgressEvent>>,
}

impl FakeProgressReporter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Return all reported events for test assertions.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn events(&self) -> Vec<ProgressEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Default for FakeProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter for FakeProgressReporter {
    fn report(&self, event: ProgressEvent) {
        self.events.lock().unwrap().push(event);
    }
}

// ---------------------------------------------------------------------------
// FakeMetricsSnapshot
// ---------------------------------------------------------------------------

/// Returns an empty/default metrics snapshot.
pub struct FakeMetricsSnapshot;

impl MetricsSnapshot for FakeMetricsSnapshot {
    fn snapshot_for_run(&self) -> PipelineMetricsSnapshot {
        PipelineMetricsSnapshot::default()
    }
}

// ---------------------------------------------------------------------------
// TestContext + factory
// ---------------------------------------------------------------------------

/// Test harness that exposes both the [`EngineContext`] and typed
/// references to each fake for inspection and pre-loading.
pub struct TestContext {
    /// The wired-up engine context (pass this to use-case functions).
    pub ctx: EngineContext,
    /// Typed reference to the fake plugin runner.
    pub runner: Arc<FakePluginRunner>,
    /// Typed reference to the fake plugin resolver.
    pub resolver: Arc<FakePluginResolver>,
    /// Typed reference to the fake cursor repository.
    pub cursors: Arc<FakeCursorRepository>,
    /// Typed reference to the fake run-record repository.
    pub runs: Arc<FakeRunRecordRepository>,
    /// Typed reference to the fake DLQ repository.
    pub dlq: Arc<FakeDlqRepository>,
    /// Typed reference to the fake progress reporter.
    pub progress: Arc<FakeProgressReporter>,
    /// Typed reference to the fake metrics snapshot.
    pub metrics: Arc<FakeMetricsSnapshot>,
}

/// Build an [`EngineContext`] wired with all in-memory fakes.
///
/// The returned [`TestContext`] exposes typed references to each fake
/// so tests can enqueue results, inspect state, and assert on events.
#[must_use]
pub fn fake_context() -> TestContext {
    let runner = Arc::new(FakePluginRunner::new());
    let resolver = Arc::new(FakePluginResolver::new());
    let cursors = Arc::new(FakeCursorRepository::new());
    let runs = Arc::new(FakeRunRecordRepository::new());
    let dlq = Arc::new(FakeDlqRepository::new());
    let progress = Arc::new(FakeProgressReporter::new());
    let metrics = Arc::new(FakeMetricsSnapshot);

    let ctx = EngineContext {
        runner: Arc::clone(&runner) as Arc<dyn PluginRunner>,
        resolver: Arc::clone(&resolver) as Arc<dyn PluginResolver>,
        cursors: Arc::clone(&cursors) as Arc<dyn CursorRepository>,
        runs: Arc::clone(&runs) as Arc<dyn RunRecordRepository>,
        dlq: Arc::clone(&dlq) as Arc<dyn DlqRepository>,
        progress: Arc::clone(&progress) as Arc<dyn ProgressReporter>,
        metrics: Arc::clone(&metrics) as Arc<dyn MetricsSnapshot>,
        config: EngineConfig {
            max_retries: 0,
            channel_capacity: 64,
        },
    };

    TestContext {
        ctx,
        runner,
        resolver,
        cursors,
        runs,
        dlq,
        progress,
        metrics,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::metric::ReadSummary;

    #[tokio::test]
    async fn fake_context_is_wired_correctly() {
        let tc = fake_context();

        // Enqueue a source result through the typed fake reference.
        let outcome = SourceOutcome {
            duration_secs: 1.23,
            summary: ReadSummary {
                records_read: 42,
                bytes_read: 1024,
                batches_emitted: 1,
                checkpoint_count: 0,
                records_skipped: 0,
            },
            checkpoints: vec![],
        };
        tc.runner.enqueue_source(Ok(outcome));

        // Call through the trait-object in EngineContext.
        // We need a SourceRunParams, but the runner ignores it.
        let params = make_source_params();
        let result = tc.ctx.runner.run_source(params).await;
        assert!(result.is_ok());
        let out = result.unwrap();
        assert_eq!(out.summary.records_read, 42);
    }

    #[tokio::test]
    async fn fake_runner_returns_error_when_queue_empty() {
        let tc = fake_context();
        let params = make_source_params();
        let result = tc.ctx.runner.run_source(params).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fake_cursor_round_trips() {
        let tc = fake_context();
        let pid = PipelineId::new("test-pipe");
        let stream = StreamName::new("public.users");

        let got = tc.ctx.cursors.get(&pid, &stream).await.unwrap();
        assert!(got.is_none());

        let cursor = CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some("2024-01-01".to_string()),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        };
        tc.ctx.cursors.set(&pid, &stream, &cursor).await.unwrap();

        let got = tc.ctx.cursors.get(&pid, &stream).await.unwrap();
        assert!(got.is_some());
        assert_eq!(got.unwrap().cursor_value, Some("2024-01-01".to_string()));
    }

    #[tokio::test]
    async fn fake_progress_collects_events() {
        let tc = fake_context();
        tc.ctx.progress.report(ProgressEvent::StreamStarted {
            stream: "users".to_string(),
        });
        let events = tc.progress.events();
        assert_eq!(events.len(), 1);
    }

    /// Build a minimal `SourceRunParams` for testing. The fake runner
    /// ignores the params, so the values don't matter.
    fn make_source_params() -> SourceRunParams {
        use std::path::PathBuf;
        use std::sync::{mpsc, Arc, Mutex};

        use rapidbyte_runtime::Frame;
        use rapidbyte_types::catalog::SchemaHint;
        use rapidbyte_types::state::RunStats;
        use rapidbyte_types::stream::StreamContext;
        use rapidbyte_types::wire::SyncMode;

        let (tx, _rx) = mpsc::sync_channel::<Frame>(1);
        SourceRunParams {
            wasm_path: PathBuf::from("/tmp/fake.wasm"),
            pipeline_name: "test".to_string(),
            metric_run_label: "test-run".to_string(),
            plugin_id: "fake/source".to_string(),
            plugin_version: "0.1.0".to_string(),
            stream_ctx: StreamContext {
                stream_name: "test".to_string(),
                source_stream_name: None,
                schema: SchemaHint::Columns(vec![]),
                sync_mode: SyncMode::FullRefresh,
                cursor_info: None,
                limits: Default::default(),
                policies: Default::default(),
                write_mode: None,
                selected_columns: None,
                partition_key: None,
                partition_count: None,
                partition_index: None,
                effective_parallelism: None,
                partition_strategy: None,
                copy_flush_bytes_override: None,
            },
            config: serde_json::Value::Null,
            permissions: None,
            compression: None,
            frame_sender: tx,
            stats: Arc::new(Mutex::new(RunStats::default())),
            on_batch_emitted: None,
        }
    }
}
