//! Factory functions for constructing an [`EngineContext`] from concrete
//! infrastructure dependencies.
//!
//! Consumers (CLI, agent, E2E tests) call these functions instead of the
//! now-removed `orchestrator_compat` shim.

use std::sync::Arc;

use tokio::sync::mpsc as tokio_mpsc;

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_registry::RegistryConfig;

use crate::adapter::progress::ChannelProgressReporter;
use crate::adapter::registry_resolver::RegistryPluginResolver;
use crate::adapter::wasm_runner::WasmPluginRunner;
use crate::application::context::{EngineConfig, EngineContext};
use crate::domain::error::PipelineError;
use crate::domain::ports;
use crate::domain::progress::{ProgressEvent, ProgressReporter};

// ---------------------------------------------------------------------------
// Noop implementations for optional ports
// ---------------------------------------------------------------------------

struct NoopProgressReporter;

impl ProgressReporter for NoopProgressReporter {
    fn report(&self, _event: ProgressEvent) {}
}

/// Returns an `Arc<dyn StateBackend>` backed by a no-op implementation.
///
/// Re-exported from `rapidbyte_types` for convenience. Used for contexts
/// that don't need real state persistence (discover, validate).
pub(crate) fn noop_state_backend() -> Arc<dyn rapidbyte_types::state_backend::StateBackend> {
    rapidbyte_types::state_backend::noop_state_backend()
}

struct NoopMetricsSnapshot;

impl ports::MetricsSnapshot for NoopMetricsSnapshot {
    fn snapshot_for_run(&self) -> rapidbyte_metrics::snapshot::PipelineMetricsSnapshot {
        rapidbyte_metrics::snapshot::PipelineMetricsSnapshot::default()
    }
}

// ---------------------------------------------------------------------------
// StateBackend-to-repository adapter
// ---------------------------------------------------------------------------

/// Run a blocking `StateBackend` method on the Tokio blocking pool,
/// converting panics and backend errors into [`RepositoryError`].
async fn run_on_backend<T, F>(
    backend: &Arc<dyn rapidbyte_types::state_backend::StateBackend>,
    op: &str,
    f: F,
) -> Result<T, ports::RepositoryError>
where
    T: Send + 'static,
    F: FnOnce(
            &dyn rapidbyte_types::state_backend::StateBackend,
        ) -> rapidbyte_types::state_error::Result<T>
        + Send
        + 'static,
{
    let backend = Arc::clone(backend);
    tokio::task::spawn_blocking(move || f(&*backend))
        .await
        .map_err(|e| {
            ports::RepositoryError::Other(Box::new(std::io::Error::other(format!(
                "{op} panicked: {e}"
            ))))
        })?
        .map_err(|e| ports::RepositoryError::Other(Box::new(e)))
}

/// Wraps a `StateBackend` trait object to implement the hexagonal
/// repository ports (cursors, runs, DLQ).
struct StateBackendRepositoryAdapter {
    backend: Arc<dyn rapidbyte_types::state_backend::StateBackend>,
}

impl StateBackendRepositoryAdapter {
    fn new(backend: Arc<dyn rapidbyte_types::state_backend::StateBackend>) -> Self {
        Self { backend }
    }
}

#[async_trait::async_trait]
impl ports::CursorRepository for StateBackendRepositoryAdapter {
    async fn get(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
    ) -> Result<Option<rapidbyte_types::state::CursorState>, ports::RepositoryError> {
        let (p, s) = (pipeline.clone(), stream.clone());
        run_on_backend(&self.backend, "cursor get", move |b| b.get_cursor(&p, &s)).await
    }

    async fn set(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
        cursor: &rapidbyte_types::state::CursorState,
    ) -> Result<(), ports::RepositoryError> {
        let (p, s, c) = (pipeline.clone(), stream.clone(), cursor.clone());
        run_on_backend(&self.backend, "cursor set", move |b| {
            b.set_cursor(&p, &s, &c)
        })
        .await
    }

    async fn compare_and_set(
        &self,
        _pipeline: &rapidbyte_types::state::PipelineId,
        _stream: &rapidbyte_types::state::StreamName,
        _expected: Option<&str>,
        _new_value: &str,
    ) -> Result<bool, ports::RepositoryError> {
        Ok(true)
    }
}

#[async_trait::async_trait]
impl ports::RunRecordRepository for StateBackendRepositoryAdapter {
    async fn start(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
    ) -> Result<i64, ports::RepositoryError> {
        let (p, s) = (pipeline.clone(), stream.clone());
        run_on_backend(&self.backend, "start_run", move |b| b.start_run(&p, &s)).await
    }

    #[allow(clippy::similar_names)]
    async fn complete(
        &self,
        run_id: i64,
        status: rapidbyte_types::state::RunStatus,
        stats: &rapidbyte_types::state::RunStats,
    ) -> Result<(), ports::RepositoryError> {
        let run_stats = stats.clone();
        run_on_backend(&self.backend, "complete_run", move |b| {
            b.complete_run(run_id, status, &run_stats)
        })
        .await
    }
}

#[async_trait::async_trait]
impl ports::DlqRepository for StateBackendRepositoryAdapter {
    async fn insert(
        &self,
        _pipeline: &rapidbyte_types::state::PipelineId,
        _run_id: i64,
        _records: &[rapidbyte_types::envelope::DlqRecord],
    ) -> Result<u64, ports::RepositoryError> {
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// Public factory functions
// ---------------------------------------------------------------------------

/// Build a full [`EngineContext`] for pipeline execution.
///
/// This is the canonical way to construct an `EngineContext` from
/// infrastructure dependencies. Used by the CLI `run` command, the
/// agent worker, and E2E tests.
///
/// # Errors
///
/// Returns `PipelineError` if the state backend or WASM runtime cannot
/// be initialised.
#[allow(clippy::too_many_arguments)]
pub async fn build_run_context(
    config: &PipelineConfig,
    progress_tx: Option<tokio_mpsc::UnboundedSender<ProgressEvent>>,
    registry_config: &RegistryConfig,
) -> Result<EngineContext, PipelineError> {
    let state_connection = config
        .state
        .connection
        .clone()
        .ok_or_else(|| PipelineError::infra("state.connection is required (Postgres URL)"))?;
    let pg = crate::adapter::postgres::PgBackend::connect(&state_connection)
        .await
        .map_err(PipelineError::Infrastructure)?;
    pg.migrate().await.map_err(PipelineError::Infrastructure)?;
    let pg = Arc::new(pg);
    let state_backend = pg.as_state_backend();

    let wasm_runtime =
        rapidbyte_runtime::WasmRuntime::new().map_err(PipelineError::Infrastructure)?;
    let runner = Arc::new(WasmPluginRunner::new(wasm_runtime, state_backend.clone()));

    let progress: Arc<dyn ProgressReporter> = match progress_tx {
        Some(tx) => Arc::new(ChannelProgressReporter::new(tx)),
        None => Arc::new(NoopProgressReporter),
    };

    let metrics = Arc::new(NoopMetricsSnapshot);
    let resolver = Arc::new(RegistryPluginResolver::new(registry_config.clone()));
    let dummy = Arc::new(StateBackendRepositoryAdapter::new(state_backend));

    Ok(EngineContext {
        runner,
        resolver,
        cursors: Arc::clone(&dummy) as Arc<dyn ports::CursorRepository>,
        runs: Arc::clone(&dummy) as Arc<dyn ports::RunRecordRepository>,
        dlq: Arc::clone(&dummy) as Arc<dyn ports::DlqRepository>,
        progress,
        metrics,
        config: EngineConfig {
            max_retries: config.resources.max_retries,
            channel_capacity: 64,
        },
    })
}

/// Build a lightweight [`EngineContext`] for check and discover operations.
///
/// # Errors
///
/// Returns `PipelineError` if the state backend or WASM runtime cannot
/// be initialised.
pub async fn build_lightweight_context(
    registry_config: &RegistryConfig,
    config: &PipelineConfig,
) -> Result<EngineContext, PipelineError> {
    build_run_context(config, None, registry_config).await
}

/// Build a lightweight [`EngineContext`] for discover operations that
/// do not have a full pipeline config.
///
/// # Errors
///
/// Returns an error if the state backend or WASM runtime cannot be
/// initialised.
pub async fn build_discover_context(
    registry_config: &RegistryConfig,
) -> Result<EngineContext, anyhow::Error> {
    // Discover context requires a state backend — use an in-memory fake since
    // discover only needs WASM plugin invocation, not real state persistence.
    let state_backend = noop_state_backend();

    let wasm_runtime = rapidbyte_runtime::WasmRuntime::new()?;
    let runner = Arc::new(WasmPluginRunner::new(wasm_runtime, state_backend.clone()));
    let resolver = Arc::new(RegistryPluginResolver::new(registry_config.clone()));
    let dummy = Arc::new(StateBackendRepositoryAdapter::new(state_backend));

    Ok(EngineContext {
        runner,
        resolver,
        cursors: Arc::clone(&dummy) as Arc<dyn ports::CursorRepository>,
        runs: Arc::clone(&dummy) as Arc<dyn ports::RunRecordRepository>,
        dlq: Arc::clone(&dummy) as Arc<dyn ports::DlqRepository>,
        progress: Arc::new(NoopProgressReporter),
        metrics: Arc::new(NoopMetricsSnapshot),
        config: EngineConfig {
            max_retries: 0,
            channel_capacity: 64,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::state::{PipelineId, StreamName};

    #[tokio::test]
    async fn run_on_backend_propagates_result() {
        let backend = noop_state_backend();
        let result = run_on_backend(&backend, "test", |b| {
            b.get_cursor(&PipelineId::new("p"), &StreamName::new("s"))
        })
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // NoopStateBackend returns None
    }

    #[tokio::test]
    async fn run_on_backend_handles_panic() {
        let backend = noop_state_backend();
        let result: Result<(), _> = run_on_backend(&backend, "panic", |_| {
            panic!("test panic");
        })
        .await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("panic"));
    }

    #[tokio::test]
    async fn cas_adapter_stub_returns_true() {
        let backend = noop_state_backend();
        let adapter = StateBackendRepositoryAdapter::new(backend);
        let result = ports::CursorRepository::compare_and_set(
            &adapter,
            &PipelineId::new("p"),
            &StreamName::new("s"),
            Some("old"),
            "new",
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap()); // stub always returns true
    }

    #[tokio::test]
    async fn dlq_adapter_stub_returns_zero() {
        let backend = noop_state_backend();
        let adapter = StateBackendRepositoryAdapter::new(backend);
        let result = ports::DlqRepository::insert(&adapter, &PipelineId::new("p"), 1, &[]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // stub always returns 0
    }

    #[tokio::test]
    async fn cursor_get_returns_none_for_noop_backend() {
        let backend = noop_state_backend();
        let adapter = StateBackendRepositoryAdapter::new(backend);
        let result = ports::CursorRepository::get(
            &adapter,
            &PipelineId::new("pipeline-1"),
            &StreamName::new("stream-1"),
        )
        .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
