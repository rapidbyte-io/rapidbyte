//! Compatibility shim — bridges the old `orchestrator::run_pipeline` call
//! signature to the new hexagonal [`EngineContext`] API.
//!
//! This module exists so consumers (CLI, agent, E2E tests) that called
//! `orchestrator::run_pipeline(&config, &opts, progress_tx, cancel, reader, provider, registry)`
//! can migrate with minimal changes.
//!
//! Internally, it constructs the correct [`EngineContext`] and delegates to
//! [`crate::application::run::run_pipeline`].

use std::sync::Arc;

use tokio::sync::mpsc as tokio_mpsc;
use tokio_util::sync::CancellationToken;

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_registry::RegistryConfig;

use crate::adapter::progress::ChannelProgressReporter;
use crate::adapter::registry_resolver::RegistryPluginResolver;
use crate::adapter::wasm_runner::WasmPluginRunner;
use crate::application::context::{EngineConfig, EngineContext};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{CheckResult, ExecutionOptions, PipelineOutcome};
use crate::domain::progress::{ProgressEvent, ProgressReporter};

// ---------------------------------------------------------------------------
// Noop progress reporter (used when no channel is provided)
// ---------------------------------------------------------------------------

struct NoopProgressReporter;

impl ProgressReporter for NoopProgressReporter {
    fn report(&self, _event: ProgressEvent) {}
}

struct NoopMetricsSnapshot;

impl crate::domain::ports::MetricsSnapshot for NoopMetricsSnapshot {
    fn snapshot_for_run(&self) -> rapidbyte_metrics::snapshot::PipelineMetricsSnapshot {
        rapidbyte_metrics::snapshot::PipelineMetricsSnapshot::default()
    }
}

// ---------------------------------------------------------------------------
// Public compatibility API
// ---------------------------------------------------------------------------

/// Run a pipeline using the old orchestrator-compatible signature.
///
/// Constructs an [`EngineContext`] from the provided parameters and delegates
/// to [`crate::application::run::run_pipeline`].
///
/// # Errors
///
/// Returns [`PipelineError`] on resolution, execution, or finalization errors.
#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline_compat(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    progress_tx: Option<tokio_mpsc::UnboundedSender<ProgressEvent>>,
    cancel_token: CancellationToken,
    snapshot_reader: &rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
    registry_config: &RegistryConfig,
) -> Result<PipelineOutcome, PipelineError> {
    // Build state backend from pipeline config
    let state_connection = config.state.connection.clone().unwrap_or_else(|| {
        rapidbyte_state::default_connection(rapidbyte_state::BackendKind::Sqlite)
    });
    let state_backend = tokio::task::spawn_blocking(move || {
        rapidbyte_state::open_backend(rapidbyte_state::BackendKind::Sqlite, &state_connection)
    })
    .await
    .map_err(|e| PipelineError::infra(format!("state backend task panicked: {e}")))?
    .map_err(PipelineError::Infrastructure)?;

    // Build WASM runtime
    let wasm_runtime =
        rapidbyte_runtime::WasmRuntime::new().map_err(PipelineError::Infrastructure)?;
    let runner = Arc::new(WasmPluginRunner::new(wasm_runtime, state_backend.clone()));

    // Build progress reporter
    let progress: Arc<dyn ProgressReporter> = match progress_tx {
        Some(tx) => Arc::new(ChannelProgressReporter::new(tx)),
        None => Arc::new(NoopProgressReporter),
    };

    // Build metrics -- unused reader/provider references are kept for future use
    let _ = (snapshot_reader, meter_provider);
    let metrics = Arc::new(NoopMetricsSnapshot);

    // Build resolver
    let resolver = Arc::new(RegistryPluginResolver::new(registry_config.clone()));

    // Use DummyRepository implementations for cursors/runs/dlq since legacy
    // code handled this internally via the state backend.
    let dummy = Arc::new(StateBackendRepositoryAdapter::new(state_backend));
    let ctx = EngineContext {
        runner,
        resolver,
        cursors: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::CursorRepository>,
        runs: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::RunRecordRepository>,
        dlq: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::DlqRepository>,
        progress,
        metrics,
        config: EngineConfig {
            max_retries: config.resources.max_retries,
            channel_capacity: 64,
        },
    };

    crate::application::run::run_pipeline(&ctx, config, options, cancel_token).await
}

/// Check a pipeline using the old orchestrator-compatible signature.
///
/// # Errors
///
/// Returns [`PipelineError`] on resolution or validation errors.
pub async fn check_pipeline_compat(
    config: &PipelineConfig,
    registry_config: &RegistryConfig,
) -> Result<CheckResult, PipelineError> {
    let ctx = build_lightweight_context(registry_config, config).await?;
    crate::application::check::check_pipeline(&ctx, config).await
}

/// Discover available streams from a source plugin using the old signature.
///
/// # Errors
///
/// Returns an error if resolution or discovery fails.
pub async fn discover_plugin_compat(
    plugin_ref: &str,
    config: &serde_json::Value,
    registry_config: &RegistryConfig,
) -> Result<rapidbyte_types::catalog::Catalog, anyhow::Error> {
    let state_backend = tokio::task::spawn_blocking(|| {
        let conn = rapidbyte_state::default_connection(rapidbyte_state::BackendKind::Sqlite);
        rapidbyte_state::open_backend(rapidbyte_state::BackendKind::Sqlite, &conn)
    })
    .await
    .map_err(|e| anyhow::anyhow!("state backend task panicked: {e}"))??;

    let wasm_runtime = rapidbyte_runtime::WasmRuntime::new()?;
    let runner = Arc::new(WasmPluginRunner::new(wasm_runtime, state_backend.clone()));
    let resolver = Arc::new(RegistryPluginResolver::new(registry_config.clone()));
    let dummy = Arc::new(StateBackendRepositoryAdapter::new(state_backend));

    let ctx = EngineContext {
        runner,
        resolver,
        cursors: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::CursorRepository>,
        runs: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::RunRecordRepository>,
        dlq: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::DlqRepository>,
        progress: Arc::new(NoopProgressReporter),
        metrics: Arc::new(NoopMetricsSnapshot),
        config: EngineConfig {
            max_retries: 0,
            channel_capacity: 64,
        },
    };

    let streams = crate::application::discover::discover_plugin(&ctx, plugin_ref, Some(config))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Convert DiscoveredStream to Catalog
    let catalog_streams: Vec<rapidbyte_types::catalog::Stream> = streams
        .into_iter()
        .filter_map(|s| serde_json::from_str(&s.catalog_json).ok())
        .collect();

    Ok(rapidbyte_types::catalog::Catalog {
        streams: catalog_streams,
    })
}

/// Build a lightweight `EngineContext` for check/discover operations.
async fn build_lightweight_context(
    registry_config: &RegistryConfig,
    config: &PipelineConfig,
) -> Result<EngineContext, PipelineError> {
    let state_connection = config.state.connection.clone().unwrap_or_else(|| {
        rapidbyte_state::default_connection(rapidbyte_state::BackendKind::Sqlite)
    });
    let state_backend = tokio::task::spawn_blocking(move || {
        rapidbyte_state::open_backend(rapidbyte_state::BackendKind::Sqlite, &state_connection)
    })
    .await
    .map_err(|e| PipelineError::infra(format!("state backend task panicked: {e}")))?
    .map_err(PipelineError::Infrastructure)?;

    let wasm_runtime =
        rapidbyte_runtime::WasmRuntime::new().map_err(PipelineError::Infrastructure)?;
    let runner = Arc::new(WasmPluginRunner::new(wasm_runtime, state_backend.clone()));
    let resolver = Arc::new(RegistryPluginResolver::new(registry_config.clone()));
    let dummy = Arc::new(StateBackendRepositoryAdapter::new(state_backend));

    Ok(EngineContext {
        runner,
        resolver,
        cursors: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::CursorRepository>,
        runs: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::RunRecordRepository>,
        dlq: Arc::clone(&dummy) as Arc<dyn crate::domain::ports::DlqRepository>,
        progress: Arc::new(NoopProgressReporter),
        metrics: Arc::new(NoopMetricsSnapshot),
        config: EngineConfig {
            max_retries: 0,
            channel_capacity: 64,
        },
    })
}

// ---------------------------------------------------------------------------
// StateBackend-to-repository adapter
// ---------------------------------------------------------------------------

/// Wraps the legacy `StateBackend` trait object to implement the hexagonal
/// repository ports.
struct StateBackendRepositoryAdapter {
    backend: Arc<dyn rapidbyte_types::state_backend::StateBackend>,
}

impl StateBackendRepositoryAdapter {
    fn new(backend: Arc<dyn rapidbyte_types::state_backend::StateBackend>) -> Self {
        Self { backend }
    }
}

#[async_trait::async_trait]
impl crate::domain::ports::CursorRepository for StateBackendRepositoryAdapter {
    async fn get(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
    ) -> Result<Option<rapidbyte_types::state::CursorState>, crate::domain::ports::RepositoryError>
    {
        let backend = Arc::clone(&self.backend);
        let pipeline = pipeline.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || backend.get_cursor(&pipeline, &stream))
            .await
            .map_err(|e| {
                crate::domain::ports::RepositoryError::Other(Box::new(std::io::Error::other(
                    format!("cursor get task panicked: {e}"),
                )))
            })?
            .map_err(|e| crate::domain::ports::RepositoryError::Other(Box::new(e)))
    }

    async fn set(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
        cursor: &rapidbyte_types::state::CursorState,
    ) -> Result<(), crate::domain::ports::RepositoryError> {
        let backend = Arc::clone(&self.backend);
        let pipeline = pipeline.clone();
        let stream = stream.clone();
        let cursor = cursor.clone();
        tokio::task::spawn_blocking(move || backend.set_cursor(&pipeline, &stream, &cursor))
            .await
            .map_err(|e| {
                crate::domain::ports::RepositoryError::Other(Box::new(std::io::Error::other(
                    format!("cursor set task panicked: {e}"),
                )))
            })?
            .map_err(|e| crate::domain::ports::RepositoryError::Other(Box::new(e)))
    }

    async fn compare_and_set(
        &self,
        _pipeline: &rapidbyte_types::state::PipelineId,
        _stream: &rapidbyte_types::state::StreamName,
        _expected: Option<&str>,
        _new_value: &str,
    ) -> Result<bool, crate::domain::ports::RepositoryError> {
        Ok(true)
    }
}

#[async_trait::async_trait]
impl crate::domain::ports::RunRecordRepository for StateBackendRepositoryAdapter {
    async fn start(
        &self,
        pipeline: &rapidbyte_types::state::PipelineId,
        stream: &rapidbyte_types::state::StreamName,
    ) -> Result<i64, crate::domain::ports::RepositoryError> {
        let backend = Arc::clone(&self.backend);
        let pipeline = pipeline.clone();
        let stream = stream.clone();
        tokio::task::spawn_blocking(move || backend.start_run(&pipeline, &stream))
            .await
            .map_err(|e| {
                crate::domain::ports::RepositoryError::Other(Box::new(std::io::Error::other(
                    format!("start_run task panicked: {e}"),
                )))
            })?
            .map_err(|e| crate::domain::ports::RepositoryError::Other(Box::new(e)))
    }

    #[allow(clippy::similar_names)]
    async fn complete(
        &self,
        run_id: i64,
        status: rapidbyte_types::state::RunStatus,
        stats: &rapidbyte_types::state::RunStats,
    ) -> Result<(), crate::domain::ports::RepositoryError> {
        let backend = Arc::clone(&self.backend);
        let run_stats = stats.clone();
        tokio::task::spawn_blocking(move || backend.complete_run(run_id, status, &run_stats))
            .await
            .map_err(|e| {
                crate::domain::ports::RepositoryError::Other(Box::new(std::io::Error::other(
                    format!("complete_run task panicked: {e}"),
                )))
            })?
            .map_err(|e| crate::domain::ports::RepositoryError::Other(Box::new(e)))
    }
}

#[async_trait::async_trait]
impl crate::domain::ports::DlqRepository for StateBackendRepositoryAdapter {
    async fn insert(
        &self,
        _pipeline: &rapidbyte_types::state::PipelineId,
        _run_id: i64,
        _records: &[rapidbyte_types::envelope::DlqRecord],
    ) -> Result<u64, crate::domain::ports::RepositoryError> {
        Ok(0)
    }
}
