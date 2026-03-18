//! WASM plugin runner adapter.
//!
//! Wraps `rapidbyte-runtime` to implement the [`PluginRunner`] port trait,
//! delegating to the existing runner functions in [`crate::runner`].

use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_runtime::WasmRuntime;
use rapidbyte_state::StateBackend;

use crate::domain::error::PipelineError;
use crate::domain::ports::runner::{
    CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    DiscoveredStream, PluginRunner, SourceOutcome, SourceRunParams, TransformOutcome,
    TransformRunParams, ValidateParams,
};
use crate::error::PipelineError as OldPipelineError;
use crate::runner::StreamRunContext;

/// Convert old `PipelineError` to domain `PipelineError`.
fn from_old_error(e: OldPipelineError) -> PipelineError {
    match e {
        OldPipelineError::Plugin(pe) => PipelineError::Plugin(pe),
        OldPipelineError::Infrastructure(ae) => PipelineError::Infrastructure(ae),
    }
}

/// Adapter that implements [`PluginRunner`] by delegating to the WASM runtime
/// and existing runner functions in [`crate::runner`].
pub struct WasmPluginRunner {
    runtime: WasmRuntime,
    state_backend: Arc<dyn StateBackend>,
}

impl WasmPluginRunner {
    /// Create a new `WasmPluginRunner`.
    pub fn new(runtime: WasmRuntime, state_backend: Arc<dyn StateBackend>) -> Self {
        Self {
            runtime,
            state_backend,
        }
    }
}

/// Parse an optional compression string into a [`CompressionCodec`].
fn parse_compression(
    s: Option<&str>,
) -> Result<Option<rapidbyte_runtime::CompressionCodec>, PipelineError> {
    match s {
        None | Some("") => Ok(None),
        Some("lz4") => Ok(Some(rapidbyte_runtime::CompressionCodec::Lz4)),
        Some("zstd") => Ok(Some(rapidbyte_runtime::CompressionCodec::Zstd)),
        Some(other) => Err(PipelineError::infra(format!(
            "unknown compression codec: {other}"
        ))),
    }
}

#[async_trait]
impl PluginRunner for WasmPluginRunner {
    async fn run_source(&self, params: SourceRunParams) -> Result<SourceOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        let result = tokio::task::spawn_blocking(move || {
            let ctx = StreamRunContext {
                component: &component,
                state_backend,
                pipeline_name: &params.pipeline_name,
                metric_run_label: &params.metric_run_label,
                plugin_id: &params.plugin_id,
                plugin_version: &params.plugin_version,
                stream_ctx: &params.stream_ctx,
                permissions: params.permissions.as_ref(),
                compression,
                overrides: None,
            };
            crate::runner::run_source_stream(
                &ctx,
                params.frame_sender,
                &params.config,
                params.stats,
                params.on_batch_emitted,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("source task panicked: {e}")))?;

        let outcome = result.map_err(from_old_error)?;
        Ok(SourceOutcome {
            duration_secs: outcome.duration_secs,
            summary: outcome.summary,
            checkpoints: outcome.checkpoints,
        })
    }

    async fn run_transform(
        &self,
        params: TransformRunParams,
    ) -> Result<TransformOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        let result = tokio::task::spawn_blocking(move || {
            let ctx = StreamRunContext {
                component: &component,
                state_backend,
                pipeline_name: &params.pipeline_name,
                metric_run_label: &params.metric_run_label,
                plugin_id: &params.plugin_id,
                plugin_version: &params.plugin_version,
                stream_ctx: &params.stream_ctx,
                permissions: params.permissions.as_ref(),
                compression,
                overrides: None,
            };
            crate::runner::run_transform_stream(
                &ctx,
                params.frame_receiver,
                params.frame_sender,
                params.dlq_records,
                params.transform_index,
                &params.config,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("transform task panicked: {e}")))?;

        let outcome = result.map_err(from_old_error)?;
        Ok(TransformOutcome {
            duration_secs: outcome.duration_secs,
            summary: outcome.summary,
        })
    }

    async fn run_destination(
        &self,
        params: DestinationRunParams,
    ) -> Result<DestinationOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        let result = tokio::task::spawn_blocking(move || {
            let ctx = StreamRunContext {
                component: &component,
                state_backend,
                pipeline_name: &params.pipeline_name,
                metric_run_label: &params.metric_run_label,
                plugin_id: &params.plugin_id,
                plugin_version: &params.plugin_version,
                stream_ctx: &params.stream_ctx,
                permissions: params.permissions.as_ref(),
                compression,
                overrides: None,
            };
            crate::runner::run_destination_stream(
                &ctx,
                params.frame_receiver,
                params.dlq_records,
                &params.config,
                params.stats,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("destination task panicked: {e}")))?;

        let outcome = result.map_err(from_old_error)?;
        Ok(DestinationOutcome {
            duration_secs: outcome.duration_secs,
            summary: outcome.summary,
            wasm_instantiation_secs: outcome.wasm_instantiation_secs,
            frame_receive_secs: outcome.frame_receive_secs,
            checkpoints: outcome.checkpoints,
        })
    }

    async fn validate_plugin(
        &self,
        params: &ValidateParams,
    ) -> Result<CheckComponentStatus, PipelineError> {
        let validation = crate::runner::validate_plugin(
            &params.wasm_path,
            params.kind,
            &params.plugin_id,
            &params.plugin_version,
            &params.config,
            &params.stream_name,
            params.permissions.as_ref(),
        )
        .map_err(PipelineError::Infrastructure)?;

        Ok(CheckComponentStatus { validation })
    }

    async fn discover(
        &self,
        params: &DiscoverParams,
    ) -> Result<Vec<DiscoveredStream>, PipelineError> {
        let catalog = crate::runner::run_discover(
            &params.wasm_path,
            &params.plugin_id,
            &params.plugin_version,
            &params.config,
            params.permissions.as_ref(),
        )
        .map_err(PipelineError::Infrastructure)?;

        let streams = catalog
            .streams
            .into_iter()
            .map(|s| {
                let catalog_json = serde_json::to_string(&s).unwrap_or_default();
                DiscoveredStream {
                    name: s.name,
                    catalog_json,
                }
            })
            .collect();

        Ok(streams)
    }
}
