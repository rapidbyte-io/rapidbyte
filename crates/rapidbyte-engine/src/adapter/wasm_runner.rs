//! WASM plugin runner adapter.
//!
//! Wraps `rapidbyte-runtime` to implement the [`PluginRunner`] port trait,
//! delegating to the existing runner functions in [`crate::runner`].
//!
//! The `validate_plugin` and `discover` methods are fully implemented.
//! The `run_source`, `run_transform`, and `run_destination` methods are
//! stubbed with `todo!()` because the port trait's channel types
//! (`SyncSender<Vec<u8>>`) do not match the runner functions' channel types
//! (`SyncSender<Frame>`).  These will be wired during consumer migration
//! when the channel types are unified.

use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_runtime::WasmRuntime;
use rapidbyte_state::StateBackend;

use crate::domain::ports::runner::{
    CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    DiscoveredStream, PluginRunner, SourceOutcome, SourceRunParams, TransformOutcome,
    TransformRunParams, ValidateParams,
};
use crate::error::PipelineError;

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
    async fn run_source(&self, _params: &SourceRunParams) -> Result<SourceOutcome, PipelineError> {
        // The port trait uses `SyncSender<Vec<u8>>` for frame channels, but
        // the underlying `run_source_stream` requires `SyncSender<Frame>`.
        // Bridging these types requires creating internal Frame channels and
        // forwarding data, which will be done during consumer migration when
        // the orchestrator is ported to use the trait interface.
        let _ = (&self.runtime, &self.state_backend, parse_compression(None));
        todo!(
            "run_source: channel type bridging (Vec<u8> <-> Frame) \
             will be wired during consumer migration"
        )
    }

    async fn run_transform(
        &self,
        _params: &TransformRunParams,
    ) -> Result<TransformOutcome, PipelineError> {
        // Same channel type mismatch as run_source, plus the runner consumes
        // the receiver by value (ownership transfer through `&self` params).
        todo!(
            "run_transform: channel type bridging and ownership transfer \
             will be wired during consumer migration"
        )
    }

    async fn run_destination(
        &self,
        _params: &DestinationRunParams,
    ) -> Result<DestinationOutcome, PipelineError> {
        // Same channel type mismatch as run_source, plus the runner consumes
        // the receiver by value (ownership transfer through `&self` params).
        todo!(
            "run_destination: channel type bridging and ownership transfer \
             will be wired during consumer migration"
        )
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
