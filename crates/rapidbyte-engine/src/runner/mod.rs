//! Plugin runners: shared lifecycle, per-kind implementations, and validation.

mod destination;
mod source;
mod transform;
mod validator;

// Re-export runner functions and types (pub for benchmarks crate)
pub use destination::{run_destination_stream, DestinationOutcome};
pub use source::{run_source_stream, SourceOutcome};
pub use transform::{run_transform_stream, TransformOutcome};
pub(crate) use validator::{run_discover, validate_plugin};

use std::sync::{Arc, Mutex};

use anyhow::Context;
use rapidbyte_runtime::{
    CompressionCodec, HostStateBuilder, HostTimings, LoadedComponent, SandboxOverrides,
};
use rapidbyte_state::StateBackend;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::stream::StreamContext;

use crate::error::PipelineError;

/// Shared context for running a plugin stream (source, destination, or
/// transform).  Built once per stream by the orchestrator and passed to
/// each runner function.
pub struct StreamRunContext<'a> {
    pub component: &'a LoadedComponent,
    pub state_backend: Arc<dyn StateBackend>,
    pub pipeline_name: &'a str,
    pub metric_run_label: &'a str,
    pub plugin_id: &'a str,
    pub plugin_version: &'a str,
    pub stream_ctx: &'a StreamContext,
    pub permissions: Option<&'a Permissions>,
    pub compression: Option<CompressionCodec>,
    pub overrides: Option<&'a SandboxOverrides>,
}

// ── Shared lifecycle helpers ────────────────────────────────────────

/// Build a [`HostStateBuilder`] pre-populated with every field that is
/// common to all three runner kinds (source, destination, transform).
///
/// The caller should chain kind-specific setters (`.sender()`,
/// `.receiver()`, `.source_checkpoints()`, etc.) and then call `.build()`.
pub(crate) fn build_base_host_state(
    ctx: &StreamRunContext<'_>,
    stage: &str,
    config: &serde_json::Value,
    instance_ordinal: Option<usize>,
) -> HostStateBuilder {
    let shard_index = ctx.stream_ctx.partition_index.unwrap_or(0) as usize;
    let host_timings =
        HostTimings::new(ctx.pipeline_name, &ctx.stream_ctx.stream_name, shard_index)
            .with_run_label(ctx.metric_run_label);

    let mut builder = rapidbyte_runtime::ComponentHostState::builder()
        .pipeline(ctx.pipeline_name)
        .plugin_id(ctx.plugin_id)
        .plugin_instance_key(plugin_instance_key(
            stage,
            ctx.plugin_id,
            ctx.stream_ctx,
            instance_ordinal,
        ))
        .stream(ctx.stream_ctx.stream_name.clone())
        .metric_run_label(ctx.metric_run_label)
        .state_backend(ctx.state_backend.clone())
        .timings(host_timings)
        .config(config)
        .compression(ctx.compression);

    if let Some(p) = ctx.permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = ctx.overrides {
        builder = builder.overrides(o);
    }
    builder
}

/// Serialize a plugin config value to JSON.
pub(crate) fn serialize_plugin_config(
    config: &serde_json::Value,
    role: &str,
) -> Result<String, PipelineError> {
    serde_json::to_string(config)
        .with_context(|| format!("Failed to serialize {role} config"))
        .map_err(PipelineError::Infrastructure)
}

/// Serialize a [`StreamContext`] to JSON for the `RunRequest`.
pub(crate) fn serialize_stream_context(
    stream_ctx: &StreamContext,
) -> Result<String, PipelineError> {
    serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)
}

/// Drain checkpoints from a shared mutex, returning an owned `Vec`.
pub(crate) fn extract_checkpoints(
    checkpoints: &Arc<Mutex<Vec<Checkpoint>>>,
    role: &str,
) -> Result<Vec<Checkpoint>, PipelineError> {
    checkpoints
        .lock()
        .map_err(|_| PipelineError::infra(format!("{role} checkpoint mutex poisoned")))
        .map(|mut guard| guard.drain(..).collect())
}

// ── Existing helpers ────────────────────────────────────────────────

pub(crate) fn plugin_instance_key(
    stage: &str,
    plugin_id: &str,
    stream_ctx: &StreamContext,
    instance_ordinal: Option<usize>,
) -> String {
    let partition_index = stream_ctx.partition_index.unwrap_or(0);
    match instance_ordinal {
        Some(ordinal) => format!(
            "{stage}:{plugin_id}:{}:p{partition_index}:i{ordinal}",
            stream_ctx.stream_name
        ),
        None => format!(
            "{stage}:{plugin_id}:{}:p{partition_index}",
            stream_ctx.stream_name
        ),
    }
}

pub(crate) fn handle_close_result<E, F>(
    result: std::result::Result<
        std::result::Result<(), E>,
        rapidbyte_runtime::wasmtime_reexport::Error,
    >,
    role: &str,
    stream_name: &str,
    convert: F,
) where
    F: Fn(E) -> String,
{
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!(
                stream = stream_name,
                "{} close failed: {}",
                role,
                convert(err)
            );
        }
        Err(err) => {
            tracing::warn!(stream = stream_name, "{} close trap: {}", role, err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::handle_close_result;

    #[test]
    fn test_handle_close_result_ok() {
        handle_close_result::<String, _>(Ok(Ok(())), "Source", "users", |e| e);
    }
}
