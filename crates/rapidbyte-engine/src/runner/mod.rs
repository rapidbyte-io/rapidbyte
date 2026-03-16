//! Plugin runners: shared lifecycle, per-kind implementations, and validation.

pub(crate) mod destination;
pub(crate) mod source;
pub(crate) mod transform;
pub(crate) mod validator;

// Re-export main types for crate-internal use
// DestRunResult is re-exported for API completeness; callers use type inference
#[allow(unused_imports)]
pub(crate) use destination::{run_destination_stream, DestRunResult};
pub(crate) use source::{run_source_stream, SourceRunResult};
pub(crate) use transform::{run_transform_stream, TransformRunResult};
pub(crate) use validator::{run_discover, validate_plugin};

use rapidbyte_runtime::{CompressionCodec, LoadedComponent, SandboxOverrides};
use rapidbyte_state::StateBackend;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::stream::StreamContext;
use std::sync::Arc;

/// Shared context for running a plugin stream (source, destination, or
/// transform).  Built once per stream by the orchestrator and passed to
/// each runner function.
pub(crate) struct StreamRunContext<'a> {
    pub module: &'a LoadedComponent,
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
