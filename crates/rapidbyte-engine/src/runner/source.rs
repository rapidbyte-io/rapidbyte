//! Source plugin runner.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{create_component_linker, source_bindings, source_error_to_sdk, Frame};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::metric::ReadSummary;
use rapidbyte_types::state::RunStats;

use super::{handle_close_result, plugin_instance_key, StreamRunContext};
use crate::error::PipelineError;

/// Result of running a source plugin for a single stream.
pub(crate) struct SourceRunResult {
    pub duration_secs: f64,
    pub summary: ReadSummary,
    pub checkpoints: Vec<Checkpoint>,
}

/// Run a source plugin for a single stream.
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, run, or
/// closed cleanly for the given stream.
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub(crate) fn run_source_stream(
    ctx: &StreamRunContext<'_>,
    sender: mpsc::SyncSender<Frame>,
    source_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
    on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
) -> Result<SourceRunResult, PipelineError> {
    let StreamRunContext {
        module,
        ref state_backend,
        pipeline_name,
        metric_run_label,
        plugin_id,
        plugin_version,
        stream_ctx,
        permissions,
        compression,
        overrides,
    } = *ctx;

    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let shard_index = stream_ctx.partition_index.unwrap_or(0) as usize;
    let host_timings =
        rapidbyte_runtime::HostTimings::new(pipeline_name, &stream_ctx.stream_name, shard_index)
            .with_run_label(metric_run_label);

    let mut builder = rapidbyte_runtime::ComponentHostState::builder()
        .pipeline(pipeline_name)
        .plugin_id(plugin_id)
        .plugin_instance_key(plugin_instance_key("source", plugin_id, stream_ctx, None))
        .stream(stream_ctx.stream_name.clone())
        .metric_run_label(metric_run_label)
        .state_backend(state_backend.clone())
        .sender(sender.clone())
        .source_checkpoints(source_checkpoints.clone())
        .timings(host_timings)
        .config(source_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    if let Some(cb) = on_emit {
        builder = builder.on_emit(cb);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte source host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_plugin_source();

    let source_config_json = serde_json::to_string(source_config)
        .context("Failed to serialize source config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening source plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?
        .map_err(|err| PipelineError::Plugin(source_error_to_sdk(err)))?;

    let ctx_json = serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
    let run_request = source_bindings::rapidbyte::plugin::types::RunRequest {
        phase: source_bindings::rapidbyte::plugin::types::RunPhase::Read,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.read else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "source run summary missing read section"
                )));
            };
            ReadSummary {
                records_read: summary.records_read,
                bytes_read: summary.bytes_read,
                batches_emitted: summary.batches_emitted,
                checkpoint_count: summary.checkpoint_count,
                records_skipped: summary.records_skipped,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(source_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_read,
        bytes = summary.bytes_read,
        "Source read complete for stream"
    );

    {
        let mut s = stats.lock().map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned"))
        })?;
        s.records_read += summary.records_read;
        s.bytes_read += summary.bytes_read;
    }

    let _ = sender.send(Frame::EndStream);

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing source plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Source",
        &stream_ctx.stream_name,
        |err| source_error_to_sdk(err).to_string(),
    );

    let checkpoints = source_checkpoints
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("source checkpoint mutex poisoned"))
        })?
        .drain(..)
        .collect::<Vec<_>>();

    Ok(SourceRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        checkpoints,
    })
}
