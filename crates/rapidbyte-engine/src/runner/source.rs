//! Source plugin runner.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{create_component_linker, source_bindings, source_error_to_sdk, Frame};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::metric::ReadSummary;
use rapidbyte_types::state::RunStats;

use super::{
    build_base_host_state, extract_checkpoints, handle_close_result, serialize_plugin_config,
    serialize_stream_context, StreamRunContext,
};
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
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run_source_stream(
    ctx: &StreamRunContext<'_>,
    sender: mpsc::SyncSender<Frame>,
    source_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
    on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
) -> Result<SourceRunResult, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    // Build host state: shared fields + source-specific additions
    let mut builder = build_base_host_state(ctx, "source", source_config, None)
        .sender(sender.clone())
        .source_checkpoints(source_checkpoints.clone());
    if let Some(cb) = on_emit {
        builder = builder.on_emit(cb);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = ctx.overrides.and_then(|o| o.timeout_seconds);
    let mut store = ctx.module.new_store(host_state, timeout);
    let linker = create_component_linker(&ctx.module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte source host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &ctx.module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_plugin_source();

    let source_config_json = serialize_plugin_config(source_config, "source")?;

    tracing::info!(
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Opening source plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?
        .map_err(|err| PipelineError::Plugin(source_error_to_sdk(err)))?;

    let ctx_json = serialize_stream_context(ctx.stream_ctx)?;

    tracing::info!(stream = ctx.stream_ctx.stream_name, "Starting source read");
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
        stream = ctx.stream_ctx.stream_name,
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
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Closing source plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Source",
        &ctx.stream_ctx.stream_name,
        |err| source_error_to_sdk(err).to_string(),
    );

    let checkpoints = extract_checkpoints(&source_checkpoints, "source")?;

    Ok(SourceRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        checkpoints,
    })
}
