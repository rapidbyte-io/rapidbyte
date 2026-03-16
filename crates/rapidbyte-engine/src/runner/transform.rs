//! Transform plugin runner.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, transform_bindings, transform_error_to_sdk, Frame,
};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::metric::TransformSummary;

use super::{
    build_base_host_state, handle_close_result, serialize_plugin_config, serialize_stream_context,
    StreamRunContext,
};
use crate::error::PipelineError;

/// Result of running a transform plugin for a single stream.
pub(crate) struct TransformRunResult {
    pub duration_secs: f64,
    pub summary: TransformSummary,
}

/// Run a transform plugin for a single stream.
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, consume
/// input frames, emit output frames, or close cleanly for the given stream.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn run_transform_stream(
    ctx: &StreamRunContext<'_>,
    receiver: mpsc::Receiver<Frame>,
    sender: mpsc::SyncSender<Frame>,
    dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    transform_index: usize,
    transform_config: &serde_json::Value,
) -> Result<TransformRunResult, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    // Build host state: shared fields + transform-specific additions
    let builder = build_base_host_state(ctx, "transform", transform_config, Some(transform_index))
        .sender(sender.clone())
        .receiver(receiver)
        .dlq_records(dlq_records)
        .source_checkpoints(source_checkpoints)
        .dest_checkpoints(dest_checkpoints);
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = ctx.overrides.and_then(|o| o.timeout_seconds);
    let mut store = ctx.module.new_store(host_state, timeout);
    let linker = create_component_linker(&ctx.module.engine, "transform", |linker| {
        transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(linker, |state| {
            state
        })
        .context("Failed to add rapidbyte transform host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings = transform_bindings::RapidbyteTransform::instantiate(
        &mut store,
        &ctx.module.component,
        &linker,
    )
    .map_err(|e| PipelineError::infra(format!("Failed to instantiate transform bindings: {e}")))?;

    let iface = bindings.rapidbyte_plugin_transform();

    let transform_config_json = serialize_plugin_config(transform_config, "transform")?;

    tracing::info!(
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Opening transform plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &transform_config_json)
        .map_err(|e| PipelineError::infra(format!("Failed to call transform open: {e}")))?
        .map_err(|err| PipelineError::Plugin(transform_error_to_sdk(err)))?;

    let ctx_json = serialize_stream_context(ctx.stream_ctx)?;

    tracing::info!(stream = ctx.stream_ctx.stream_name, "Starting transform");
    let run_request = transform_bindings::rapidbyte::plugin::types::RunRequest {
        phase: transform_bindings::rapidbyte::plugin::types::RunPhase::Transform,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call transform run: {e}")))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.transform else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(
                    "transform run summary missing transform section",
                ));
            };
            TransformSummary {
                records_in: summary.records_in,
                records_out: summary.records_out,
                bytes_in: summary.bytes_in,
                bytes_out: summary.bytes_out,
                batches_processed: summary.batches_processed,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(transform_error_to_sdk(err)));
        }
    };

    let _ = sender.send(Frame::EndStream);

    tracing::info!(
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Closing transform plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Transform",
        &ctx.stream_ctx.stream_name,
        |err| transform_error_to_sdk(err).to_string(),
    );

    Ok(TransformRunResult {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
    })
}
