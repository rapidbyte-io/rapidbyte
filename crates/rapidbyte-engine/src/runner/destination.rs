//! Destination plugin runner.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{create_component_linker, dest_bindings, dest_error_to_sdk, Frame};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::metric::WriteSummary;
use rapidbyte_types::state::RunStats;

use super::{
    build_base_host_state, extract_checkpoints, handle_close_result, serialize_plugin_config,
    serialize_stream_context, StreamRunContext,
};
use crate::error::PipelineError;

/// Result of running a destination plugin for a single stream.
pub(crate) struct DestinationOutcome {
    pub duration_secs: f64,
    pub summary: WriteSummary,
    pub wasm_instantiation_secs: f64,
    pub frame_receive_secs: f64,
    pub checkpoints: Vec<Checkpoint>,
}

/// Run a destination plugin for a single stream.
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, consume
/// all input frames, or close cleanly for the given stream.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub(crate) fn run_destination_stream(
    ctx: &StreamRunContext<'_>,
    frame_receiver: mpsc::Receiver<Frame>,
    dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    dest_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
) -> Result<DestinationOutcome, PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    // Build host state: shared fields + destination-specific additions
    let builder = build_base_host_state(ctx, "destination", dest_config, None)
        .receiver(frame_receiver)
        .dest_checkpoints(dest_checkpoints.clone())
        .dlq_records(dlq_records.clone());
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = ctx.overrides.and_then(|o| o.timeout_seconds);
    let mut store = ctx.component.new_store(host_state, timeout);
    let linker = create_component_linker(&ctx.component.engine, "destination", |linker| {
        dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte destination host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings = dest_bindings::RapidbyteDestination::instantiate(
        &mut store,
        &ctx.component.component,
        &linker,
    )
    .map_err(|e| {
        PipelineError::infra(format!("Failed to instantiate destination bindings: {e}"))
    })?;

    let iface = bindings.rapidbyte_plugin_destination();
    let wasm_instantiation_secs = vm_setup_start.elapsed().as_secs_f64();

    let dest_config_json = serialize_plugin_config(dest_config, "destination")?;

    tracing::info!(
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Opening destination plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &dest_config_json)
        .map_err(|e| PipelineError::infra(format!("Failed to call destination open: {e}")))?
        .map_err(|err| PipelineError::Plugin(dest_error_to_sdk(err)))?;

    let recv_start = Instant::now();
    let ctx_json = serialize_stream_context(ctx.stream_ctx)?;

    tracing::info!(
        stream = ctx.stream_ctx.stream_name,
        "Starting destination write"
    );
    let run_request = dest_bindings::rapidbyte::plugin::types::RunRequest {
        phase: dest_bindings::rapidbyte::plugin::types::RunPhase::Write,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call destination run: {e}")))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.write else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(
                    "destination run summary missing write section",
                ));
            };
            WriteSummary {
                records_written: summary.records_written,
                bytes_written: summary.bytes_written,
                batches_written: summary.batches_written,
                checkpoint_count: summary.checkpoint_count,
                records_failed: summary.records_failed,
            }
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(dest_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = ctx.stream_ctx.stream_name,
        records = summary.records_written,
        bytes = summary.bytes_written,
        "Destination write complete for stream"
    );

    {
        let mut s = stats
            .lock()
            .map_err(|_| PipelineError::infra("run stats mutex poisoned"))?;
        s.records_written += summary.records_written;
        s.bytes_written += summary.bytes_written;
    }

    let frame_receive_secs = recv_start.elapsed().as_secs_f64();

    tracing::info!(
        plugin = ctx.plugin_id,
        version = ctx.plugin_version,
        stream = ctx.stream_ctx.stream_name,
        "Closing destination plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Destination",
        &ctx.stream_ctx.stream_name,
        |err| dest_error_to_sdk(err).to_string(),
    );

    let checkpoints = extract_checkpoints(&dest_checkpoints, "destination")?;

    Ok(DestinationOutcome {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        wasm_instantiation_secs,
        frame_receive_secs,
        checkpoints,
    })
}
