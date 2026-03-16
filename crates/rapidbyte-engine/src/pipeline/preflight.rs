//! Destination DDL preflight logic.
//!
//! Runs the destination plugin once per logical stream with an empty batch
//! to trigger DDL creation (CREATE TABLE, etc.) before shard workers start.

use std::sync::{mpsc as sync_mpsc, Arc, Mutex};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rapidbyte_runtime::{Frame, LoadedComponent};
use rapidbyte_state::StateBackend;
use rapidbyte_types::state::RunStats;
use rapidbyte_types::stream::StreamContext;

use crate::error::PipelineError;
use crate::pipeline::planner::StreamParams;
use crate::pipeline::scheduler::{acquire_permit_cancellable, ensure_not_cancelled};
use crate::runner::{run_destination_stream, StreamRunContext};

/// Run destination DDL preflight for each unique logical stream.
///
/// Sends an empty `EndStream` frame through the destination plugin to
/// trigger DDL creation (e.g. `CREATE TABLE`) before shard workers start.
/// Preflight tasks are bounded by the given `parallelism` via a semaphore.
///
/// # Errors
///
/// Returns `PipelineError` if any preflight task fails or panics.
pub(crate) async fn run_destination_preflight(
    preflight_streams: Vec<StreamContext>,
    dest_module: &LoadedComponent,
    state: Arc<dyn StateBackend>,
    params: &Arc<StreamParams>,
    parallelism: usize,
    cancel_token: &CancellationToken,
) -> Result<(), PipelineError> {
    let preflight_parallelism = parallelism.min(preflight_streams.len()).max(1);
    tracing::info!(
        unique_streams = preflight_streams.len(),
        preflight_parallelism,
        "Running destination DDL preflight before shard workers"
    );

    let mut preflight_join_set: JoinSet<Result<(), PipelineError>> = JoinSet::new();
    let preflight_semaphore = Arc::new(tokio::sync::Semaphore::new(preflight_parallelism));

    for stream_ctx in preflight_streams {
        ensure_not_cancelled(
            cancel_token,
            "Pipeline cancelled before destination preflight",
        )?;
        let permit = acquire_permit_cancellable(
            &preflight_semaphore,
            cancel_token,
            "Pipeline cancelled before destination preflight",
        )
        .await?;

        let stream_name = stream_ctx.stream_name.clone();
        let state_dst = state.clone();
        let dest_module = dest_module.clone();
        let params = params.clone();

        preflight_join_set.spawn(async move {
            let _permit = permit;
            let (tx, rx) = sync_mpsc::sync_channel::<Frame>(1);
            tx.send(Frame::EndStream).map_err(|e| {
                PipelineError::infra(format!(
                    "Failed to prime destination preflight channel for stream '{stream_name}': {e}",
                ))
            })?;
            drop(tx);

            // Empty run label so preflight metrics are unscoped and don't
            // accumulate in the SnapshotReader's finished_run_snapshots map.
            let preflight_result = tokio::task::spawn_blocking(move || {
                let ctx = StreamRunContext {
                    module: &dest_module,
                    state_backend: state_dst,
                    pipeline_name: &params.pipeline_name,
                    metric_run_label: "",
                    plugin_id: &params.dest_plugin_id,
                    plugin_version: &params.dest_plugin_version,
                    stream_ctx: &stream_ctx,
                    permissions: params.dest_permissions.as_ref(),
                    compression: params.compression,
                    overrides: params.dest_overrides.as_ref(),
                };
                run_destination_stream(
                    &ctx,
                    rx,
                    Arc::new(Mutex::new(Vec::new())),
                    &params.dest_config,
                    Arc::new(Mutex::new(RunStats::default())),
                )
            })
            .await
            .map_err(|e| {
                PipelineError::task_panicked(
                    &format!("Destination preflight for stream '{stream_name}'"),
                    e,
                )
            })??;

            tracing::info!(
                stream = stream_name,
                duration_secs = preflight_result.duration_secs,
                "Destination preflight completed"
            );

            Ok(())
        });
    }

    while let Some(joined) = preflight_join_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                preflight_join_set.abort_all();
                return Err(err);
            }
            Err(join_err) => {
                preflight_join_set.abort_all();
                return Err(PipelineError::infra(format!(
                    "Destination preflight join error: {join_err}"
                )));
            }
        }
    }

    Ok(())
}
