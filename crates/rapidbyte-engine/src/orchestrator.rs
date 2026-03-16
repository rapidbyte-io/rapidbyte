//! Pipeline orchestrator: resolves plugins, loads modules, executes streams, and finalizes state.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc as sync_mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rapidbyte_runtime::{parse_plugin_ref, Frame, SandboxOverrides};
use rapidbyte_state::StateBackend;
use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::{CommitState, PluginError, ValidationResult, ValidationStatus};
use rapidbyte_types::manifest::PluginManifest;
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::wire::PluginKind;

use crate::arrow::ipc_to_record_batches;
use crate::config::types::PipelineConfig;
use crate::error::{compute_backoff, PipelineError};
use crate::execution::{DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome};
use crate::finalizers::checkpoint::correlate_and_persist_cursors;
use crate::pipeline::planner::{
    build_stream_contexts, compute_pipeline_parallelism, destination_preflight_streams,
    execution_parallelism, ExecutionPlan, StreamParams,
};
use crate::plugin::loader::{load_all_modules, PluginModules};
use crate::plugin::resolver::{
    load_and_validate_manifest, resolve_plugins, validate_config_against_schema, ResolvedPlugins,
};
use crate::plugin::sandbox::{build_sandbox_overrides, check_state_backend, create_state_backend};
use crate::progress::{Phase, ProgressEvent};
use crate::result::{
    CheckItemResult, CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming,
    StreamShardMetric,
};
use crate::runner::{
    run_destination_stream, run_discover, run_source_stream, run_transform_stream, validate_plugin,
    StreamRunContext, TransformRunResult,
};

async fn run_blocking_infrastructure_task<T, F>(
    task: F,
    context: &'static str,
) -> Result<T, PipelineError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("{context} task panicked: {e}"))
        })?
        .map_err(PipelineError::Infrastructure)
}

struct StreamResult {
    stream_name: String,
    partition_index: Option<u32>,
    partition_count: Option<u32>,
    read_summary: ReadSummary,
    write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    src_duration: f64,
    dst_duration: f64,
    vm_setup_secs: f64,
    recv_secs: f64,
    transform_durations: Vec<f64>,
    dry_run_result: Option<DryRunStreamResult>,
}

struct AggregatedStreamResults {
    execution_parallelism: u32,
    total_read_summary: ReadSummary,
    total_write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    max_source_duration: f64,
    max_dest_duration: f64,
    max_vm_setup_secs: f64,
    max_recv_secs: f64,
    transform_durations: Vec<f64>,
    dlq_records: Vec<DlqRecord>,
    final_stats: RunStats,
    first_error: Option<PipelineError>,
    dry_run_streams: Vec<DryRunStreamResult>,
    stream_metrics: Vec<StreamShardMetric>,
}

struct StreamTaskCollection {
    successes: Vec<StreamResult>,
    first_error: Option<PipelineError>,
}

static NEXT_METRIC_RUN_LABEL: AtomicU64 = AtomicU64::new(1);

/// Type alias for the progress channel sender used throughout the orchestrator.
type ProgressTx = Option<tokio_mpsc::UnboundedSender<ProgressEvent>>;

fn send_progress(tx: &ProgressTx, event: ProgressEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event);
    }
}

async fn collect_stream_task_results(
    mut stream_join_set: JoinSet<Result<StreamResult, PipelineError>>,
    progress_tx: &ProgressTx,
) -> Result<StreamTaskCollection, PipelineError> {
    let mut successes = Vec::new();
    let mut first_error: Option<PipelineError> = None;

    while let Some(joined) = stream_join_set.join_next().await {
        match joined {
            Ok(Ok(sr)) if first_error.is_none() => {
                send_progress(
                    progress_tx,
                    ProgressEvent::StreamCompleted {
                        stream: sr.stream_name.clone(),
                    },
                );
                successes.push(sr);
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::error!("Stream failed: {}", error);
                if first_error.is_none() {
                    first_error = Some(error);
                    stream_join_set.abort_all();
                }
            }
            Err(join_err) if join_err.is_cancelled() && first_error.is_some() => {
                // Expected: sibling tasks cancelled after first stream failure.
            }
            Err(join_err) => {
                return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                    "Stream task panicked: {join_err}"
                )));
            }
        }
    }

    Ok(StreamTaskCollection {
        successes,
        first_error,
    })
}

/// Run a pipeline with OpenTelemetry metric snapshot support.
///
/// `finalize_run()` reads timing data from the OpenTelemetry metric snapshot
/// provided by `snapshot_reader` and `meter_provider`.
///
/// # Errors
///
/// Returns a `PipelineError` if the pipeline fails.
pub async fn run_pipeline(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    progress_tx: Option<tokio_mpsc::UnboundedSender<ProgressEvent>>,
    cancel_token: CancellationToken,
    snapshot_reader: &rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
    registry_config: &rapidbyte_registry::RegistryConfig,
) -> Result<PipelineOutcome, PipelineError> {
    let max_retries = config.resources.max_retries;
    let mut attempt = 0u32;

    loop {
        ensure_not_cancelled(&cancel_token, "Pipeline cancelled before execution")?;
        attempt += 1;
        let result = execute_pipeline_once(
            config,
            options,
            attempt,
            progress_tx.clone(),
            &cancel_token,
            snapshot_reader,
            meter_provider,
            registry_config,
        )
        .await;

        match result {
            Ok(outcome) => return Ok(outcome),
            Err(ref err) if err.is_retryable() && attempt <= max_retries => {
                if let Some(plugin_err) = err.as_plugin_error() {
                    let delay = compute_backoff(plugin_err, attempt);
                    let commit_state_str =
                        plugin_err.commit_state.as_ref().map(|cs| format!("{cs:?}"));
                    #[allow(clippy::cast_possible_truncation)]
                    // Safety: delay.as_millis() is always well under u64::MAX
                    let delay_ms = delay.as_millis() as u64;
                    tracing::warn!(
                        attempt,
                        max_retries,
                        delay_ms,
                        category = %plugin_err.category,
                        code = %plugin_err.code,
                        commit_state = commit_state_str.as_deref(),
                        safe_to_retry = plugin_err.safe_to_retry,
                        "Retryable error, will retry"
                    );
                    send_progress(
                        &progress_tx,
                        ProgressEvent::Retry {
                            attempt,
                            max_retries,
                            message: format!(
                                "[{}] {}: {}",
                                plugin_err.category, plugin_err.code, plugin_err.message
                            ),
                            delay_secs: delay.as_secs_f64(),
                        },
                    );
                    tokio::select! {
                        () = cancel_token.cancelled() => {
                            return Err(cancelled_pipeline_error("Pipeline cancelled during retry backoff"));
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
            }
            Err(err) => {
                if let Some(plugin_err) = err.as_plugin_error() {
                    let commit_state_str =
                        plugin_err.commit_state.as_ref().map(|cs| format!("{cs:?}"));
                    if err.is_retryable() {
                        tracing::error!(
                            attempt,
                            max_retries,
                            category = %plugin_err.category,
                            code = %plugin_err.code,
                            commit_state = commit_state_str.as_deref(),
                            safe_to_retry = plugin_err.safe_to_retry,
                            "Max retries exhausted, failing pipeline"
                        );
                    } else {
                        tracing::error!(
                            category = %plugin_err.category,
                            code = %plugin_err.code,
                            commit_state = commit_state_str.as_deref(),
                            "Non-retryable plugin error, failing pipeline"
                        );
                    }
                } else {
                    tracing::error!("Infrastructure error, failing pipeline: {}", err);
                }
                return Err(err);
            }
        }
    }
}

struct MetricsRuntime<'a> {
    snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl MetricsRuntime<'_> {
    fn snapshot_for_run(
        &self,
        pipeline: &str,
        run: Option<&str>,
    ) -> rapidbyte_metrics::snapshot::PipelineMetricsSnapshot {
        self.snapshot_reader
            .flush_and_snapshot_for_run(self.meter_provider, pipeline, run)
    }
}

fn prepare_metrics_runtime<'a>(
    snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
) -> MetricsRuntime<'a> {
    MetricsRuntime {
        snapshot_reader,
        meter_provider,
    }
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn execute_pipeline_once(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    attempt: u32,
    progress_tx: ProgressTx,
    cancel_token: &CancellationToken,
    snapshot_reader: &rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
    registry_config: &rapidbyte_registry::RegistryConfig,
) -> Result<PipelineOutcome, PipelineError> {
    let start = Instant::now();
    let pipeline_id = PipelineId::new(config.pipeline.clone());
    tracing::info!(
        pipeline = config.pipeline,
        dry_run = options.dry_run,
        "Starting pipeline run"
    );

    send_progress(
        &progress_tx,
        ProgressEvent::PhaseChange {
            phase: Phase::Resolving,
        },
    );
    let plugins = resolve_plugins(config, registry_config).await?;
    if let Some(ref manifest) = plugins.source_manifest {
        validate_config_against_schema(&config.source.use_ref, &config.source.config, manifest)
            .map_err(PipelineError::Infrastructure)?;
    }
    if let Some(ref manifest) = plugins.dest_manifest {
        validate_config_against_schema(
            &config.destination.use_ref,
            &config.destination.config,
            manifest,
        )
        .map_err(PipelineError::Infrastructure)?;
    }
    let config_for_state = config.clone();
    let state = run_blocking_infrastructure_task(
        move || create_state_backend(&config_for_state),
        "create_state_backend",
    )
    .await?;

    let state_for_execution = state.clone();
    let execution_result = async move {
        let metrics_runtime = prepare_metrics_runtime(snapshot_reader, meter_provider);

        // Skip run tracking in dry-run mode to avoid orphaned run records.
        let run_id = if options.dry_run {
            0
        } else {
            let state_for_run = state_for_execution.clone();
            let pipeline_id_for_run = pipeline_id.clone();
            tokio::task::spawn_blocking(move || {
                state_for_run.start_run(&pipeline_id_for_run, &StreamName::new("all"))
            })
            .await
            .map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!("start_run task panicked: {e}"))
            })?
            .map_err(|e| PipelineError::Infrastructure(e.into()))?
        };
        let metric_run_label = if options.dry_run {
            format!(
                "dry-run-{}-{attempt}",
                NEXT_METRIC_RUN_LABEL.fetch_add(1, Ordering::Relaxed)
            )
        } else {
            format!("{run_id}:{attempt}")
        };

        send_progress(
            &progress_tx,
            ProgressEvent::PhaseChange {
                phase: Phase::Loading,
            },
        );
        let modules = load_all_modules(config, &plugins, registry_config).await?;
        let config_for_build = config.clone();
        let state_for_build = state_for_execution.clone();
        let max_records = options.limit;
        let source_manifest_for_build = plugins.source_manifest.clone();
        let stream_build = tokio::task::spawn_blocking(move || {
            build_stream_contexts(
                &config_for_build,
                state_for_build.as_ref(),
                max_records,
                source_manifest_for_build.as_ref(),
            )
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "build_stream_contexts task panicked: {e}"
            ))
        })??;
        send_progress(
            &progress_tx,
            ProgressEvent::PhaseChange {
                phase: Phase::Running,
            },
        );
        ensure_not_cancelled(cancel_token, "Pipeline cancelled before stream execution")?;
        let aggregated = match execute_streams(
            config,
            &plugins,
            &modules,
            &stream_build,
            state_for_execution.clone(),
            options,
            &metric_run_label,
            &progress_tx,
            cancel_token,
        )
        .await
        {
            Ok(agg) => agg,
            Err(err) => {
                // Drain the run's snapshot entry to prevent memory leaks in
                // long-lived processes with repeated failed attempts.
                let _ = metrics_runtime.snapshot_for_run(&config.pipeline, Some(&metric_run_label));
                return Err(err);
            }
        };

        send_progress(
            &progress_tx,
            ProgressEvent::PhaseChange {
                phase: Phase::Finished,
            },
        );

        preserve_real_outcome_after_stream_execution(cancel_token, async move {
            if options.dry_run {
                let duration_secs = start.elapsed().as_secs_f64();

                let snap =
                    metrics_runtime.snapshot_for_run(&config.pipeline, Some(&metric_run_label));

                return Ok(PipelineOutcome::DryRun(build_dry_run_result(
                    &snap,
                    aggregated,
                    modules.source_module_load_ms,
                    duration_secs,
                )));
            }

            let result = finalize_run(
                config,
                &pipeline_id,
                state_for_execution.clone(),
                run_id,
                attempt,
                start,
                &metric_run_label,
                &modules,
                aggregated,
                &metrics_runtime,
            )
            .await?;
            Ok(PipelineOutcome::Run(result))
        })
        .await
    }
    .await;

    let state_for_drop = state;
    run_blocking_infrastructure_task(
        move || {
            drop(state_for_drop);
            Ok(())
        },
        "drop_state_backend",
    )
    .await?;

    execution_result
}

struct CollectedTransforms {
    durations: Vec<f64>,
    first_error: Option<PipelineError>,
}

async fn collect_transform_results(
    transform_handles: Vec<(
        usize,
        tokio::task::JoinHandle<Result<TransformRunResult, PipelineError>>,
    )>,
    stream_name: &str,
) -> Result<CollectedTransforms, PipelineError> {
    let mut durations = Vec::new();
    let mut first_error: Option<PipelineError> = None;
    for (i, t_handle) in transform_handles {
        let result = t_handle.await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Transform {i} task panicked for stream '{stream_name}': {e}"
            ))
        })?;
        match result {
            Ok(tr) => {
                tracing::info!(
                    transform_index = i,
                    stream = stream_name,
                    duration_secs = tr.duration_secs,
                    records_in = tr.summary.records_in,
                    records_out = tr.summary.records_out,
                    "Transform stage completed for stream"
                );
                durations.push(tr.duration_secs);
            }
            Err(e) => {
                tracing::error!(
                    transform_index = i,
                    stream = stream_name,
                    "Transform failed: {e}",
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    Ok(CollectedTransforms {
        durations,
        first_error,
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::similar_names
)]
async fn execute_streams(
    config: &PipelineConfig,
    plugins: &ResolvedPlugins,
    modules: &PluginModules,
    stream_build: &ExecutionPlan,
    state: Arc<dyn StateBackend>,
    options: &ExecutionOptions,
    metric_run_label: &str,
    progress_tx: &ProgressTx,
    cancel_token: &CancellationToken,
) -> Result<AggregatedStreamResults, PipelineError> {
    let (source_plugin_id, source_plugin_version) = parse_plugin_ref(&config.source.use_ref);
    let (dest_plugin_id, dest_plugin_version) = parse_plugin_ref(&config.destination.use_ref);
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let num_transforms = config.transforms.len();
    let parallelism = execution_parallelism(config, &stream_build.stream_ctxs);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));

    tracing::info!(
        pipeline = config.pipeline,
        parallelism,
        num_streams = stream_build.stream_ctxs.len(),
        num_transforms,
        "Starting per-stream pipeline execution"
    );

    let source_manifest_limits = plugins
        .source_manifest
        .as_ref()
        .map(|m| m.limits.clone())
        .unwrap_or_default();
    let dest_manifest_limits = plugins
        .dest_manifest
        .as_ref()
        .map(|m| m.limits.clone())
        .unwrap_or_default();
    let transform_overrides: Vec<Option<SandboxOverrides>> = config
        .transforms
        .iter()
        .zip(modules.transform_modules.iter())
        .map(|(tc, tm)| {
            build_sandbox_overrides(
                tc.permissions.as_ref(),
                tc.limits.as_ref(),
                &tm.manifest_limits,
            )
        })
        .collect();

    let params = Arc::new(StreamParams {
        pipeline_name: config.pipeline.clone(),
        metric_run_label: metric_run_label.to_owned(),
        source_config: config.source.config.clone(),
        dest_config: config.destination.config.clone(),
        source_plugin_id,
        source_plugin_version,
        dest_plugin_id,
        dest_plugin_version,
        source_permissions: plugins.source_permissions.clone(),
        dest_permissions: plugins.dest_permissions.clone(),
        source_overrides: build_sandbox_overrides(
            config.source.permissions.as_ref(),
            config.source.limits.as_ref(),
            &source_manifest_limits,
        ),
        dest_overrides: build_sandbox_overrides(
            config.destination.permissions.as_ref(),
            config.destination.limits.as_ref(),
            &dest_manifest_limits,
        ),
        transform_overrides,
        compression: stream_build.compression,
        channel_capacity: (stream_build.limits.max_inflight_batches as usize).max(1),
    });

    let mut stream_join_set: JoinSet<Result<StreamResult, PipelineError>> = JoinSet::new();
    let run_dlq_records: Arc<Mutex<Vec<DlqRecord>>> = Arc::new(Mutex::new(Vec::new()));

    if !options.dry_run {
        ensure_not_cancelled(
            cancel_token,
            "Pipeline cancelled before destination preflight",
        )?;
        let preflight_streams = destination_preflight_streams(&stream_build.stream_ctxs);
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
            let permit = tokio::select! {
                () = cancel_token.cancelled() => {
                    return Err(cancelled_pipeline_error("Pipeline cancelled before destination preflight"));
                }
                permit = preflight_semaphore.clone().acquire_owned() => {
                    permit.map_err(|e| {
                        PipelineError::Infrastructure(anyhow::anyhow!(
                            "Preflight semaphore closed: {e}"
                        ))
                    })?
                }
            };

            let stream_name = stream_ctx.stream_name.clone();
            let state_dst = state.clone();
            let dest_module = modules.dest_module.clone();
            let params = params.clone();

            preflight_join_set.spawn(async move {
                let _permit = permit;
                let (tx, rx) = sync_mpsc::sync_channel::<Frame>(1);
                tx.send(Frame::EndStream).map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
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
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination preflight task panicked for stream '{stream_name}': {e}",
                    ))
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
                    return Err(PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination preflight join error: {join_err}"
                    )));
                }
            }
        }
    }

    for stream_ctx in &stream_build.stream_ctxs {
        ensure_not_cancelled(cancel_token, "Pipeline cancelled before stream execution")?;
        let permit = tokio::select! {
            () = cancel_token.cancelled() => {
                return Err(cancelled_pipeline_error("Pipeline cancelled before stream execution"));
            }
            permit = semaphore.clone().acquire_owned() => {
                permit.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!("Semaphore closed: {e}"))
                })?
            }
        };

        let params = params.clone();
        let source_module = modules.source_module.clone();
        let dest_module = modules.dest_module.clone();
        let state_src = state.clone();
        let state_dst = state.clone();
        let stats_src = stats.clone();
        let stats_dst = stats.clone();
        let stream_ctx = stream_ctx.clone();
        let run_dlq_records = run_dlq_records.clone();
        let transforms = modules.transform_modules.clone();
        let is_dry_run = options.dry_run;
        let dry_run_limit = options.limit;
        let progress_tx_for_stream = progress_tx.clone();

        let handle = tokio::spawn(async move {
            let num_t = transforms.len();
            let mut channels = Vec::with_capacity(num_t + 1);
            for _ in 0..=num_t {
                channels.push(sync_mpsc::sync_channel::<Frame>(params.channel_capacity));
            }

            let (mut senders, mut receivers): (
                Vec<sync_mpsc::SyncSender<Frame>>,
                Vec<sync_mpsc::Receiver<Frame>>,
            ) = channels.into_iter().unzip();

            let source_tx = senders.remove(0);
            let dest_rx = receivers.pop().ok_or_else(|| {
                PipelineError::Infrastructure(anyhow::anyhow!("Missing destination receiver"))
            })?;

            let stream_ctx_for_src = stream_ctx.clone();
            let stream_ctx_for_dst = stream_ctx.clone();

            // Build per-batch progress callback for the source runner
            let on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>> =
                progress_tx_for_stream.as_ref().map(|tx| {
                    let tx = tx.clone();
                    Arc::new(move |bytes: u64| {
                        let _ = tx.send(ProgressEvent::BatchEmitted { bytes });
                    }) as Arc<dyn Fn(u64) + Send + Sync>
                });

            let params_src = params.clone();
            let src_handle = tokio::task::spawn_blocking(move || {
                let ctx = StreamRunContext {
                    module: &source_module,
                    state_backend: state_src,
                    pipeline_name: &params_src.pipeline_name,
                    metric_run_label: &params_src.metric_run_label,
                    plugin_id: &params_src.source_plugin_id,
                    plugin_version: &params_src.source_plugin_version,
                    stream_ctx: &stream_ctx_for_src,
                    permissions: params_src.source_permissions.as_ref(),
                    compression: params_src.compression,
                    overrides: params_src.source_overrides.as_ref(),
                };
                run_source_stream(
                    &ctx,
                    source_tx,
                    &params_src.source_config,
                    stats_src,
                    on_emit,
                )
            });

            let mut transform_handles = Vec::with_capacity(num_t);
            for (i, t) in transforms.into_iter().enumerate() {
                let rx = receivers.remove(0);
                let tx = senders.remove(0);
                let state_t = state_dst.clone();
                let dlq_records_t = run_dlq_records.clone();
                let stream_ctx_t = stream_ctx.clone();
                let params_t = params.clone();
                let t_handle = tokio::task::spawn_blocking(move || {
                    let ctx = StreamRunContext {
                        module: &t.module,
                        state_backend: state_t,
                        pipeline_name: &params_t.pipeline_name,
                        metric_run_label: &params_t.metric_run_label,
                        plugin_id: &t.plugin_id,
                        plugin_version: &t.plugin_version,
                        stream_ctx: &stream_ctx_t,
                        permissions: t.permissions.as_ref(),
                        compression: params_t.compression,
                        overrides: params_t.transform_overrides.get(i).and_then(Option::as_ref),
                    };
                    run_transform_stream(&ctx, rx, tx, dlq_records_t, i, &t.config)
                });
                transform_handles.push((i, t_handle));
            }

            if is_dry_run {
                // Dry-run: collect frames instead of running destination plugin
                let compression = params.compression;
                let dry_run_stream_name = stream_ctx_for_dst.stream_name.clone();
                let collector_handle = tokio::task::spawn_blocking(move || {
                    collect_dry_run_frames(
                        &dry_run_stream_name,
                        &dest_rx,
                        dry_run_limit,
                        compression,
                    )
                });

                let src_result = src_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Source task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                let transforms =
                    collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

                let collected = collector_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Dry-run collector task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })??;

                drop(permit);

                if let Some(transform_err) = transforms.first_error {
                    return Err(transform_err);
                }

                let src = src_result?;

                Ok(StreamResult {
                    stream_name: stream_ctx.stream_name.clone(),
                    partition_index: stream_ctx.partition_index,
                    partition_count: stream_ctx.partition_count,
                    read_summary: src.summary,
                    write_summary: WriteSummary {
                        records_written: 0,
                        bytes_written: 0,
                        batches_written: 0,
                        checkpoint_count: 0,
                        records_failed: 0,
                    },
                    source_checkpoints: src.checkpoints,
                    dest_checkpoints: Vec::new(),
                    src_duration: src.duration_secs,
                    dst_duration: 0.0,
                    vm_setup_secs: 0.0,
                    recv_secs: 0.0,
                    transform_durations: transforms.durations,
                    dry_run_result: Some(collected),
                })
            } else {
                // Normal mode: run destination plugin
                let dst_handle = tokio::task::spawn_blocking(move || {
                    let ctx = StreamRunContext {
                        module: &dest_module,
                        state_backend: state_dst,
                        pipeline_name: &params.pipeline_name,
                        metric_run_label: &params.metric_run_label,
                        plugin_id: &params.dest_plugin_id,
                        plugin_version: &params.dest_plugin_version,
                        stream_ctx: &stream_ctx_for_dst,
                        permissions: params.dest_permissions.as_ref(),
                        compression: params.compression,
                        overrides: params.dest_overrides.as_ref(),
                    };
                    run_destination_stream(
                        &ctx,
                        dest_rx,
                        run_dlq_records,
                        &params.dest_config,
                        stats_dst,
                    )
                });

                let src_result = src_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Source task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                let transforms =
                    collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

                let dst_result = dst_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Destination task panicked for stream '{}': {}",
                        stream_ctx.stream_name,
                        e
                    ))
                })?;

                drop(permit);

                if let Some(transform_err) = transforms.first_error {
                    return Err(transform_err);
                }

                let src = src_result?;

                let dst = dst_result?;

                Ok(StreamResult {
                    stream_name: stream_ctx.stream_name.clone(),
                    partition_index: stream_ctx.partition_index,
                    partition_count: stream_ctx.partition_count,
                    read_summary: src.summary,
                    write_summary: dst.summary,
                    source_checkpoints: src.checkpoints,
                    dest_checkpoints: dst.checkpoints,
                    src_duration: src.duration_secs,
                    dst_duration: dst.duration_secs,
                    vm_setup_secs: dst.vm_setup_secs,
                    recv_secs: dst.recv_secs,
                    transform_durations: transforms.durations,
                    dry_run_result: None,
                })
            }
        });

        stream_join_set.spawn(async move {
            handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!("Stream task panicked: {e}"))
            })?
        });
    }

    let mut source_checkpoints = Vec::new();
    let mut dest_checkpoints = Vec::new();
    let mut total_read_summary = ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
    };
    let mut total_write_summary = WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
    };
    let mut max_source_duration: f64 = 0.0;
    let mut max_dest_duration: f64 = 0.0;
    let mut max_vm_setup_secs: f64 = 0.0;
    let mut max_recv_secs: f64 = 0.0;
    let mut transform_durations = Vec::new();
    let mut dry_run_streams: Vec<DryRunStreamResult> = Vec::new();
    let mut stream_metrics: Vec<StreamShardMetric> = Vec::new();
    let stream_collection = collect_stream_task_results(stream_join_set, progress_tx).await?;

    for sr in stream_collection.successes {
        max_source_duration = max_source_duration.max(sr.src_duration);
        max_dest_duration = max_dest_duration.max(sr.dst_duration);
        max_vm_setup_secs = max_vm_setup_secs.max(sr.vm_setup_secs);
        max_recv_secs = max_recv_secs.max(sr.recv_secs);

        total_read_summary.records_read += sr.read_summary.records_read;
        total_read_summary.bytes_read += sr.read_summary.bytes_read;
        total_read_summary.batches_emitted += sr.read_summary.batches_emitted;
        total_read_summary.checkpoint_count += sr.read_summary.checkpoint_count;
        total_read_summary.records_skipped += sr.read_summary.records_skipped;

        total_write_summary.records_written += sr.write_summary.records_written;
        total_write_summary.bytes_written += sr.write_summary.bytes_written;
        total_write_summary.batches_written += sr.write_summary.batches_written;
        total_write_summary.checkpoint_count += sr.write_summary.checkpoint_count;
        total_write_summary.records_failed += sr.write_summary.records_failed;

        source_checkpoints.extend(sr.source_checkpoints);
        dest_checkpoints.extend(sr.dest_checkpoints);

        transform_durations.extend(sr.transform_durations);

        stream_metrics.push(StreamShardMetric {
            stream_name: sr.stream_name,
            partition_index: sr.partition_index,
            partition_count: sr.partition_count,
            records_read: sr.read_summary.records_read,
            records_written: sr.write_summary.records_written,
            bytes_read: sr.read_summary.bytes_read,
            bytes_written: sr.write_summary.bytes_written,
            source_duration_secs: sr.src_duration,
            dest_duration_secs: sr.dst_duration,
            dest_vm_setup_secs: sr.vm_setup_secs,
            dest_recv_secs: sr.recv_secs,
        });

        if let Some(dr) = sr.dry_run_result {
            dry_run_streams.push(dr);
        }
    }

    // Sanity-check partitioned read results
    {
        let shard_row_counts: Vec<u64> = stream_metrics
            .iter()
            .filter(|m| m.partition_index.is_some())
            .map(|m| m.records_read)
            .collect();

        if !shard_row_counts.is_empty() {
            if shard_row_counts.contains(&0) {
                tracing::warn!(
                    "PartitionedRead sanity check: one or more shards returned 0 rows \
                     — the source may not be honoring partition coordinates"
                );
            }
            if shard_row_counts.len() > 1
                && shard_row_counts.iter().all(|&c| c == shard_row_counts[0])
                && shard_row_counts[0] > 100
            {
                tracing::warn!(
                    shard_count = shard_row_counts.len(),
                    rows_per_shard = shard_row_counts[0],
                    "PartitionedRead sanity check: all shards returned identical row counts \
                     — the source may be ignoring partition coordinates"
                );
            }
        }
    }

    let first_error = stream_collection.first_error;

    let dlq_records = run_dlq_records
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("DLQ collection mutex poisoned"))
        })?
        .drain(..)
        .collect();

    let final_stats = stats
        .lock()
        .map_err(|_| PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned")))?
        .clone();

    Ok(AggregatedStreamResults {
        execution_parallelism: u32::try_from(parallelism).unwrap_or(u32::MAX),
        total_read_summary,
        total_write_summary,
        source_checkpoints,
        dest_checkpoints,
        max_source_duration,
        max_dest_duration,
        max_vm_setup_secs,
        max_recv_secs,
        transform_durations,
        dlq_records,
        final_stats,
        first_error,
        dry_run_streams,
        stream_metrics,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn finalize_run(
    config: &PipelineConfig,
    pipeline_id: &PipelineId,
    state: Arc<dyn StateBackend>,
    run_id: i64,
    attempt: u32,
    start: Instant,
    metric_run_label: &str,
    modules: &PluginModules,
    mut aggregated: AggregatedStreamResults,
    metrics_runtime: &MetricsRuntime<'_>,
) -> Result<PipelineResult, PipelineError> {
    if let Some(err) = aggregated.first_error {
        // Drain the run's snapshot entry so it doesn't accumulate in the
        // SnapshotReader's finished_run_snapshots map (memory leak in
        // long-lived agent processes).
        let _ = metrics_runtime.snapshot_for_run(&config.pipeline, Some(metric_run_label));

        let state_for_complete = state.clone();
        let run_stats = RunStats {
            records_read: aggregated.final_stats.records_read,
            records_written: aggregated.final_stats.records_written,
            bytes_read: aggregated.final_stats.bytes_read,
            bytes_written: aggregated.final_stats.bytes_written,
            error_message: Some(format!("Stream error: {err}")),
        };
        tokio::task::spawn_blocking(move || {
            state_for_complete.complete_run(run_id, RunStatus::Failed, &run_stats)
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("complete_run task panicked: {e}"))
        })?
        .map_err(|e| PipelineError::Infrastructure(e.into()))?;

        let state_for_dlq = state.clone();
        let pipeline_id_for_dlq = pipeline_id.clone();
        let dlq_records = std::mem::take(&mut aggregated.dlq_records);
        tokio::task::spawn_blocking(move || {
            crate::finalizers::dlq::persist_dlq_records(
                state_for_dlq.as_ref(),
                &pipeline_id_for_dlq,
                run_id,
                &dlq_records,
            )
        })
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("persist_dlq_records task panicked: {e}"))
        })?
        .map_err(|e| PipelineError::Infrastructure(e.into()))?;

        return Err(err);
    }

    let snap = metrics_runtime.snapshot_for_run(&config.pipeline, Some(metric_run_label));

    let wasm_overhead_secs = compute_wasm_overhead_secs(&snap, &aggregated);

    tracing::debug!(
        pipeline = config.pipeline,
        source_checkpoint_count = aggregated.source_checkpoints.len(),
        dest_checkpoint_count = aggregated.dest_checkpoints.len(),
        "About to correlate checkpoints"
    );

    let cursors_advanced =
        finalize_successful_run_state(state.clone(), pipeline_id, run_id, &mut aggregated).await?;
    if cursors_advanced > 0 {
        tracing::info!(
            pipeline = config.pipeline,
            cursors_advanced,
            "Checkpoint coordination complete"
        );
    }

    let duration = start.elapsed();
    let transform_module_load_ms = modules
        .transform_modules
        .iter()
        .map(|m| m.load_ms)
        .collect::<Vec<_>>();

    tracing::info!(
        pipeline = config.pipeline,
        records_read = aggregated.total_read_summary.records_read,
        records_written = aggregated.total_write_summary.records_written,
        duration_secs = duration.as_secs_f64(),
        "Pipeline run completed"
    );

    Ok(PipelineResult {
        counts: PipelineCounts {
            records_read: aggregated.total_read_summary.records_read,
            records_written: aggregated.total_write_summary.records_written,
            bytes_read: aggregated.total_read_summary.bytes_read,
            bytes_written: aggregated.total_write_summary.bytes_written,
        },
        source: build_source_timing(
            &snap,
            aggregated.max_source_duration,
            modules.source_module_load_ms,
        ),
        dest: build_dest_timing(&snap, &aggregated, modules.dest_module_load_ms),
        transform_count: aggregated.transform_durations.len(),
        transform_duration_secs: aggregated.transform_durations.iter().sum(),
        transform_module_load_ms,
        duration_secs: duration.as_secs_f64(),
        wasm_overhead_secs,
        retry_count: attempt.saturating_sub(1),
        parallelism: reported_parallelism(config, &aggregated),
        stream_metrics: aggregated.stream_metrics,
    })
}

fn reported_parallelism(config: &PipelineConfig, aggregated: &AggregatedStreamResults) -> u32 {
    if aggregated.execution_parallelism > 0 {
        aggregated.execution_parallelism
    } else {
        compute_pipeline_parallelism(config, false)
    }
}

fn build_source_timing(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    max_source_duration: f64,
    source_module_load_ms: u64,
) -> SourceTiming {
    SourceTiming {
        duration_secs: max_source_duration,
        module_load_ms: source_module_load_ms,
        connect_secs: snap.source_connect_secs,
        query_secs: snap.source_query_secs,
        fetch_secs: snap.source_fetch_secs,
        arrow_encode_secs: snap.source_encode_secs,
        emit_nanos: snap.emit_batch_nanos,
        compress_nanos: snap.compress_nanos,
        emit_count: snap.emit_count,
    }
}

fn build_dry_run_result(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: AggregatedStreamResults,
    source_module_load_ms: u64,
    duration_secs: f64,
) -> DryRunResult {
    DryRunResult {
        streams: aggregated.dry_run_streams,
        source: build_source_timing(snap, aggregated.max_source_duration, source_module_load_ms),
        transform_count: aggregated.transform_durations.len(),
        transform_duration_secs: aggregated.transform_durations.iter().sum(),
        duration_secs,
    }
}

fn compute_wasm_overhead_secs(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: &AggregatedStreamResults,
) -> f64 {
    let plugin_internal_secs =
        snap.dest_connect_secs + snap.dest_flush_secs + snap.dest_commit_secs;

    (aggregated.max_dest_duration
        - aggregated.max_vm_setup_secs
        - aggregated.max_recv_secs
        - plugin_internal_secs)
        .max(0.0)
}

fn build_dest_timing(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: &AggregatedStreamResults,
    dest_module_load_ms: u64,
) -> DestTiming {
    DestTiming {
        duration_secs: aggregated.max_dest_duration,
        module_load_ms: dest_module_load_ms,
        connect_secs: snap.dest_connect_secs,
        flush_secs: snap.dest_flush_secs,
        commit_secs: snap.dest_commit_secs,
        arrow_decode_secs: snap.dest_decode_secs,
        vm_setup_secs: aggregated.max_vm_setup_secs,
        recv_secs: aggregated.max_recv_secs,
        recv_nanos: snap.next_batch_nanos,
        recv_wait_nanos: snap.next_batch_wait_nanos,
        recv_process_nanos: snap.next_batch_process_nanos,
        decompress_nanos: snap.decompress_nanos,
        recv_count: snap.next_batch_count,
    }
}

fn cancelled_pipeline_error(message: &str) -> PipelineError {
    let mut error = PluginError::internal("CANCELLED", message);
    error.safe_to_retry = true;
    error.commit_state = Some(CommitState::BeforeCommit);
    PipelineError::Plugin(error)
}

fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    message: &str,
) -> Result<(), PipelineError> {
    if cancel_token.is_cancelled() {
        return Err(cancelled_pipeline_error(message));
    }
    Ok(())
}

async fn preserve_real_outcome_after_stream_execution<T, Fut>(
    _cancel_token: &CancellationToken,
    finalize: Fut,
) -> Result<T, PipelineError>
where
    Fut: Future<Output = Result<T, PipelineError>>,
{
    finalize.await
}

async fn complete_run_status(
    backend: Arc<dyn StateBackend>,
    run_id: i64,
    status: RunStatus,
    run_stats: RunStats,
) -> Result<(), PipelineError> {
    tokio::task::spawn_blocking(move || backend.complete_run(run_id, status, &run_stats))
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("complete_run task panicked: {e}"))
        })?
        .map_err(|e| PipelineError::Infrastructure(e.into()))
}

async fn finalize_successful_run_state(
    state: Arc<dyn StateBackend>,
    pipeline_id: &PipelineId,
    run_id: i64,
    aggregated: &mut AggregatedStreamResults,
) -> Result<u64, PipelineError> {
    let state_for_cursor = state.clone();
    let pipeline_id_for_cursor = pipeline_id.clone();
    let source_checkpoints = std::mem::take(&mut aggregated.source_checkpoints);
    let dest_checkpoints = std::mem::take(&mut aggregated.dest_checkpoints);
    let cursor_result = tokio::task::spawn_blocking(move || {
        correlate_and_persist_cursors(
            state_for_cursor.as_ref(),
            &pipeline_id_for_cursor,
            &source_checkpoints,
            &dest_checkpoints,
        )
    })
    .await
    .map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "correlate_and_persist_cursors task panicked: {e}"
        ))
    })?;

    let cursors_advanced = match cursor_result {
        Ok(cursors_advanced) => cursors_advanced,
        Err(error) => {
            let failed_stats = finalization_failed_stats(aggregated, &error.to_string());
            let _ = complete_run_status(state, run_id, RunStatus::Failed, failed_stats).await;
            return Err(PipelineError::Infrastructure(error));
        }
    };

    let state_for_dlq = state.clone();
    let pipeline_id_for_dlq = pipeline_id.clone();
    let dlq_records = std::mem::take(&mut aggregated.dlq_records);
    let dlq_result = tokio::task::spawn_blocking(move || {
        crate::finalizers::dlq::persist_dlq_records(
            state_for_dlq.as_ref(),
            &pipeline_id_for_dlq,
            run_id,
            &dlq_records,
        )
    })
    .await
    .map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("persist_dlq_records task panicked: {e}"))
    })?;
    if let Err(error) = dlq_result {
        let failed_stats = finalization_failed_stats(aggregated, &error.to_string());
        let _ = complete_run_status(state, run_id, RunStatus::Failed, failed_stats).await;
        return Err(PipelineError::Infrastructure(error.into()));
    }

    let complete_stats = RunStats {
        records_read: aggregated.total_read_summary.records_read,
        records_written: aggregated.total_write_summary.records_written,
        bytes_read: aggregated.total_read_summary.bytes_read,
        bytes_written: aggregated.total_write_summary.bytes_written,
        error_message: None,
    };
    complete_run_status(state, run_id, RunStatus::Completed, complete_stats).await?;

    Ok(cursors_advanced)
}

fn finalization_failed_stats(
    aggregated: &AggregatedStreamResults,
    error_message: &str,
) -> RunStats {
    RunStats {
        records_read: aggregated.total_read_summary.records_read,
        records_written: aggregated.total_write_summary.records_written,
        bytes_read: aggregated.total_read_summary.bytes_read,
        bytes_written: aggregated.total_write_summary.bytes_written,
        error_message: Some(format!("Post-run finalization failed: {error_message}")),
    }
}

/// Collect frames from a channel, decode IPC, enforce row limit.
/// Used in dry-run mode instead of the destination runner.
fn collect_dry_run_frames(
    stream_name: &str,
    receiver: &sync_mpsc::Receiver<Frame>,
    limit: Option<u64>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
) -> Result<DryRunStreamResult, PipelineError> {
    let mut batches = Vec::new();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;

    'recv: while let Ok(frame) = receiver.recv() {
        let Frame::Data { payload: data, .. } = frame else {
            break 'recv;
        };
        let ipc_bytes = match compression {
            Some(codec) => {
                rapidbyte_runtime::compression::decompress(codec, &data).map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Dry-run decompression failed: {e}"
                    ))
                })?
            }
            None => data.to_vec(),
        };

        let decoded = ipc_to_record_batches(&ipc_bytes).map_err(PipelineError::Infrastructure)?;

        for batch in decoded {
            let rows = batch.num_rows() as u64;
            total_bytes += batch.get_array_memory_size() as u64;

            if let Some(max) = limit {
                let remaining = max.saturating_sub(total_rows);
                if remaining == 0 {
                    break 'recv;
                }
                if rows > remaining {
                    #[allow(clippy::cast_possible_truncation)]
                    batches.push(batch.slice(0, remaining as usize));
                    total_rows += remaining;
                    break 'recv;
                }
            }

            total_rows += rows;
            batches.push(batch);
        }
    }

    Ok(DryRunStreamResult {
        stream_name: stream_name.to_string(),
        batches,
        total_rows,
        total_bytes,
    })
}

/// Check a pipeline: validate configuration and connectivity without running.
///
/// # Errors
///
/// Returns an error if plugin resolution, module loading, or validation fails.
#[allow(clippy::too_many_lines)]
pub async fn check_pipeline(
    config: &PipelineConfig,
    registry_config: &rapidbyte_registry::RegistryConfig,
) -> Result<CheckResult> {
    tracing::info!(
        pipeline = config.pipeline,
        "Checking pipeline configuration"
    );

    let plugins = resolve_plugins(config, registry_config)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let source_manifest = plugins.source_manifest.as_ref().map(|_| CheckItemResult {
        ok: true,
        message: String::new(),
    });
    let destination_manifest = plugins.dest_manifest.as_ref().map(|_| CheckItemResult {
        ok: true,
        message: String::new(),
    });
    let source_config = plugins.source_manifest.as_ref().map(|manifest| {
        config_check_result(&config.source.use_ref, &config.source.config, manifest)
    });
    let destination_config = plugins.dest_manifest.as_ref().map(|manifest| {
        config_check_result(
            &config.destination.use_ref,
            &config.destination.config,
            manifest,
        )
    });

    let state = check_state_backend(config);

    let source_config_json = config.source.config.clone();
    let source_permissions = plugins.source_permissions.clone();
    let (src_id, src_ver) = parse_plugin_ref(&config.source.use_ref);
    let source_wasm = plugins.source_wasm.clone();
    let source_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_plugin(
                &source_wasm,
                PluginKind::Source,
                &src_id,
                &src_ver,
                &source_config_json,
                "check",
                source_permissions.as_ref(),
            )
        });

    let dest_config_json = config.destination.config.clone();
    let dest_permissions = plugins.dest_permissions.clone();
    let (dst_id, dst_ver) = parse_plugin_ref(&config.destination.use_ref);
    let dest_wasm = plugins.dest_wasm.clone();
    let dest_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_plugin(
                &dest_wasm,
                PluginKind::Destination,
                &dst_id,
                &dst_ver,
                &dest_config_json,
                "check",
                dest_permissions.as_ref(),
            )
        });

    let source_validation = source_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Source validation task panicked: {e}"))??;
    let dest_validation = dest_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Destination validation task panicked: {e}"))??;

    let mut transform_tasks = Vec::with_capacity(config.transforms.len());
    let mut transform_configs = Vec::with_capacity(config.transforms.len());
    let source_stream_names = config
        .source
        .streams
        .iter()
        .map(|stream| stream.name.clone())
        .collect::<Vec<_>>();
    for (index, tc) in config.transforms.iter().enumerate() {
        let wasm_path =
            rapidbyte_runtime::resolve_plugin(&tc.use_ref, PluginKind::Transform, registry_config)
                .await?;
        let manifest = load_and_validate_manifest(&wasm_path, &tc.use_ref, PluginKind::Transform)?;
        if let Some(ref m) = manifest {
            transform_configs.push(config_check_result(&tc.use_ref, &tc.config, m));
        } else {
            transform_configs.push(CheckItemResult {
                ok: true,
                message: String::new(),
            });
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let config_val = tc.config.clone();
        let plugin_ref = tc.use_ref.clone();
        let (tc_id, tc_ver) = parse_plugin_ref(&tc.use_ref);
        let stream_names = source_stream_names.clone();
        let handle = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_transform_for_streams(&stream_names, |stream_name| {
                validate_plugin(
                    &wasm_path,
                    PluginKind::Transform,
                    &tc_id,
                    &tc_ver,
                    &config_val,
                    stream_name,
                    transform_perms.as_ref(),
                )
            })
        });
        transform_tasks.push((index, plugin_ref, handle));
    }

    let mut transform_validations = Vec::with_capacity(transform_tasks.len());
    for (index, plugin_ref, handle) in transform_tasks {
        let result = handle.await.map_err(|e| {
            anyhow::anyhow!("Transform validation task panicked (index {index}, {plugin_ref}): {e}")
        })??;
        transform_validations.push(result);
    }

    Ok(CheckResult {
        source_manifest,
        destination_manifest,
        source_config,
        destination_config,
        transform_configs,
        source_validation,
        destination_validation: dest_validation,
        transform_validations,
        state,
    })
}

fn validate_transform_for_streams<F>(
    stream_names: &[String],
    mut validate: F,
) -> Result<ValidationResult>
where
    F: FnMut(&str) -> Result<ValidationResult>,
{
    let mut failures = Vec::new();
    let mut warnings = Vec::new();
    let mut saw_warning = false;

    for stream_name in stream_names {
        let result = validate(stream_name)?;
        match result.status {
            ValidationStatus::Success => {}
            ValidationStatus::Warning => {
                saw_warning = true;
                if !result.message.is_empty() {
                    warnings.push(format!("stream '{stream_name}': {}", result.message));
                }
                warnings.extend(
                    result
                        .warnings
                        .into_iter()
                        .map(|warning| format!("stream '{stream_name}': {warning}")),
                );
            }
            ValidationStatus::Failed => {
                failures.push(if result.message.is_empty() {
                    format!("stream '{stream_name}': validation failed")
                } else {
                    format!("stream '{stream_name}': {}", result.message)
                });
                warnings.extend(
                    result
                        .warnings
                        .into_iter()
                        .map(|warning| format!("stream '{stream_name}': {warning}")),
                );
            }
        }
    }

    if !failures.is_empty() {
        return Ok(ValidationResult {
            status: ValidationStatus::Failed,
            message: failures.join("; "),
            warnings,
        });
    }

    if saw_warning {
        return Ok(ValidationResult {
            status: ValidationStatus::Warning,
            message: "Transform configuration emitted validation warnings".to_string(),
            warnings,
        });
    }

    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Transform configuration is valid for all source streams".to_string(),
        warnings,
    })
}

fn config_check_result(
    plugin_ref: &str,
    config: &serde_json::Value,
    manifest: &PluginManifest,
) -> CheckItemResult {
    match validate_config_against_schema(plugin_ref, config, manifest) {
        Ok(()) => CheckItemResult {
            ok: true,
            message: String::new(),
        },
        Err(error) => CheckItemResult {
            ok: false,
            message: error.to_string(),
        },
    }
}

/// Discover available streams from a source plugin.
///
/// # Errors
///
/// Returns an error if the plugin cannot be loaded, opened, or discovery fails.
pub async fn discover_plugin(
    plugin_ref: &str,
    config: &serde_json::Value,
    registry_config: &rapidbyte_registry::RegistryConfig,
) -> Result<Catalog> {
    let wasm_path =
        rapidbyte_runtime::resolve_plugin(plugin_ref, PluginKind::Source, registry_config).await?;
    let manifest = load_and_validate_manifest(&wasm_path, plugin_ref, PluginKind::Source)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());
    let (plugin_id, plugin_version) = parse_plugin_ref(plugin_ref);
    let config = config.clone();

    tokio::task::spawn_blocking(move || {
        run_discover(
            &wasm_path,
            &plugin_id,
            &plugin_version,
            &config,
            permissions.as_ref(),
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("Discover task panicked: {e}"))?
}

#[cfg(test)]
mod stream_task_collection_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn success_result(stream_name: &str) -> StreamResult {
        StreamResult {
            stream_name: stream_name.to_string(),
            partition_index: None,
            partition_count: None,
            read_summary: ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            },
            write_summary: WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
            },
            source_checkpoints: Vec::new(),
            dest_checkpoints: Vec::new(),
            src_duration: 0.0,
            dst_duration: 0.0,
            vm_setup_secs: 0.0,
            recv_secs: 0.0,
            transform_durations: Vec::new(),
            dry_run_result: None,
        }
    }

    #[tokio::test]
    async fn collect_stream_tasks_fails_fast_and_cancels_siblings() {
        let mut join_set: JoinSet<Result<StreamResult, PipelineError>> = JoinSet::new();
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(success_result("slow_stream"))
        });
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Err(PipelineError::Infrastructure(anyhow::anyhow!(
                "expected failure"
            )))
        });

        let start = Instant::now();
        let collected = collect_stream_task_results(join_set, &None)
            .await
            .expect("collector should return first error, not infra panic");

        assert!(collected.first_error.is_some());
        assert!(collected.successes.is_empty());
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}

#[cfg(test)]
mod dry_run_tests {
    use super::*;
    use crate::arrow::record_batch_to_ipc;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let max = i64::try_from(n).expect("test row count must fit in i64");
        let ids: Vec<i64> = (0..max).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(names)) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[test]
    fn collect_dry_run_frames_basic() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        let batch = make_test_batch(5);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .unwrap();
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, None, None).unwrap();
        assert_eq!(result.stream_name, "public.users");
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.batches.len(), 1);
    }

    #[test]
    fn collect_dry_run_frames_with_limit() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        let batch = make_test_batch(100);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .unwrap();
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, Some(10), None).unwrap();
        assert_eq!(result.total_rows, 10);
        let total: usize = result.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn collect_dry_run_frames_multiple_batches() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        for _ in 0..3 {
            let batch = make_test_batch(5);
            let ipc = record_batch_to_ipc(&batch).unwrap();
            tx.send(Frame::Data {
                payload: bytes::Bytes::from(ipc),
                checkpoint_id: 1,
            })
            .unwrap();
        }
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, Some(12), None).unwrap();
        assert_eq!(result.stream_name, "public.users");
        assert_eq!(result.total_rows, 12);
    }
}

#[cfg(test)]
mod metrics_runtime_tests {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use rapidbyte_metrics::snapshot::SnapshotReader;
    use std::sync::Mutex;

    /// Tests in this module set the process-global meter provider and must not
    /// run concurrently with each other.
    static PROVIDER_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn snapshot_for_run_drains_entry_so_repeated_calls_return_default() {
        let _guard = PROVIDER_LOCK.lock().expect("provider lock poisoned");
        let reader = SnapshotReader::new();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.build_reader())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());

        // Record a metric tagged with a specific run label.
        rapidbyte_metrics::instruments::pipeline::records_read().add(
            42,
            &[
                KeyValue::new(rapidbyte_metrics::labels::PIPELINE, "pipe"),
                KeyValue::new(rapidbyte_metrics::labels::RUN, "run-1"),
            ],
        );

        // First snapshot drains the entry.
        let snap1 = reader.flush_and_snapshot_for_run(&provider, "pipe", Some("run-1"));
        assert_eq!(snap1.records_read, 42);

        // Second call for the same run returns default (entry was removed).
        let snap2 = reader.flush_and_snapshot_for_run(&provider, "pipe", Some("run-1"));
        assert_eq!(
            snap2.records_read, 0,
            "finished_run_snapshots should not retain entries after take"
        );
    }
}

#[cfg(test)]
mod finalize_run_state_tests {
    use super::*;
    use rapidbyte_state::error::{Result as StateResult, StateError};
    use rapidbyte_types::checkpoint::{Checkpoint, CheckpointKind};
    use rapidbyte_types::cursor::CursorValue;
    use rapidbyte_types::state::CursorState;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestStateBackend {
        complete_statuses: Mutex<Vec<(RunStatus, Option<String>)>>,
        cursor_written: AtomicBool,
        fail_set_cursor: bool,
        fail_insert_dlq: bool,
    }

    impl TestStateBackend {
        fn new(fail_set_cursor: bool, fail_insert_dlq: bool) -> Self {
            Self {
                complete_statuses: Mutex::new(Vec::new()),
                cursor_written: AtomicBool::new(false),
                fail_set_cursor,
                fail_insert_dlq,
            }
        }
    }

    impl StateBackend for TestStateBackend {
        fn get_cursor(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
        ) -> StateResult<Option<CursorState>> {
            Ok(None)
        }

        fn set_cursor(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
            _cursor: &CursorState,
        ) -> StateResult<()> {
            if self.fail_set_cursor {
                return Err(StateError::backend(std::io::Error::other(
                    "cursor write failed",
                )));
            }
            self.cursor_written.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn start_run(&self, _pipeline: &PipelineId, _stream: &StreamName) -> StateResult<i64> {
            Ok(1)
        }

        fn complete_run(
            &self,
            _run_id: i64,
            run_status: RunStatus,
            completion_stats: &RunStats,
        ) -> StateResult<()> {
            self.complete_statuses
                .lock()
                .expect("complete statuses lock poisoned")
                .push((run_status, completion_stats.error_message.clone()));
            Ok(())
        }

        fn compare_and_set(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
            _expected: Option<&str>,
            _new_value: &str,
        ) -> StateResult<bool> {
            Ok(true)
        }

        fn insert_dlq_records(
            &self,
            _pipeline: &PipelineId,
            _run_id: i64,
            _records: &[DlqRecord],
        ) -> StateResult<u64> {
            if self.fail_insert_dlq {
                return Err(StateError::backend(std::io::Error::other(
                    "dlq insert failed",
                )));
            }
            Ok(0)
        }
    }

    fn make_aggregated_results() -> AggregatedStreamResults {
        AggregatedStreamResults {
            execution_parallelism: 1,
            total_read_summary: ReadSummary {
                records_read: 10,
                bytes_read: 100,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
            },
            total_write_summary: WriteSummary {
                records_written: 10,
                bytes_written: 100,
                batches_written: 1,
                checkpoint_count: 1,
                records_failed: 0,
            },
            source_checkpoints: vec![Checkpoint {
                id: 7,
                kind: CheckpointKind::Source,
                stream: "users".to_string(),
                cursor_field: Some("id".to_string()),
                cursor_value: Some(CursorValue::Int64 { value: 42 }),
                records_processed: 10,
                bytes_processed: 100,
            }],
            dest_checkpoints: vec![Checkpoint {
                id: 7,
                kind: CheckpointKind::Dest,
                stream: "users".to_string(),
                cursor_field: None,
                cursor_value: None,
                records_processed: 10,
                bytes_processed: 100,
            }],
            max_source_duration: 0.0,
            max_dest_duration: 0.0,
            max_vm_setup_secs: 0.0,
            max_recv_secs: 0.0,
            transform_durations: Vec::new(),
            dlq_records: Vec::new(),
            final_stats: RunStats::default(),
            first_error: None,
            dry_run_streams: Vec::new(),
            stream_metrics: Vec::new(),
        }
    }

    #[test]
    fn reported_parallelism_prefers_aggregated_execution_parallelism() {
        let yaml = r#"
version: "1.0"
pipeline: test_parallelism
source:
  use: postgres
  config: {}
  streams:
    - name: bench_events
      sync_mode: full_refresh
destination:
  use: postgres
  config: {}
  write_mode: append
resources:
  parallelism: auto
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).expect("valid pipeline yaml");
        let mut aggregated = make_aggregated_results();
        aggregated.execution_parallelism = 12;

        assert_eq!(reported_parallelism(&config, &aggregated), 12);
    }

    #[tokio::test]
    async fn successful_finalization_marks_run_completed_after_cursor_persist() {
        let backend = Arc::new(TestStateBackend::new(false, false));
        let mut aggregated = make_aggregated_results();

        let advanced = finalize_successful_run_state(
            backend.clone(),
            &PipelineId::new("p"),
            1,
            &mut aggregated,
        )
        .await
        .expect("finalization should succeed");

        assert_eq!(advanced, 1);
        assert!(backend.cursor_written.load(Ordering::SeqCst));
        assert_eq!(
            backend
                .complete_statuses
                .lock()
                .expect("complete statuses lock poisoned")
                .as_slice(),
            &[(RunStatus::Completed, None)]
        );
    }

    #[tokio::test]
    async fn cursor_finalization_failure_marks_run_failed_not_completed() {
        let backend = Arc::new(TestStateBackend::new(true, false));
        let mut aggregated = make_aggregated_results();

        let result = finalize_successful_run_state(
            backend.clone(),
            &PipelineId::new("p"),
            1,
            &mut aggregated,
        )
        .await;

        assert!(result.is_err());
        assert!(!backend.cursor_written.load(Ordering::SeqCst));

        let statuses = backend
            .complete_statuses
            .lock()
            .expect("complete statuses lock poisoned");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].0, RunStatus::Failed);
        assert!(statuses[0]
            .1
            .as_deref()
            .unwrap_or_default()
            .contains("Post-run finalization failed"));
    }

    #[tokio::test]
    async fn dlq_finalization_failure_marks_run_failed_not_completed() {
        let backend = Arc::new(TestStateBackend::new(false, true));
        let mut aggregated = make_aggregated_results();
        aggregated.dlq_records.push(DlqRecord {
            stream_name: "users".to_string(),
            record_json: "{\"id\":42}".to_string(),
            error_message: "bad row".to_string(),
            error_category: rapidbyte_types::error::ErrorCategory::Data,
            failed_at: rapidbyte_types::envelope::Timestamp::new("2026-03-08T00:00:00Z"),
        });

        let result = finalize_successful_run_state(
            backend.clone(),
            &PipelineId::new("p"),
            1,
            &mut aggregated,
        )
        .await;

        assert!(result.is_err());
        assert!(backend.cursor_written.load(Ordering::SeqCst));

        let statuses = backend
            .complete_statuses
            .lock()
            .expect("complete statuses lock poisoned");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].0, RunStatus::Failed);
        assert!(statuses[0]
            .1
            .as_deref()
            .unwrap_or_default()
            .contains("Post-run finalization failed"));
    }

    #[tokio::test]
    async fn cancellation_after_stream_execution_preserves_real_finalization_outcome() {
        let backend = Arc::new(TestStateBackend::new(false, false));
        let mut aggregated = make_aggregated_results();
        let token = CancellationToken::new();
        token.cancel();

        let advanced = preserve_real_outcome_after_stream_execution(&token, async {
            finalize_successful_run_state(
                backend.clone(),
                &PipelineId::new("p"),
                1,
                &mut aggregated,
            )
            .await
        })
        .await
        .expect("finalization should succeed even after late cancellation");

        assert_eq!(advanced, 1);
        assert_eq!(
            backend
                .complete_statuses
                .lock()
                .expect("complete statuses lock poisoned")
                .as_slice(),
            &[(RunStatus::Completed, None)]
        );
    }

    #[tokio::test]
    async fn blocking_infrastructure_helper_moves_runtime_sensitive_init_off_worker_threads() {
        let value = run_blocking_infrastructure_task(
            || {
                let runtime = tokio::runtime::Runtime::new().expect("runtime");
                runtime.block_on(async { Ok::<_, anyhow::Error>(7) })
            },
            "test_blocking_init",
        )
        .await
        .expect("blocking helper should allow runtime creation");

        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn blocking_infrastructure_helper_moves_runtime_sensitive_drop_off_worker_threads() {
        struct RuntimeOnDrop;

        impl Drop for RuntimeOnDrop {
            fn drop(&mut self) {
                let runtime = tokio::runtime::Runtime::new().expect("runtime in drop");
                runtime.block_on(async {});
            }
        }

        run_blocking_infrastructure_task(
            || {
                let value = RuntimeOnDrop;
                drop(value);
                Ok::<_, anyhow::Error>(())
            },
            "test_blocking_drop",
        )
        .await
        .expect("blocking helper should allow runtime-sensitive drop");
    }
}

#[cfg(test)]
mod check_pipeline_validation_tests {
    use super::validate_transform_for_streams;
    use anyhow::Result;
    use rapidbyte_types::error::{ValidationResult, ValidationStatus};
    use std::sync::Mutex;

    #[test]
    fn transform_check_uses_actual_source_stream_names() {
        let seen = Mutex::new(Vec::new());
        let stream_names = vec!["users".to_string()];

        let result = validate_transform_for_streams(&stream_names, |stream_name| -> Result<_> {
            seen.lock()
                .expect("seen lock poisoned")
                .push(stream_name.to_string());
            Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "ok".to_string(),
                warnings: Vec::new(),
            })
        })
        .expect("validation should succeed");

        assert_eq!(
            seen.lock().expect("seen lock poisoned").as_slice(),
            ["users"]
        );
        assert_eq!(result.status, ValidationStatus::Success);
    }

    #[test]
    fn transform_check_fails_when_any_source_stream_validation_fails() {
        let stream_names = vec!["users".to_string(), "orders".to_string()];

        let result = validate_transform_for_streams(&stream_names, |stream_name| -> Result<_> {
            let status = if stream_name == "orders" {
                ValidationStatus::Failed
            } else {
                ValidationStatus::Success
            };
            let message = if stream_name == "orders" {
                "SQL query must reference current stream table 'orders'".to_string()
            } else {
                "ok".to_string()
            };
            Ok(ValidationResult {
                status,
                message,
                warnings: Vec::new(),
            })
        })
        .expect("validation should return aggregated result");

        assert_eq!(result.status, ValidationStatus::Failed);
        assert!(result.message.contains("orders"));
        assert!(result.message.contains("current stream table 'orders'"));
    }
}
