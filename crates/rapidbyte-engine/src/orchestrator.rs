//! Pipeline orchestrator: resolves plugins, loads modules, executes streams, and finalizes state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rapidbyte_runtime::{parse_plugin_ref, SandboxOverrides};
use rapidbyte_state::StateBackend;
use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::{ValidationResult, ValidationStatus};
use rapidbyte_types::manifest::PluginManifest;
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::state::{PipelineId, RunStats, StreamName};
use rapidbyte_types::wire::PluginKind;

use crate::config::types::PipelineConfig;
use crate::error::{compute_backoff, PipelineError};
use crate::execution::{DryRunStreamResult, ExecutionOptions, PipelineOutcome};
use crate::finalizers::run::{
    build_dry_run_result, finalize_pipeline_execution, snapshot_for_run, ExecutionDiagnostics,
    ReadWriteTotals, StateOutcome, StreamAggregation, TimingMaxima,
};
use crate::pipeline::executor::{execute_single_stream, DestinationMode};
use crate::pipeline::planner::{
    build_stream_contexts, destination_preflight_streams, execution_parallelism, ExecutionPlan,
    PipelineIdentity, PluginSpec, StreamExecutionParams,
};
use crate::pipeline::preflight::run_destination_preflight;
use crate::pipeline::scheduler::{
    acquire_permit_cancellable, collect_stream_task_results, ensure_not_cancelled,
    StreamShardOutcome,
};
use crate::plugin::loader::{load_all_modules, PluginModules};
use crate::plugin::resolver::{
    load_and_validate_manifest, resolve_plugins, validate_config_against_schema, ResolvedPlugins,
};
use crate::plugin::sandbox::{build_sandbox_overrides, check_state_backend, create_state_backend};
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use crate::result::{CheckResult, CheckStatus, StreamShardMetric};
use crate::runner::{run_discover, validate_plugin};

static NEXT_METRIC_RUN_LABEL: AtomicU64 = AtomicU64::new(1);

/// Holds shared references across retry attempts for a single pipeline run.
struct PipelineAttempt<'a> {
    config: &'a PipelineConfig,
    options: &'a ExecutionOptions,
    cancel_token: &'a CancellationToken,
    snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
    registry_config: &'a rapidbyte_registry::RegistryConfig,
}

impl<'a> PipelineAttempt<'a> {
    async fn execute(
        &self,
        attempt: u32,
        progress_tx: ProgressSender,
    ) -> Result<PipelineOutcome, PipelineError> {
        let config = self.config;
        let options = self.options;
        let cancel_token = self.cancel_token;
        let snapshot_reader = self.snapshot_reader;
        let meter_provider = self.meter_provider;
        let registry_config = self.registry_config;
        let start = Instant::now();
        let pipeline_id = PipelineId::new(config.pipeline.clone());
        tracing::info!(
            pipeline = config.pipeline,
            dry_run = options.dry_run,
            "Starting pipeline run"
        );

        progress_tx.emit(ProgressEvent::PhaseChange {
            phase: Phase::Resolving,
        });
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
        let state = tokio::task::spawn_blocking(move || create_state_backend(&config_for_state))
            .await
            .map_err(|e| PipelineError::task_panicked("create_state_backend", e))?
            .map_err(PipelineError::Infrastructure)?;

        let state_for_execution = state.clone();
        let execution_result = async move {
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
                .map_err(|e| PipelineError::task_panicked("start_run", e))?
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

            progress_tx.emit(ProgressEvent::PhaseChange {
                phase: Phase::Loading,
            });
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
            .map_err(|e| PipelineError::task_panicked("build_stream_contexts", e))??;
            progress_tx.emit(ProgressEvent::PhaseChange {
                phase: Phase::Running,
            });
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
                    let _ = snapshot_for_run(
                        snapshot_reader,
                        meter_provider,
                        &config.pipeline,
                        Some(&metric_run_label),
                    );
                    return Err(err);
                }
            };

            progress_tx.emit(ProgressEvent::PhaseChange {
                phase: Phase::Finished,
            });

            if options.dry_run {
                let duration_secs = start.elapsed().as_secs_f64();

                let snap = snapshot_for_run(
                    snapshot_reader,
                    meter_provider,
                    &config.pipeline,
                    Some(&metric_run_label),
                );

                return Ok(PipelineOutcome::DryRun(build_dry_run_result(
                    &snap,
                    aggregated,
                    modules.source_module_load_ms,
                    duration_secs,
                )));
            }

            let result = finalize_pipeline_execution(
                config,
                &pipeline_id,
                state_for_execution.clone(),
                run_id,
                attempt,
                start,
                &metric_run_label,
                &modules,
                aggregated,
                snapshot_reader,
                meter_provider,
            )
            .await?;
            Ok(PipelineOutcome::Run(result))
        }
        .await;

        let state_for_drop = state;
        tokio::task::spawn_blocking(move || drop(state_for_drop))
            .await
            .map_err(|e| PipelineError::task_panicked("drop_state_backend", e))?;

        execution_result
    }
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
    let progress_tx = ProgressSender::new(progress_tx);
    let max_retries = config.resources.max_retries;
    let mut attempt_num = 0u32;
    let attempt = PipelineAttempt {
        config,
        options,
        cancel_token: &cancel_token,
        snapshot_reader,
        meter_provider,
        registry_config,
    };

    loop {
        ensure_not_cancelled(&cancel_token, "Pipeline cancelled before execution")?;
        attempt_num += 1;
        let result = attempt.execute(attempt_num, progress_tx.clone()).await;

        match result {
            Ok(outcome) => return Ok(outcome),
            Err(ref err) if err.is_retryable() && attempt_num <= max_retries => {
                if let Some(plugin_err) = err.as_plugin_error() {
                    let delay = compute_backoff(plugin_err, attempt_num);
                    let commit_state_str =
                        plugin_err.commit_state.as_ref().map(|cs| format!("{cs:?}"));
                    #[allow(clippy::cast_possible_truncation)]
                    // Safety: delay.as_millis() is always well under u64::MAX
                    let delay_ms = delay.as_millis() as u64;
                    tracing::warn!(
                        attempt = attempt_num,
                        max_retries,
                        delay_ms,
                        category = %plugin_err.category,
                        code = %plugin_err.code,
                        commit_state = commit_state_str.as_deref(),
                        safe_to_retry = plugin_err.safe_to_retry,
                        "Retryable error, will retry"
                    );
                    progress_tx.emit(ProgressEvent::Retry {
                        attempt: attempt_num,
                        max_retries,
                        message: format!(
                            "[{}] {}: {}",
                            plugin_err.category, plugin_err.code, plugin_err.message
                        ),
                        delay_secs: delay.as_secs_f64(),
                    });
                    tokio::select! {
                        () = cancel_token.cancelled() => {
                            return Err(PipelineError::cancelled("Pipeline cancelled during retry backoff"));
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
                            attempt = attempt_num,
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
    progress_tx: &ProgressSender,
    cancel_token: &CancellationToken,
) -> Result<StreamAggregation, PipelineError> {
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

    let params = Arc::new(StreamExecutionParams {
        pipeline: PipelineIdentity {
            name: config.pipeline.clone(),
            metric_run_label: metric_run_label.to_owned(),
        },
        source: PluginSpec {
            id: source_plugin_id,
            version: source_plugin_version,
            config: config.source.config.clone(),
            permissions: plugins.source_permissions.clone(),
            overrides: build_sandbox_overrides(
                config.source.permissions.as_ref(),
                config.source.limits.as_ref(),
                &source_manifest_limits,
            ),
        },
        destination: PluginSpec {
            id: dest_plugin_id,
            version: dest_plugin_version,
            config: config.destination.config.clone(),
            permissions: plugins.dest_permissions.clone(),
            overrides: build_sandbox_overrides(
                config.destination.permissions.as_ref(),
                config.destination.limits.as_ref(),
                &dest_manifest_limits,
            ),
        },
        transform_overrides,
        compression: stream_build.compression,
        channel_capacity: (stream_build.limits.max_inflight_batches as usize).max(1),
    });

    let mut stream_join_set: JoinSet<Result<StreamShardOutcome, PipelineError>> = JoinSet::new();
    let run_dlq_records: Arc<Mutex<Vec<DlqRecord>>> = Arc::new(Mutex::new(Vec::new()));

    // --- Destination DDL preflight (skipped in dry-run mode) ---
    if !options.dry_run {
        ensure_not_cancelled(
            cancel_token,
            "Pipeline cancelled before destination preflight",
        )?;
        let preflight_streams = destination_preflight_streams(&stream_build.stream_ctxs);
        run_destination_preflight(
            preflight_streams,
            &modules.dest_module,
            state.clone(),
            &params,
            parallelism,
            cancel_token,
        )
        .await?;
    }

    // --- Per-stream execution ---
    let mode = if options.dry_run {
        DestinationMode::DryRun {
            limit: options.limit,
        }
    } else {
        DestinationMode::Normal
    };

    for stream_ctx in &stream_build.stream_ctxs {
        ensure_not_cancelled(cancel_token, "Pipeline cancelled before stream execution")?;
        let permit = acquire_permit_cancellable(
            &semaphore,
            cancel_token,
            "Pipeline cancelled before stream execution",
        )
        .await?;

        let stream_ctx = stream_ctx.clone();
        let params = params.clone();
        let source_module = modules.source_module.clone();
        let dest_module = modules.dest_module.clone();
        let transforms = modules.transform_modules.clone();
        let state = state.clone();
        let stats = stats.clone();
        let run_dlq_records = run_dlq_records.clone();
        let progress_tx_for_stream = progress_tx.clone();
        let stream_mode = match &mode {
            DestinationMode::Normal => DestinationMode::Normal,
            DestinationMode::DryRun { limit } => DestinationMode::DryRun { limit: *limit },
        };

        stream_join_set.spawn(async move {
            let _permit = permit;
            execute_single_stream(
                stream_ctx,
                params,
                source_module,
                dest_module,
                transforms,
                state,
                stats,
                run_dlq_records,
                stream_mode,
                progress_tx_for_stream,
            )
            .await
        });
    }

    // --- Aggregate results ---
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
        max_source_duration = max_source_duration.max(sr.source_duration_secs);
        max_dest_duration = max_dest_duration.max(sr.dest_duration_secs);
        max_vm_setup_secs = max_vm_setup_secs.max(sr.wasm_instantiation_secs);
        max_recv_secs = max_recv_secs.max(sr.frame_receive_secs);

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
            source_duration_secs: sr.source_duration_secs,
            dest_duration_secs: sr.dest_duration_secs,
            dest_wasm_instantiation_secs: sr.wasm_instantiation_secs,
            dest_frame_receive_secs: sr.frame_receive_secs,
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

    let dlq_records = PipelineError::lock_or_infra(&run_dlq_records, "DLQ collection")?
        .drain(..)
        .collect();

    let final_stats = PipelineError::lock_or_infra(&stats, "run stats")?.clone();

    Ok(StreamAggregation {
        totals: ReadWriteTotals {
            total_read_summary,
            total_write_summary,
        },
        timing: TimingMaxima {
            max_source_duration,
            max_dest_duration,
            max_wasm_instantiation_secs: max_vm_setup_secs,
            max_frame_receive_secs: max_recv_secs,
            transform_durations,
        },
        diagnostics: ExecutionDiagnostics {
            execution_parallelism: u32::try_from(parallelism).unwrap_or(u32::MAX),
            stream_metrics,
            dry_run_streams,
        },
        state: StateOutcome {
            source_checkpoints,
            dest_checkpoints,
            dlq_records,
            final_stats,
            first_error,
        },
    })
}

/// Check a pipeline: validate configuration and connectivity without running.
///
/// # Errors
///
/// Returns an error if plugin resolution, module loading, or validation fails.
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
    let source_manifest = plugins.source_manifest.as_ref().map(|_| CheckStatus {
        ok: true,
        message: String::new(),
    });
    let destination_manifest = plugins.dest_manifest.as_ref().map(|_| CheckStatus {
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
            transform_configs.push(CheckStatus {
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
) -> CheckStatus {
    match validate_config_against_schema(plugin_ref, config, manifest) {
        Ok(()) => CheckStatus {
            ok: true,
            message: String::new(),
        },
        Err(error) => CheckStatus {
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
mod orchestrator_helper_tests {
    use super::*;
    use crate::finalizers::run::persist_run_state;
    use rapidbyte_state::error::Result as StateResult;
    use rapidbyte_types::checkpoint::{Checkpoint, CheckpointKind};
    use rapidbyte_types::cursor::CursorValue;
    use rapidbyte_types::state::{CursorState, RunStatus};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestStateBackend {
        complete_statuses: Mutex<Vec<(RunStatus, Option<String>)>>,
        cursor_written: AtomicBool,
    }

    impl TestStateBackend {
        fn new() -> Self {
            Self {
                complete_statuses: Mutex::new(Vec::new()),
                cursor_written: AtomicBool::new(false),
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
            Ok(0)
        }
    }

    fn make_aggregated_results() -> StreamAggregation {
        StreamAggregation {
            totals: ReadWriteTotals {
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
            },
            timing: TimingMaxima {
                max_source_duration: 0.0,
                max_dest_duration: 0.0,
                max_wasm_instantiation_secs: 0.0,
                max_frame_receive_secs: 0.0,
                transform_durations: Vec::new(),
            },
            diagnostics: ExecutionDiagnostics {
                execution_parallelism: 1,
                stream_metrics: Vec::new(),
                dry_run_streams: Vec::new(),
            },
            state: StateOutcome {
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
                dlq_records: Vec::new(),
                final_stats: RunStats::default(),
                first_error: None,
            },
        }
    }

    #[tokio::test]
    async fn cancellation_after_stream_execution_preserves_real_finalization_outcome() {
        let backend = Arc::new(TestStateBackend::new());
        let mut aggregated = make_aggregated_results();
        // Even when the token is already cancelled, finalization still runs directly
        // because preserve_real_outcome_after_stream_execution was a no-op passthrough.
        let advanced =
            persist_run_state(backend.clone(), &PipelineId::new("p"), 1, &mut aggregated)
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
    async fn spawn_blocking_allows_runtime_sensitive_init() {
        let value = tokio::task::spawn_blocking(|| {
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async { Ok::<_, anyhow::Error>(7) })
        })
        .await
        .map_err(|e| PipelineError::task_panicked("test_blocking_init", e))
        .expect("spawn_blocking should succeed")
        .expect("inner result should succeed");

        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn spawn_blocking_allows_runtime_sensitive_drop() {
        struct RuntimeOnDrop;

        impl Drop for RuntimeOnDrop {
            fn drop(&mut self) {
                let runtime = tokio::runtime::Runtime::new().expect("runtime in drop");
                runtime.block_on(async {});
            }
        }

        tokio::task::spawn_blocking(|| {
            let value = RuntimeOnDrop;
            drop(value);
        })
        .await
        .map_err(|e| PipelineError::task_panicked("test_blocking_drop", e))
        .expect("spawn_blocking should allow runtime-sensitive drop");
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
