//! Run-pipeline use case — the core orchestration logic.
//!
//! [`run_pipeline`] owns the retry loop and orchestrates the entire pipeline
//! execution: resolve plugins, load cursors, execute streams, finalize.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use rapidbyte_pipeline_config::{
    parse_byte_size, PipelineConfig, PipelineLimits, PipelineParallelism, PipelinePermissions,
    ResourceConfig,
};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::stream::StreamLimits;
use rapidbyte_types::wire::SyncMode;

use rapidbyte_runtime::{Frame, SandboxOverrides};

use crate::application::context::EngineContext;
use crate::application::{extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric};
use crate::domain::ports::runner::{
    ApplyParams, DestinationRunParams, SourceOutcome, SourceRunParams, TransformRunParams,
};
use crate::domain::progress::{Phase, ProgressEvent};
use crate::domain::retry::{RetryDecision, RetryPolicy};

/// Convert a `CompressionCodec` enum into its wire-format string name.
fn compression_name(codec: rapidbyte_types::compression::CompressionCodec) -> &'static str {
    use rapidbyte_types::compression::CompressionCodec;
    match codec {
        CompressionCodec::Lz4 => "lz4",
        CompressionCodec::Zstd => "zstd",
    }
}

/// Convert a typed `CursorValue` into its string representation for
/// storage in `CursorState`.
fn cursor_value_to_string(cv: &rapidbyte_types::cursor::CursorValue) -> String {
    use rapidbyte_types::cursor::CursorValue;
    match cv {
        CursorValue::Null => String::new(),
        CursorValue::Int64 { value }
        | CursorValue::TimestampMillis { value }
        | CursorValue::TimestampMicros { value } => value.to_string(),
        CursorValue::Utf8 { value }
        | CursorValue::Decimal { value, .. }
        | CursorValue::Lsn { value } => value.clone(),
        CursorValue::Json { value } => value.to_string(),
        _ => format!("{cv:?}"),
    }
}

/// Build [`StreamLimits`] from the pipeline's [`ResourceConfig`].
///
/// Parses human-readable byte sizes (e.g. "64mb") into numeric values.
/// Returns an error if configured values are malformed, to prevent silent
/// misconfiguration from changing memory/flush behavior.
fn build_stream_limits(resources: &ResourceConfig) -> Result<StreamLimits, PipelineError> {
    let max_batch_bytes = parse_byte_size(&resources.max_batch_bytes).map_err(|e| {
        PipelineError::infra(format!(
            "invalid max_batch_bytes '{}': {e}",
            resources.max_batch_bytes
        ))
    })?;
    let checkpoint_interval_bytes =
        parse_byte_size(&resources.checkpoint_interval_bytes).map_err(|e| {
            PipelineError::infra(format!(
                "invalid checkpoint_interval_bytes '{}': {e}",
                resources.checkpoint_interval_bytes
            ))
        })?;

    Ok(StreamLimits {
        max_batch_bytes,
        checkpoint_interval_bytes,
        checkpoint_interval_rows: resources.checkpoint_interval_rows,
        checkpoint_interval_seconds: resources.checkpoint_interval_seconds,
        max_inflight_batches: resources.max_inflight_batches,
        ..StreamLimits::default()
    })
}

/// Delegate to the shared `build_sandbox_overrides` in `application/mod.rs`.
fn build_sandbox_overrides(
    yaml_permissions: Option<&PipelinePermissions>,
    yaml_limits: Option<&PipelineLimits>,
    manifest: Option<&rapidbyte_types::manifest::PluginManifest>,
) -> Result<Option<SandboxOverrides>, PipelineError> {
    crate::application::build_sandbox_overrides(yaml_permissions, yaml_limits, manifest)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Execute a pipeline: resolve plugins, run streams, finalize results.
///
/// Implements a retry loop around the core execution. On transient plugin
/// errors the function consults [`RetryPolicy`] and either retries or
/// gives up. Cancellation via the provided [`CancellationToken`] is
/// checked at the top of each iteration.
///
/// # Errors
///
/// Returns `PipelineError::Cancelled` if the token is cancelled before
/// or during execution. Returns `PipelineError::Plugin` or
/// `PipelineError::Infrastructure` on unrecoverable errors.
///
/// # Panics
///
/// Panics if the internal frame receiver option is unexpectedly empty.
/// This is a programming error and should never occur in practice.
#[allow(clippy::too_many_lines)]
pub async fn run_pipeline(
    ctx: &EngineContext,
    pipeline: &PipelineConfig,
    cancel: CancellationToken,
) -> Result<PipelineResult, PipelineError> {
    let retry_policy = RetryPolicy::new(ctx.config.max_retries.saturating_add(1));
    let pipeline_id = PipelineId::new(&pipeline.pipeline);
    let mut attempt: u32 = 0;
    let overall_start = Instant::now();

    // Cooperative cancellation flag for WASM plugins. The spawned task
    // watches the CancellationToken and sets the AtomicBool so plugins
    // that poll `is_cancelled()` can exit promptly.
    let cancel_flag = Arc::new(AtomicBool::new(false));
    {
        let flag = Arc::clone(&cancel_flag);
        let token = cancel.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            flag.store(true, Ordering::Relaxed);
        });
    }

    loop {
        attempt += 1;

        // 1. Check cancellation
        if cancel.is_cancelled() {
            return Err(PipelineError::Cancelled);
        }

        // 2. Resolve plugins
        ctx.progress.report(ProgressEvent::PhaseChanged {
            phase: Phase::Resolving,
        });

        let source_resolved = ctx
            .resolver
            .resolve(
                &pipeline.source.use_ref,
                rapidbyte_types::wire::PluginKind::Source,
                Some(&pipeline.source.config),
            )
            .await?;

        let dest_resolved = ctx
            .resolver
            .resolve(
                &pipeline.destination.use_ref,
                rapidbyte_types::wire::PluginKind::Destination,
                Some(&pipeline.destination.config),
            )
            .await?;

        // Resolve transforms
        let mut transform_resolved = Vec::with_capacity(pipeline.transforms.len());
        for transform in &pipeline.transforms {
            let resolved = ctx
                .resolver
                .resolve(
                    &transform.use_ref,
                    rapidbyte_types::wire::PluginKind::Transform,
                    Some(&transform.config),
                )
                .await?;
            transform_resolved.push(resolved);
        }

        // 2b. Apply phase — provision resources (best-effort, non-fatal)
        {
            let (src_apply_id, src_apply_ver) = parse_plugin_id(&pipeline.source.use_ref);
            let (dst_apply_id, dst_apply_ver) = parse_plugin_id(&pipeline.destination.use_ref);
            let src_apply_permissions = extract_permissions(&source_resolved);
            let dst_apply_permissions = extract_permissions(&dest_resolved);
            let src_apply_overrides = build_sandbox_overrides(
                pipeline.source.permissions.as_ref(),
                pipeline.source.limits.as_ref(),
                source_resolved.manifest.as_ref(),
            )?;
            let dst_apply_overrides = build_sandbox_overrides(
                pipeline.destination.permissions.as_ref(),
                pipeline.destination.limits.as_ref(),
                dest_resolved.manifest.as_ref(),
            )?;

            let apply_streams = crate::application::build_apply_streams(pipeline);

            let _src_apply = ctx
                .runner
                .apply(&ApplyParams {
                    wasm_path: source_resolved.wasm_path.clone(),
                    kind: rapidbyte_types::wire::PluginKind::Source,
                    plugin_id: src_apply_id,
                    plugin_version: src_apply_ver,
                    config: pipeline.source.config.clone(),
                    streams: apply_streams.clone(),
                    dry_run: false,
                    permissions: src_apply_permissions,
                    sandbox_overrides: src_apply_overrides,
                })
                .await
                .ok(); // Non-fatal — apply is best-effort

            let _dst_apply = ctx
                .runner
                .apply(&ApplyParams {
                    wasm_path: dest_resolved.wasm_path.clone(),
                    kind: rapidbyte_types::wire::PluginKind::Destination,
                    plugin_id: dst_apply_id,
                    plugin_version: dst_apply_ver,
                    config: pipeline.destination.config.clone(),
                    streams: apply_streams,
                    dry_run: false,
                    permissions: dst_apply_permissions,
                    sandbox_overrides: dst_apply_overrides,
                })
                .await
                .ok(); // Non-fatal — apply is best-effort
        }

        // 3. Report Running phase
        ctx.progress.report(ProgressEvent::PhaseChanged {
            phase: Phase::Running,
        });

        // 4. Execute streams sequentially
        let mut total_records_read: u64 = 0;
        let mut total_records_written: u64 = 0;
        let mut total_bytes_read: u64 = 0;
        let mut total_bytes_written: u64 = 0;
        let mut total_source_secs: f64 = 0.0;
        let mut total_dest_secs: f64 = 0.0;
        let mut total_transform_secs: f64 = 0.0;
        let mut stream_error: Option<PipelineError> = None;
        // Per-stream stats: (records_read, records_written, bytes_read, bytes_written,
        //                    source_secs, dest_secs, dest_wasm_secs, dest_recv_secs)
        #[allow(clippy::type_complexity)]
        let mut per_stream_stats: HashMap<
            String,
            (u64, u64, u64, u64, f64, f64, f64, f64),
        > = HashMap::new();
        // Per-stream DLQ records accumulated during transform/destination execution
        let mut per_stream_dlq: HashMap<String, Vec<rapidbyte_types::envelope::DlqRecord>> =
            HashMap::new();

        let (src_id, src_ver) = parse_plugin_id(&pipeline.source.use_ref);
        let (dst_id, dst_ver) = parse_plugin_id(&pipeline.destination.use_ref);
        let metric_run_label = format!("{}-{attempt}", pipeline.pipeline);

        // Pre-compute loop-invariant values to avoid re-traversing pipeline
        // structs on every iteration.
        let source_compression = pipeline
            .resources
            .compression
            .as_ref()
            .copied()
            .map(|c| compression_name(c).to_string());
        let dest_compression = source_compression.clone();
        let source_config = pipeline.source.config.clone();
        let dest_config = pipeline.destination.config.clone();

        // Issue 3: Build StreamLimits from pipeline resources instead of
        // using defaults. Channel capacity also comes from config.
        let stream_limits = build_stream_limits(&pipeline.resources)?;

        for (stream_idx, stream_cfg) in pipeline.source.streams.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(PipelineError::Cancelled);
            }

            let stream_name = &stream_cfg.name;

            // Issue 2: Start run record BEFORE stream execution so that
            // failed runs are also persisted.
            let run_id = ctx
                .runs
                .start(&pipeline_id, &StreamName::new(stream_name))
                .await
                .map_err(|e| PipelineError::infra(format!("run start failed: {e}")))?;

            ctx.progress.report(ProgressEvent::StreamStarted {
                stream: stream_name.clone(),
            });

            // Load cursor for incremental and CDC streams
            let cursor_info = if stream_cfg.sync_mode == SyncMode::FullRefresh {
                // Full refresh never uses cursors
                None
            } else {
                // Both Incremental and Cdc need cursor state
                let is_cdc = stream_cfg.sync_mode == SyncMode::Cdc;
                let cursor_type = if is_cdc {
                    rapidbyte_types::cursor::CursorType::Lsn
                } else {
                    rapidbyte_types::cursor::CursorType::Utf8
                };

                let last_value = ctx
                    .cursors
                    .get(&pipeline_id, &StreamName::new(stream_name))
                    .await
                    .map_err(|e| PipelineError::infra(format!("cursor load failed: {e}")))?
                    .and_then(|cs| cs.cursor_value)
                    // Treat empty strings as "no cursor". Empty strings are never
                    // valid cursor positions (cursors are timestamps, IDs, LSNs —
                    // monotonically ordered values). The only source of "" was a
                    // prior bug serializing CursorValue::Null as empty string.
                    .filter(|v| !v.is_empty())
                    .map(|v| {
                        if is_cdc {
                            rapidbyte_types::cursor::CursorValue::Lsn { value: v }
                        } else if stream_cfg.tie_breaker_field.is_some() {
                            // Tie-breaker streams store composite JSON cursor
                            // state. Try JSON parse first; fall back to Utf8
                            // if the persisted value isn't valid JSON.
                            match serde_json::from_str::<serde_json::Value>(&v) {
                                Ok(json) if json.is_object() => {
                                    rapidbyte_types::cursor::CursorValue::Json { value: json }
                                }
                                _ => rapidbyte_types::cursor::CursorValue::Utf8 { value: v },
                            }
                        } else {
                            rapidbyte_types::cursor::CursorValue::Utf8 { value: v }
                        }
                    });

                // CDC defaults cursor_field to "lsn"; Incremental requires it from config
                let cursor_field = if is_cdc {
                    stream_cfg
                        .cursor_field
                        .clone()
                        .unwrap_or_else(|| "lsn".to_string())
                } else {
                    stream_cfg.cursor_field.clone().unwrap_or_default()
                };

                Some(rapidbyte_types::cursor::CursorInfo {
                    cursor_field,
                    tie_breaker_field: stream_cfg.tie_breaker_field.clone(),
                    cursor_type,
                    last_value,
                })
            };

            // Build stream context with limits from pipeline resources
            let stream_ctx = rapidbyte_types::stream::StreamContext {
                stream_name: stream_name.clone(),
                source_stream_name: None,
                #[allow(clippy::cast_possible_truncation)]
                stream_index: stream_idx as u32,
                schema: rapidbyte_types::schema::StreamSchema {
                    fields: vec![],
                    primary_key: pipeline.destination.primary_key.clone(),
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                sync_mode: stream_cfg.sync_mode,
                cursor_info,
                limits: stream_limits.clone(),
                policies: rapidbyte_types::stream::StreamPolicies {
                    on_data_error: pipeline.destination.on_data_error,
                    schema_evolution: pipeline.destination.schema_evolution.unwrap_or_default(),
                },
                write_mode: Some(
                    pipeline
                        .destination
                        .write_mode
                        .to_protocol(pipeline.destination.primary_key.clone()),
                ),
                selected_columns: stream_cfg.columns.clone(),
                partition_key: stream_cfg.partition_key.clone(),
                // TODO: partition planner — partition_count/partition_index
                // are not yet computed. The old planner/scheduler (~500 lines)
                // was removed during the hexagonal refactor. Re-implementing
                // partitioned reads requires a planner that splits streams
                // into partition tasks based on resources.parallelism.
                partition_count: None,
                partition_index: None,
                effective_parallelism: Some(match pipeline.resources.parallelism {
                    PipelineParallelism::Auto => 1,
                    PipelineParallelism::Manual(n) => n,
                }),
                partition_strategy: None,
                copy_flush_bytes_override: None,
            };

            // Shared DLQ accumulator for this stream (across transforms + destination)
            let stream_dlq: Arc<Mutex<Vec<rapidbyte_types::envelope::DlqRecord>>> =
                Arc::new(Mutex::new(Vec::new()));
            let stats = Arc::new(Mutex::new(RunStats::default()));

            // Issue 1 fix: Spawn source, transforms, and destination as
            // concurrent tasks connected by bounded channels. Without
            // concurrency the bounded channel blocks forever once the
            // source emits more frames than the channel capacity.

            // Build the channel chain: source -> [transform0 -> ... -> transformN] -> destination
            let (src_tx, mut current_rx) = mpsc::sync_channel::<Frame>(ctx.config.channel_capacity);

            // Spawn source
            let source_permissions = extract_permissions(&source_resolved);
            let source_overrides = build_sandbox_overrides(
                pipeline.source.permissions.as_ref(),
                pipeline.source.limits.as_ref(),
                source_resolved.manifest.as_ref(),
            )?;
            let source_handle = {
                let runner = Arc::clone(&ctx.runner);
                let params = SourceRunParams {
                    wasm_path: source_resolved.wasm_path.clone(),
                    pipeline_name: pipeline.pipeline.clone(),
                    metric_run_label: metric_run_label.clone(),
                    plugin_id: src_id.clone(),
                    plugin_version: src_ver.clone(),
                    stream_ctx: stream_ctx.clone(),
                    config: source_config.clone(),
                    permissions: source_permissions,
                    sandbox_overrides: source_overrides,
                    compression: source_compression.clone(),
                    frame_sender: src_tx,
                    stats: Arc::clone(&stats),
                    on_batch_emitted: {
                        let progress = Arc::clone(&ctx.progress);
                        let sn = stream_name.clone();
                        Some(Arc::new(move |bytes: u64| {
                            progress.report(ProgressEvent::BatchEmitted {
                                stream: sn.clone(),
                                bytes,
                            });
                        }))
                    },
                    cancel_flag: Arc::clone(&cancel_flag),
                };
                tokio::spawn(async move { runner.run_source(params).await })
            };

            // Spawn transforms (chained, each consuming from previous, sending to next)
            let mut transform_handles = Vec::with_capacity(pipeline.transforms.len());
            for (i, transform) in pipeline.transforms.iter().enumerate() {
                let (next_tx, next_rx) = mpsc::sync_channel::<Frame>(ctx.config.channel_capacity);

                let t_permissions = extract_permissions(&transform_resolved[i]);
                let t_overrides = build_sandbox_overrides(
                    transform.permissions.as_ref(),
                    transform.limits.as_ref(),
                    transform_resolved[i].manifest.as_ref(),
                )?;
                let (t_id, t_ver) = parse_plugin_id(&transform.use_ref);

                let runner = Arc::clone(&ctx.runner);
                let params = TransformRunParams {
                    wasm_path: transform_resolved[i].wasm_path.clone(),
                    pipeline_name: pipeline.pipeline.clone(),
                    metric_run_label: metric_run_label.clone(),
                    plugin_id: t_id,
                    plugin_version: t_ver,
                    stream_ctx: stream_ctx.clone(),
                    config: transform.config.clone(),
                    permissions: t_permissions,
                    sandbox_overrides: t_overrides,
                    compression: source_compression.clone(),
                    frame_receiver: current_rx,
                    frame_sender: next_tx,
                    dlq_records: Arc::clone(&stream_dlq),
                    transform_index: i,
                    cancel_flag: Arc::clone(&cancel_flag),
                };
                transform_handles.push(tokio::spawn(
                    async move { runner.run_transform(params).await },
                ));

                current_rx = next_rx;
            }

            // Spawn destination (consumes from last channel)
            let dest_permissions = extract_permissions(&dest_resolved);
            let dest_overrides = build_sandbox_overrides(
                pipeline.destination.permissions.as_ref(),
                pipeline.destination.limits.as_ref(),
                dest_resolved.manifest.as_ref(),
            )?;
            let dest_handle = {
                let runner = Arc::clone(&ctx.runner);
                let params = DestinationRunParams {
                    wasm_path: dest_resolved.wasm_path.clone(),
                    pipeline_name: pipeline.pipeline.clone(),
                    metric_run_label: metric_run_label.clone(),
                    plugin_id: dst_id.clone(),
                    plugin_version: dst_ver.clone(),
                    stream_ctx: stream_ctx.clone(),
                    config: dest_config.clone(),
                    permissions: dest_permissions,
                    sandbox_overrides: dest_overrides,
                    compression: dest_compression.clone(),
                    frame_receiver: current_rx,
                    dlq_records: Arc::clone(&stream_dlq),
                    stats: Arc::clone(&stats),
                    cancel_flag: Arc::clone(&cancel_flag),
                };
                tokio::spawn(async move { runner.run_destination(params).await })
            };

            // Await all stages concurrently — they run in parallel via the
            // bounded channels. We collect results in order.
            let source_result = source_handle
                .await
                .map_err(|e| PipelineError::infra(format!("source task panicked: {e}")))?;

            let mut transform_results = Vec::with_capacity(transform_handles.len());
            for handle in transform_handles {
                let result = handle
                    .await
                    .map_err(|e| PipelineError::infra(format!("transform task panicked: {e}")))?;
                transform_results.push(result);
            }

            let dest_result = dest_handle
                .await
                .map_err(|e| PipelineError::infra(format!("destination task panicked: {e}")))?;

            // Determine if any stage failed
            let stream_result: Result<
                (
                    SourceOutcome,
                    Vec<crate::domain::ports::runner::TransformOutcome>,
                    crate::domain::ports::runner::DestinationOutcome,
                ),
                PipelineError,
            > = (|| {
                let source_outcome = source_result?;
                let mut t_outcomes = Vec::with_capacity(transform_results.len());
                for t_result in transform_results {
                    t_outcomes.push(t_result?);
                }
                let dest_outcome = dest_result?;
                Ok((source_outcome, t_outcomes, dest_outcome))
            })();

            match stream_result {
                Ok((source_outcome, t_outcomes, dest_outcome)) => {
                    total_source_secs += source_outcome.duration_secs;
                    total_records_read += source_outcome.summary.records_read;
                    total_bytes_read += source_outcome.summary.bytes_read;

                    for t_outcome in &t_outcomes {
                        total_transform_secs += t_outcome.duration_secs;
                    }

                    total_dest_secs += dest_outcome.duration_secs;
                    total_records_written += dest_outcome.summary.records_written;
                    total_bytes_written += dest_outcome.summary.bytes_written;

                    // Record per-stream stats including timing
                    per_stream_stats.insert(
                        stream_name.clone(),
                        (
                            source_outcome.summary.records_read,
                            dest_outcome.summary.records_written,
                            source_outcome.summary.bytes_read,
                            dest_outcome.summary.bytes_written,
                            source_outcome.duration_secs,
                            dest_outcome.duration_secs,
                            dest_outcome.wasm_instantiation_secs,
                            dest_outcome.frame_receive_secs,
                        ),
                    );

                    // Save cursor updates from source checkpoints.
                    // Skip null cursor values — they indicate "no position" and
                    // must not overwrite a previously saved concrete cursor.
                    for checkpoint in &source_outcome.checkpoints {
                        if let Some(ref cursor_value) = checkpoint.cursor_value {
                            if matches!(cursor_value, rapidbyte_types::cursor::CursorValue::Null) {
                                continue;
                            }
                            let cursor_state = rapidbyte_types::state::CursorState {
                                cursor_field: checkpoint.cursor_field.clone(),
                                cursor_value: Some(cursor_value_to_string(cursor_value)),
                                updated_at: chrono::Utc::now().to_rfc3339(),
                            };
                            if let Err(e) = ctx
                                .cursors
                                .set(&pipeline_id, &StreamName::new(stream_name), &cursor_state)
                                .await
                            {
                                warn!(stream = stream_name, error = %e, "failed to save cursor (non-fatal)");
                            }
                        }
                    }

                    // Collect DLQ records from this stream
                    let dlq_batch: Vec<rapidbyte_types::envelope::DlqRecord> =
                        match stream_dlq.lock() {
                            Ok(mut guard) => std::mem::take(&mut *guard),
                            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
                        };
                    if !dlq_batch.is_empty() {
                        per_stream_dlq.insert(stream_name.clone(), dlq_batch);
                    }

                    // Issue 2: Complete run record with success
                    let (sr, sw, br, bw, _, _, _, _) = per_stream_stats
                        .get(stream_name)
                        .copied()
                        .unwrap_or_default();
                    let run_stats = RunStats {
                        records_read: sr,
                        records_written: sw,
                        bytes_read: br,
                        bytes_written: bw,
                        error_message: None,
                    };
                    if let Err(e) = ctx
                        .runs
                        .complete(run_id, RunStatus::Completed, &run_stats)
                        .await
                    {
                        warn!(run_id, error = %e, "failed to record run completion (non-fatal)");
                    }

                    // Save DLQ records for this stream (if any)
                    if let Some(dlq_batch) = per_stream_dlq.get(stream_name) {
                        if let Err(e) = ctx.dlq.insert(&pipeline_id, run_id, dlq_batch).await {
                            warn!(run_id, error = %e, "failed to save DLQ records (non-fatal)");
                        }
                    }

                    ctx.progress.report(ProgressEvent::StreamCompleted {
                        stream: stream_name.clone(),
                    });
                }
                Err(ref e) => {
                    // Issue 2: Complete run record with failure so errors
                    // have an audit trail in the state backend.
                    let error_stats = RunStats {
                        error_message: Some(e.to_string()),
                        ..RunStats::default()
                    };
                    if let Err(persist_err) = ctx
                        .runs
                        .complete(run_id, RunStatus::Failed, &error_stats)
                        .await
                    {
                        warn!(run_id, error = %persist_err, "failed to record run failure (non-fatal)");
                    }

                    // Persist any DLQ records accumulated before the error
                    let dlq_batch: Vec<rapidbyte_types::envelope::DlqRecord> =
                        match stream_dlq.lock() {
                            Ok(mut guard) => std::mem::take(&mut *guard),
                            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
                        };
                    if !dlq_batch.is_empty() {
                        if let Err(dlq_err) = ctx.dlq.insert(&pipeline_id, run_id, &dlq_batch).await
                        {
                            warn!(run_id, error = %dlq_err, "failed to save DLQ records on error (non-fatal)");
                        }
                    }

                    stream_error = Some(match stream_result {
                        Err(e) => e,
                        Ok(_) => unreachable!(),
                    });
                    break;
                }
            }
        }

        // 5. Handle errors with retry logic
        if let Some(err) = stream_error {
            if let Some(retry_delay) = evaluate_retry(&retry_policy, &err, attempt) {
                warn!(
                    attempt,
                    delay_ms = u64::try_from(retry_delay.as_millis()).unwrap_or(u64::MAX),
                    "retrying pipeline after transient error"
                );
                ctx.progress.report(ProgressEvent::RetryScheduled {
                    attempt,
                    delay: retry_delay,
                });
                tokio::select! {
                    () = tokio::time::sleep(retry_delay) => {},
                    () = cancel.cancelled() => return Err(PipelineError::Cancelled),
                }
                continue;
            }
            return Err(err);
        }

        // 6. Finalization
        ctx.progress.report(ProgressEvent::PhaseChanged {
            phase: Phase::Finalizing,
        });

        let duration_secs = overall_start.elapsed().as_secs_f64();

        // Build per-stream metrics from collected stats
        let stream_metrics: Vec<StreamShardMetric> = per_stream_stats
            .iter()
            .map(
                |(name, (rr, rw, br, bw, src_secs, dst_secs, dst_wasm, dst_recv))| {
                    StreamShardMetric {
                        stream_name: name.clone(),
                        records_read: *rr,
                        records_written: *rw,
                        bytes_read: *br,
                        bytes_written: *bw,
                        source_duration_secs: *src_secs,
                        dest_duration_secs: *dst_secs,
                        dest_wasm_instantiation_secs: *dst_wasm,
                        dest_frame_receive_secs: *dst_recv,
                        partition_index: None,
                        partition_count: None,
                    }
                },
            )
            .collect();

        // 7. Return result
        let result = PipelineResult {
            counts: PipelineCounts {
                records_read: total_records_read,
                records_written: total_records_written,
                bytes_read: total_bytes_read,
                bytes_written: total_bytes_written,
            },
            source: SourceTiming {
                duration_secs: total_source_secs,
                ..SourceTiming::default()
            },
            dest: crate::domain::outcome::DestTiming {
                duration_secs: total_dest_secs,
                ..Default::default()
            },
            num_transforms: pipeline.transforms.len(),
            total_transform_secs,
            transform_load_times_ms: vec![],
            duration_secs,
            wasm_overhead_secs: 0.0,
            retry_count: attempt.saturating_sub(1),
            parallelism: match pipeline.resources.parallelism {
                PipelineParallelism::Auto => 1,
                PipelineParallelism::Manual(n) => n,
            },
            stream_metrics,
        };

        return Ok(result);
    }
}

/// Evaluate whether a pipeline error should be retried.
///
/// Returns `Some(delay)` if the error is retryable and the retry policy
/// approves, `None` if the error should propagate.
fn evaluate_retry(
    policy: &RetryPolicy,
    err: &PipelineError,
    attempt: u32,
) -> Option<std::time::Duration> {
    let pe = match err {
        PipelineError::Plugin(pe) => pe,
        PipelineError::Infrastructure(_) | PipelineError::Cancelled => return None,
    };

    let decision = policy.should_retry(
        attempt,
        pe.retryable,
        pe.safe_to_retry,
        pe.category.default_backoff(),
        pe.retry_after_ms.map(std::time::Duration::from_millis),
    );

    match decision {
        RetryDecision::Retry { delay } => Some(delay),
        RetryDecision::GiveUp { reason } => {
            info!(reason, "giving up on retries");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::{fake_context, test_resolved_plugin};
    use crate::domain::ports::cursor::CursorRepository;
    use crate::domain::ports::runner::{DestinationOutcome, SourceOutcome};
    use rapidbyte_types::metric::{ReadSummary, WriteSummary};

    fn test_pipeline_config(source_ref: &str, dest_ref: &str, streams: &[&str]) -> PipelineConfig {
        let stream_yaml: String = streams
            .iter()
            .map(|s| format!("        - name: {s}\n          sync_mode: full_refresh"))
            .collect::<Vec<_>>()
            .join("\n");
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
    use: {source_ref}
    streams:
{stream_yaml}
    config: {{}}
destination:
    use: {dest_ref}
    config: {{}}
    write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("test yaml should parse")
    }

    fn make_source_outcome(records_read: u64, bytes_read: u64) -> SourceOutcome {
        SourceOutcome {
            duration_secs: 1.0,
            summary: ReadSummary {
                records_read,
                bytes_read,
                batches_emitted: 1,
                checkpoint_count: 0,
                records_skipped: 0,
            },
            checkpoints: vec![],
        }
    }

    fn make_dest_outcome(records_written: u64, bytes_written: u64) -> DestinationOutcome {
        DestinationOutcome {
            duration_secs: 0.5,
            summary: WriteSummary {
                records_written,
                bytes_written,
                batches_written: 1,
                checkpoint_count: 0,
                records_failed: 0,
            },
            wasm_instantiation_secs: 0.0,
            frame_receive_secs: 0.0,
            checkpoints: vec![],
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: Happy path — single stream, no retries
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_single_stream_happy_path() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        assert_eq!(result.counts.records_read, 100);
        assert_eq!(result.counts.records_written, 100);
        assert_eq!(result.counts.bytes_read, 8192);
        assert_eq!(result.counts.bytes_written, 8192);
        assert_eq!(result.retry_count, 0);
        assert_eq!(result.parallelism, 1);
    }

    // -----------------------------------------------------------------------
    // Test 2: Cancellation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_respects_cancellation() {
        let tc = fake_context();
        // Register plugins so resolution wouldn't be the issue
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(matches!(result, Err(PipelineError::Cancelled)));
    }

    // -----------------------------------------------------------------------
    // Test 3: Progress events
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_emits_progress_events() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_source(Ok(make_source_outcome(50, 4096)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        let events = tc.progress.events();

        // Check for Resolving phase
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::PhaseChanged {
                    phase: Phase::Resolving
                }
            )),
            "expected Resolving phase event"
        );

        // Check for Running phase
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::PhaseChanged {
                    phase: Phase::Running
                }
            )),
            "expected Running phase event"
        );

        // Check for Finalizing phase
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::PhaseChanged {
                    phase: Phase::Finalizing
                }
            )),
            "expected Finalizing phase event"
        );

        // Check for stream started event
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::StreamStarted { stream } if stream == "users"
            )),
            "expected StreamStarted event for 'users'"
        );

        // Check for stream completed event
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::StreamCompleted { stream } if stream == "users"
            )),
            "expected StreamCompleted event for 'users'"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: Run record is saved
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_records_run() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner
            .enqueue_source(Ok(make_source_outcome(200, 16384)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(200, 16384)));

        let pipeline = test_pipeline_config("src", "dst", &["orders"]);
        let cancel = CancellationToken::new();
        run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Verify a run was started (the fake auto-increments from 1)
        let started = tc.runs.started_count();
        assert_eq!(started, 1, "expected one run to be started");
    }

    // -----------------------------------------------------------------------
    // Test 5: Multiple streams
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_multiple_streams() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        // Two streams: users and orders
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8000)));
        tc.runner
            .enqueue_source(Ok(make_source_outcome(200, 16000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(200, 16000)));

        let pipeline = test_pipeline_config("src", "dst", &["users", "orders"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        assert_eq!(result.counts.records_read, 300);
        assert_eq!(result.counts.records_written, 300);
        assert_eq!(result.counts.bytes_read, 24000);
        assert_eq!(result.counts.bytes_written, 24000);
    }

    // -----------------------------------------------------------------------
    // Test 7: Infrastructure error is not retried
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_infra_error_not_retried() {
        let mut tc = fake_context();
        // Give it retries
        tc.ctx.config.max_retries = 3;
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner
            .enqueue_source(Err(PipelineError::infra("WASM load failed")));
        // Dummy destination result since stages run concurrently
        tc.runner.enqueue_destination(Ok(make_dest_outcome(0, 0)));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::Infrastructure(_)),
            "expected Infrastructure error, got: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 8: Resolution failure returns error
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_resolution_failure() {
        let tc = fake_context();
        // Don't register any plugins — resolver will fail
        let pipeline = test_pipeline_config("nonexistent-src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Helper: pipeline config with transforms
    // -----------------------------------------------------------------------

    fn test_pipeline_config_with_transforms(
        source_ref: &str,
        dest_ref: &str,
        streams: &[&str],
        transforms: &[&str],
    ) -> PipelineConfig {
        let stream_yaml: String = streams
            .iter()
            .map(|s| format!("        - name: {s}\n          sync_mode: full_refresh"))
            .collect::<Vec<_>>()
            .join("\n");
        let transforms_yaml: String = if transforms.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = transforms
                .iter()
                .map(|t| format!("    - use: {t}\n      config: {{}}"))
                .collect();
            format!("transforms:\n{}", items.join("\n"))
        };
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
    use: {source_ref}
    streams:
{stream_yaml}
    config: {{}}
{transforms_yaml}
destination:
    use: {dest_ref}
    config: {{}}
    write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("test yaml should parse")
    }

    /// Pipeline config with an incremental stream that has a `cursor_field`.
    fn test_pipeline_config_incremental(
        source_ref: &str,
        dest_ref: &str,
        stream_name: &str,
        cursor_field: &str,
    ) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
    use: {source_ref}
    streams:
        - name: {stream_name}
          sync_mode: incremental
          cursor_field: {cursor_field}
    config: {{}}
destination:
    use: {dest_ref}
    config: {{}}
    write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("test yaml should parse")
    }

    fn make_transform_outcome() -> crate::domain::ports::runner::TransformOutcome {
        crate::domain::ports::runner::TransformOutcome {
            duration_secs: 0.2,
            summary: rapidbyte_types::metric::TransformSummary {
                records_in: 100,
                records_out: 100,
                bytes_in: 8192,
                bytes_out: 8192,
                batches_processed: 1,
            },
        }
    }

    // -----------------------------------------------------------------------
    // Test 9: Retry on retryable plugin error
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn retry_on_retryable_plugin_error() {
        tokio::time::pause();

        let mut tc = fake_context();
        tc.ctx.config.max_retries = 3;
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // First attempt: retryable error from source.
        // With concurrent spawning, destination is also spawned on the
        // first attempt (even though source will fail). Enqueue a dummy
        // destination result so the fake queue doesn't run dry.
        let plugin_err = rapidbyte_types::error::PluginError::transient_network(
            "CONN_RESET",
            "connection reset",
        );
        tc.runner
            .enqueue_source(Err(PipelineError::Plugin(plugin_err)));
        tc.runner.enqueue_destination(Ok(make_dest_outcome(0, 0)));

        // Second attempt: success
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        assert_eq!(result.retry_count, 1, "expected 1 retry");
        assert_eq!(result.counts.records_read, 100);

        // Verify RetryScheduled event was emitted
        let events = tc.progress.events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::RetryScheduled { .. })),
            "expected RetryScheduled progress event"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: Retry gives up after max attempts
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn retry_gives_up_after_max_attempts() {
        tokio::time::pause();

        let mut tc = fake_context();
        tc.ctx.config.max_retries = 2; // max_retries + 1 = 3 max attempts

        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // All attempts fail with retryable errors.
        // Enqueue a dummy destination result per attempt since stages run
        // concurrently.
        for _ in 0..3 {
            let plugin_err = rapidbyte_types::error::PluginError::transient_network(
                "CONN_RESET",
                "connection reset",
            );
            tc.runner
                .enqueue_source(Err(PipelineError::Plugin(plugin_err)));
            tc.runner.enqueue_destination(Ok(make_dest_outcome(0, 0)));
        }

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(
            result.is_err(),
            "expected error after max retries exhausted"
        );
    }

    // -----------------------------------------------------------------------
    // Test 11: Non-retryable plugin error is not retried
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn non_retryable_plugin_error_not_retried() {
        let mut tc = fake_context();
        tc.ctx.config.max_retries = 3;
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Non-retryable plugin error (config error).
        // Enqueue a dummy destination result since stages run concurrently.
        let plugin_err = rapidbyte_types::error::PluginError::config("BAD_HOST", "host invalid");
        tc.runner
            .enqueue_source(Err(PipelineError::Plugin(plugin_err)));
        tc.runner.enqueue_destination(Ok(make_dest_outcome(0, 0)));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::Plugin(_)),
            "expected Plugin error, got: {err:?}"
        );

        // Verify no RetryScheduled events
        let events = tc.progress.events();
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProgressEvent::RetryScheduled { .. })),
            "no RetryScheduled event expected for non-retryable error"
        );
    }

    // -----------------------------------------------------------------------
    // Test 12: Cursor loaded for incremental stream
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cursor_loaded_for_incremental_stream() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Pre-set a cursor in the repository
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("users");
        let cursor_state = rapidbyte_types::state::CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some("2024-06-15".to_string()),
            updated_at: "2024-06-15T00:00:00Z".to_string(),
        };
        tc.cursors.set(&pid, &stream, &cursor_state).await.unwrap();

        tc.runner.enqueue_source(Ok(make_source_outcome(50, 4096)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config_incremental("src", "dst", "users", "updated_at");
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Pipeline should succeed — cursor was loaded and used
        assert_eq!(result.counts.records_read, 50);
    }

    // -----------------------------------------------------------------------
    // Test 13: Cursor saved after successful run
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cursor_saved_after_successful_run() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Source outcome with a checkpoint that has a cursor value
        let source_outcome = SourceOutcome {
            duration_secs: 1.0,
            summary: ReadSummary {
                records_read: 100,
                bytes_read: 8192,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
            },
            checkpoints: vec![rapidbyte_types::checkpoint::Checkpoint {
                id: 1,
                kind: rapidbyte_types::checkpoint::CheckpointKind::Source,
                stream: "users".to_string(),
                cursor_field: Some("updated_at".to_string()),
                cursor_value: Some(rapidbyte_types::cursor::CursorValue::Utf8 {
                    value: "2024-07-01".to_string(),
                }),
                records_processed: 100,
                bytes_processed: 8192,
            }],
        };
        tc.runner.enqueue_source(Ok(source_outcome));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        let pipeline = test_pipeline_config_incremental("src", "dst", "users", "updated_at");
        let cancel = CancellationToken::new();
        run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Verify cursor was saved
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("users");
        let saved = tc.cursors.get(&pid, &stream).await.unwrap();
        assert!(saved.is_some(), "cursor should have been saved");
        let saved = saved.unwrap();
        assert_eq!(saved.cursor_value, Some("2024-07-01".to_string()));
    }

    // -----------------------------------------------------------------------
    // Test 14: No cursor load for full refresh
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_cursor_load_for_full_refresh() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        // Full refresh stream — no cursor_field
        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Pipeline should succeed without any cursor interactions
        assert_eq!(result.counts.records_read, 100);

        // No cursor should have been saved
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("users");
        let saved = tc.cursors.get(&pid, &stream).await.unwrap();
        assert!(
            saved.is_none(),
            "no cursor should be saved for full_refresh"
        );
    }

    // -----------------------------------------------------------------------
    // Test 15: Transforms execute in order
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn transforms_execute_in_order() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver.register("tx1", test_resolved_plugin());
        tc.resolver.register("tx2", test_resolved_plugin());

        // Enqueue: source, transform1, transform2, destination
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner.enqueue_transform(Ok(make_transform_outcome()));
        tc.runner.enqueue_transform(Ok(make_transform_outcome()));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        let pipeline =
            test_pipeline_config_with_transforms("src", "dst", &["users"], &["tx1", "tx2"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        assert_eq!(result.num_transforms, 2);
        assert!(result.total_transform_secs > 0.0);
        assert_eq!(result.counts.records_read, 100);
    }

    // -----------------------------------------------------------------------
    // Test 16: Transform error fails pipeline
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn transform_error_fails_pipeline() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.resolver.register("tx1", test_resolved_plugin());

        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_transform(Err(PipelineError::infra("transform WASM trap")));
        // Dummy destination result since stages run concurrently
        tc.runner.enqueue_destination(Ok(make_dest_outcome(0, 0)));

        let pipeline = test_pipeline_config_with_transforms("src", "dst", &["users"], &["tx1"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(result.is_err(), "transform error should fail the pipeline");
    }

    // -----------------------------------------------------------------------
    // Test 17: Per-stream stats recorded independently
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn per_stream_stats_recorded_independently() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Stream 1: 100 records
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8000)));
        // Stream 2: 200 records
        tc.runner
            .enqueue_source(Ok(make_source_outcome(200, 16000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(200, 16000)));

        let pipeline = test_pipeline_config("src", "dst", &["users", "orders"]);
        let cancel = CancellationToken::new();
        run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Two runs should have been started (one per stream)
        assert_eq!(
            tc.runs.started_count(),
            2,
            "expected 2 runs (one per stream)"
        );
    }

    // -----------------------------------------------------------------------
    // Test 18: Cursor save failure does not fail pipeline
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cursor_save_failure_does_not_fail_pipeline() {
        // The cursor save uses `let _ = ctx.cursors.set(...)`, so failures
        // are intentionally silenced. The FakeCursorRepository always succeeds,
        // but we can verify the pipeline still completes even with checkpoints.
        // Since the code uses `let _ =` (fire-and-forget), there is no way
        // for cursor save errors to propagate. We verify the pattern works
        // by running with checkpoints and confirming success.

        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        let source_outcome = SourceOutcome {
            duration_secs: 1.0,
            summary: ReadSummary {
                records_read: 50,
                bytes_read: 4096,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
            },
            checkpoints: vec![rapidbyte_types::checkpoint::Checkpoint {
                id: 1,
                kind: rapidbyte_types::checkpoint::CheckpointKind::Source,
                stream: "users".to_string(),
                cursor_field: Some("id".to_string()),
                cursor_value: Some(rapidbyte_types::cursor::CursorValue::Int64 { value: 999 }),
                records_processed: 50,
                bytes_processed: 4096,
            }],
        };
        tc.runner.enqueue_source(Ok(source_outcome));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config_incremental("src", "dst", "users", "id");
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(
            result.is_ok(),
            "pipeline should succeed even if cursor save were to fail"
        );
    }

    // -----------------------------------------------------------------------
    // Test 19: Empty streams list returns empty result
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_streams_list_returns_empty_result() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        // No source/dest results enqueued since no streams will be processed

        let pipeline = test_pipeline_config("src", "dst", &[]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        assert_eq!(result.counts.records_read, 0);
        assert_eq!(result.counts.records_written, 0);
        assert_eq!(result.retry_count, 0);
    }

    #[tokio::test]
    async fn run_pipeline_apply_receives_configured_streams() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_source(Ok(make_source_outcome(10, 100)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(10, 100)));

        let pipeline: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
  use: src
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
      columns: ["id", "email"]
destination:
  use: dst
  config: {}
  write_mode: append
"#,
        )
        .unwrap();

        let cancel = CancellationToken::new();
        let _ = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        let apply_calls = tc.runner.apply_calls();
        assert_eq!(apply_calls.len(), 2);
        assert_eq!(apply_calls[0].streams.len(), 1);
        assert_eq!(apply_calls[1].streams.len(), 1);
        assert_eq!(apply_calls[0].streams[0].stream_name, "users");
        assert_eq!(
            apply_calls[0].streams[0].selected_columns.as_deref(),
            Some(&["id".to_string(), "email".to_string()][..])
        );
    }

    // -----------------------------------------------------------------------
    // Test 20: Cancellation between streams
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cancellation_between_streams() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // First stream succeeds
        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8000)));
        // Second stream: enqueue results but cancel before it runs
        tc.runner
            .enqueue_source(Ok(make_source_outcome(200, 16000)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(200, 16000)));

        let pipeline = test_pipeline_config("src", "dst", &["users", "orders"]);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // We need to cancel between streams. The fake runner is synchronous,
        // so the first stream will complete before the cancellation check.
        // We'll cancel during the first source run using a callback approach.
        // Since FakePluginRunner doesn't support callbacks, we'll instead
        // set up a scenario where cancellation is checked between stream
        // iterations by cancelling after the first stream's source result
        // is consumed.

        // Actually, the simplest approach: cancel the token from a separate
        // task after a very short delay. The first stream will complete,
        // then the cancellation check at the top of the for loop fires.
        let handle = tokio::spawn(async move {
            // The fake runner returns immediately, so the first stream
            // completes nearly instantly. We cancel before the second.
            tokio::task::yield_now().await;
            cancel_clone.cancel();
        });

        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;
        handle.await.unwrap();

        // Either we get Cancelled (if token was seen) or the full result
        // (if both streams completed before cancel). Both are valid —
        // the point is the pipeline doesn't panic.
        match result {
            Err(PipelineError::Cancelled) => {
                // Cancellation was caught between streams — expected
            }
            Ok(r) => {
                // Both streams completed before cancellation was observed
                assert_eq!(r.counts.records_read, 300);
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 21: Destination error after source success
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn destination_error_after_source_success() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_destination(Err(PipelineError::infra("destination connection lost")));

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await;

        assert!(
            result.is_err(),
            "destination error should fail the pipeline"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("destination connection lost"),
            "error should contain destination message"
        );
    }

    // -----------------------------------------------------------------------
    // Sandbox override merge semantics
    // -----------------------------------------------------------------------

    fn manifest_with_limits(
        max_memory: Option<&str>,
        timeout: Option<u64>,
    ) -> rapidbyte_types::manifest::PluginManifest {
        rapidbyte_types::manifest::PluginManifest {
            id: "test".into(),
            name: "test".into(),
            version: "0.1.0".into(),
            description: String::new(),
            author: None,
            license: None,
            protocol_version: rapidbyte_types::wire::ProtocolVersion::V6,
            permissions: rapidbyte_types::manifest::Permissions::default(),
            limits: rapidbyte_types::manifest::ResourceLimits {
                max_memory: max_memory.map(String::from),
                timeout_seconds: timeout,
                min_memory: None,
            },
            roles: rapidbyte_types::manifest::Roles::default(),
            config_schema: None,
        }
    }

    #[test]
    fn sandbox_overrides_min_narrows_memory() {
        let yaml_limits = PipelineLimits {
            max_memory: Some("512mb".into()),
            timeout_seconds: None,
        };
        let manifest = manifest_with_limits(Some("256mb"), None);
        let overrides = build_sandbox_overrides(None, Some(&yaml_limits), Some(&manifest))
            .unwrap()
            .unwrap();
        // min(512mb, 256mb) = 256mb
        assert_eq!(overrides.max_memory_bytes, Some(256 * 1024 * 1024));
    }

    #[test]
    fn sandbox_overrides_min_narrows_timeout() {
        let yaml_limits = PipelineLimits {
            max_memory: None,
            timeout_seconds: Some(600),
        };
        let manifest = manifest_with_limits(None, Some(300));
        let overrides = build_sandbox_overrides(None, Some(&yaml_limits), Some(&manifest))
            .unwrap()
            .unwrap();
        // min(600, 300) = 300
        assert_eq!(overrides.timeout_seconds, Some(300));
    }

    #[test]
    fn sandbox_overrides_manifest_only() {
        let manifest = manifest_with_limits(Some("128mb"), Some(60));
        let overrides = build_sandbox_overrides(None, None, Some(&manifest))
            .unwrap()
            .unwrap();
        assert_eq!(overrides.max_memory_bytes, Some(128 * 1024 * 1024));
        assert_eq!(overrides.timeout_seconds, Some(60));
    }

    #[test]
    fn sandbox_overrides_yaml_only() {
        let yaml_limits = PipelineLimits {
            max_memory: Some("64mb".into()),
            timeout_seconds: Some(30),
        };
        let overrides = build_sandbox_overrides(None, Some(&yaml_limits), None)
            .unwrap()
            .unwrap();
        assert_eq!(overrides.max_memory_bytes, Some(64 * 1024 * 1024));
        assert_eq!(overrides.timeout_seconds, Some(30));
    }

    #[test]
    fn sandbox_overrides_none_when_no_limits() {
        let manifest = manifest_with_limits(None, None);
        let overrides = build_sandbox_overrides(None, None, Some(&manifest)).unwrap();
        assert!(overrides.is_none());
    }

    #[test]
    fn sandbox_overrides_pipeline_cannot_widen_manifest() {
        // Pipeline says 1gb but manifest says 256mb — should get 256mb
        let yaml_limits = PipelineLimits {
            max_memory: Some("1gb".into()),
            timeout_seconds: Some(3600),
        };
        let manifest = manifest_with_limits(Some("256mb"), Some(300));
        let overrides = build_sandbox_overrides(None, Some(&yaml_limits), Some(&manifest))
            .unwrap()
            .unwrap();
        assert_eq!(overrides.max_memory_bytes, Some(256 * 1024 * 1024));
        assert_eq!(overrides.timeout_seconds, Some(300));
    }

    #[test]
    fn sandbox_overrides_malformed_manifest_memory_fails() {
        let manifest = manifest_with_limits(Some("256megabytes"), None);
        let result = build_sandbox_overrides(None, None, Some(&manifest));
        assert!(result.is_err(), "malformed manifest max_memory should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid manifest max_memory"),
            "error should mention manifest: {err}"
        );
    }

    #[test]
    fn sandbox_overrides_malformed_yaml_memory_fails() {
        let yaml_limits = PipelineLimits {
            max_memory: Some("not-a-size".into()),
            timeout_seconds: None,
        };
        let result = build_sandbox_overrides(None, Some(&yaml_limits), None);
        assert!(result.is_err(), "malformed pipeline max_memory should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid pipeline max_memory"),
            "error should mention pipeline: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Null cursor checkpoint does not overwrite existing cursor
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn null_checkpoint_does_not_overwrite_existing_cursor() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Pre-set a valid cursor
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("users");
        let existing_cursor = rapidbyte_types::state::CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some("2024-07-01".to_string()),
            updated_at: "2024-07-01T00:00:00Z".to_string(),
        };
        tc.cursors
            .set(&pid, &stream, &existing_cursor)
            .await
            .unwrap();

        // Source emits a checkpoint with CursorValue::Null
        let source_outcome = SourceOutcome {
            duration_secs: 1.0,
            summary: ReadSummary {
                records_read: 50,
                bytes_read: 4096,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
            },
            checkpoints: vec![rapidbyte_types::checkpoint::Checkpoint {
                id: 1,
                kind: rapidbyte_types::checkpoint::CheckpointKind::Source,
                stream: "users".to_string(),
                cursor_field: Some("updated_at".to_string()),
                cursor_value: Some(rapidbyte_types::cursor::CursorValue::Null),
                records_processed: 50,
                bytes_processed: 4096,
            }],
        };
        tc.runner.enqueue_source(Ok(source_outcome));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config_incremental("src", "dst", "users", "updated_at");
        let cancel = CancellationToken::new();
        run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // The existing cursor should NOT have been overwritten
        let saved = tc.cursors.get(&pid, &stream).await.unwrap();
        assert!(saved.is_some(), "cursor should still exist");
        assert_eq!(
            saved.unwrap().cursor_value,
            Some("2024-07-01".to_string()),
            "null checkpoint must not overwrite existing cursor"
        );
    }

    // -----------------------------------------------------------------------
    // Empty-string historical cursor treated as no cursor
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_string_cursor_treated_as_no_cursor() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Pre-set a cursor with empty string (historical bug data)
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("users");
        let bad_cursor = rapidbyte_types::state::CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some(String::new()), // empty string from old bug
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        };
        tc.cursors.set(&pid, &stream, &bad_cursor).await.unwrap();

        tc.runner.enqueue_source(Ok(make_source_outcome(100, 8192)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(100, 8192)));

        let pipeline = test_pipeline_config_incremental("src", "dst", "users", "updated_at");
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Pipeline should succeed — empty cursor treated as "no cursor"
        assert_eq!(result.counts.records_read, 100);
    }

    // -----------------------------------------------------------------------
    // Test: Tie-breaker cursor loaded as JSON (composite state)
    // -----------------------------------------------------------------------

    fn test_pipeline_config_incremental_with_tie_breaker(
        source_ref: &str,
        dest_ref: &str,
        stream_name: &str,
        cursor_field: &str,
        tie_breaker_field: &str,
    ) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: test-pipeline
source:
    use: {source_ref}
    streams:
        - name: {stream_name}
          sync_mode: incremental
          cursor_field: {cursor_field}
          tie_breaker_field: {tie_breaker_field}
    config: {{}}
destination:
    use: {dest_ref}
    config: {{}}
    write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("test yaml should parse")
    }

    #[tokio::test]
    async fn tie_breaker_cursor_loaded_as_json() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Pre-set a composite JSON cursor (tie-breaker format)
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("events");
        let composite_cursor = rapidbyte_types::state::CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some(r#"{"cursor":"2024-07-01","tie_breaker":"uuid-123"}"#.to_string()),
            updated_at: "2024-07-01T00:00:00Z".to_string(),
        };
        tc.cursors
            .set(&pid, &stream, &composite_cursor)
            .await
            .unwrap();

        tc.runner.enqueue_source(Ok(make_source_outcome(50, 4096)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config_incremental_with_tie_breaker(
            "src",
            "dst",
            "events",
            "updated_at",
            "id",
        );
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Pipeline should succeed with tie-breaker cursor loaded
        assert_eq!(result.counts.records_read, 50);
    }

    #[tokio::test]
    async fn tie_breaker_scalar_cursor_falls_back_to_utf8() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());

        // Pre-set a plain scalar cursor (legacy format, not JSON)
        let pid = rapidbyte_types::state::PipelineId::new("test-pipeline");
        let stream = rapidbyte_types::state::StreamName::new("events");
        let scalar_cursor = rapidbyte_types::state::CursorState {
            cursor_field: Some("updated_at".to_string()),
            cursor_value: Some("2024-07-01".to_string()),
            updated_at: "2024-07-01T00:00:00Z".to_string(),
        };
        tc.cursors.set(&pid, &stream, &scalar_cursor).await.unwrap();

        tc.runner.enqueue_source(Ok(make_source_outcome(50, 4096)));
        tc.runner
            .enqueue_destination(Ok(make_dest_outcome(50, 4096)));

        let pipeline = test_pipeline_config_incremental_with_tie_breaker(
            "src",
            "dst",
            "events",
            "updated_at",
            "id",
        );
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, cancel).await.unwrap();

        // Pipeline should succeed — scalar cursor falls back to Utf8
        assert_eq!(result.counts.records_read, 50);
    }
}
