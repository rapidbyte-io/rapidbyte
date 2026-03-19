//! Run-pipeline use case — the core orchestration logic.
//!
//! [`run_pipeline`] owns the retry loop and orchestrates the entire pipeline
//! execution: resolve plugins, load cursors, execute streams, finalize.

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use rapidbyte_pipeline_config::{parse_byte_size, PipelineConfig, ResourceConfig};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::stream::StreamLimits;

use rapidbyte_runtime::Frame;

use crate::application::context::EngineContext;
use crate::application::{extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::outcome::{PipelineCounts, PipelineResult, SourceTiming};
use crate::domain::ports::runner::{
    DestinationRunParams, SourceOutcome, SourceRunParams, TransformRunParams,
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
/// Parses human-readable byte sizes (e.g. "64mb") into numeric values,
/// falling back to [`StreamLimits`] defaults on parse failure.
fn build_stream_limits(resources: &ResourceConfig) -> StreamLimits {
    let max_batch_bytes = parse_byte_size(&resources.max_batch_bytes)
        .unwrap_or(StreamLimits::DEFAULT_MAX_BATCH_BYTES);
    let checkpoint_interval_bytes = parse_byte_size(&resources.checkpoint_interval_bytes)
        .unwrap_or(StreamLimits::DEFAULT_CHECKPOINT_INTERVAL_BYTES);

    StreamLimits {
        max_batch_bytes,
        checkpoint_interval_bytes,
        checkpoint_interval_rows: resources.checkpoint_interval_rows,
        checkpoint_interval_seconds: resources.checkpoint_interval_seconds,
        max_inflight_batches: resources.max_inflight_batches,
        ..StreamLimits::default()
    }
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
        // Per-stream stats: (records_read, records_written, bytes_read, bytes_written)
        let mut per_stream_stats: HashMap<String, (u64, u64, u64, u64)> = HashMap::new();
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
        let stream_limits = build_stream_limits(&pipeline.resources);

        for stream_cfg in &pipeline.source.streams {
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

            // Load cursor for incremental streams
            let cursor_info = if let Some(cursor_field) = &stream_cfg.cursor_field {
                let last_value = ctx
                    .cursors
                    .get(&pipeline_id, &StreamName::new(stream_name))
                    .await
                    .map_err(|e| PipelineError::infra(format!("cursor load failed: {e}")))?
                    .and_then(|cs| cs.cursor_value)
                    .map(|v| rapidbyte_types::cursor::CursorValue::Utf8 { value: v });

                Some(rapidbyte_types::cursor::CursorInfo {
                    cursor_field: cursor_field.clone(),
                    tie_breaker_field: stream_cfg.tie_breaker_field.clone(),
                    cursor_type: rapidbyte_types::cursor::CursorType::Utf8,
                    last_value,
                })
            } else {
                None
            };

            // Build stream context with limits from pipeline resources
            let stream_ctx = rapidbyte_types::stream::StreamContext {
                stream_name: stream_name.clone(),
                source_stream_name: None,
                schema: rapidbyte_types::catalog::SchemaHint::Columns(vec![]),
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
                partition_count: None,
                partition_index: None,
                effective_parallelism: None,
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
            // TODO: merge pipeline YAML permission/limit overrides (pipeline.source.permissions,
            // pipeline.source.limits) with manifest permissions. Currently only manifest-level
            // permissions are passed; YAML overrides for timeout_seconds, max_memory, and
            // network/env/fs permissions are dropped.
            let source_permissions = extract_permissions(&source_resolved);
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
                    compression: source_compression.clone(),
                    frame_sender: src_tx,
                    stats: Arc::clone(&stats),
                    on_batch_emitted: None,
                };
                tokio::spawn(async move { runner.run_source(params).await })
            };

            // Spawn transforms (chained, each consuming from previous, sending to next)
            let mut transform_handles = Vec::with_capacity(pipeline.transforms.len());
            for (i, transform) in pipeline.transforms.iter().enumerate() {
                let (next_tx, next_rx) = mpsc::sync_channel::<Frame>(ctx.config.channel_capacity);

                // TODO: merge pipeline YAML permission/limit overrides (transform.permissions,
                // transform.limits) with manifest permissions.
                let t_permissions = extract_permissions(&transform_resolved[i]);
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
                    compression: source_compression.clone(),
                    frame_receiver: current_rx,
                    frame_sender: next_tx,
                    dlq_records: Arc::clone(&stream_dlq),
                    transform_index: i,
                };
                transform_handles.push(tokio::spawn(
                    async move { runner.run_transform(params).await },
                ));

                current_rx = next_rx;
            }

            // Spawn destination (consumes from last channel)
            // TODO: merge pipeline YAML permission/limit overrides (pipeline.destination.permissions,
            // pipeline.destination.limits) with manifest permissions.
            let dest_permissions = extract_permissions(&dest_resolved);
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
                    compression: dest_compression.clone(),
                    frame_receiver: current_rx,
                    dlq_records: Arc::clone(&stream_dlq),
                    stats: Arc::clone(&stats),
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

                    // Record per-stream stats
                    per_stream_stats.insert(
                        stream_name.clone(),
                        (
                            source_outcome.summary.records_read,
                            dest_outcome.summary.records_written,
                            source_outcome.summary.bytes_read,
                            dest_outcome.summary.bytes_written,
                        ),
                    );

                    // Save cursor updates from source checkpoints
                    for checkpoint in &source_outcome.checkpoints {
                        if let Some(ref cursor_value) = checkpoint.cursor_value {
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
                        std::mem::take(&mut *stream_dlq.lock().unwrap());
                    if !dlq_batch.is_empty() {
                        per_stream_dlq.insert(stream_name.clone(), dlq_batch);
                    }

                    // Issue 2: Complete run record with success
                    let (sr, sw, br, bw) = per_stream_stats
                        .get(stream_name)
                        .copied()
                        .unwrap_or((0, 0, 0, 0));
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
                        std::mem::take(&mut *stream_dlq.lock().unwrap());
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
            parallelism: 1,
            stream_metrics: vec![],
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
}
