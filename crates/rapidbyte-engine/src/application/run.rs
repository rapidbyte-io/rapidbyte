//! Run-pipeline use case — the core orchestration logic.
//!
//! [`run_pipeline`] owns the retry loop and orchestrates the entire pipeline
//! execution: resolve plugins, load cursors, execute streams, finalize.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};

use rapidbyte_runtime::Frame;

use crate::application::context::EngineContext;
use crate::domain::error::PipelineError;
use crate::domain::outcome::{
    DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineCounts, PipelineOutcome,
    PipelineResult, SourceTiming,
};
use crate::domain::ports::runner::{DestinationRunParams, SourceOutcome, SourceRunParams};
use crate::domain::progress::{Phase, ProgressEvent};
use crate::domain::retry::{RetryDecision, RetryPolicy};

/// Convert a `CompressionCodec` enum into its wire-format string name.
fn compression_name(codec: rapidbyte_types::compression::CompressionCodec) -> String {
    use rapidbyte_types::compression::CompressionCodec;
    match codec {
        CompressionCodec::Lz4 => "lz4".to_string(),
        CompressionCodec::Zstd => "zstd".to_string(),
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
    options: &ExecutionOptions,
    cancel: CancellationToken,
) -> Result<PipelineOutcome, PipelineError> {
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
        let mut dry_run_streams: Vec<DryRunStreamResult> = Vec::new();
        let mut stream_error: Option<PipelineError> = None;

        let (src_id, src_ver) = parse_plugin_id(&pipeline.source.use_ref);
        let (dst_id, dst_ver) = parse_plugin_id(&pipeline.destination.use_ref);

        for stream_cfg in &pipeline.source.streams {
            if cancel.is_cancelled() {
                return Err(PipelineError::Cancelled);
            }

            let stream_name = &stream_cfg.name;
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

            // Build stream context
            let stream_ctx = rapidbyte_types::stream::StreamContext {
                stream_name: stream_name.clone(),
                source_stream_name: None,
                schema: rapidbyte_types::catalog::SchemaHint::Columns(vec![]),
                sync_mode: stream_cfg.sync_mode,
                cursor_info,
                limits: rapidbyte_types::stream::StreamLimits::default(),
                policies: rapidbyte_types::stream::StreamPolicies::default(),
                write_mode: None,
                selected_columns: stream_cfg.columns.clone(),
                partition_key: stream_cfg.partition_key.clone(),
                partition_count: None,
                partition_index: None,
                effective_parallelism: None,
                partition_strategy: None,
                copy_flush_bytes_override: None,
            };

            // Create channel for frame transport
            let (frame_tx, frame_rx) = mpsc::sync_channel::<Frame>(ctx.config.channel_capacity);

            let stats = Arc::new(Mutex::new(RunStats::default()));
            let source_permissions = source_resolved
                .manifest
                .as_ref()
                .map(|m| m.permissions.clone());

            // Run source
            let source_result = ctx
                .runner
                .run_source(SourceRunParams {
                    wasm_path: source_resolved.wasm_path.clone(),
                    pipeline_name: pipeline.pipeline.clone(),
                    metric_run_label: format!("{}-{attempt}", pipeline.pipeline),
                    plugin_id: src_id.clone(),
                    plugin_version: src_ver.clone(),
                    stream_ctx: stream_ctx.clone(),
                    config: pipeline.source.config.clone(),
                    permissions: source_permissions,
                    compression: pipeline
                        .resources
                        .compression
                        .as_ref()
                        .copied()
                        .map(compression_name),
                    frame_sender: frame_tx,
                    stats: Arc::clone(&stats),
                    on_batch_emitted: None,
                })
                .await;

            let source_outcome: SourceOutcome = match source_result {
                Ok(outcome) => outcome,
                Err(e) => {
                    stream_error = Some(e);
                    break;
                }
            };

            total_source_secs += source_outcome.duration_secs;
            total_records_read += source_outcome.summary.records_read;
            total_bytes_read += source_outcome.summary.bytes_read;

            // Run transforms (sequential pipeline)
            let mut current_rx = Some(frame_rx);
            for (i, transform) in pipeline.transforms.iter().enumerate() {
                let (next_tx, next_rx) = mpsc::sync_channel::<Frame>(ctx.config.channel_capacity);

                let t_permissions = transform_resolved[i]
                    .manifest
                    .as_ref()
                    .map(|m| m.permissions.clone());
                let (t_id, t_ver) = parse_plugin_id(&transform.use_ref);

                let rx = current_rx.take().expect("receiver should be available");
                let transform_result = ctx
                    .runner
                    .run_transform(crate::domain::ports::runner::TransformRunParams {
                        wasm_path: transform_resolved[i].wasm_path.clone(),
                        pipeline_name: pipeline.pipeline.clone(),
                        metric_run_label: format!("{}-{attempt}", pipeline.pipeline),
                        plugin_id: t_id,
                        plugin_version: t_ver,
                        stream_ctx: stream_ctx.clone(),
                        config: transform.config.clone(),
                        permissions: t_permissions,
                        compression: pipeline
                            .resources
                            .compression
                            .as_ref()
                            .copied()
                            .map(compression_name),
                        frame_receiver: rx,
                        frame_sender: next_tx,
                        dlq_records: Arc::new(Mutex::new(Vec::new())),
                        transform_index: i,
                    })
                    .await;

                match transform_result {
                    Ok(outcome) => {
                        total_transform_secs += outcome.duration_secs;
                    }
                    Err(e) => {
                        stream_error = Some(e);
                        break;
                    }
                }

                current_rx = Some(next_rx);
            }

            if stream_error.is_some() {
                break;
            }

            // Dry run: skip destination, collect source outcome info
            if options.dry_run {
                dry_run_streams.push(DryRunStreamResult {
                    stream_name: stream_name.clone(),
                    batches: vec![],
                    total_rows: source_outcome.summary.records_read,
                    total_bytes: source_outcome.summary.bytes_read,
                });
            } else {
                // Run destination
                let dest_permissions = dest_resolved
                    .manifest
                    .as_ref()
                    .map(|m| m.permissions.clone());

                let final_rx = current_rx.take().expect("receiver should be available");
                let dest_result = ctx
                    .runner
                    .run_destination(DestinationRunParams {
                        wasm_path: dest_resolved.wasm_path.clone(),
                        pipeline_name: pipeline.pipeline.clone(),
                        metric_run_label: format!("{}-{attempt}", pipeline.pipeline),
                        plugin_id: dst_id.clone(),
                        plugin_version: dst_ver.clone(),
                        stream_ctx: stream_ctx.clone(),
                        config: pipeline.destination.config.clone(),
                        permissions: dest_permissions,
                        compression: pipeline
                            .resources
                            .compression
                            .as_ref()
                            .copied()
                            .map(compression_name),
                        frame_receiver: final_rx,
                        dlq_records: Arc::new(Mutex::new(Vec::new())),
                        stats: Arc::clone(&stats),
                    })
                    .await;

                match dest_result {
                    Ok(outcome) => {
                        total_dest_secs += outcome.duration_secs;
                        total_records_written += outcome.summary.records_written;
                        total_bytes_written += outcome.summary.bytes_written;
                    }
                    Err(e) => {
                        stream_error = Some(e);
                        break;
                    }
                }
            }

            // Save cursor updates from source checkpoints
            for checkpoint in &source_outcome.checkpoints {
                if let Some(ref cursor_value) = checkpoint.cursor_value {
                    let cursor_state = rapidbyte_types::state::CursorState {
                        cursor_field: checkpoint.cursor_field.clone(),
                        cursor_value: Some(cursor_value_to_string(cursor_value)),
                        updated_at: chrono::Utc::now().to_rfc3339(),
                    };
                    let _ = ctx
                        .cursors
                        .set(&pipeline_id, &StreamName::new(stream_name), &cursor_state)
                        .await;
                }
            }

            ctx.progress.report(ProgressEvent::StreamCompleted {
                stream: stream_name.clone(),
            });
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
                tokio::time::sleep(retry_delay).await;
                continue;
            }
            return Err(err);
        }

        // 6. Finalization
        ctx.progress.report(ProgressEvent::PhaseChanged {
            phase: Phase::Finalizing,
        });

        let duration_secs = overall_start.elapsed().as_secs_f64();

        // Record runs for each stream
        for stream_cfg in &pipeline.source.streams {
            let stream_name = StreamName::new(&stream_cfg.name);
            let run_id = ctx
                .runs
                .start(&pipeline_id, &stream_name)
                .await
                .map_err(|e| PipelineError::infra(format!("run start failed: {e}")))?;

            let run_stats = RunStats {
                records_read: total_records_read,
                records_written: total_records_written,
                bytes_read: total_bytes_read,
                bytes_written: total_bytes_written,
                error_message: None,
            };

            ctx.runs
                .complete(run_id, RunStatus::Completed, &run_stats)
                .await
                .map_err(|e| PipelineError::infra(format!("run complete failed: {e}")))?;
        }

        // 7. Return outcome
        if options.dry_run {
            return Ok(PipelineOutcome::DryRun(DryRunResult {
                streams: dry_run_streams,
                source: SourceTiming {
                    duration_secs: total_source_secs,
                    ..SourceTiming::default()
                },
                num_transforms: pipeline.transforms.len(),
                total_transform_secs,
                duration_secs,
            }));
        }

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

        return Ok(PipelineOutcome::Run(result));
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

/// Split a plugin reference into (id, version).
fn parse_plugin_id(plugin_ref: &str) -> (String, String) {
    if let Some((id, ver)) = plugin_ref.rsplit_once(':') {
        (id.to_string(), ver.to_string())
    } else {
        (plugin_ref.to_string(), "0.0.0".to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_context;
    use crate::domain::ports::resolver::ResolvedPlugin;
    use crate::domain::ports::runner::{DestinationOutcome, SourceOutcome};
    use rapidbyte_types::metric::{ReadSummary, WriteSummary};
    use std::path::PathBuf;

    fn test_resolved_plugin() -> ResolvedPlugin {
        ResolvedPlugin {
            wasm_path: PathBuf::from("/tmp/test.wasm"),
            manifest: None,
        }
    }

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
        let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel)
            .await
            .unwrap();

        match result {
            PipelineOutcome::Run(r) => {
                assert_eq!(r.counts.records_read, 100);
                assert_eq!(r.counts.records_written, 100);
                assert_eq!(r.counts.bytes_read, 8192);
                assert_eq!(r.counts.bytes_written, 8192);
                assert_eq!(r.retry_count, 0);
                assert_eq!(r.parallelism, 1);
            }
            PipelineOutcome::DryRun(_) => panic!("expected Run outcome"),
        }
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

        let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await;

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
        run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel)
            .await
            .unwrap();

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
        run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel)
            .await
            .unwrap();

        // Verify a run was started (the fake auto-increments from 1)
        let started = tc.runs.started_count();
        assert_eq!(started, 1, "expected one run to be started");
    }

    // -----------------------------------------------------------------------
    // Test 5: Dry run skips destination
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_dry_run_skips_destination() {
        let tc = fake_context();
        tc.resolver.register("src", test_resolved_plugin());
        tc.resolver.register("dst", test_resolved_plugin());
        tc.runner.enqueue_source(Ok(make_source_outcome(75, 6000)));
        // Do NOT enqueue a destination result — it should not be called

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let opts = ExecutionOptions {
            dry_run: true,
            limit: None,
        };
        let result = run_pipeline(&tc.ctx, &pipeline, &opts, cancel)
            .await
            .unwrap();

        match result {
            PipelineOutcome::DryRun(r) => {
                assert_eq!(r.streams.len(), 1);
                assert_eq!(r.streams[0].stream_name, "users");
                assert_eq!(r.streams[0].total_rows, 75);
            }
            PipelineOutcome::Run(_) => panic!("expected DryRun outcome"),
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: Multiple streams
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
        let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel)
            .await
            .unwrap();

        match result {
            PipelineOutcome::Run(r) => {
                assert_eq!(r.counts.records_read, 300);
                assert_eq!(r.counts.records_written, 300);
                assert_eq!(r.counts.bytes_read, 24000);
                assert_eq!(r.counts.bytes_written, 24000);
            }
            PipelineOutcome::DryRun(_) => panic!("expected Run outcome"),
        }
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

        let pipeline = test_pipeline_config("src", "dst", &["users"]);
        let cancel = CancellationToken::new();
        let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await;

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
        let result = run_pipeline(&tc.ctx, &pipeline, &ExecutionOptions::default(), cancel).await;

        assert!(result.is_err());
    }
}
