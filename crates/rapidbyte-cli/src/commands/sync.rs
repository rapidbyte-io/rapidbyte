//! Pipeline sync subcommand — local execution and remote (distributed) execution.

// u64/i64/usize -> f64 casts are intentional lossy conversions for display and
// timing computations where sub-millisecond precision loss is acceptable.
#![allow(clippy::cast_precision_loss)]

use std::path::Path;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::output::{progress, summary};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    run_event, ExecutionOptions, RunState, SubmitPipelineRequest, WatchRunRequest,
};

#[derive(Debug, Clone)]
struct ProcessCpuMetrics {
    cpu_secs: f64,
    cpu_pct_one_core: f64,
    cpu_pct_of_available_cores: f64,
    available_cores: usize,
}

/// Execute the `sync` command: routes to local or remote execution.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or execution fails.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn execute(
    pipeline_path: &Path,
    verbosity: Verbosity,
    controller: Option<&str>,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
    otel_guard: &rapidbyte_metrics::OtelGuard,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    if let Some(url) = controller {
        return execute_remote(pipeline_path, url, auth_token, tls, verbosity).await;
    }
    execute_local(
        pipeline_path,
        verbosity,
        otel_guard,
        registry_config,
        secrets,
    )
    .await
}

// ── Local execution ───────────────────────────────────────────────────────────

async fn execute_local(
    pipeline_path: &Path,
    verbosity: Verbosity,
    _otel_guard: &rapidbyte_metrics::OtelGuard,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path, secrets).await?;

    tracing::info!(
        pipeline = config.pipeline,
        source = config.source.use_ref,
        destination = config.destination.use_ref,
        streams = config.source.streams.len(),
        "Pipeline validated"
    );

    // Spawn progress spinner (unless quiet)
    let is_tty = console::Term::stderr().is_term();
    let stream_count = config.source.streams.len() as u64;
    let show_progress = !matches!(verbosity, Verbosity::Quiet);
    let (progress_tx, spinner_handle) = if show_progress {
        let (tx, handle) = progress::spawn_progress_spinner(stream_count, is_tty);
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Run the pipeline with signal-driven cooperative cancellation.
    let cpu_start = process_cpu_seconds();
    let cancel_token = CancellationToken::new();
    let sigint_token = cancel_token.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("Interrupt received, cancelling pipeline...");
                sigint_token.cancel();
            }
            Err(e) => tracing::warn!("failed to listen for Ctrl-C: {e}"),
        }
    });
    #[cfg(unix)]
    {
        let sigterm_token = cancel_token.clone();
        tokio::spawn(async move {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    sigterm.recv().await;
                    tracing::info!("SIGTERM received, cancelling pipeline...");
                    sigterm_token.cancel();
                }
                Err(e) => tracing::warn!("failed to install SIGTERM handler: {e}"),
            }
        });
    }

    let ctx = rapidbyte_engine::build_run_context(&config, progress_tx, registry_config).await?;
    let outcome = rapidbyte_engine::run_pipeline(&ctx, &config, cancel_token).await;
    let (cpu_end, peak_rss_mb) = post_pipeline_metrics();

    // Drop context to release the progress sender, allowing the spinner
    // loop to exit when rx.recv() returns None.
    drop(ctx);

    // Wait for spinner to finish before printing results
    if let Some(handle) = spinner_handle {
        let _ = handle.await;
    }

    let result = match outcome {
        Ok(r) => r,
        Err(e) => return Err(e.into()),
    };

    let cpu_metrics = process_cpu_metrics(cpu_start, cpu_end, result.duration_secs);

    if let Some(cpu) = &cpu_metrics {
        rapidbyte_metrics::instruments::process::cpu_seconds().record(cpu.cpu_secs, &[]);
    }
    if let Some(rss) = peak_rss_mb {
        rapidbyte_metrics::instruments::process::peak_rss_bytes()
            .record(rss * 1024.0 * 1024.0, &[]);
    }

    // Human-readable summary to stderr
    summary::print_success(&result, &config.pipeline, verbosity);

    // Process-level diagnostics for -vv
    if verbosity == Verbosity::Diagnostic {
        if let Some(cpu) = &cpu_metrics {
            eprintln!(
                "    {:<20}CPU {:.0}% ({:.0}% of {} cores) | RSS {:.0} MB",
                "Process",
                cpu.cpu_pct_one_core,
                cpu.cpu_pct_of_available_cores,
                cpu.available_cores,
                peak_rss_mb.unwrap_or(0.0),
            );
        }
    }

    // Machine-readable JSON for benchmarking tools (stdout, opt-in)
    if std::env::var_os("RAPIDBYTE_BENCH").is_some() {
        let json = bench_json_from_result(&result, cpu_metrics.as_ref(), peak_rss_mb);
        println!("@@BENCH_JSON@@{json}");
    }

    Ok(())
}

fn process_cpu_metrics(
    cpu_start_secs: Option<f64>,
    cpu_end_secs: Option<f64>,
    wall_duration_secs: f64,
) -> Option<ProcessCpuMetrics> {
    let start = cpu_start_secs?;
    let end = cpu_end_secs?;
    if wall_duration_secs <= 0.0 || end < start {
        return None;
    }

    let cpu_secs = end - start;
    let cpu_pct_one_core = (cpu_secs / wall_duration_secs) * 100.0;
    let available_cores = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let cpu_pct_of_available_cores = cpu_pct_one_core / available_cores as f64;

    Some(ProcessCpuMetrics {
        cpu_secs,
        cpu_pct_one_core,
        cpu_pct_of_available_cores,
        available_cores,
    })
}

/// Single `getrusage(RUSAGE_SELF)` wrapper to avoid duplicate unsafe blocks.
#[cfg(unix)]
fn getrusage_self() -> Option<libc::rusage> {
    // Safety: getrusage writes into the provided `rusage` struct.
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // Safety: pointer is valid for writes and initialized by successful call.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // Safety: call succeeded and initialized `usage`.
    Some(unsafe { usage.assume_init() })
}

#[cfg(unix)]
#[allow(clippy::cast_precision_loss)]
fn cpu_seconds_from_rusage(usage: &libc::rusage) -> f64 {
    let user_secs = usage.ru_utime.tv_sec as f64 + (usage.ru_utime.tv_usec as f64 / 1e6);
    let sys_secs = usage.ru_stime.tv_sec as f64 + (usage.ru_stime.tv_usec as f64 / 1e6);
    user_secs + sys_secs
}

fn process_cpu_seconds() -> Option<f64> {
    #[cfg(unix)]
    {
        getrusage_self().map(|u| cpu_seconds_from_rusage(&u))
    }

    #[cfg(not(unix))]
    {
        None
    }
}

/// Get CPU seconds and peak RSS from a single post-pipeline `getrusage` call.
fn post_pipeline_metrics() -> (Option<f64>, Option<f64>) {
    #[cfg(unix)]
    {
        match getrusage_self() {
            Some(u) => {
                let cpu = cpu_seconds_from_rusage(&u);
                #[cfg(target_os = "macos")]
                let rss = u.ru_maxrss as f64 / (1024.0 * 1024.0);
                #[cfg(not(target_os = "macos"))]
                let rss = u.ru_maxrss as f64 / 1024.0;
                (Some(cpu), Some(rss))
            }
            None => (None, None),
        }
    }

    #[cfg(not(unix))]
    {
        (None, None)
    }
}

fn bench_json_from_result(
    result: &rapidbyte_engine::PipelineResult,
    cpu_metrics: Option<&ProcessCpuMetrics>,
    peak_rss_mb: Option<f64>,
) -> serde_json::Value {
    let counts = &result.counts;
    let source = &result.source;
    let dest = &result.dest;

    let stream_metrics: Vec<_> = result
        .stream_metrics
        .iter()
        .map(|m| {
            serde_json::json!({
                "stream_name": m.stream_name,
                "partition_index": m.partition_index,
                "partition_count": m.partition_count,
                "records_read": m.records_read,
                "records_written": m.records_written,
                "bytes_read": m.bytes_read,
                "bytes_written": m.bytes_written,
                "source_duration_secs": m.source_duration_secs,
                "dest_duration_secs": m.dest_duration_secs,
                "dest_wasm_instantiation_secs": m.dest_wasm_instantiation_secs,
                "dest_frame_receive_secs": m.dest_frame_receive_secs,
            })
        })
        .collect();

    let partitioned = || {
        result
            .stream_metrics
            .iter()
            .filter(|m| m.partition_count.unwrap_or(1) > 1)
    };
    let worker_records_min = partitioned().map(|m| m.records_written).min();
    let worker_records_max = partitioned().map(|m| m.records_written).max();
    let worker_records_skew_ratio = worker_records_min
        .zip(worker_records_max)
        .and_then(|(min, max)| (max > 0).then_some(min as f64 / max as f64));

    let worker_dest_total_secs: f64 = partitioned().map(|m| m.dest_duration_secs).sum();
    let worker_dest_recv_secs: f64 = partitioned().map(|m| m.dest_frame_receive_secs).sum();
    let worker_dest_vm_setup_secs: f64 =
        partitioned().map(|m| m.dest_wasm_instantiation_secs).sum();
    let worker_dest_active_secs =
        (worker_dest_total_secs - worker_dest_recv_secs - worker_dest_vm_setup_secs).max(0.0);

    serde_json::json!({
        "records_read": counts.records_read,
        "records_written": counts.records_written,
        "bytes_read": counts.bytes_read,
        "bytes_written": counts.bytes_written,
        "duration_secs": result.duration_secs,
        "source_duration_secs": source.duration_secs,
        "dest_duration_secs": dest.duration_secs,
        "dest_connect_secs": dest.connect_secs,
        "dest_flush_secs": dest.flush_secs,
        "dest_commit_secs": dest.commit_secs,
        "dest_wasm_instantiation_secs": dest.wasm_instantiation_secs,
        "dest_frame_receive_secs": dest.frame_receive_secs,
        "wasm_overhead_secs": result.wasm_overhead_secs,
        "source_connect_secs": source.connect_secs,
        "source_query_secs": source.query_secs,
        "source_fetch_secs": source.fetch_secs,
        "source_arrow_encode_secs": source.arrow_encode_secs,
        "dest_arrow_decode_secs": dest.arrow_decode_secs,
        "source_module_load_ms": source.module_load_ms,
        "dest_module_load_ms": dest.module_load_ms,
        "source_emit_nanos": source.emit_nanos,
        "source_compress_nanos": source.compress_nanos,
        "source_emit_count": source.emit_count,
        "dest_recv_nanos": dest.frame_receive_nanos,
        "dest_recv_wait_nanos": dest.frame_wait_nanos,
        "dest_recv_process_nanos": dest.frame_process_nanos,
        "dest_decompress_nanos": dest.decompress_nanos,
        "dest_recv_count": dest.frame_count,
        "num_transforms": result.num_transforms,
        "total_transform_secs": result.total_transform_secs,
        "transform_load_times_ms": result.transform_load_times_ms,
        "retry_count": result.retry_count,
        "parallelism": result.parallelism,
        "stream_metrics": stream_metrics,
        "worker_records_min": worker_records_min,
        "worker_records_max": worker_records_max,
        "worker_records_skew_ratio": worker_records_skew_ratio,
        "worker_dest_total_secs": worker_dest_total_secs,
        "worker_dest_recv_secs": worker_dest_recv_secs,
        "worker_dest_vm_setup_secs": worker_dest_vm_setup_secs,
        "worker_dest_active_secs": worker_dest_active_secs,
        "process_cpu_secs": cpu_metrics.map(|m| m.cpu_secs),
        "process_cpu_pct_one_core": cpu_metrics.map(|m| m.cpu_pct_one_core),
        "process_cpu_pct_available_cores": cpu_metrics.map(|m| m.cpu_pct_of_available_cores),
        "available_cores": cpu_metrics.map(|m| m.available_cores),
        "process_peak_rss_mb": peak_rss_mb,
    })
}

// ── Remote execution (gRPC) ───────────────────────────────────────────────────

async fn execute_remote(
    pipeline_path: &Path,
    controller_url: &str,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
    verbosity: Verbosity,
) -> Result<()> {
    let yaml = std::fs::read_to_string(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;

    let channel = connect_channel(controller_url, tls)
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = PipelineServiceClient::new(channel);

    // Submit
    let resp = client
        .submit_pipeline(request_with_bearer(
            SubmitPipelineRequest {
                pipeline_yaml: yaml,
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                options: Some(ExecutionOptions {
                    max_retries: 0,
                    timeout_seconds: 0,
                }),
            },
            auth_token,
        )?)
        .await?;
    let inner = resp.into_inner();
    let run_id = inner.run_id;

    if verbosity != Verbosity::Quiet {
        if inner.already_exists {
            eprintln!("Run already exists: {run_id}");
        } else {
            eprintln!("Submitted run: {run_id}");
        }
    }

    // Watch
    let mut stream = client
        .watch_run(request_with_bearer(
            WatchRunRequest {
                run_id: run_id.clone(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        // Show state changes
        if verbosity != Verbosity::Quiet {
            let state = state_label(event.state);
            if event.attempt > 0 {
                eprintln!("State: {state} (attempt {})", event.attempt);
            }
        }

        match event.detail {
            Some(run_event::Detail::Progress(p)) => {
                if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
                    let pct = p
                        .progress_pct
                        .map(|v| format!(" ({v:.0}%)"))
                        .unwrap_or_default();
                    eprintln!("  {}{pct}", p.message);
                }
            }
            Some(run_event::Detail::Completed(c)) => {
                if verbosity != Verbosity::Quiet {
                    if let Some(m) = &c.metrics {
                        eprintln!(
                            "Completed: {} rows read, {} rows written, {} bytes in {}ms",
                            m.rows_read, m.rows_written, m.bytes_written, m.duration_ms,
                        );
                    } else {
                        eprintln!("Completed");
                    }
                }
                emit_bench_json_from_completion(&c);
                return Ok(());
            }
            Some(run_event::Detail::Failed(f)) => {
                anyhow::bail!("Run failed: {} — {}", f.error_code, f.error_message,);
            }
            None => {
                // Check for terminal states without detail
                let state = RunState::try_from(event.state).ok();
                match state {
                    Some(RunState::Cancelled) => {
                        anyhow::bail!("Run was cancelled");
                    }
                    Some(RunState::Completed) => {
                        return Ok(());
                    }
                    Some(RunState::Failed) => {
                        anyhow::bail!("Run failed (no error details available)");
                    }
                    _ => {}
                }
            }
        }
    }

    ensure_terminal_event_received(false)
}

fn state_label(state: i32) -> &'static str {
    match RunState::try_from(state) {
        Ok(RunState::Pending) => "PENDING",
        Ok(RunState::Running) => "RUNNING",
        Ok(RunState::Completed) => "COMPLETED",
        Ok(RunState::Failed) => "FAILED",
        Ok(RunState::Cancelled) => "CANCELLED",
        _ => "UNKNOWN",
    }
}

fn ensure_terminal_event_received(seen_terminal: bool) -> Result<()> {
    if seen_terminal {
        Ok(())
    } else {
        anyhow::bail!("WatchRun stream ended before a terminal event was received");
    }
}

fn emit_bench_json_from_completion(
    completed: &rapidbyte_controller::proto::rapidbyte::v1::CompletionDetail,
) {
    if std::env::var_os("RAPIDBYTE_BENCH").is_none() {
        return;
    }

    println!("@@BENCH_JSON@@{}", bench_json_from_completion(completed));
}

fn bench_json_from_completion(
    completed: &rapidbyte_controller::proto::rapidbyte::v1::CompletionDetail,
) -> serde_json::Value {
    let m = completed.metrics.as_ref();
    let rows_read = m.map_or(0, |m| m.rows_read);
    let rows_written = m.map_or(0, |m| m.rows_written);
    let bytes_read = m.map_or(0, |m| m.bytes_read);
    let bytes_written = m.map_or(0, |m| m.bytes_written);
    let duration_ms = m.map_or(0, |m| m.duration_ms);
    #[allow(clippy::cast_precision_loss)]
    let duration_secs = duration_ms as f64 / 1000.0;

    serde_json::json!({
        "records_read": rows_read,
        "records_written": rows_written,
        "bytes_read": bytes_read,
        "bytes_written": bytes_written,
        "duration_secs": duration_secs,
        "source_duration_secs": duration_secs,
        "dest_duration_secs": duration_secs,
        "dest_recv_count": 1,
        "parallelism": 1,
        "retry_count": 0,
        "stream_metrics": [{
            "stream_name": "distributed.aggregate",
            "partition_index": 0,
            "partition_count": 1,
            "records_read": rows_read,
            "records_written": rows_written,
            "bytes_read": bytes_read,
            "bytes_written": bytes_written,
            "source_duration_secs": duration_secs,
            "dest_duration_secs": duration_secs,
            "dest_frame_receive_secs": duration_secs
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::transport::build_endpoint;
    use rapidbyte_engine::{
        DestTiming, PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric,
    };
    use tonic::metadata::MetadataValue;

    // ── Local execution tests ─────────────────────────────────────────

    #[test]
    fn bench_json_uses_effective_parallelism() {
        let result = PipelineResult {
            counts: PipelineCounts {
                records_read: 10,
                records_written: 10,
                bytes_read: 100,
                bytes_written: 100,
            },
            source: SourceTiming::default(),
            dest: DestTiming::default(),
            num_transforms: 0,
            total_transform_secs: 0.0,
            transform_load_times_ms: vec![],
            duration_secs: 1.0,
            wasm_overhead_secs: 0.0,
            retry_count: 0,
            parallelism: 4,
            stream_metrics: vec![StreamShardMetric {
                stream_name: "bench_events".to_string(),
                partition_index: Some(1),
                partition_count: Some(4),
                records_read: 3,
                records_written: 3,
                bytes_read: 30,
                bytes_written: 30,
                source_duration_secs: 0.4,
                dest_duration_secs: 0.6,
                dest_wasm_instantiation_secs: 0.1,
                dest_frame_receive_secs: 0.2,
            }],
        };

        let cpu = ProcessCpuMetrics {
            cpu_secs: 1.2,
            cpu_pct_one_core: 120.0,
            cpu_pct_of_available_cores: 30.0,
            available_cores: 4,
        };
        let json = bench_json_from_result(&result, Some(&cpu), Some(64.0));
        assert!(json["stream_metrics"].is_array());
        assert_eq!(json["stream_metrics"][0]["partition_index"], 1);
        assert_eq!(json["stream_metrics"][0]["partition_count"], 4);
        assert_eq!(json["stream_metrics"][0]["records_read"], 3);
        assert_eq!(
            json["stream_metrics"][0]["dest_wasm_instantiation_secs"],
            0.1
        );
        assert_eq!(json["stream_metrics"][0]["dest_frame_receive_secs"], 0.2);
        assert_eq!(json["parallelism"], 4);
        assert_eq!(json["process_cpu_secs"], 1.2);
        assert_eq!(json["available_cores"], 4);
        assert_eq!(json["process_cpu_pct_available_cores"], 30.0);
        assert_eq!(json["process_peak_rss_mb"], 64.0);
    }

    // ── Remote execution tests ────────────────────────────────────────

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer(
            WatchRunRequest {
                run_id: "run-1".into(),
            },
            Some("secret"),
        )
        .unwrap();

        assert_eq!(
            request.metadata().get("authorization"),
            Some(&MetadataValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn request_with_bearer_is_noop_without_token() {
        let request = request_with_bearer(
            WatchRunRequest {
                run_id: "run-1".into(),
            },
            None,
        )
        .unwrap();
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn watch_run_eof_before_terminal_is_error() {
        let err = ensure_terminal_event_received(false).unwrap_err();
        assert!(err
            .to_string()
            .contains("ended before a terminal event was received"));
    }

    #[test]
    fn distributed_run_builds_tls_channel_when_configured() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, b"ca-pem").unwrap();

        let endpoint = build_endpoint(
            "https://controller.example:9090",
            Some(&TlsClientConfig {
                ca_cert_path: Some(ca_path),
                domain_name: Some("controller.example".into()),
            }),
        )
        .unwrap();

        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn emit_bench_json_from_completion_includes_required_fields() {
        use rapidbyte_controller::proto::rapidbyte::v1::{CompletionDetail, RunMetrics};
        let completed = CompletionDetail {
            metrics: Some(RunMetrics {
                rows_read: 123,
                rows_written: 123,
                bytes_read: 456,
                bytes_written: 456,
                duration_ms: 7500,
            }),
        };

        let json = bench_json_from_completion(&completed);

        assert_eq!(json["records_read"], 123);
        assert_eq!(json["records_written"], 123);
        assert_eq!(json["bytes_written"], 456);
        assert_eq!(json["duration_secs"], 7.5);
        assert_eq!(json["stream_metrics"][0]["records_written"], 123);
        assert_eq!(json["stream_metrics"][0]["bytes_written"], 456);
    }
}
