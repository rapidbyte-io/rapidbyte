//! Distributed pipeline execution via controller.

use std::path::Path;

use anyhow::{Context, Result};

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    run_event, ExecutionOptions, RunState, SubmitPipelineRequest, WatchRunRequest,
};

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    _dry_run: bool,
    _limit: Option<u64>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
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
            None => {}
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
    use tonic::metadata::MetadataValue;

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
