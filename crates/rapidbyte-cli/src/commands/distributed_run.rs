//! Distributed pipeline execution via controller v2 API.

use std::path::Path;

use anyhow::{Context, Result};

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::{
    run_event, ExecutionOptions, GetRunRequest, StreamRunRequest, SubmitRunRequest,
};

pub async fn execute(
    controller_url: &str,
    pipeline_path: &Path,
    dry_run: bool,
    limit: Option<u64>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let pipeline_yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;
    let effective_dry_run = dry_run || limit.is_some();

    if verbosity == Verbosity::Diagnostic {
        eprintln!("Loaded pipeline YAML ({} bytes)", pipeline_yaml.len());
    }
    if (dry_run || limit.is_some()) && verbosity != Verbosity::Quiet {
        eprintln!("Forwarding --dry-run/--limit execution options to controller.");
    }

    let channel = connect_channel(controller_url, tls)
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = ControlPlaneClient::new(channel);

    let submit = client
        .submit_run(request_with_bearer(
            SubmitRunRequest {
                idempotency_key: Some(uuid::Uuid::new_v4().to_string()),
                pipeline_yaml_utf8: pipeline_yaml,
                execution: Some(ExecutionOptions {
                    dry_run: effective_dry_run,
                    limit,
                }),
            },
            auth_token,
        )?)
        .await?
        .into_inner();
    let run_id = submit.run_id;

    if verbosity != Verbosity::Quiet {
        eprintln!("Submitted run: {run_id}");
    }

    let mut stream = client
        .stream_run(request_with_bearer(
            StreamRunRequest {
                run_id: run_id.clone(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    let mut saw_event = false;
    let mut bench_metrics: Option<DistributedBenchMetrics> = None;
    while let Some(event) = stream.message().await? {
        saw_event = true;

        if let Some(run_event) = event.event {
            match run_event {
                run_event::Event::Completed(completed) => {
                    bench_metrics = Some(DistributedBenchMetrics {
                        total_records: completed.total_records,
                        total_bytes: completed.total_bytes,
                        elapsed_seconds: completed.elapsed_seconds,
                    });
                }
                run_event::Event::Failed(failed) => {
                    let message = failed
                        .error
                        .map_or_else(|| "run failed".to_owned(), |error| error.message);
                    anyhow::bail!("distributed run failed: {message}");
                }
                run_event::Event::Cancelled(_) => {
                    anyhow::bail!("distributed run was cancelled");
                }
                _ => {}
            }
        }

        if verbosity != Verbosity::Quiet {
            if event.detail.is_empty() {
                eprintln!("Event: <empty>");
            } else {
                eprintln!("Event: {}", event.detail);
            }
        }
    }

    if !saw_event {
        let run = client
            .get_run(request_with_bearer(
                GetRunRequest {
                    run_id: run_id.clone(),
                },
                auth_token,
            )?)
            .await?
            .into_inner();
        if verbosity != Verbosity::Quiet {
            eprintln!("State: {}", normalize_state(&run.state));
        }

        if normalize_state(&run.state) == "COMPLETED" && bench_metrics.is_none() {
            anyhow::bail!("distributed run reached COMPLETED without terminal metrics event");
        }
    }

    if std::env::var_os("RAPIDBYTE_BENCH").is_some() {
        let metrics = bench_metrics.ok_or_else(|| {
            anyhow::anyhow!(
                "distributed run did not emit terminal metrics required for benchmark output"
            )
        })?;
        emit_bench_json(&metrics);
    }

    Ok(())
}

fn normalize_state(state: &str) -> String {
    if state.trim().is_empty() {
        return "UNKNOWN".to_owned();
    }
    state.trim().to_ascii_uppercase()
}

struct DistributedBenchMetrics {
    total_records: u64,
    total_bytes: u64,
    elapsed_seconds: f64,
}

fn emit_bench_json(metrics: &DistributedBenchMetrics) {
    println!(
        "@@BENCH_JSON@@{}",
        serde_json::json!({
            "records_read": metrics.total_records,
            "records_written": metrics.total_records,
            "bytes_read": metrics.total_bytes,
            "bytes_written": metrics.total_bytes,
            "duration_secs": metrics.elapsed_seconds,
            "source_duration_secs": metrics.elapsed_seconds,
            "dest_duration_secs": metrics.elapsed_seconds,
            "dest_recv_count": 1,
            "parallelism": 1,
            "retry_count": 0,
            "stream_metrics": []
        })
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::transport::build_endpoint;
    use tempfile::tempdir;
    use tonic::metadata::MetadataValue;

    #[test]
    fn normalize_state_handles_empty_values() {
        assert_eq!(normalize_state(""), "UNKNOWN");
        assert_eq!(normalize_state(" accepted "), "ACCEPTED");
    }

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer(
            StreamRunRequest {
                run_id: "run-1".to_owned(),
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
    fn distributed_run_builds_tls_channel_when_configured() {
        let dir = tempdir().unwrap();
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
}
