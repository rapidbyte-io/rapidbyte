//! Distributed pipeline execution via controller v2 API.

use std::path::Path;

use anyhow::{Context, Result};

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::{
    run_event, ExecutionOptions, GetRunRequest, StreamRunRequest, SubmitRunRequest,
};

#[allow(clippy::too_many_lines)]
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
    let mut saw_terminal_event = false;
    let mut bench_metrics: Option<DistributedBenchMetrics> = None;
    let mut terminal_failure: Option<String> = None;
    while let Some(event) = stream.message().await? {
        saw_event = true;

        if let Some(run_event) = event.event {
            match run_event {
                run_event::Event::Completed(completed) => {
                    saw_terminal_event = true;
                    bench_metrics = Some(DistributedBenchMetrics {
                        total_records: completed.total_records,
                        total_bytes: completed.total_bytes,
                        elapsed_seconds: completed.elapsed_seconds,
                    });
                }
                run_event::Event::Failed(failed) => {
                    saw_terminal_event = true;
                    terminal_failure = Some(failed.error.map_or_else(
                        || "distributed run failed: run failed".to_owned(),
                        |error| format!("distributed run failed: {}", error.message),
                    ));
                }
                run_event::Event::Cancelled(_) => {
                    saw_terminal_event = true;
                    terminal_failure = Some("distributed run was cancelled".to_owned());
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

        if terminal_failure.is_some() {
            break;
        }
    }

    if let Some(message) = terminal_failure {
        anyhow::bail!(message);
    }

    if !saw_terminal_event {
        let run = client
            .get_run(request_with_bearer(
                GetRunRequest {
                    run_id: run_id.clone(),
                },
                auth_token,
            )?)
            .await?
            .into_inner();
        let normalized_state = normalize_state(&run.state);
        if verbosity != Verbosity::Quiet {
            eprintln!("State: {normalized_state}");
        }

        match classify_terminal_state(&normalized_state) {
            RunTerminalState::Succeeded => {
                if bench_metrics.is_none() {
                    anyhow::bail!(
                        "distributed run reached terminal success state without terminal metrics event"
                    );
                }
            }
            RunTerminalState::Cancelled => {
                anyhow::bail!("distributed run was cancelled");
            }
            RunTerminalState::Failed => {
                anyhow::bail!("distributed run failed (state: {normalized_state})");
            }
            RunTerminalState::NonTerminal => {
                let event_hint = if saw_event {
                    "after receiving non-terminal stream events"
                } else {
                    "without receiving any stream events"
                };
                anyhow::bail!(
                    "distributed run stream ended {event_hint}; latest state is {normalized_state}"
                );
            }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunTerminalState {
    Succeeded,
    Failed,
    Cancelled,
    NonTerminal,
}

fn classify_terminal_state(state: &str) -> RunTerminalState {
    match state {
        "COMPLETED" | "SUCCEEDED" => RunTerminalState::Succeeded,
        "CANCELLED" => RunTerminalState::Cancelled,
        "FAILED" | "RECOVERYFAILED" | "RECOVERY_FAILED" | "TIMEDOUT" | "TIMED_OUT" => {
            RunTerminalState::Failed
        }
        _ => RunTerminalState::NonTerminal,
    }
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
    fn classify_terminal_state_maps_controller_states() {
        assert_eq!(
            classify_terminal_state("COMPLETED"),
            RunTerminalState::Succeeded
        );
        assert_eq!(
            classify_terminal_state("SUCCEEDED"),
            RunTerminalState::Succeeded
        );
        assert_eq!(classify_terminal_state("FAILED"), RunTerminalState::Failed);
        assert_eq!(
            classify_terminal_state("TIMED_OUT"),
            RunTerminalState::Failed
        );
        assert_eq!(
            classify_terminal_state("CANCELLED"),
            RunTerminalState::Cancelled
        );
        assert_eq!(
            classify_terminal_state("RUNNING"),
            RunTerminalState::NonTerminal
        );
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
