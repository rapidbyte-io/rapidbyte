//! Distributed pipeline execution via controller v2 API.

use std::path::Path;

use anyhow::{Context, Result};

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::{
    GetRunRequest, StreamRunRequest, SubmitRunRequest,
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
    let _yaml = std::fs::read(pipeline_path)
        .with_context(|| format!("Failed to read pipeline: {}", pipeline_path.display()))?;
    let _effective_dry_run = dry_run || limit.is_some();

    let channel = connect_channel(controller_url, tls)
        .await
        .with_context(|| format!("Failed to connect to controller at {controller_url}"))?;

    let mut client = ControlPlaneClient::new(channel);

    let submit = client
        .submit_run(request_with_bearer(
            SubmitRunRequest {
                idempotency_key: Some(uuid::Uuid::new_v4().to_string()),
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
    while let Some(event) = stream.message().await? {
        saw_event = true;
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
    }

    emit_bench_json_stub();
    Ok(())
}

fn normalize_state(state: &str) -> String {
    if state.trim().is_empty() {
        return "UNKNOWN".to_owned();
    }
    state.trim().to_ascii_uppercase()
}

fn emit_bench_json_stub() {
    if std::env::var_os("RAPIDBYTE_BENCH").is_none() {
        return;
    }

    println!(
        "@@BENCH_JSON@@{}",
        serde_json::json!({
            "records_read": 0,
            "records_written": 0,
            "bytes_read": 0,
            "bytes_written": 0,
            "duration_secs": 0.0,
            "source_duration_secs": 0.0,
            "dest_duration_secs": 0.0,
            "dest_recv_count": 0,
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
