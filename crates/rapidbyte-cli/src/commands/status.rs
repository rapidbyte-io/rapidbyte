//! Distributed run status command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{GetRunRequest, RunState};

pub async fn execute(
    controller_url: Option<&str>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "status requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);
    let resp = client
        .get_run(request_with_bearer(
            GetRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    let run = resp
        .run
        .ok_or_else(|| anyhow::anyhow!("GetRun response missing run detail"))?;

    if verbosity != Verbosity::Quiet {
        eprintln!("Run: {}", run.run_id);
        eprintln!("Pipeline: {}", run.pipeline_name);
        eprintln!("State: {}", state_label(run.state));
        eprintln!(
            "Attempt: {} (max retries: {})",
            run.attempt, run.max_retries
        );
        if run.cancel_requested {
            eprintln!("Cancel requested: yes");
        }
        if let Some(error) = run.error {
            eprintln!("Error: {} — {}", error.code, error.message);
        }
        if let Some(metrics) = run.metrics {
            eprintln!(
                "Metrics: {} rows read, {} rows written, {} bytes in {}ms",
                metrics.rows_read, metrics.rows_written, metrics.bytes_written, metrics.duration_ms,
            );
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn status_requires_controller() {
        let err = execute(None, "run-1", Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("status requires --controller"));
    }
}
