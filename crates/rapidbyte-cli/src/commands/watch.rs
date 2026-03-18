//! Distributed run watch command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{run_event, RunState, WatchRunRequest};

pub async fn execute(
    controller_url: Option<&str>,
    run_id: &str,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "watch requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);
    let mut stream = client
        .watch_run(request_with_bearer(
            WatchRunRequest {
                run_id: run_id.to_string(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        if verbosity != Verbosity::Quiet {
            let state = state_label(event.state);
            if event.attempt > 0 {
                eprintln!("State: {state} (attempt {})", event.attempt);
            }
        }

        match event.detail {
            Some(run_event::Detail::Progress(progress)) => {
                if verbosity != Verbosity::Quiet {
                    let pct = progress
                        .progress_pct
                        .map(|v| format!(" ({v:.0}%)"))
                        .unwrap_or_default();
                    eprintln!("  {}{pct}", progress.message);
                }
            }
            Some(run_event::Detail::Completed(done)) => {
                if verbosity != Verbosity::Quiet {
                    if let Some(m) = &done.metrics {
                        eprintln!(
                            "Completed: {} rows read, {} rows written, {} bytes in {}ms",
                            m.rows_read, m.rows_written, m.bytes_written, m.duration_ms,
                        );
                    } else {
                        eprintln!("Completed");
                    }
                }
                return Ok(());
            }
            Some(run_event::Detail::Failed(failed)) => {
                anyhow::bail!(
                    "Run failed: {} — {}",
                    failed.error_code,
                    failed.error_message,
                );
            }
            None => {
                // Check for terminal states without detail
                let state = RunState::try_from(event.state).ok();
                match state {
                    Some(RunState::Cancelled) => {
                        anyhow::bail!("Run was cancelled");
                    }
                    Some(RunState::Completed) => {
                        if verbosity != Verbosity::Quiet {
                            eprintln!("Completed");
                        }
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

    anyhow::bail!("WatchRun stream ended before a terminal event was received");
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
    async fn watch_requires_controller() {
        let err = execute(None, "run-1", Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("watch requires --controller"));
    }
}
