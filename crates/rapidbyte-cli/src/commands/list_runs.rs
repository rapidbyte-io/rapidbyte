//! Distributed run listing command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{ListRunsRequest, RunState};

pub async fn execute(
    controller_url: Option<&str>,
    limit: i32,
    state: Option<&str>,
    verbosity: Verbosity,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
) -> Result<()> {
    let controller_url = controller_url.ok_or_else(|| {
        anyhow::anyhow!(
            "list-runs requires --controller, RAPIDBYTE_CONTROLLER, or ~/.rapidbyte/config.yaml"
        )
    })?;

    let channel = connect_channel(controller_url, tls).await?;
    let mut client = PipelineServiceClient::new(channel);

    let filter_state = state.map(parse_state_filter).transpose()?;
    let resp = client
        .list_runs(request_with_bearer(
            ListRunsRequest {
                state_filter: filter_state,
                page_size: u32::try_from(limit).unwrap_or(u32::MAX),
                page_token: String::new(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    if verbosity != Verbosity::Quiet {
        for run in resp.runs {
            eprintln!(
                "{}  {}  {}  attempt {}",
                run.run_id,
                state_label(run.state),
                run.pipeline_name,
                run.attempt,
            );
        }
        if !resp.next_page_token.is_empty() {
            eprintln!("(more results available)");
        }
    }

    Ok(())
}

fn parse_state_filter(value: &str) -> Result<i32> {
    let state = match value {
        "pending" => RunState::Pending,
        "running" => RunState::Running,
        "completed" => RunState::Completed,
        "failed" => RunState::Failed,
        "cancelled" => RunState::Cancelled,
        _ => anyhow::bail!("invalid run state filter: {value}"),
    };
    Ok(state.into())
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
    async fn list_runs_requires_controller() {
        let err = execute(None, 20, None, Verbosity::Default, None, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("list-runs requires --controller"));
    }

    #[test]
    fn parse_state_filter_accepts_running() {
        assert_eq!(
            parse_state_filter("running").unwrap(),
            RunState::Running as i32
        );
    }

    #[test]
    fn parse_state_filter_rejects_removed_states() {
        assert!(parse_state_filter("assigned").is_err());
        assert!(parse_state_filter("reconciling").is_err());
        assert!(parse_state_filter("recovery_failed").is_err());
        assert!(parse_state_filter("preview_ready").is_err());
    }
}
