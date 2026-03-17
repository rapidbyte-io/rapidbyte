//! Distributed run listing command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::ListRunsRequest;

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
    let mut client = ControlPlaneClient::new(channel);

    let request_limit = if limit < 0 { 0 } else { limit };
    let resp = client
        .list_runs(request_with_bearer(
            ListRunsRequest {
                limit: Some(request_limit),
                state: state.map(ToOwned::to_owned),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    if verbosity != Verbosity::Quiet {
        for run in resp.runs {
            let normalized = normalize_state(&run.state);
            eprintln!("{}  {}", run.run_id, normalized);
        }
    }

    Ok(())
}

fn normalize_state(value: &str) -> String {
    if value.trim().is_empty() {
        return "UNKNOWN".to_owned();
    }
    value.trim().to_ascii_uppercase()
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
    fn normalize_state_handles_empty_and_whitespace() {
        assert_eq!(normalize_state(""), "UNKNOWN");
        assert_eq!(normalize_state(" accepted "), "ACCEPTED");
    }
}
