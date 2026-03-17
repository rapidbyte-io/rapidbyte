//! Distributed run status command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::GetRunRequest;

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
    let mut client = ControlPlaneClient::new(channel);
    let resp = client
        .get_run(request_with_bearer(
            GetRunRequest {
                run_id: run_id.to_owned(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    if verbosity != Verbosity::Quiet {
        eprintln!("Run: {}", resp.run_id);
        eprintln!("State: {}", state_label(&resp.state));
    }

    Ok(())
}

fn state_label(state: &str) -> String {
    if state.trim().is_empty() {
        return "UNKNOWN".to_owned();
    }
    state.trim().to_ascii_uppercase()
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

    #[test]
    fn state_label_handles_empty_and_normal_values() {
        assert_eq!(state_label(""), "UNKNOWN");
        assert_eq!(state_label("Accepted"), "ACCEPTED");
    }
}
