//! Distributed run watch command.

use anyhow::Result;

use crate::commands::transport::{connect_channel, request_with_bearer, TlsClientConfig};
use crate::Verbosity;

use rapidbyte_controller::proto::rapidbyte::v2::control_plane_client::ControlPlaneClient;
use rapidbyte_controller::proto::rapidbyte::v2::{run_event, StreamRunRequest};

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
    let mut client = ControlPlaneClient::new(channel);
    let mut stream = client
        .stream_run(request_with_bearer(
            StreamRunRequest {
                run_id: run_id.to_owned(),
            },
            auth_token,
        )?)
        .await?
        .into_inner();

    let mut saw_event = false;
    let mut saw_terminal_event = false;
    while let Some(event) = stream.message().await? {
        saw_event = true;

        if let Some(run_event) = event.event {
            match run_event {
                run_event::Event::Completed(_) => {
                    saw_terminal_event = true;
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
            if event.detail.trim().is_empty() {
                eprintln!("Event: <empty>");
            } else {
                eprintln!("Event: {}", event.detail);
            }
        }

        if saw_terminal_event {
            break;
        }
    }

    if !saw_event {
        anyhow::bail!("StreamRun ended before yielding any events");
    }
    if !saw_terminal_event {
        anyhow::bail!("StreamRun ended before yielding a terminal event");
    }

    Ok(())
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
