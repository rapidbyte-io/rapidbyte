//! Agent worker subcommand.

use anyhow::Result;

pub async fn execute(
    controller: &str,
    flight_listen: &str,
    flight_advertise: &str,
    max_tasks: u32,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
) -> Result<()> {
    let mut config = rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        flight_listen: flight_listen.into(),
        flight_advertise: flight_advertise.into(),
        max_tasks,
        ..Default::default()
    };
    if let Some(key) = signing_key {
        config.signing_key = key.as_bytes().to_vec();
    }
    config.auth_token = auth_token.map(str::to_owned);
    rapidbyte_agent::run(config).await
}
