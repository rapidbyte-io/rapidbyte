use rapidbyte_sdk::errors::ConnectorError;
use rapidbyte_sdk::protocol::{ConfigBlob, OpenContext};
use serde::Deserialize;

/// PostgreSQL connection config from pipeline YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    pub fn from_open_context(ctx: &OpenContext) -> Result<Self, ConnectorError> {
        let value = match &ctx.config {
            ConfigBlob::Json(v) => v.clone(),
        };
        serde_json::from_value(value).map_err(|e| {
            ConnectorError::config("INVALID_CONFIG", format!("Config parse error: {}", e))
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

/// Create a tokio runtime suitable for the Wasm environment.
pub fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Failed to create tokio runtime")
}
