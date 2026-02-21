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
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_load_method")]
    pub load_method: String,
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_load_method() -> String {
    "insert".to_string()
}

impl Config {
    pub fn from_open_context(ctx: &OpenContext) -> Result<Self, ConnectorError> {
        let value = match &ctx.config {
            ConfigBlob::Json(v) => v.clone(),
        };
        let config: Self = serde_json::from_value(value).map_err(|e| {
            ConnectorError::config("INVALID_CONFIG", format!("Config parse error: {}", e))
        })?;
        // Validate load_method
        if config.load_method != "insert" && config.load_method != "copy" {
            return Err(ConnectorError::config(
                "INVALID_CONFIG",
                format!(
                    "Invalid load_method: '{}'. Must be 'insert' or 'copy'",
                    config.load_method
                ),
            ));
        }
        Ok(config)
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
