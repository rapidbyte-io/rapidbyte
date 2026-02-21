use rapidbyte_sdk::errors::ConnectorError;
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
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.load_method != "insert" && self.load_method != "copy" {
            return Err(ConnectorError::config(
                "INVALID_CONFIG",
                format!(
                    "Invalid load_method: '{}'. Must be 'insert' or 'copy'",
                    self.load_method
                ),
            ));
        }
        Ok(())
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
