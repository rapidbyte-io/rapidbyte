//! Source `PostgreSQL` connector configuration.

use rapidbyte_sdk::error::ConnectorError;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// `PostgreSQL` connection config from pipeline YAML.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Database hostname
    pub host: String,
    /// Database port
    #[serde(default = "default_port")]
    #[schema(default = 5432)]
    pub port: u16,
    /// Database user
    pub user: String,
    /// Database password
    #[serde(default)]
    #[schema(secret)]
    pub password: String,
    /// Database name
    pub database: String,
    /// Logical replication slot name for CDC mode. Defaults to rapidbyte_{`stream_name`}.
    #[serde(default)]
    pub replication_slot: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    /// # Errors
    /// Returns `Err` if `replication_slot` is empty or exceeds the 63-byte `PostgreSQL` limit.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Some(slot) = self.replication_slot.as_ref() {
            if slot.is_empty() {
                return Err(ConnectorError::config(
                    "INVALID_CONFIG",
                    "replication_slot must not be empty".to_string(),
                ));
            }
            if slot.len() > 63 {
                return Err(ConnectorError::config(
                    "INVALID_CONFIG",
                    format!(
                        "replication_slot '{slot}' exceeds PostgreSQL 63-byte limit"
                    ),
                ));
            }
        }
        Ok(())
    }

    #[must_use] 
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
