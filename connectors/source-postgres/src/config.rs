//! Source PostgreSQL connector configuration.

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
    /// Logical replication slot name for CDC mode. Defaults to `rapidbyte_{stream_name}`.
    #[serde(default)]
    pub replication_slot: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl Config {
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
                        "replication_slot '{}' exceeds PostgreSQL 63-byte limit",
                        slot
                    ),
                ));
            }
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
