//! Source `PostgreSQL` plugin configuration.

use rapidbyte_sdk::error::PluginError;
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
    /// Publication name for CDC mode (pgoutput). If not set, defaults to rapidbyte_{`stream_name`}.
    #[serde(default)]
    pub publication: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    /// Returns the configured replication slot name, if any.
    #[must_use]
    pub(crate) fn configured_replication_slot(&self) -> Option<&str> {
        self.replication_slot.as_deref()
    }

    /// Returns the configured publication name, if any.
    #[must_use]
    pub(crate) fn configured_publication(&self) -> Option<&str> {
        self.publication.as_deref()
    }

    /// # Errors
    /// Returns `Err` if `replication_slot` or `publication` is empty or exceeds the
    /// 63-byte `PostgreSQL` identifier limit.
    pub fn validate(&self) -> Result<(), PluginError> {
        if let Some(slot) = self.replication_slot.as_ref() {
            if slot.is_empty() {
                return Err(PluginError::config(
                    "INVALID_CONFIG",
                    "replication_slot must not be empty".to_string(),
                ));
            }
            if slot.len() > 63 {
                return Err(PluginError::config(
                    "INVALID_CONFIG",
                    format!("replication_slot '{slot}' exceeds PostgreSQL 63-byte limit"),
                ));
            }
        }
        if let Some(pub_name) = self.publication.as_ref() {
            if pub_name.is_empty() {
                return Err(PluginError::config(
                    "INVALID_CONFIG",
                    "publication must not be empty".to_string(),
                ));
            }
            if pub_name.len() > 63 {
                return Err(PluginError::config(
                    "INVALID_CONFIG",
                    format!("publication '{pub_name}' exceeds PostgreSQL 63-byte limit"),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a minimal valid `Config` for tests.
    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test".to_string(),
            replication_slot: None,
            publication: None,
        }
    }

    #[test]
    fn validate_accepts_publication_name() {
        let cfg = Config {
            publication: Some("my_pub".to_string()),
            ..base_config()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_publication() {
        let cfg = Config {
            publication: Some(String::new()),
            ..base_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("publication must not be empty"));
    }

    #[test]
    fn validate_rejects_long_publication() {
        let cfg = Config {
            publication: Some("a".repeat(64)),
            ..base_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("exceeds PostgreSQL 63-byte limit"));
    }
}
