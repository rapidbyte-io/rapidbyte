//! Source `PostgreSQL` plugin configuration.

use rapidbyte_sdk::error::PluginError;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoverySettings {
    pub schema: Option<String>,
    pub publication: Option<String>,
}

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
    /// Schema to discover. Defaults to `public`.
    #[serde(default)]
    pub schema: Option<String>,
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
    /// # Errors
    /// Returns `Err` if `replication_slot` or `publication` is empty or exceeds the
    /// 63-byte `PostgreSQL` identifier limit.
    pub fn validate(&self) -> Result<(), PluginError> {
        if let Some(schema) = self.schema.as_ref() {
            validate_identifier("schema", schema)?;
        }
        if let Some(slot) = self.replication_slot.as_ref() {
            validate_replication_slot_name(slot)?;
        }
        if let Some(pub_name) = self.publication.as_ref() {
            validate_identifier("publication", pub_name)?;
        }
        Ok(())
    }
    
    #[must_use]
    pub(crate) fn discovery_settings(&self) -> DiscoverySettings {
        DiscoverySettings {
            schema: self.schema.clone(),
            publication: self.publication.clone(),
        }
    }
}

fn validate_identifier(field: &str, value: &str) -> Result<(), PluginError> {
    if value.is_empty() {
        return Err(PluginError::config(
            "INVALID_CONFIG",
            format!("{field} must not be empty"),
        ));
    }
    if value.len() > 63 {
        return Err(PluginError::config(
            "INVALID_CONFIG",
            format!("{field} '{value}' exceeds PostgreSQL 63-byte limit"),
        ));
    }
    Ok(())
}

fn validate_replication_slot_name(value: &str) -> Result<(), PluginError> {
    if value.is_empty() {
        return Err(PluginError::config(
            "INVALID_CONFIG",
            "replication_slot must not be empty",
        ));
    }
    if value.len() > 63 {
        return Err(PluginError::config(
            "INVALID_CONFIG",
            format!("replication_slot '{value}' exceeds PostgreSQL 63-byte limit"),
        ));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(PluginError::config(
            "INVALID_CONFIG",
            format!(
                "replication_slot '{value}' may only contain lowercase letters, digits, and underscores"
            ),
        ));
    }
    Ok(())
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
            schema: None,
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
    fn validate_accepts_schema_selection() {
        let cfg = Config {
            schema: Some("analytics".to_string()),
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

    #[test]
    fn validate_rejects_empty_slot() {
        let cfg = Config {
            replication_slot: Some(String::new()),
            ..base_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("replication_slot must not be empty"));
    }

    #[test]
    fn validate_rejects_invalid_slot_characters() {
        let cfg = Config {
            replication_slot: Some("slot-name".to_string()),
            ..base_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("may only contain lowercase letters, digits, and underscores"));
    }
}
