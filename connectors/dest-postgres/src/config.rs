//! Destination `PostgreSQL` connector configuration.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoadMethod {
    #[default]
    Insert,
    Copy,
}

impl std::fmt::Display for LoadMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert => f.write_str("insert"),
            Self::Copy => f.write_str("copy"),
        }
    }
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
    /// Destination schema
    #[serde(default = "default_schema")]
    #[schema(default = "public")]
    pub schema: String,
    /// Load method for writing data
    #[serde(default)]
    #[schema(default = "insert", values("insert", "copy"))]
    pub load_method: LoadMethod,
    /// COPY flush threshold in bytes
    #[serde(default)]
    #[schema(advanced)]
    pub copy_flush_bytes: Option<usize>,
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
}
