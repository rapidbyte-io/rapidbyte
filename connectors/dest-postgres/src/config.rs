//! Destination PostgreSQL connector configuration.

use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoadMethod {
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
    #[serde(default)]
    pub load_method: LoadMethod,
    /// COPY flush threshold in bytes. Defaults to connector constant (4 MiB).
    #[serde(default)]
    pub copy_flush_bytes: Option<usize>,
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
}

impl Config {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

impl Default for LoadMethod {
    fn default() -> Self {
        Self::Insert
    }
}
