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
    /// Publication name for CDC mode. Defaults to `rapidbyte_{stream_name}`.
    #[serde(default)]
    pub publication_name: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
