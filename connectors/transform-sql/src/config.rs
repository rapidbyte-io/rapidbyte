//! SQL transform configuration.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Configuration for the SQL transform connector.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// SQL query to execute against each incoming batch.
    /// Must reference `input` as the table name.
    pub query: String,
}
