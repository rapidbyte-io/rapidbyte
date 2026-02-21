use serde::{Deserialize, Serialize};

/// Information about the WASM binary artifact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactInfo {
    /// WASM binary filename (e.g., "source_postgres.wasm").
    pub entry_point: String,
    /// Checksum of the WASM binary (e.g., "sha256:abcd1234...").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// Minimum recommended memory in MB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory_mb: Option<u32>,
}
