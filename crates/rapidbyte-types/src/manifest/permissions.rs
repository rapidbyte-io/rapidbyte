use serde::{Deserialize, Serialize};

/// Network permissions declared by a connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct NetworkPermissions {
    /// Static list of allowed domains or IPs. Use ["*"] to allow all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_domains: Option<Vec<String>>,

    /// If true, the host can inspect validated config for fields like
    /// "host" or "url" and dynamically allow network access at runtime.
    pub allow_runtime_config_domains: bool,
}

/// Filesystem permissions for the WASI sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct FsPermissions {
    /// Directories the connector needs mounted (read-only or read-write).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub preopens: Vec<String>,
}

/// Environment variable permissions for the WASI sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct EnvPermissions {
    /// Environment variables this connector is explicitly allowed to read.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_vars: Vec<String>,
}

/// Combined permissions controlling the WASI sandbox boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct Permissions {
    pub network: NetworkPermissions,
    pub fs: FsPermissions,
    pub env: EnvPermissions,
}

/// Resource limits for the WASI sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct ResourceLimits {
    /// Maximum WASI linear memory in human-readable format (e.g. "256mb").
    /// Enforced via Wasmtime StoreLimits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,

    /// Maximum execution time in seconds. Enforced via Wasmtime epoch interruption.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    /// Minimum recommended memory in human-readable format (e.g. "128mb").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_memory: Option<String>,
}
