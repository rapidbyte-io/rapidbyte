use serde::{Deserialize, Serialize};

/// TLS requirement for connector network access.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum TlsRequirement {
    Required,
    #[default]
    Optional,
    Forbidden,
}

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

    /// TLS requirement for outbound connections.
    pub tls: TlsRequirement,
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
