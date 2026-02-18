use serde::{Deserialize, Serialize};

use crate::protocol::{ConnectorRole, Feature, WriteMode};

/// TLS requirement for connector network access.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TlsRequirement {
    Required,
    Optional,
    Forbidden,
}

/// Network capabilities declared by a connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkCapabilities {
    /// Static list of allowed hostnames. None = determined at runtime from config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_hosts: Option<Vec<String>>,
    /// TLS requirement for outbound connections.
    #[serde(default = "default_tls")]
    pub tls: TlsRequirement,
    /// Whether hosts are resolved from pipeline config at runtime.
    #[serde(default)]
    pub runtime_resolved_hosts: bool,
}

fn default_tls() -> TlsRequirement {
    TlsRequirement::Optional
}

impl Default for NetworkCapabilities {
    fn default() -> Self {
        Self {
            allowed_hosts: None,
            tls: TlsRequirement::Optional,
            runtime_resolved_hosts: false,
        }
    }
}

/// Connector manifest — declares identity, roles, capabilities, and security requirements.
///
/// Stored as `<connector_name>.manifest.json` alongside the `.wasm` binary
/// in the connector plugins directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorManifest {
    /// Connector identifier (e.g., "source-postgres").
    pub id: String,
    /// Semantic version (e.g., "0.1.0").
    pub version: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Protocol version this connector implements (e.g., "1").
    pub protocol_version: String,
    /// Roles this connector supports.
    pub roles: Vec<ConnectorRole>,
    /// Features this connector advertises.
    #[serde(default)]
    pub features: Vec<Feature>,
    /// Network capability declarations for security policy.
    #[serde(default)]
    pub network: NetworkCapabilities,
    /// WASM binary filename (e.g., "source_postgres.wasm").
    pub entry_point: String,
    /// Minimum recommended memory in MB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory_mb: Option<u32>,
    /// Recommended max batch size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_batch_bytes: Option<u64>,
    /// Write modes supported (destination/transform only).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_modes: Vec<WriteMode>,
    /// SHA-256 checksum of the WASM binary (hex-encoded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

impl ConnectorManifest {
    /// Check if this connector supports a given role.
    pub fn supports_role(&self, role: ConnectorRole) -> bool {
        self.roles.contains(&role)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_manifest_roundtrip() {
        let manifest = ConnectorManifest {
            id: "source-postgres".to_string(),
            version: "0.1.0".to_string(),
            description: "PostgreSQL source connector".to_string(),
            protocol_version: "1".to_string(),
            roles: vec![ConnectorRole::Source],
            features: vec![Feature::BulkLoadCopy],
            network: NetworkCapabilities::default(),
            entry_point: "source_postgres.wasm".to_string(),
            min_memory_mb: None,
            recommended_batch_bytes: None,
            write_modes: vec![],
            checksum: None,
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, back);
        assert!(json.contains("\"source-postgres\""));
        assert!(json.contains("\"source\""));
    }

    #[test]
    fn test_dest_manifest_with_write_modes() {
        let manifest = ConnectorManifest {
            id: "dest-postgres".to_string(),
            version: "0.1.0".to_string(),
            description: "PostgreSQL destination connector".to_string(),
            protocol_version: "1".to_string(),
            roles: vec![ConnectorRole::Destination],
            features: vec![Feature::BulkLoadCopy, Feature::BulkLoadCopyBinary],
            network: NetworkCapabilities::default(),
            entry_point: "dest_postgres.wasm".to_string(),
            min_memory_mb: Some(128),
            recommended_batch_bytes: Some(64 * 1024 * 1024),
            write_modes: vec![WriteMode::Append, WriteMode::Replace],
            checksum: None,
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, back);
    }

    #[test]
    fn test_transform_manifest_minimal() {
        let manifest = ConnectorManifest {
            id: "transform-mask".to_string(),
            version: "0.1.0".to_string(),
            description: "Masks PII columns".to_string(),
            protocol_version: "1".to_string(),
            roles: vec![ConnectorRole::Transform],
            features: vec![],
            network: NetworkCapabilities {
                tls: TlsRequirement::Forbidden,
                ..Default::default()
            },
            entry_point: "transform_mask.wasm".to_string(),
            min_memory_mb: None,
            recommended_batch_bytes: None,
            write_modes: vec![],
            checksum: None,
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, back);
        assert_eq!(manifest.network.tls, TlsRequirement::Forbidden);
    }

    #[test]
    fn test_network_capabilities_defaults() {
        let caps = NetworkCapabilities::default();
        assert!(caps.allowed_hosts.is_none());
        assert_eq!(caps.tls, TlsRequirement::Optional);
        assert!(!caps.runtime_resolved_hosts);
    }

    #[test]
    fn test_manifest_roles_validation() {
        let manifest = ConnectorManifest {
            id: "source-postgres".to_string(),
            version: "0.1.0".to_string(),
            description: "".to_string(),
            protocol_version: "1".to_string(),
            roles: vec![ConnectorRole::Source],
            features: vec![],
            network: NetworkCapabilities::default(),
            entry_point: "source_postgres.wasm".to_string(),
            min_memory_mb: None,
            recommended_batch_bytes: None,
            write_modes: vec![],
            checksum: None,
        };
        assert!(manifest.supports_role(ConnectorRole::Source));
        assert!(!manifest.supports_role(ConnectorRole::Destination));
        assert!(!manifest.supports_role(ConnectorRole::Transform));
    }

    #[test]
    fn test_deserialize_from_json_file_format() {
        let json = r#"{
            "id": "source-postgres",
            "version": "0.1.0",
            "description": "PostgreSQL source connector",
            "protocol_version": "1",
            "roles": ["source"],
            "features": ["bulk_load_copy"],
            "network": {
                "tls": "optional",
                "runtime_resolved_hosts": true
            },
            "entry_point": "source_postgres.wasm"
        }"#;
        let manifest: ConnectorManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.id, "source-postgres");
        assert_eq!(manifest.roles, vec![ConnectorRole::Source]);
        assert!(manifest.network.runtime_resolved_hosts);
        assert!(manifest.write_modes.is_empty());
        assert!(manifest.checksum.is_none());
    }
}
