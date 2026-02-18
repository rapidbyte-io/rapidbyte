use serde::{Deserialize, Serialize};

use crate::protocol::{ConnectorRole, SyncMode, WriteMode};

// ---------------------------------------------------------------------------
// Permissions & WASI Sandbox Boundaries
// ---------------------------------------------------------------------------

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
pub struct NetworkPermissions {
    /// Static list of allowed domains or IPs. Use ["*"] to allow all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_domains: Option<Vec<String>>,

    /// If true, the host can inspect validated config for fields like
    /// "host" or "url" and dynamically allow network access at runtime.
    #[serde(default)]
    pub allow_runtime_config_domains: bool,

    /// TLS requirement for outbound connections.
    #[serde(default = "default_tls")]
    pub tls: TlsRequirement,
}

fn default_tls() -> TlsRequirement {
    TlsRequirement::Optional
}

/// Filesystem permissions for the WASI sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct FsPermissions {
    /// Directories the connector needs mounted (read-only or read-write).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preopens: Vec<String>,
}

/// Environment variable permissions for the WASI sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct EnvPermissions {
    /// Environment variables this connector is explicitly allowed to read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_vars: Vec<String>,
}

/// Combined permissions controlling the WASI sandbox boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Permissions {
    #[serde(default)]
    pub network: NetworkPermissions,
    #[serde(default)]
    pub fs: FsPermissions,
    #[serde(default)]
    pub env: EnvPermissions,
}

// ---------------------------------------------------------------------------
// Roles & Capabilities
// ---------------------------------------------------------------------------

/// Source-specific features.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceFeature {
    Cdc,
    Stateful,
}

/// Capabilities declared when a connector supports the Source role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceCapabilities {
    pub supported_sync_modes: Vec<SyncMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<SourceFeature>,
}

/// Destination-specific features.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DestinationFeature {
    ExactlyOnce,
    SchemaAutoMigrate,
    BulkLoadCopy,
    BulkLoadCopyBinary,
}

/// Capabilities declared when a connector supports the Destination role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DestinationCapabilities {
    pub supported_write_modes: Vec<WriteMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<DestinationFeature>,
}

/// Capabilities declared when a connector supports the Transform role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TransformCapabilities {}

/// Capabilities declared when a connector supports the Utility role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct UtilityCapabilities {}

/// Role declarations — each `Some` variant means the connector supports that role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Roles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utility: Option<UtilityCapabilities>,
}

// ---------------------------------------------------------------------------
// Artifact Metadata
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Root Manifest
// ---------------------------------------------------------------------------

fn default_manifest_version() -> String {
    "1.0".to_string()
}

/// Connector manifest — declares identity, roles, capabilities, config schema,
/// and security requirements.
///
/// Stored as `<connector_name>.manifest.json` alongside the `.wasm` binary
/// in the connector plugins directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorManifest {
    /// Manifest format version (e.g., "1.0").
    #[serde(default = "default_manifest_version")]
    pub manifest_version: String,

    // -- Metadata --
    /// Connector identifier (e.g., "rapidbyte/dest-postgres").
    pub id: String,
    /// Human-readable display name.
    pub name: String,
    /// Semantic version (e.g., "0.1.0").
    pub version: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Author or organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// SPDX license identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,

    /// Protocol version this connector implements (e.g., "1").
    pub protocol_version: String,

    // -- Artifact --
    /// WASM binary artifact info.
    pub artifact: ArtifactInfo,

    // -- Security --
    /// Permissions controlling the WASI sandbox boundary.
    #[serde(default)]
    pub permissions: Permissions,

    // -- Roles --
    /// Role-specific capability declarations.
    pub roles: Roles,

    /// JSON Schema (Draft 7) defining the required config for this connector.
    /// Used by the host to validate config BEFORE starting the WASM guest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<serde_json::Value>,
}

impl ConnectorManifest {
    /// Check if this connector supports a given role.
    pub fn supports_role(&self, role: ConnectorRole) -> bool {
        match role {
            ConnectorRole::Source => self.roles.source.is_some(),
            ConnectorRole::Destination => self.roles.destination.is_some(),
            ConnectorRole::Transform => self.roles.transform.is_some(),
            ConnectorRole::Utility => self.roles.utility.is_some(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{SyncMode, WriteMode};

    #[test]
    fn test_dest_manifest_roundtrip() {
        let manifest = ConnectorManifest {
            manifest_version: "1.0".to_string(),
            id: "rapidbyte/dest-postgres".to_string(),
            name: "PostgreSQL Destination".to_string(),
            version: "0.1.0".to_string(),
            description: "Writes data to PostgreSQL using INSERT or COPY".to_string(),
            author: Some("Rapidbyte Inc.".to_string()),
            license: Some("Apache-2.0".to_string()),
            protocol_version: "1".to_string(),
            artifact: ArtifactInfo {
                entry_point: "dest_postgres.wasm".to_string(),
                checksum: None,
                min_memory_mb: Some(128),
            },
            permissions: Permissions::default(),
            roles: Roles {
                destination: Some(DestinationCapabilities {
                    supported_write_modes: vec![WriteMode::Append, WriteMode::Replace],
                    features: vec![DestinationFeature::BulkLoadCopy],
                }),
                ..Default::default()
            },
            config_schema: None,
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, back);
    }

    #[test]
    fn test_source_manifest_roundtrip() {
        let manifest = ConnectorManifest {
            manifest_version: "1.0".to_string(),
            id: "rapidbyte/source-postgres".to_string(),
            name: "PostgreSQL Source".to_string(),
            version: "0.1.0".to_string(),
            description: "Reads from PostgreSQL".to_string(),
            author: None,
            license: None,
            protocol_version: "1".to_string(),
            artifact: ArtifactInfo {
                entry_point: "source_postgres.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions {
                network: NetworkPermissions {
                    allowed_domains: None,
                    allow_runtime_config_domains: true,
                    tls: TlsRequirement::Optional,
                },
                env: EnvPermissions {
                    allowed_vars: vec!["PGSSLROOTCERT".to_string()],
                },
                fs: FsPermissions::default(),
            },
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
                    features: vec![],
                }),
                ..Default::default()
            },
            config_schema: None,
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest, back);
        assert!(manifest.supports_role(ConnectorRole::Source));
        assert!(!manifest.supports_role(ConnectorRole::Destination));
    }

    #[test]
    fn test_transform_manifest_no_network() {
        let manifest = ConnectorManifest {
            manifest_version: "1.0".to_string(),
            id: "rapidbyte/transform-mask".to_string(),
            name: "PII Mask".to_string(),
            version: "0.1.0".to_string(),
            description: "Masks PII columns".to_string(),
            author: None,
            license: None,
            protocol_version: "1".to_string(),
            artifact: ArtifactInfo {
                entry_point: "transform_mask.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions {
                network: NetworkPermissions {
                    tls: TlsRequirement::Forbidden,
                    ..Default::default()
                },
                ..Default::default()
            },
            roles: Roles {
                transform: Some(TransformCapabilities {}),
                ..Default::default()
            },
            config_schema: None,
        };
        assert!(manifest.supports_role(ConnectorRole::Transform));
        assert!(!manifest.supports_role(ConnectorRole::Source));
        assert_eq!(manifest.permissions.network.tls, TlsRequirement::Forbidden);
    }

    #[test]
    fn test_permissions_defaults() {
        let perms = Permissions::default();
        assert!(perms.network.allowed_domains.is_none());
        assert_eq!(perms.network.tls, TlsRequirement::Optional);
        assert!(!perms.network.allow_runtime_config_domains);
        assert!(perms.env.allowed_vars.is_empty());
        assert!(perms.fs.preopens.is_empty());
    }

    #[test]
    fn test_deserialize_from_json_file_format() {
        let json = r#"{
            "manifest_version": "1.0",
            "id": "rapidbyte/dest-postgres",
            "name": "PostgreSQL Destination",
            "version": "0.1.0",
            "description": "Writes data to PostgreSQL using INSERT or COPY",
            "author": "Rapidbyte Inc.",
            "license": "Apache-2.0",
            "protocol_version": "1",
            "artifact": {
                "entry_point": "dest_postgres.wasm",
                "checksum": "sha256:abcd1234",
                "min_memory_mb": 128
            },
            "permissions": {
                "network": {
                    "tls": "optional",
                    "allow_runtime_config_domains": true
                },
                "env": {
                    "allowed_vars": ["PGSSLROOTCERT"]
                },
                "fs": {
                    "preopens": []
                }
            },
            "roles": {
                "destination": {
                    "supported_write_modes": ["append", "replace"],
                    "features": ["bulk_load_copy"]
                }
            },
            "config_schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["host", "port", "user", "database"],
                "properties": {
                    "host": { "type": "string" },
                    "port": { "type": "integer", "default": 5432 },
                    "user": { "type": "string" },
                    "password": { "type": "string" },
                    "database": { "type": "string" }
                }
            }
        }"#;
        let manifest: ConnectorManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.id, "rapidbyte/dest-postgres");
        assert!(manifest.supports_role(ConnectorRole::Destination));
        assert!(!manifest.supports_role(ConnectorRole::Source));
        assert_eq!(manifest.artifact.entry_point, "dest_postgres.wasm");
        assert_eq!(
            manifest.artifact.checksum,
            Some("sha256:abcd1234".to_string())
        );
        assert_eq!(manifest.permissions.env.allowed_vars, vec!["PGSSLROOTCERT"]);
        assert!(manifest.config_schema.is_some());
        let schema = manifest.config_schema.unwrap();
        let required = schema.get("required").unwrap().as_array().unwrap();
        assert_eq!(required.len(), 4);
    }

    #[test]
    fn test_manifest_version_defaults() {
        let json = r#"{
            "id": "test",
            "name": "Test",
            "version": "0.1.0",
            "protocol_version": "1",
            "artifact": { "entry_point": "test.wasm" },
            "roles": {}
        }"#;
        let manifest: ConnectorManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.manifest_version, "1.0");
        assert!(manifest.permissions.env.allowed_vars.is_empty());
    }

    #[test]
    fn test_dual_role_connector() {
        let manifest = ConnectorManifest {
            manifest_version: "1.0".to_string(),
            id: "rapidbyte/pg-bidirectional".to_string(),
            name: "PG Bidirectional".to_string(),
            version: "0.1.0".to_string(),
            description: "".to_string(),
            author: None,
            license: None,
            protocol_version: "1".to_string(),
            artifact: ArtifactInfo {
                entry_point: "pg_bidir.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions::default(),
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh],
                    features: vec![],
                }),
                destination: Some(DestinationCapabilities {
                    supported_write_modes: vec![WriteMode::Append],
                    features: vec![],
                }),
                ..Default::default()
            },
            config_schema: None,
        };
        assert!(manifest.supports_role(ConnectorRole::Source));
        assert!(manifest.supports_role(ConnectorRole::Destination));
        assert!(!manifest.supports_role(ConnectorRole::Transform));
    }
}
