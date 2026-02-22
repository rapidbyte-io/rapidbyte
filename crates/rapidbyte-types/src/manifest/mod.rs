//! Connector manifest types, permissions, and role capability declarations.

mod artifact;
mod permissions;
mod roles;

pub use artifact::*;
pub use permissions::*;
pub use roles::*;

use serde::{Deserialize, Serialize};

use crate::protocol::{ConnectorRole, ProtocolVersion};

pub const DEFAULT_MANIFEST_VERSION: &str = "1.0";

fn default_manifest_version() -> String {
    DEFAULT_MANIFEST_VERSION.to_string()
}

/// Connector manifest - declares identity, roles, capabilities, config schema,
/// and security requirements.
///
/// Stored as `<connector_name>.manifest.json` alongside the `.wasm` binary
/// in the connector plugins directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorManifest {
    /// Manifest format version (e.g., "1.0").
    #[serde(default = "default_manifest_version")]
    pub manifest_version: String,

    // Metadata.
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

    /// Protocol version this connector implements.
    pub protocol_version: ProtocolVersion,

    // Artifact.
    /// WASM binary artifact info.
    pub artifact: ArtifactInfo,

    // Security.
    /// Permissions controlling the WASI sandbox boundary.
    #[serde(default)]
    pub permissions: Permissions,

    /// Resource limits for the WASI sandbox (memory, timeout).
    #[serde(default)]
    pub limits: ResourceLimits,

    // Roles.
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
    use crate::protocol::{ProtocolVersion, SyncMode, WriteMode};

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
            protocol_version: ProtocolVersion::V2,
            artifact: ArtifactInfo {
                entry_point: "dest_postgres.wasm".to_string(),
                checksum: None,
                min_memory_mb: Some(128),
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            roles: Roles {
                destination: Some(DestinationCapabilities {
                    supported_write_modes: vec![WriteMode::Append, WriteMode::Replace],
                    features: vec![DestinationFeature::BulkLoadCopy],
                }),
                ..Default::default()
            },
            config_schema: None,
        };
        let json = serde_json::to_string_pretty(&manifest).expect("serialize");
        let back: ConnectorManifest = serde_json::from_str(&json).expect("deserialize");
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
            protocol_version: ProtocolVersion::V2,
            artifact: ArtifactInfo {
                entry_point: "source_postgres.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions {
                network: NetworkPermissions {
                    allowed_domains: None,
                    allow_runtime_config_domains: true,
                },
                env: EnvPermissions {
                    allowed_vars: vec!["PGSSLROOTCERT".to_string()],
                },
                fs: FsPermissions::default(),
            },
            limits: ResourceLimits::default(),
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
                    features: vec![],
                }),
                ..Default::default()
            },
            config_schema: None,
        };
        let json = serde_json::to_string_pretty(&manifest).expect("serialize");
        let back: ConnectorManifest = serde_json::from_str(&json).expect("deserialize");
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
            protocol_version: ProtocolVersion::V2,
            artifact: ArtifactInfo {
                entry_point: "transform_mask.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            roles: Roles {
                transform: Some(TransformCapabilities {}),
                ..Default::default()
            },
            config_schema: None,
        };
        assert!(manifest.supports_role(ConnectorRole::Transform));
        assert!(!manifest.supports_role(ConnectorRole::Source));
    }

    #[test]
    fn test_permissions_defaults() {
        let perms = Permissions::default();
        assert!(perms.network.allowed_domains.is_none());
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
            "protocol_version": "2",
            "artifact": {
                "entry_point": "dest_postgres.wasm",
                "checksum": "sha256:abcd1234",
                "min_memory_mb": 128
            },
            "permissions": {
                "network": {
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
        let manifest: ConnectorManifest = serde_json::from_str(json).expect("deserialize");
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
        let schema = manifest.config_schema.expect("schema");
        let required = schema
            .get("required")
            .expect("required")
            .as_array()
            .expect("array");
        assert_eq!(required.len(), 4);
    }

    #[test]
    fn test_manifest_version_defaults() {
        let json = r#"{
            "id": "test",
            "name": "Test",
            "version": "0.1.0",
            "protocol_version": "2",
            "artifact": { "entry_point": "test.wasm" },
            "roles": {}
        }"#;
        let manifest: ConnectorManifest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(manifest.manifest_version, "1.0");
        assert!(manifest.permissions.env.allowed_vars.is_empty());
    }

    #[test]
    fn manifest_resource_limits_parsed() {
        let yaml = r#"
id: test-connector
name: Test Connector
version: "1.0.0"
protocol_version: "2"
artifact:
  entry_point: test.wasm
roles: {}
limits:
  max_memory: 128mb
  timeout_seconds: 60
"#;
        let manifest: ConnectorManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.limits.max_memory, Some("128mb".to_string()));
        assert_eq!(manifest.limits.timeout_seconds, Some(60));
    }

    #[test]
    fn manifest_resource_limits_absent_is_default() {
        let yaml = r#"
id: test-connector
name: Test Connector
version: "1.0.0"
protocol_version: "2"
artifact:
  entry_point: test.wasm
roles: {}
"#;
        let manifest: ConnectorManifest = serde_yaml::from_str(yaml).unwrap();
        assert!(manifest.limits.max_memory.is_none());
        assert!(manifest.limits.timeout_seconds.is_none());
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
            protocol_version: ProtocolVersion::V2,
            artifact: ArtifactInfo {
                entry_point: "pg_bidir.wasm".to_string(),
                checksum: None,
                min_memory_mb: None,
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
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
