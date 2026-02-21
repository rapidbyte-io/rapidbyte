use serde::{Deserialize, Serialize};

use crate::protocol::{SyncMode, WriteMode};

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

/// Role declarations - each `Some` variant means the connector supports that role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
pub struct Roles {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utility: Option<UtilityCapabilities>,
}
