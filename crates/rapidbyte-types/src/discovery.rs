//! Discovery and plugin specification types for the v7 wire protocol.
//!
//! [`DiscoveredStream`] represents a stream found during source discovery,
//! pairing a stream name with its schema and sync mode capabilities.
//!
//! [`PluginSpec`] carries the plugin's self-description: protocol version,
//! config/resource JSON schemas, features, and supported modes.

use serde::{Deserialize, Serialize};

use crate::schema::StreamSchema;
use crate::wire::{SyncMode, WriteMode};

// ── Discovered Stream ────────────────────────────────────────────

/// A stream discovered by a source plugin during catalog discovery.
///
/// Combines the stream's fully-qualified name, its schema definition,
/// and the sync modes the source supports for this stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredStream {
    /// Fully-qualified stream name (e.g., `"public.users"`).
    pub name: String,
    /// Schema definition for this stream.
    pub schema: StreamSchema,
    /// Sync modes the source supports for this stream.
    pub supported_sync_modes: Vec<SyncMode>,
    /// Default cursor field for incremental sync, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_cursor_field: Option<String>,
    /// Estimated total row count, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_row_count: Option<u64>,
    /// Opaque JSON metadata from the source, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_json: Option<String>,
}

// ── Plugin Spec ──────────────────────────────────────────────────

/// Plugin specification returned during the `spec` handshake.
///
/// Carries the plugin's protocol version, JSON schemas for configuration
/// and resource validation, feature flags, and supported modes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSpec {
    /// Protocol version this plugin implements (must be 7).
    pub protocol_version: u32,
    /// JSON Schema describing the plugin's configuration.
    pub config_schema_json: String,
    /// JSON Schema describing per-resource configuration, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_schema_json: Option<String>,
    /// URL to the plugin's documentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    /// Feature flags advertised by this plugin (free-form strings).
    #[serde(default)]
    pub features: Vec<String>,
    /// Sync modes the plugin supports globally.
    #[serde(default)]
    pub supported_sync_modes: Vec<SyncMode>,
    /// Write modes the plugin supports (destination plugins only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_write_modes: Option<Vec<WriteMode>>,
}

impl PluginSpec {
    /// Create a default spec from manifest defaults.
    ///
    /// Returns a minimal v7 spec with empty schemas and no features.
    /// Plugin authors should override fields as needed.
    #[must_use]
    pub fn from_manifest() -> Self {
        Self {
            protocol_version: 7,
            config_schema_json: "{}".into(),
            resource_schema_json: None,
            documentation_url: None,
            features: vec![],
            supported_sync_modes: vec![],
            supported_write_modes: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaField;

    fn sample_schema() -> StreamSchema {
        StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
                SchemaField::new("created_at", "timestamp_tz", false),
            ],
            primary_key: vec!["id".into()],
            partition_keys: vec![],
            source_defined_cursor: Some("created_at".into()),
            schema_id: None,
        }
    }

    #[test]
    fn discovered_stream_roundtrip() {
        let stream = DiscoveredStream {
            name: "public.users".into(),
            schema: sample_schema(),
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
            default_cursor_field: Some("created_at".into()),
            estimated_row_count: Some(100_000),
            metadata_json: Some(r#"{"replication_slot":"main"}"#.into()),
        };

        let json = serde_json::to_string(&stream).unwrap();
        let back: DiscoveredStream = serde_json::from_str(&json).unwrap();
        assert_eq!(stream, back);
    }

    #[test]
    fn discovered_stream_optional_fields_skipped() {
        let stream = DiscoveredStream {
            name: "public.orders".into(),
            schema: sample_schema(),
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        };

        let json = serde_json::to_value(&stream).unwrap();
        let obj = json.as_object().unwrap();

        assert!(!obj.contains_key("default_cursor_field"));
        assert!(!obj.contains_key("estimated_row_count"));
        assert!(!obj.contains_key("metadata_json"));

        // Required fields are present.
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("schema"));
        assert!(obj.contains_key("supported_sync_modes"));
    }

    #[test]
    fn plugin_spec_source_roundtrip() {
        let spec = PluginSpec {
            protocol_version: 7,
            config_schema_json: r#"{"type":"object","properties":{"host":{"type":"string"}}}"#
                .into(),
            resource_schema_json: None,
            documentation_url: Some("https://docs.rapidbyte.io/sources/postgres".into()),
            features: vec!["cdc".into(), "partitioned_read".into()],
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc],
            supported_write_modes: None,
        };

        let json = serde_json::to_string(&spec).unwrap();
        let back: PluginSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
        assert_eq!(back.protocol_version, 7);
        assert_eq!(back.features.len(), 2);
        assert_eq!(back.features[0], "cdc");
        assert_eq!(back.features[1], "partitioned_read");
    }

    #[test]
    fn plugin_spec_destination_with_write_modes() {
        let spec = PluginSpec {
            protocol_version: 7,
            config_schema_json: r#"{"type":"object"}"#.into(),
            resource_schema_json: Some(
                r#"{"type":"object","properties":{"table":{"type":"string"}}}"#.into(),
            ),
            documentation_url: None,
            features: vec!["bulk_load".into(), "schema_auto_migrate".into()],
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
            supported_write_modes: Some(vec![
                WriteMode::Append,
                WriteMode::Replace,
                WriteMode::Upsert {
                    primary_key: vec!["id".into()],
                },
            ]),
        };

        let json = serde_json::to_string(&spec).unwrap();
        let back: PluginSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);

        let write_modes = back.supported_write_modes.unwrap();
        assert_eq!(write_modes.len(), 3);
        assert_eq!(write_modes[0], WriteMode::Append);
        assert_eq!(write_modes[1], WriteMode::Replace);
        assert!(
            matches!(write_modes[2], WriteMode::Upsert { ref primary_key } if primary_key == &["id"])
        );
    }
}
