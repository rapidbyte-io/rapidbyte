//! Stream catalog and schema types.
//!
//! A [`Catalog`] is the set of [`Stream`]s a source connector exposes.
//! Each stream carries a schema of [`ColumnSchema`] entries describing
//! the Arrow-typed columns available for replication.

use crate::arrow::ArrowDataType;
use crate::wire::SyncMode;
use serde::{Deserialize, Serialize};

/// Column definition within a stream schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// Arrow logical data type.
    pub data_type: ArrowDataType,
    /// Whether the column permits null values.
    pub nullable: bool,
}

/// A discoverable stream exposed by a source connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    /// Fully-qualified stream name (e.g., `"public.users"`).
    pub name: String,
    /// Column schema for this stream.
    pub schema: Vec<ColumnSchema>,
    /// Sync modes this stream supports.
    pub supported_sync_modes: Vec<SyncMode>,
    /// Source-defined cursor column for incremental sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_defined_cursor: Option<String>,
    /// Source-defined primary key columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_defined_primary_key: Option<Vec<String>>,
}

/// Collection of streams discovered by a source connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub streams: Vec<Stream>,
}

/// Schema representation for stream context.
///
/// Connectors receive schema information in one of these formats
/// when starting a read or write operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "format", content = "data", rename_all = "snake_case")]
pub enum SchemaHint {
    /// Column-level schema as structured types.
    Columns(Vec<ColumnSchema>),
    /// Schema serialized as Arrow IPC bytes.
    ArrowIpc(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_schema_roundtrip() {
        let col = ColumnSchema {
            name: "id".into(),
            data_type: ArrowDataType::Int64,
            nullable: false,
        };
        let json = serde_json::to_string(&col).unwrap();
        let back: ColumnSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(col, back);
    }

    #[test]
    fn schema_hint_columns_json_format() {
        let hint = SchemaHint::Columns(vec![ColumnSchema {
            name: "name".into(),
            data_type: ArrowDataType::Utf8,
            nullable: true,
        }]);
        let json = serde_json::to_value(&hint).unwrap();
        assert_eq!(json["format"], "columns");
        assert!(json["data"].is_array());
    }

    #[test]
    fn catalog_empty_streams() {
        let cat = Catalog { streams: vec![] };
        let json = serde_json::to_string(&cat).unwrap();
        let back: Catalog = serde_json::from_str(&json).unwrap();
        assert_eq!(cat, back);
    }

    #[test]
    fn stream_optional_fields_skipped() {
        let s = Stream {
            name: "users".into(),
            schema: vec![],
            supported_sync_modes: vec![SyncMode::FullRefresh],
            source_defined_cursor: None,
            source_defined_primary_key: None,
        };
        let json = serde_json::to_value(&s).unwrap();
        assert!(json.get("source_defined_cursor").is_none());
        assert!(json.get("source_defined_primary_key").is_none());
    }
}
