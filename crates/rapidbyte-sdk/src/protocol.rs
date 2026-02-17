use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    FullRefresh,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Stream {
    pub name: String,
    pub schema: Vec<ColumnSchema>,
    pub supported_sync_modes: Vec<SyncMode>,
    pub source_defined_cursor: Option<String>,
    pub source_defined_primary_key: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Catalog {
    pub streams: Vec<Stream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamSelection {
    pub name: String,
    pub sync_mode: SyncMode,
    pub cursor_field: Option<String>,
    pub max_batch_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadRequest {
    pub streams: Vec<StreamSelection>,
    pub state: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Replace,
    Upsert { primary_key: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteRequest {
    pub stream: String,
    pub schema: Vec<ColumnSchema>,
    pub write_mode: WriteMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteSummary {
    pub records_written: u64,
    pub bytes_written: u64,
    /// Time spent establishing the database connection (seconds).
    #[serde(default)]
    pub connect_secs: f64,
    /// Time spent executing INSERT statements (seconds).
    #[serde(default)]
    pub flush_secs: f64,
    /// Time spent on COMMIT (seconds).
    #[serde(default)]
    pub commit_secs: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_mode_roundtrip() {
        let mode = SyncMode::Incremental;
        let json = serde_json::to_string(&mode).unwrap();
        let back: SyncMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, back);
    }

    #[test]
    fn test_catalog_roundtrip() {
        let catalog = Catalog {
            streams: vec![Stream {
                name: "users".to_string(),
                schema: vec![ColumnSchema {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                }],
                supported_sync_modes: vec![SyncMode::FullRefresh],
                source_defined_cursor: None,
                source_defined_primary_key: Some(vec!["id".to_string()]),
            }],
        };
        let json = serde_json::to_string(&catalog).unwrap();
        let back: Catalog = serde_json::from_str(&json).unwrap();
        assert_eq!(catalog, back);
    }

    #[test]
    fn test_read_request_roundtrip() {
        let req = ReadRequest {
            streams: vec![StreamSelection {
                name: "orders".to_string(),
                sync_mode: SyncMode::FullRefresh,
                cursor_field: None,
                max_batch_bytes: 64 * 1024 * 1024,
            }],
            state: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ReadRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn test_write_mode_upsert_roundtrip() {
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let json = serde_json::to_string(&mode).unwrap();
        let back: WriteMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, back);
    }

    #[test]
    fn test_write_request_roundtrip() {
        let req = WriteRequest {
            stream: "users".to_string(),
            schema: vec![ColumnSchema {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
            }],
            write_mode: WriteMode::Append,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: WriteRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }
}
