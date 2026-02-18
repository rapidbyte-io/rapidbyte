use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Replace,
    Upsert { primary_key: Vec<String> },
}

// ---------------------------------------------------------------------------
// Protocol types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConfigBlob {
    Json(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenContext {
    pub config: ConfigBlob,
    pub connector_id: String,
    pub connector_version: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    ExactlyOnce,
    Cdc,
    SchemaAutoMigrate,
    BulkLoadCopy,
    BulkLoadCopyBinary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenInfo {
    pub protocol_version: String,
    pub features: Vec<Feature>,
    pub default_max_batch_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaHint {
    Columns(Vec<ColumnSchema>),
    ArrowIpcSchema(Vec<u8>),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CursorType {
    Int64,
    Utf8,
    TimestampMillis,
    TimestampMicros,
    Decimal,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CursorValue {
    Null,
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    TimestampMicros(i64),
    Decimal { value: String, scale: i32 },
    Json(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorInfo {
    pub cursor_field: String,
    pub cursor_type: CursorType,
    pub last_value: Option<CursorValue>,
}

fn default_checkpoint_interval_bytes() -> u64 {
    64 * 1024 * 1024 // 64 MB
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamLimits {
    pub max_batch_bytes: u64,
    pub max_record_bytes: u64,
    pub max_inflight_batches: u32,
    pub max_parallel_requests: u32,
    #[serde(default = "default_checkpoint_interval_bytes")]
    pub checkpoint_interval_bytes: u64,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: 64 * 1024 * 1024,  // 64 MB
            max_record_bytes: 16 * 1024 * 1024, // 16 MB
            max_inflight_batches: 16,
            max_parallel_requests: 1,
            checkpoint_interval_bytes: default_checkpoint_interval_bytes(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DataErrorPolicy {
    Skip,
    Fail,
    Dlq,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ColumnPolicy {
    Add,
    Ignore,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TypeChangePolicy {
    Coerce,
    Fail,
    Null,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityPolicy {
    Allow,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct SchemaEvolutionPolicy {
    pub new_column: ColumnPolicy,
    /// Policy for columns removed at source. Note: `ColumnPolicy::Add` is not meaningful
    /// for removed columns; use `Ignore` or `Fail`.
    pub removed_column: ColumnPolicy,
    pub type_change: TypeChangePolicy,
    pub nullability_change: NullabilityPolicy,
}

impl Default for SchemaEvolutionPolicy {
    fn default() -> Self {
        Self {
            new_column: ColumnPolicy::Add,
            removed_column: ColumnPolicy::Ignore,
            type_change: TypeChangePolicy::Fail,
            nullability_change: NullabilityPolicy::Allow,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct StreamPolicies {
    pub on_data_error: DataErrorPolicy,
    pub schema_evolution: SchemaEvolutionPolicy,
}

impl Default for StreamPolicies {
    fn default() -> Self {
        Self {
            on_data_error: DataErrorPolicy::Fail,
            schema_evolution: SchemaEvolutionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamContext {
    pub stream_name: String,
    pub schema: SchemaHint,
    pub sync_mode: SyncMode,
    pub cursor_info: Option<CursorInfo>,
    pub limits: StreamLimits,
    pub policies: StreamPolicies,
    pub write_mode: Option<WriteMode>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRole {
    Source,
    Destination,
    Transform,
    Utility,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    Source,
    Dest,
    Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub id: u64,
    pub kind: CheckpointKind,
    pub stream: String,
    pub cursor_field: Option<String>,
    pub cursor_value: Option<CursorValue>,
    pub records_processed: u64,
    pub bytes_processed: u64,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StateScope {
    Pipeline = 0,
    Stream = 1,
    ConnectorInstance = 2,
}

impl StateScope {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Pipeline),
            1 => Some(Self::Stream),
            2 => Some(Self::ConnectorInstance),
            _ => None,
        }
    }

    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metric {
    pub name: String,
    pub value: MetricValue,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WritePerf {
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
    pub batches_emitted: u64,
    pub checkpoint_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteSummary {
    pub records_written: u64,
    pub bytes_written: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    pub perf: Option<WritePerf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformSummary {
    pub records_in: u64,
    pub records_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub batches_processed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PayloadEnvelope<T> {
    pub protocol_version: String,
    pub connector_id: String,
    pub stream_name: String,
    #[serde(flatten)]
    pub payload: T,
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
    fn test_write_mode_upsert_roundtrip() {
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let json = serde_json::to_string(&mode).unwrap();
        let back: WriteMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, back);
    }

    #[test]
    fn test_config_blob_roundtrip() {
        let blob = ConfigBlob::Json(serde_json::json!({"host": "localhost", "port": 5432}));
        let json = serde_json::to_string(&blob).unwrap();
        let back: ConfigBlob = serde_json::from_str(&json).unwrap();
        assert_eq!(blob, back);
    }

    #[test]
    fn test_open_context_roundtrip() {
        let ctx = OpenContext {
            config: ConfigBlob::Json(serde_json::json!({"dsn": "postgres://localhost"})),
            connector_id: "source-postgres".to_string(),
            connector_version: "0.1.0".to_string(),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: OpenContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn test_feature_roundtrip() {
        let f = Feature::BulkLoadCopy;
        let json = serde_json::to_string(&f).unwrap();
        assert_eq!(json, "\"bulk_load_copy\"");
        let back: Feature = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn test_open_info_roundtrip() {
        let info = OpenInfo {
            protocol_version: "1".to_string(),
            features: vec![Feature::ExactlyOnce, Feature::BulkLoadCopy],
            default_max_batch_bytes: 64 * 1024 * 1024,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: OpenInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }

    #[test]
    fn test_schema_hint_columns_roundtrip() {
        let hint = SchemaHint::Columns(vec![ColumnSchema {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
        }]);
        let json = serde_json::to_string(&hint).unwrap();
        let back: SchemaHint = serde_json::from_str(&json).unwrap();
        assert_eq!(hint, back);
    }

    #[test]
    fn test_cursor_value_roundtrip() {
        let vals = vec![
            CursorValue::Null,
            CursorValue::Int64(42),
            CursorValue::Utf8("abc".to_string()),
            CursorValue::TimestampMillis(1700000000000),
            CursorValue::TimestampMicros(1700000000000000),
            CursorValue::Decimal {
                value: "123.45".to_string(),
                scale: 2,
            },
            CursorValue::Json(serde_json::json!({"key": "val"})),
        ];
        for v in vals {
            let json = serde_json::to_string(&v).unwrap();
            let back: CursorValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn test_stream_limits_default() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_batch_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_record_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_inflight_batches, 16);
        assert_eq!(limits.max_parallel_requests, 1);
        assert_eq!(limits.checkpoint_interval_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_stream_policies_default() {
        let policies = StreamPolicies::default();
        assert_eq!(policies.on_data_error, DataErrorPolicy::Fail);
        assert_eq!(policies.schema_evolution.new_column, ColumnPolicy::Add);
        assert_eq!(
            policies.schema_evolution.removed_column,
            ColumnPolicy::Ignore
        );
        assert_eq!(
            policies.schema_evolution.type_change,
            TypeChangePolicy::Fail
        );
        assert_eq!(
            policies.schema_evolution.nullability_change,
            NullabilityPolicy::Allow
        );
    }

    #[test]
    fn test_stream_context_roundtrip() {
        let ctx = StreamContext {
            stream_name: "orders".to_string(),
            schema: SchemaHint::Columns(vec![ColumnSchema {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }]),
            sync_mode: SyncMode::Incremental,
            cursor_info: Some(CursorInfo {
                cursor_field: "updated_at".to_string(),
                cursor_type: CursorType::TimestampMicros,
                last_value: Some(CursorValue::TimestampMicros(1700000000000000)),
            }),
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: StreamContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "users".to_string(),
            cursor_field: Some("id".to_string()),
            cursor_value: Some(CursorValue::Int64(100)),
            records_processed: 500,
            bytes_processed: 65536,
        };
        let json = serde_json::to_string(&cp).unwrap();
        let back: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(cp, back);
    }

    #[test]
    fn test_state_scope_conversion() {
        assert_eq!(StateScope::from_i32(0), Some(StateScope::Pipeline));
        assert_eq!(StateScope::from_i32(1), Some(StateScope::Stream));
        assert_eq!(StateScope::from_i32(2), Some(StateScope::ConnectorInstance));
        assert_eq!(StateScope::from_i32(99), None);
        assert_eq!(StateScope::Pipeline.to_i32(), 0);
        assert_eq!(StateScope::Stream.to_i32(), 1);
        assert_eq!(StateScope::ConnectorInstance.to_i32(), 2);
    }

    #[test]
    fn test_metric_roundtrip() {
        let m = Metric {
            name: "rows_read".to_string(),
            value: MetricValue::Counter(42),
            labels: vec![("stream".to_string(), "users".to_string())],
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: Metric = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn test_read_summary_roundtrip() {
        let s = ReadSummary {
            records_read: 1000,
            bytes_read: 65536,
            batches_emitted: 10,
            checkpoint_count: 2,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: ReadSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn test_write_summary_roundtrip() {
        let s = WriteSummary {
            records_written: 1000,
            bytes_written: 65536,
            batches_written: 10,
            checkpoint_count: 2,
            perf: Some(WritePerf {
                connect_secs: 0.1,
                flush_secs: 1.5,
                commit_secs: 0.05,
            }),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: WriteSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn test_payload_envelope_roundtrip() {
        let envelope = PayloadEnvelope {
            protocol_version: "1".to_string(),
            connector_id: "source-postgres".to_string(),
            stream_name: "users".to_string(),
            payload: ReadSummary {
                records_read: 100,
                bytes_read: 4096,
                batches_emitted: 1,
                checkpoint_count: 1,
            },
        };
        let json = serde_json::to_string(&envelope).unwrap();
        // Verify flatten works — fields should be at top level
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(v.get("protocol_version").is_some());
        assert!(v.get("records_read").is_some());
        let back: PayloadEnvelope<ReadSummary> = serde_json::from_str(&json).unwrap();
        assert_eq!(envelope, back);
    }

    #[test]
    fn test_connector_role_roundtrip() {
        let roles = vec![
            ConnectorRole::Source,
            ConnectorRole::Destination,
            ConnectorRole::Transform,
            ConnectorRole::Utility,
        ];
        for role in roles {
            let json = serde_json::to_string(&role).unwrap();
            let back: ConnectorRole = serde_json::from_str(&json).unwrap();
            assert_eq!(role, back);
        }
    }

    #[test]
    fn test_transform_summary_roundtrip() {
        let s = TransformSummary {
            records_in: 1000,
            records_out: 950,
            bytes_in: 65536,
            bytes_out: 60000,
            batches_processed: 10,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: TransformSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn test_checkpoint_kind_transform_roundtrip() {
        let kind = CheckpointKind::Transform;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, "\"transform\"");
        let back: CheckpointKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, back);
    }
}
