use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    FullRefresh,
    Incremental,
    Cdc,
}

/// Wire protocol version for host/guest communication.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum ProtocolVersion {
    #[serde(rename = "2")]
    #[default]
    V2,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "PascalCase")]
pub enum ArrowDataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    Decimal128,
    Json,
}

impl ArrowDataType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Int8 => "Int8",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::UInt8 => "UInt8",
            Self::UInt16 => "UInt16",
            Self::UInt32 => "UInt32",
            Self::UInt64 => "UInt64",
            Self::Float16 => "Float16",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Utf8 => "Utf8",
            Self::LargeUtf8 => "LargeUtf8",
            Self::Binary => "Binary",
            Self::LargeBinary => "LargeBinary",
            Self::Date32 => "Date32",
            Self::Date64 => "Date64",
            Self::TimestampMillis => "TimestampMillis",
            Self::TimestampMicros => "TimestampMicros",
            Self::TimestampNanos => "TimestampNanos",
            Self::Decimal128 => "Decimal128",
            Self::Json => "Json",
        }
    }
}

impl std::fmt::Display for ArrowDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ArrowDataType,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    ExactlyOnce,
    Cdc,
    SchemaAutoMigrate,
    BulkLoadCopy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorInfo {
    pub protocol_version: ProtocolVersion,
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
pub enum ConnectorRole {
    Source,
    Destination,
    Transform,
    Utility,
}
