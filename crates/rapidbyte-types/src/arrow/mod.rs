//! Arrow data type identifiers and IPC codec utilities.
//!
//! Maps database column types to a portable subset of Apache Arrow
//! logical types. Used in v7 `SchemaField` records to describe stream
//! schemas without depending on the Arrow crate itself.
//!
//! The [`ipc`] submodule provides Arrow IPC serialization/deserialization
//! helpers for `RecordBatch` transport between pipeline stages.

mod ipc;

pub use ipc::*;

use serde::{Deserialize, Serialize};
use std::fmt;

/// Arrow-compatible logical data type.
///
/// Covers the subset of Arrow types needed for database column mapping.
/// New variants may be added as plugin support expands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
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
    /// Returns the canonical string representation.
    #[must_use]
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

    /// Parse a schema type name from the v7 protocol surface.
    ///
    /// Accepts canonical lowercase protocol names and a narrow set of legacy
    /// aliases that still appear in older fixtures or compatibility paths.
    #[must_use]
    pub fn from_schema_name(name: &str) -> Option<Self> {
        match name {
            "boolean" | "Boolean" => Some(Self::Boolean),
            "int8" | "Int8" => Some(Self::Int8),
            "int16" | "Int16" => Some(Self::Int16),
            "int32" | "Int32" => Some(Self::Int32),
            "int64" | "Int64" => Some(Self::Int64),
            "uint8" | "UInt8" => Some(Self::UInt8),
            "uint16" | "UInt16" => Some(Self::UInt16),
            "uint32" | "UInt32" => Some(Self::UInt32),
            "uint64" | "UInt64" => Some(Self::UInt64),
            "float16" | "Float16" => Some(Self::Float16),
            "float32" | "Float32" => Some(Self::Float32),
            "float64" | "Float64" => Some(Self::Float64),
            "utf8" | "Utf8" => Some(Self::Utf8),
            "large_utf8" | "LargeUtf8" => Some(Self::LargeUtf8),
            "binary" | "Binary" => Some(Self::Binary),
            "large_binary" | "LargeBinary" => Some(Self::LargeBinary),
            "date32" | "Date32" => Some(Self::Date32),
            "date64" | "Date64" => Some(Self::Date64),
            "timestamp_millis" | "TimestampMillis" => Some(Self::TimestampMillis),
            "timestamp_micros" | "TimestampMicros" | "timestamp_tz" | "timestamptz" => {
                Some(Self::TimestampMicros)
            }
            "timestamp_nanos" | "TimestampNanos" => Some(Self::TimestampNanos),
            "decimal128" | "Decimal128" => Some(Self::Decimal128),
            "json" | "Json" | "jsonb" => Some(Self::Json),
            _ => None,
        }
    }
}

impl fmt::Display for ArrowDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_all_variants() {
        let types = [
            ArrowDataType::Boolean,
            ArrowDataType::Int32,
            ArrowDataType::Float64,
            ArrowDataType::Utf8,
            ArrowDataType::TimestampMicros,
            ArrowDataType::Decimal128,
            ArrowDataType::Json,
        ];
        for dt in types {
            let json = serde_json::to_string(&dt).unwrap();
            let back: ArrowDataType = serde_json::from_str(&json).unwrap();
            assert_eq!(dt, back, "roundtrip failed for {dt}");
        }
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(ArrowDataType::Int64.to_string(), "Int64");
        assert_eq!(ArrowDataType::TimestampMicros.as_str(), "TimestampMicros");
    }

    #[test]
    fn parses_canonical_and_legacy_schema_names() {
        assert_eq!(
            ArrowDataType::from_schema_name("int64"),
            Some(ArrowDataType::Int64)
        );
        assert_eq!(
            ArrowDataType::from_schema_name("Int64"),
            Some(ArrowDataType::Int64)
        );
        assert_eq!(
            ArrowDataType::from_schema_name("timestamp_tz"),
            Some(ArrowDataType::TimestampMicros)
        );
        assert_eq!(ArrowDataType::from_schema_name("unknown_type"), None);
    }
}
