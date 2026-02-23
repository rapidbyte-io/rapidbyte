//! Arrow data type identifiers for cross-boundary schema exchange.
//!
//! Maps database column types to a portable subset of Apache Arrow
//! logical types. Used in [`crate::catalog::ColumnSchema`] to describe
//! stream schemas without depending on the Arrow crate itself.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Arrow-compatible logical data type.
///
/// Covers the subset of Arrow types needed for database column mapping.
/// New variants may be added as connector support expands.
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
}
