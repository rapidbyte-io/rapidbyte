//! Conversion between protocol-level ArrowDataType and arrow crate DataType.

use crate::arrow_types::ArrowDataType;
use arrow::datatypes::{DataType, TimeUnit};

/// Convert protocol ArrowDataType to the actual arrow crate DataType.
///
/// This is the single source of truth for type mapping. Connectors should
/// use this function instead of hand-rolling match blocks.
pub fn arrow_data_type(proto: ArrowDataType) -> DataType {
    match proto {
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Int8,
        ArrowDataType::Int16 => DataType::Int16,
        ArrowDataType::Int32 => DataType::Int32,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::UInt8 => DataType::UInt8,
        ArrowDataType::UInt16 => DataType::UInt16,
        ArrowDataType::UInt32 => DataType::UInt32,
        ArrowDataType::UInt64 => DataType::UInt64,
        ArrowDataType::Float16 => DataType::Float16,
        ArrowDataType::Float32 => DataType::Float32,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Utf8 => DataType::Utf8,
        ArrowDataType::LargeUtf8 => DataType::LargeUtf8,
        ArrowDataType::Binary => DataType::Binary,
        ArrowDataType::LargeBinary => DataType::LargeBinary,
        ArrowDataType::Date32 => DataType::Date32,
        ArrowDataType::Date64 => DataType::Date64,
        ArrowDataType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        ArrowDataType::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        ArrowDataType::TimestampNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
        // JSON and Decimal128 have no dedicated Arrow type — store as Utf8 strings.
        ArrowDataType::Decimal128 => DataType::Utf8,
        ArrowDataType::Json => DataType::Utf8,
        // Future-proof: treat unknown variants as Utf8.
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_all_protocol_types() {
        assert_eq!(arrow_data_type(ArrowDataType::Boolean), DataType::Boolean);
        assert_eq!(arrow_data_type(ArrowDataType::Int8), DataType::Int8);
        assert_eq!(arrow_data_type(ArrowDataType::Int16), DataType::Int16);
        assert_eq!(arrow_data_type(ArrowDataType::Int32), DataType::Int32);
        assert_eq!(arrow_data_type(ArrowDataType::Int64), DataType::Int64);
        assert_eq!(arrow_data_type(ArrowDataType::UInt8), DataType::UInt8);
        assert_eq!(arrow_data_type(ArrowDataType::UInt16), DataType::UInt16);
        assert_eq!(arrow_data_type(ArrowDataType::UInt32), DataType::UInt32);
        assert_eq!(arrow_data_type(ArrowDataType::UInt64), DataType::UInt64);
        assert_eq!(arrow_data_type(ArrowDataType::Float32), DataType::Float32);
        assert_eq!(arrow_data_type(ArrowDataType::Float64), DataType::Float64);
        assert_eq!(arrow_data_type(ArrowDataType::Utf8), DataType::Utf8);
        assert_eq!(
            arrow_data_type(ArrowDataType::LargeUtf8),
            DataType::LargeUtf8
        );
        assert_eq!(arrow_data_type(ArrowDataType::Binary), DataType::Binary);
        assert_eq!(
            arrow_data_type(ArrowDataType::LargeBinary),
            DataType::LargeBinary
        );
        assert_eq!(arrow_data_type(ArrowDataType::Date32), DataType::Date32);
        assert_eq!(arrow_data_type(ArrowDataType::Date64), DataType::Date64);
        assert_eq!(
            arrow_data_type(ArrowDataType::TimestampMicros),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            arrow_data_type(ArrowDataType::TimestampMillis),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            arrow_data_type(ArrowDataType::TimestampNanos),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn json_and_decimal_map_to_utf8() {
        assert_eq!(arrow_data_type(ArrowDataType::Json), DataType::Utf8);
        assert_eq!(arrow_data_type(ArrowDataType::Decimal128), DataType::Utf8);
    }
}
