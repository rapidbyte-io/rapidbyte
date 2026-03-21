//! Arrow schema construction from v7 stream schema fields.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};

use super::types::arrow_data_type;
use crate::schema::SchemaField;

/// Build an Arrow Schema from v7 schema-field definitions.
///
/// This is the canonical way to convert `SchemaField` values from discovery or
/// `StreamContext` into a real Arrow `Schema` for RecordBatch construction.
pub fn build_arrow_schema(columns: &[SchemaField]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            Field::new(
                &col.name,
                arrow_data_type_name(&col.arrow_type),
                col.nullable,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn arrow_data_type_name(name: &str) -> arrow::datatypes::DataType {
    let arrow_type = match name {
        "boolean" => crate::arrow_types::ArrowDataType::Boolean,
        "int8" => crate::arrow_types::ArrowDataType::Int8,
        "int16" => crate::arrow_types::ArrowDataType::Int16,
        "int32" => crate::arrow_types::ArrowDataType::Int32,
        "int64" => crate::arrow_types::ArrowDataType::Int64,
        "uint8" => crate::arrow_types::ArrowDataType::UInt8,
        "uint16" => crate::arrow_types::ArrowDataType::UInt16,
        "uint32" => crate::arrow_types::ArrowDataType::UInt32,
        "uint64" => crate::arrow_types::ArrowDataType::UInt64,
        "float16" => crate::arrow_types::ArrowDataType::Float16,
        "float32" => crate::arrow_types::ArrowDataType::Float32,
        "float64" => crate::arrow_types::ArrowDataType::Float64,
        "utf8" => crate::arrow_types::ArrowDataType::Utf8,
        "large_utf8" => crate::arrow_types::ArrowDataType::LargeUtf8,
        "binary" => crate::arrow_types::ArrowDataType::Binary,
        "large_binary" => crate::arrow_types::ArrowDataType::LargeBinary,
        "date32" => crate::arrow_types::ArrowDataType::Date32,
        "date64" => crate::arrow_types::ArrowDataType::Date64,
        "timestamp_millis" => crate::arrow_types::ArrowDataType::TimestampMillis,
        "timestamp_micros" => crate::arrow_types::ArrowDataType::TimestampMicros,
        "timestamp_nanos" => crate::arrow_types::ArrowDataType::TimestampNanos,
        "decimal128" => crate::arrow_types::ArrowDataType::Decimal128,
        "json" => crate::arrow_types::ArrowDataType::Json,
        _ => crate::arrow_types::ArrowDataType::Utf8,
    };
    arrow_data_type(arrow_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn builds_schema_from_schema_fields() {
        let columns = vec![
            SchemaField::new("id", "int64", false),
            SchemaField::new("name", "utf8", true),
            SchemaField::new("created_at", "timestamp_micros", true),
        ];

        let schema = build_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert!(schema.field(1).is_nullable());
        assert_eq!(
            *schema.field(2).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn empty_columns_produces_empty_schema() {
        let schema = build_arrow_schema(&[]);
        assert_eq!(schema.fields().len(), 0);
    }
}
