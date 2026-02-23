//! Arrow schema construction from protocol ColumnSchema.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};

use super::types::arrow_data_type;
use crate::catalog::ColumnSchema;

/// Build an Arrow Schema from protocol column definitions.
///
/// This is the canonical way to convert `ColumnSchema` (from catalog discovery
/// or StreamContext) into a real Arrow Schema for RecordBatch construction.
pub fn build_arrow_schema(columns: &[ColumnSchema]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, arrow_data_type(col.data_type), col.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_types::ArrowDataType;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn builds_schema_from_column_schemas() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: ArrowDataType::Utf8,
                nullable: true,
            },
            ColumnSchema {
                name: "created_at".to_string(),
                data_type: ArrowDataType::TimestampMicros,
                nullable: true,
            },
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
