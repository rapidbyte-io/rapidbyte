//! Arrow schema construction and row-to-batch encoding helpers.

use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rapidbyte_sdk::protocol::ColumnSchema;

pub(crate) fn build_arrow_schema(columns: &[ColumnSchema]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = match col.data_type.as_str() {
                "Int16" => DataType::Int16,
                "Int32" => DataType::Int32,
                "Int64" => DataType::Int64,
                "Float32" => DataType::Float32,
                "Float64" => DataType::Float64,
                "Boolean" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(&col.name, dt, col.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

pub(crate) fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    columns: &[ColumnSchema],
    schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .iter()
        .enumerate()
        .map(
            |(col_idx, col)| -> anyhow::Result<Arc<dyn arrow::array::Array>> {
                match col.data_type.as_str() {
                    "Int16" => {
                        let arr: Int16Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i16>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Int32" => {
                        let arr: Int32Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Int64" => {
                        let arr: Int64Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Float32" => {
                        let arr: Float32Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, f32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Float64" => {
                        let arr: Float64Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, f64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Boolean" => {
                        let arr: BooleanArray = rows
                            .iter()
                            .map(|row| row.try_get::<_, bool>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    _ => {
                        let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                        for row in rows {
                            match row.try_get::<_, String>(col_idx).ok() {
                                Some(s) => builder.append_value(&s),
                                None => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                }
            },
        )
        .collect::<anyhow::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), arrays).context("Failed to create RecordBatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::protocol::ArrowDataType;

    #[test]
    fn build_arrow_schema_maps_types() {
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
        ];
        let schema = build_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }
}
