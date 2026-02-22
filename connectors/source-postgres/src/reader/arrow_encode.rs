//! Arrow schema construction and row-to-batch encoding helpers.

use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringBuilder, TimestampMicrosecondArray,
};
use chrono::{NaiveDate, NaiveDateTime};
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
                "TimestampMicros" => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
                "Date32" => DataType::Date32,
                "Binary" => DataType::Binary,
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
    pg_types: &[String],
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
                    "TimestampMicros" => {
                        let arr: TimestampMicrosecondArray = rows
                            .iter()
                            .map(|row| {
                                // Try NaiveDateTime first (TIMESTAMP without TZ),
                                // then DateTime<Utc> (TIMESTAMPTZ). tokio-postgres
                                // uses different FromSql impls for each PG type.
                                row.try_get::<_, NaiveDateTime>(col_idx)
                                    .ok()
                                    .map(|dt| dt.and_utc().timestamp_micros())
                                    .or_else(|| {
                                        row.try_get::<_, chrono::DateTime<chrono::Utc>>(col_idx)
                                            .ok()
                                            .map(|dt| dt.timestamp_micros())
                                    })
                            })
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Date32" => {
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let arr: Date32Array = rows
                            .iter()
                            .map(|row| {
                                row.try_get::<_, NaiveDate>(col_idx)
                                    .ok()
                                    .map(|d| (d - epoch).num_days() as i32)
                            })
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Binary" => {
                        let mut builder =
                            BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
                        for row in rows {
                            match row.try_get::<_, Vec<u8>>(col_idx).ok() {
                                Some(bytes) => builder.append_value(&bytes),
                                None => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => {
                        // Utf8 path: handles text, varchar, json/jsonb, and all ::text-cast types.
                        let mut builder =
                            StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                        let pg_type = pg_types.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                        for row in rows {
                            let val = if pg_type == "json" || pg_type == "jsonb" {
                                row.try_get::<_, serde_json::Value>(col_idx)
                                    .ok()
                                    .map(|v| v.to_string())
                            } else {
                                row.try_get::<_, String>(col_idx).ok()
                            };
                            match val {
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
    fn build_arrow_schema_maps_timestamp_date_binary() {
        let columns = vec![
            ColumnSchema {
                name: "created_at".to_string(),
                data_type: ArrowDataType::TimestampMicros,
                nullable: true,
            },
            ColumnSchema {
                name: "birth_date".to_string(),
                data_type: ArrowDataType::Date32,
                nullable: true,
            },
            ColumnSchema {
                name: "avatar".to_string(),
                data_type: ArrowDataType::Binary,
                nullable: true,
            },
        ];
        let schema = build_arrow_schema(&columns);
        assert_eq!(
            *schema.field(0).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(*schema.field(1).data_type(), DataType::Date32);
        assert_eq!(*schema.field(2).data_type(), DataType::Binary);
    }

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
