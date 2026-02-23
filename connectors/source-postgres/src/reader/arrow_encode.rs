//! Arrow schema construction and row-to-batch encoding helpers.

use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use rapidbyte_sdk::arrow::array::{
    BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringBuilder, TimestampMicrosecondArray,
};
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::protocol::ColumnSchema;
use std::sync::LazyLock;

/// Unix epoch date — used as the base for Arrow Date32 day offsets.
static UNIX_EPOCH_DATE: LazyLock<NaiveDate> =
    LazyLock::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is always valid"));

pub(crate) fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    columns: &[ColumnSchema],
    pg_types: &[String],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
    let arrays: Vec<Arc<dyn rapidbyte_sdk::arrow::array::Array>> = columns
        .iter()
        .enumerate()
        .map(
            |(col_idx, col)| -> Result<Arc<dyn rapidbyte_sdk::arrow::array::Array>, String> {
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
                        let epoch = *UNIX_EPOCH_DATE;
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
                        let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
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
                        let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
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
        .collect::<Result<Vec<_>, String>>()?;

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {e}"))
}
