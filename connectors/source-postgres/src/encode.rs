//! Arrow `RecordBatch` encoding from `PostgreSQL` rows.
//!
//! Converts `tokio_postgres::Row` slices into Arrow `RecordBatch` using the
//! `Column` type registry. Each `Column` carries its `ArrowDataType` and PG
//! type name, eliminating the need for a parallel `pg_types` array.

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use rapidbyte_sdk::arrow::array::{
    Array, BinaryBuilder, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringBuilder, TimestampMicrosecondArray,
};
use rapidbyte_sdk::arrow::datatypes::{Field, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::{arrow_data_type, ArrowDataType};
use std::sync::LazyLock;
use tokio_postgres::Row;

use crate::types::Column;

/// Unix epoch date -- used as the base for Arrow Date32 day offsets.
static UNIX_EPOCH_DATE: LazyLock<NaiveDate> =
    LazyLock::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is always valid"));

/// Build an Arrow `Schema` from column definitions.
///
/// Uses the SDK's `arrow_data_type` mapping so the type conversion stays in
/// one place (the SDK).
pub fn arrow_schema(columns: &[Column]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = arrow_data_type(col.arrow_type);
            Field::new(&col.name, dt, col.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Encode `PostgreSQL` rows into an Arrow `RecordBatch`.
///
/// Matches on `col.arrow_type` (enum) and delegates to specialised per-type
/// encoders. JSON columns are detected via `col.is_json()` instead of a
/// separate `pg_types` array.
pub fn rows_to_record_batch(
    rows: &[Row],
    columns: &[Column],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
    let arrays: Vec<Arc<dyn Array>> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| encode_column(rows, i, col))
        .collect();

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("failed to create RecordBatch: {e}"))
}

fn encode_column(rows: &[Row], idx: usize, col: &Column) -> Arc<dyn Array> {
    match col.arrow_type {
        ArrowDataType::Int16 => {
            let vals: Vec<Option<i16>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(Int16Array::from(vals))
        }
        ArrowDataType::Int32 => {
            let vals: Vec<Option<i32>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(Int32Array::from(vals))
        }
        ArrowDataType::Int64 => {
            let vals: Vec<Option<i64>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(Int64Array::from(vals))
        }
        ArrowDataType::Float32 => {
            let vals: Vec<Option<f32>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(Float32Array::from(vals))
        }
        ArrowDataType::Float64 => {
            let vals: Vec<Option<f64>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(Float64Array::from(vals))
        }
        ArrowDataType::Boolean => {
            let vals: Vec<Option<bool>> = rows.iter().map(|r| r.try_get(idx).ok()).collect();
            Arc::new(BooleanArray::from(vals))
        }
        ArrowDataType::TimestampMicros => encode_timestamp(rows, idx),
        ArrowDataType::Date32 => encode_date(rows, idx),
        ArrowDataType::Binary => encode_binary(rows, idx),
        _ if col.is_json() => encode_json(rows, idx),
        _ => encode_utf8(rows, idx),
    }
}

fn encode_timestamp(rows: &[Row], idx: usize) -> Arc<dyn Array> {
    let vals: Vec<Option<i64>> = rows
        .iter()
        .map(|r| {
            // Try NaiveDateTime first (TIMESTAMP without TZ),
            // then DateTime<Utc> (TIMESTAMPTZ). tokio-postgres
            // uses different FromSql impls for each PG type.
            r.try_get::<_, NaiveDateTime>(idx)
                .map(|dt| dt.and_utc().timestamp_micros())
                .or_else(|_| {
                    r.try_get::<_, DateTime<Utc>>(idx)
                        .map(|dt| dt.timestamp_micros())
                })
                .ok()
        })
        .collect();
    Arc::new(TimestampMicrosecondArray::from(vals))
}

fn encode_date(rows: &[Row], idx: usize) -> Arc<dyn Array> {
    let vals: Vec<Option<i32>> = rows
        .iter()
        .map(|r| {
            r.try_get::<_, NaiveDate>(idx).ok().map(|d| {
                // Safety: Date32 represents days since epoch; realistic dates always fit in i32.
                #[allow(clippy::cast_possible_truncation)]
                let days = (d - *UNIX_EPOCH_DATE).num_days() as i32;
                days
            })
        })
        .collect();
    Arc::new(Date32Array::from(vals))
}

fn encode_binary(rows: &[Row], idx: usize) -> Arc<dyn Array> {
    let mut builder = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
    for row in rows {
        match row.try_get::<_, Vec<u8>>(idx) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn encode_json(rows: &[Row], idx: usize) -> Arc<dyn Array> {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    for row in rows {
        match row.try_get::<_, serde_json::Value>(idx) {
            Ok(v) => builder.append_value(v.to_string()),
            Err(_) => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn encode_utf8(rows: &[Row], idx: usize) -> Arc<dyn Array> {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    for row in rows {
        match row.try_get::<_, String>(idx) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
