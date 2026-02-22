//! Typed Arrow column helpers used by INSERT/COPY write paths.

use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use tokio_postgres::types::ToSql;

/// Pre-downcast Arrow column reference. Eliminates per-cell `downcast_ref()` calls
/// by resolving the concrete array type once per column per batch.
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a arrow::array::StringArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Date32(&'a Date32Array),
    Binary(&'a BinaryArray),
    Null,
}

/// Pre-downcast active columns from a RecordBatch into TypedCol references.
pub(crate) fn downcast_columns<'a>(
    batch: &'a RecordBatch,
    active_cols: &[usize],
) -> Vec<TypedCol<'a>> {
    active_cols
        .iter()
        .map(|&i| {
            let col = batch.column(i);
            match col.data_type() {
                DataType::Int16 => TypedCol::Int16(col.as_any().downcast_ref().unwrap()),
                DataType::Int32 => TypedCol::Int32(col.as_any().downcast_ref().unwrap()),
                DataType::Int64 => TypedCol::Int64(col.as_any().downcast_ref().unwrap()),
                DataType::Float32 => TypedCol::Float32(col.as_any().downcast_ref().unwrap()),
                DataType::Float64 => TypedCol::Float64(col.as_any().downcast_ref().unwrap()),
                DataType::Boolean => TypedCol::Boolean(col.as_any().downcast_ref().unwrap()),
                DataType::Utf8 => TypedCol::Utf8(col.as_string::<i32>()),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                    TypedCol::TimestampMicros(col.as_any().downcast_ref().unwrap())
                }
                DataType::Date32 => TypedCol::Date32(col.as_any().downcast_ref().unwrap()),
                DataType::Binary => TypedCol::Binary(col.as_any().downcast_ref().unwrap()),
                _ => TypedCol::Null,
            }
        })
        .collect()
}

pub(crate) enum SqlParamValue<'a> {
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Boolean(Option<bool>),
    Text(Option<&'a str>),
    Timestamp(Option<NaiveDateTime>),
    Date(Option<NaiveDate>),
    Bytes(Option<&'a [u8]>),
}

impl<'a> SqlParamValue<'a> {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int16(v) => v,
            Self::Int32(v) => v,
            Self::Int64(v) => v,
            Self::Float32(v) => v,
            Self::Float64(v) => v,
            Self::Boolean(v) => v,
            Self::Text(v) => v,
            Self::Timestamp(v) => v,
            Self::Date(v) => v,
            Self::Bytes(v) => v,
        }
    }
}

pub(crate) fn sql_param_value<'a>(col: &'a TypedCol<'a>, row_idx: usize) -> SqlParamValue<'a> {
    match col {
        TypedCol::Null => SqlParamValue::Text(None),
        TypedCol::Int16(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Int16(None)
            } else {
                SqlParamValue::Int16(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Int32(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Int32(None)
            } else {
                SqlParamValue::Int32(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Int64(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Int64(None)
            } else {
                SqlParamValue::Int64(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Float32(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Float32(None)
            } else {
                SqlParamValue::Float32(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Float64(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Float64(None)
            } else {
                SqlParamValue::Float64(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Boolean(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Boolean(None)
            } else {
                SqlParamValue::Boolean(Some(arr.value(row_idx)))
            }
        }
        TypedCol::Utf8(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Text(None)
            } else {
                SqlParamValue::Text(Some(arr.value(row_idx)))
            }
        }
        TypedCol::TimestampMicros(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Timestamp(None)
            } else {
                let micros = arr.value(row_idx);
                let secs = micros.div_euclid(1_000_000);
                let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
                let dt = DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc());
                SqlParamValue::Timestamp(dt)
            }
        }
        TypedCol::Date32(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Date(None)
            } else {
                let days = arr.value(row_idx);
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch.checked_add_signed(chrono::Duration::days(days as i64));
                SqlParamValue::Date(date)
            }
        }
        TypedCol::Binary(arr) => {
            if arr.is_null(row_idx) {
                SqlParamValue::Bytes(None)
            } else {
                SqlParamValue::Bytes(Some(arr.value(row_idx)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn sql_param_value_handles_numeric_and_nulls() {
        let arr = Int32Array::from(vec![Some(7), None]);
        let col = TypedCol::Int32(&arr);

        match sql_param_value(&col, 0) {
            SqlParamValue::Int32(Some(v)) => assert_eq!(v, 7),
            _ => panic!("expected Int32(Some(7))"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Int32(None) => {}
            _ => panic!("expected Int32(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_utf8() {
        let arr = StringArray::from(vec![Some("alice"), None]);
        let col = TypedCol::Utf8(&arr);

        match sql_param_value(&col, 0) {
            SqlParamValue::Text(Some(v)) => assert_eq!(v, "alice"),
            _ => panic!("expected Text(Some('alice'))"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Text(None) => {}
            _ => panic!("expected Text(None)"),
        }
    }

    #[test]
    fn downcast_columns_handles_timestamp_date_binary() {
        use arrow::array::{BinaryArray, Date32Array, TimestampMicrosecondArray};
        use arrow::datatypes::{Field, Schema, TimeUnit};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("d", DataType::Date32, true),
            Field::new("b", DataType::Binary, true),
        ]));

        let ts_arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64)]);
        let d_arr = Date32Array::from(vec![Some(19737)]);
        let b_arr = BinaryArray::from(vec![Some(b"test" as &[u8])]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts_arr), Arc::new(d_arr), Arc::new(b_arr)],
        )
        .unwrap();

        let cols = downcast_columns(&batch, &[0, 1, 2]);
        assert!(matches!(cols[0], TypedCol::TimestampMicros(_)));
        assert!(matches!(cols[1], TypedCol::Date32(_)));
        assert!(matches!(cols[2], TypedCol::Binary(_)));
    }

    #[test]
    fn sql_param_value_handles_timestamp() {
        use arrow::array::TimestampMicrosecondArray;
        // 2024-01-15 09:50:00 UTC = 1705312200000000 micros
        let arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64), None]);
        let col = TypedCol::TimestampMicros(&arr);

        match sql_param_value(&col, 0) {
            SqlParamValue::Timestamp(Some(dt)) => {
                assert_eq!(
                    dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                    "2024-01-15 09:50:00"
                );
            }
            _ => panic!("expected Timestamp(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Timestamp(None) => {}
            _ => panic!("expected Timestamp(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_date32() {
        use arrow::array::Date32Array;
        // 2024-01-15 = 19737 days since epoch
        let arr = Date32Array::from(vec![Some(19737), None]);
        let col = TypedCol::Date32(&arr);

        match sql_param_value(&col, 0) {
            SqlParamValue::Date(Some(d)) => {
                assert_eq!(d.to_string(), "2024-01-15");
            }
            _ => panic!("expected Date(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Date(None) => {}
            _ => panic!("expected Date(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_binary() {
        use arrow::array::BinaryArray;
        let arr = BinaryArray::from(vec![Some(b"hello" as &[u8]), None]);
        let col = TypedCol::Binary(&arr);

        match sql_param_value(&col, 0) {
            SqlParamValue::Bytes(Some(b)) => assert_eq!(b, b"hello"),
            _ => panic!("expected Bytes(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Bytes(None) => {}
            _ => panic!("expected Bytes(None)"),
        }
    }
}
