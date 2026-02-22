//! Typed Arrow column helpers used by INSERT/COPY write paths.

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
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
}
