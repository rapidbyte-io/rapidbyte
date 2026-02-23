//! Arrow RecordBatch decoding helpers for INSERT and COPY write paths.
//!
//! Provides typed column extraction (one downcast per column per batch),
//! SQL parameter value extraction for INSERT, COPY text serialization,
//! and shared helpers for column filtering and SQL building.

use std::collections::HashSet;
use std::io::Write;
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, TimestampMicrosecondArray,
};
use rapidbyte_sdk::arrow::datatypes::{DataType, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::*;
use tokio_postgres::types::ToSql;

/// Unix epoch date — base for Arrow Date32 day offsets.
static UNIX_EPOCH_DATE: LazyLock<NaiveDate> =
    LazyLock::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date is always valid"));

// ── Qualified name helper ────────────────────────────────────────────

/// Build a schema-qualified table name: `"schema"."table"`.
pub(crate) fn qualified_name(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
}

// ── Column filtering ─────────────────────────────────────────────────

/// Indices of Arrow columns that are not in the ignored set.
pub(crate) fn active_column_indices(
    arrow_schema: &Arc<Schema>,
    ignored_columns: &HashSet<String>,
) -> Vec<usize> {
    (0..arrow_schema.fields().len())
        .filter(|&i| !ignored_columns.contains(arrow_schema.field(i).name()))
        .collect()
}

/// Build ON CONFLICT clause for upsert mode. Returns None for non-upsert modes.
pub(crate) fn build_upsert_clause(
    write_mode: Option<&WriteMode>,
    arrow_schema: &Arc<Schema>,
    active_cols: &[usize],
) -> Option<String> {
    if let Some(WriteMode::Upsert { primary_key }) = write_mode {
        let pk_cols = primary_key
            .iter()
            .map(|k| quote_identifier(k))
            .collect::<Vec<_>>()
            .join(", ");

        let update_cols: Vec<String> = active_cols
            .iter()
            .map(|&i| arrow_schema.field(i).name())
            .filter(|name| !primary_key.contains(name))
            .map(|name| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_identifier(name),
                    quote_identifier(name)
                )
            })
            .collect();

        if update_cols.is_empty() {
            Some(format!(" ON CONFLICT ({}) DO NOTHING", pk_cols))
        } else {
            Some(format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                pk_cols,
                update_cols.join(", ")
            ))
        }
    } else {
        None
    }
}

/// Build type-null flags: true for columns whose type is incompatible (values forced to NULL).
pub(crate) fn type_null_flags(
    active_cols: &[usize],
    arrow_schema: &Schema,
    type_null_columns: &HashSet<String>,
) -> Vec<bool> {
    active_cols
        .iter()
        .map(|&i| type_null_columns.contains(arrow_schema.field(i).name()))
        .collect()
}

// ── TypedCol: pre-downcast Arrow columns ─────────────────────────────

/// Pre-downcast Arrow column reference. Eliminates per-cell `downcast_ref()` calls
/// by resolving the concrete array type once per column per batch.
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a rapidbyte_sdk::arrow::array::StringArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Date32(&'a Date32Array),
    Binary(&'a BinaryArray),
    Null,
}

/// Pre-downcast active columns from a RecordBatch into TypedCol references.
pub(crate) fn downcast_columns<'a>(
    batch: &'a RecordBatch,
    active_cols: &[usize],
) -> Result<Vec<TypedCol<'a>>, String> {
    active_cols
        .iter()
        .map(|&i| {
            let col = batch.column(i);
            match col.data_type() {
                DataType::Int16 => Ok(TypedCol::Int16(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int16Array)"))?)),
                DataType::Int32 => Ok(TypedCol::Int32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int32Array)"))?)),
                DataType::Int64 => Ok(TypedCol::Int64(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Int64Array)"))?)),
                DataType::Float32 => Ok(TypedCol::Float32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Float32Array)"))?)),
                DataType::Float64 => Ok(TypedCol::Float64(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Float64Array)"))?)),
                DataType::Boolean => Ok(TypedCol::Boolean(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected BooleanArray)"))?)),
                DataType::Utf8 => Ok(TypedCol::Utf8(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected StringArray)"))?)),
                DataType::Timestamp(rapidbyte_sdk::arrow::datatypes::TimeUnit::Microsecond, _) => {
                    Ok(TypedCol::TimestampMicros(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected TimestampMicrosecondArray)"))?))
                }
                DataType::Date32 => Ok(TypedCol::Date32(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected Date32Array)"))?)),
                DataType::Binary => Ok(TypedCol::Binary(col.as_any().downcast_ref().ok_or_else(|| format!("downcast failed for column {i} (expected BinaryArray)"))?)),
                _ => Ok(TypedCol::Null),
            }
        })
        .collect()
}

// ── SqlParamValue: typed INSERT bind parameters ──────────────────────

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
                let date = UNIX_EPOCH_DATE.checked_add_signed(chrono::Duration::days(days as i64));
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

// ── COPY text format serialization ───────────────────────────────────

/// Format a pre-downcast value at a given row index for COPY text format.
///
/// COPY text format rules:
/// - NULL: `\N`
/// - Strings: backslash-escape `\`, tab, newline, CR; strip null bytes
/// - Booleans: `t` / `f`
/// - Numbers: decimal representation (NaN, Infinity as literals)
pub(crate) fn format_copy_value(buf: &mut Vec<u8>, col: &TypedCol<'_>, row_idx: usize) {
    match col {
        TypedCol::Null => buf.extend_from_slice(b"\\N"),
        TypedCol::Int16(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        TypedCol::Int32(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        TypedCol::Int64(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        TypedCol::Float32(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        TypedCol::Float64(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        TypedCol::Boolean(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            buf.push(if arr.value(row_idx) { b't' } else { b'f' });
        }
        TypedCol::Utf8(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            for byte in arr.value(row_idx).bytes() {
                match byte {
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    0 => {}
                    _ => buf.push(byte),
                }
            }
        }
        TypedCol::TimestampMicros(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let micros = arr.value(row_idx);
            let secs = micros.div_euclid(1_000_000);
            let nsecs = (micros.rem_euclid(1_000_000) * 1_000) as u32;
            if let Some(dt) = DateTime::from_timestamp(secs, nsecs) {
                let _ = write!(buf, "{}", dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.f"));
            } else {
                buf.extend_from_slice(b"\\N");
            }
        }
        TypedCol::Date32(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            let days = arr.value(row_idx);
            if let Some(date) =
                UNIX_EPOCH_DATE.checked_add_signed(chrono::Duration::days(days as i64))
            {
                let _ = write!(buf, "{}", date);
            } else {
                buf.extend_from_slice(b"\\N");
            }
        }
        TypedCol::Binary(arr) => {
            if arr.is_null(row_idx) {
                buf.extend_from_slice(b"\\N");
                return;
            }
            buf.extend_from_slice(b"\\\\x");
            for byte in arr.value(row_idx) {
                let _ = write!(buf, "{:02x}", byte);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
        StringArray, TimestampMicrosecondArray,
    };
    use rapidbyte_sdk::arrow::datatypes::{Field, TimeUnit};

    // ── qualified_name ───────────────────────────────────────────────

    #[test]
    fn qualified_name_formats_correctly() {
        assert_eq!(qualified_name("raw", "users"), "raw.users");
        assert_eq!(qualified_name("public", "orders"), "public.orders");
    }

    // ── active_column_indices ────────────────────────────────────────

    #[test]
    fn active_column_indices_respects_ignored_set() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ignored = HashSet::from(["name".to_string()]);
        assert_eq!(active_column_indices(&schema, &ignored), vec![0]);
    }

    // ── build_upsert_clause ──────────────────────────────────────────

    #[test]
    fn build_upsert_clause_generates_on_conflict() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let clause = build_upsert_clause(Some(&mode), &schema, &[0, 1]).expect("upsert clause");
        assert_eq!(
            clause,
            " ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        );
    }

    #[test]
    fn build_upsert_clause_none_for_append() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(build_upsert_clause(Some(&WriteMode::Append), &schema, &[0]).is_none());
        assert!(build_upsert_clause(None, &schema, &[0]).is_none());
    }

    // ── type_null_flags ──────────────────────────────────────────────

    #[test]
    fn type_null_flags_marks_incompatible_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]);
        let nulls = HashSet::from(["name".to_string()]);
        let flags = type_null_flags(&[0, 1, 2], &schema, &nulls);
        assert_eq!(flags, vec![false, true, false]);
    }

    // ── TypedCol + SqlParamValue ─────────────────────────────────────

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
            _ => panic!("expected Text(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Text(None) => {}
            _ => panic!("expected Text(None)"),
        }
    }

    #[test]
    fn downcast_columns_handles_timestamp_date_binary() {
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
        let cols = downcast_columns(&batch, &[0, 1, 2]).unwrap();
        assert!(matches!(cols[0], TypedCol::TimestampMicros(_)));
        assert!(matches!(cols[1], TypedCol::Date32(_)));
        assert!(matches!(cols[2], TypedCol::Binary(_)));
    }

    #[test]
    fn sql_param_value_handles_timestamp() {
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
        let arr = Date32Array::from(vec![Some(19737), None]);
        let col = TypedCol::Date32(&arr);
        match sql_param_value(&col, 0) {
            SqlParamValue::Date(Some(d)) => assert_eq!(d.to_string(), "2024-01-15"),
            _ => panic!("expected Date(Some)"),
        }
        match sql_param_value(&col, 1) {
            SqlParamValue::Date(None) => {}
            _ => panic!("expected Date(None)"),
        }
    }

    #[test]
    fn sql_param_value_handles_binary() {
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

    // ── COPY text format ─────────────────────────────────────────────

    #[test]
    fn format_copy_value_escapes_utf8_text() {
        let arr = StringArray::from(vec![Some("a\tb\nc\rd\\e\0f")]);
        let col = TypedCol::Utf8(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "a\\tb\\nc\\rd\\\\ef");
    }

    #[test]
    fn format_copy_value_formats_float_specials() {
        let arr = Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(-f64::INFINITY),
        ]);
        let col = TypedCol::Float64(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "NaN");
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 1);
        assert_eq!(String::from_utf8(buf).unwrap(), "Infinity");
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 2);
        assert_eq!(String::from_utf8(buf).unwrap(), "-Infinity");
    }

    #[test]
    fn format_copy_timestamp() {
        let micros = 1705312200_i64 * 1_000_000;
        let arr = TimestampMicrosecondArray::from(vec![Some(micros)]);
        let col = TypedCol::TimestampMicros(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        let result = String::from_utf8(buf).unwrap();
        assert!(result.starts_with("2024-01-15 09:50:00"), "got: {}", result);
    }

    #[test]
    fn format_copy_date32() {
        let arr = Date32Array::from(vec![Some(19737)]);
        let col = TypedCol::Date32(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "2024-01-15");
    }

    #[test]
    fn format_copy_binary_hex() {
        let arr = BinaryArray::from(vec![Some(&[0xDE_u8, 0xAD, 0xBE, 0xEF] as &[u8])]);
        let col = TypedCol::Binary(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "\\\\xdeadbeef");
    }

    #[test]
    fn format_copy_nulls() {
        let arr = TimestampMicrosecondArray::from(vec![None as Option<i64>]);
        let col = TypedCol::TimestampMicros(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "\\N");
    }

    #[test]
    fn format_copy_int_types() {
        let arr16 = Int16Array::from(vec![Some(-32768_i16)]);
        let col = TypedCol::Int16(&arr16);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "-32768");
    }

    #[test]
    fn format_copy_boolean() {
        let arr = BooleanArray::from(vec![Some(true), Some(false)]);
        let col = TypedCol::Boolean(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "t");
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 1);
        assert_eq!(String::from_utf8(buf).unwrap(), "f");
    }

    #[test]
    fn format_copy_float32_specials() {
        let arr = Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
        ]);
        let col = TypedCol::Float32(&arr);
        let mut buf = Vec::new();
        format_copy_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).unwrap(), "NaN");
    }
}
