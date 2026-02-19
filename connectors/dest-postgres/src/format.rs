use std::fmt::Write as FmtWrite;
use std::io::Write;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

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
pub(crate) fn downcast_columns<'a>(batch: &'a RecordBatch, active_cols: &[usize]) -> Vec<TypedCol<'a>> {
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

/// Write a SQL literal for the value at `row_idx` directly into `buf`.
/// No heap allocation — writes in-place via `std::fmt::Write`.
pub(crate) fn write_sql_value(buf: &mut String, col: &TypedCol, row_idx: usize) {
    match col {
        TypedCol::Null => buf.push_str("NULL"),
        TypedCol::Int16(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                let _ = write!(buf, "{}", arr.value(row_idx));
            }
        }
        TypedCol::Int32(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                let _ = write!(buf, "{}", arr.value(row_idx));
            }
        }
        TypedCol::Int64(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                let _ = write!(buf, "{}", arr.value(row_idx));
            }
        }
        TypedCol::Float32(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                let v = arr.value(row_idx);
                if v.is_nan() {
                    buf.push_str("'NaN'::real");
                } else if v.is_infinite() {
                    if v > 0.0 {
                        buf.push_str("'Infinity'::real");
                    } else {
                        buf.push_str("'-Infinity'::real");
                    }
                } else {
                    let _ = write!(buf, "{}", v);
                }
            }
        }
        TypedCol::Float64(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                let v = arr.value(row_idx);
                if v.is_nan() {
                    buf.push_str("'NaN'::double precision");
                } else if v.is_infinite() {
                    if v > 0.0 {
                        buf.push_str("'Infinity'::double precision");
                    } else {
                        buf.push_str("'-Infinity'::double precision");
                    }
                } else {
                    let _ = write!(buf, "{}", v);
                }
            }
        }
        TypedCol::Boolean(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else if arr.value(row_idx) {
                buf.push_str("TRUE");
            } else {
                buf.push_str("FALSE");
            }
        }
        TypedCol::Utf8(arr) => {
            if arr.is_null(row_idx) {
                buf.push_str("NULL");
            } else {
                buf.push('\'');
                for ch in arr.value(row_idx).chars() {
                    match ch {
                        '\'' => buf.push_str("''"),
                        '\0' => {} // strip null bytes
                        _ => buf.push(ch),
                    }
                }
                buf.push('\'');
            }
        }
    }
}

/// Format an Arrow array value at a given row index for COPY text format.
///
/// COPY text format rules:
/// - NULL: `\N`
/// - Strings: backslash-escape `\`, tab, newline, carriage return; strip null bytes
/// - Booleans: `t` / `f`
/// - Numbers: decimal representation (NaN, Infinity as literals)
pub(crate) fn format_copy_value(buf: &mut Vec<u8>, col: &dyn Array, row_idx: usize) {
    if col.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }

    match col.data_type() {
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
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
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
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
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            buf.push(if arr.value(row_idx) { b't' } else { b'f' });
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // COPY text format: escape backslash, tab, newline, CR; strip null bytes
            for byte in val.bytes() {
                match byte {
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    0 => {} // skip null bytes
                    _ => buf.push(byte),
                }
            }
        }
        _ => {
            buf.extend_from_slice(b"\\N");
        }
    }
}
