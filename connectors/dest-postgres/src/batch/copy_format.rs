//! COPY text-format value serialization helpers.

use std::io::Write;

use arrow::array::Array;
use chrono::{DateTime, NaiveDate};
use crate::batch::typed_col::TypedCol;

/// Format a pre-downcast value at a given row index for COPY text format.
///
/// COPY text format rules:
/// - NULL: `\\N`
/// - Strings: backslash-escape `\\`, tab, newline, carriage return; strip null bytes
/// - Booleans: `t` / `f`
/// - Numbers: decimal representation (NaN, Infinity as literals)
pub(crate) fn format_copy_typed_value(buf: &mut Vec<u8>, col: &TypedCol<'_>, row_idx: usize) {
    match col {
        TypedCol::Null => {
            buf.extend_from_slice(b"\\N");
        }
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
            let val = arr.value(row_idx);
            // COPY text format: escape backslash, tab, newline, CR; strip null bytes.
            for byte in val.bytes() {
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
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
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
            // COPY text format for bytea: hex encoding with \\x prefix
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
    use arrow::array::{Float64Array, StringArray};

    #[test]
    fn format_copy_typed_value_escapes_utf8_text() {
        let arr = StringArray::from(vec![Some("a\tb\nc\rd\\e\0f")]);
        let col = TypedCol::Utf8(&arr);
        let mut buf = Vec::new();
        format_copy_typed_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "a\\tb\\nc\\rd\\\\ef");
    }

    #[test]
    fn format_copy_typed_value_formats_float_specials() {
        let arr = Float64Array::from(vec![Some(f64::NAN), Some(f64::INFINITY), Some(-f64::INFINITY)]);
        let col = TypedCol::Float64(&arr);

        let mut buf = Vec::new();
        format_copy_typed_value(&mut buf, &col, 0);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "NaN");

        let mut buf = Vec::new();
        format_copy_typed_value(&mut buf, &col, 1);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "Infinity");

        let mut buf = Vec::new();
        format_copy_typed_value(&mut buf, &col, 2);
        assert_eq!(String::from_utf8(buf).expect("utf8"), "-Infinity");
    }
}
