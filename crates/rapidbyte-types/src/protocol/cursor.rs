use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CursorType {
    Int64,
    Utf8,
    TimestampMillis,
    TimestampMicros,
    Decimal,
    Json,
    Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CursorValue {
    Null,
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    TimestampMicros(i64),
    Decimal { value: String, scale: i32 },
    Json(serde_json::Value),
    Lsn(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorInfo {
    pub cursor_field: String,
    pub cursor_type: CursorType,
    pub last_value: Option<CursorValue>,
}
