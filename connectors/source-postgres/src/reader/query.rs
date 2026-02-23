//! SQL and cursor parameter helpers for source incremental reads.

use chrono::{DateTime, SecondsFormat, Utc};
use pg_escape::quote_identifier;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::{ColumnSchema, CursorType};
use tokio_postgres::types::ToSql;

use crate::schema::needs_text_cast;

#[derive(Debug)]
pub(crate) enum CursorBindParam {
    Int64(i64),
    Text(String),
    Json(serde_json::Value),
}

impl CursorBindParam {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(v) => v,
            Self::Text(v) => v,
            Self::Json(v) => v,
        }
    }
}

pub(crate) struct CursorQuery {
    pub(crate) sql: String,
    pub(crate) bind: Option<CursorBindParam>,
}

pub(crate) fn effective_cursor_type(
    cursor_type: CursorType,
    cursor_column_arrow_type: &str,
) -> CursorType {
    match (cursor_type, cursor_column_arrow_type) {
        // Host state currently stores incremental cursor values as UTF-8.
        // If the actual cursor column is numeric, bind as Int64 for a valid
        // typed predicate instead of comparing against text.
        (CursorType::Utf8, "Int16" | "Int32" | "Int64") => CursorType::Int64,
        // Timestamp columns need ::timestamptz cast, not ::text.
        (CursorType::Utf8, "TimestampMicros") => CursorType::TimestampMicros,
        _ => cursor_type,
    }
}

pub(crate) fn build_base_query(
    ctx: &Context,
    stream: &StreamContext,
    columns: &[ColumnSchema],
    pg_types: &[String],
) -> Result<CursorQuery, String> {
    let col_list = columns
        .iter()
        .zip(pg_types.iter())
        .map(|(c, pg_type)| {
            let ident = quote_identifier(&c.name);
            if needs_text_cast(pg_type) {
                format!("{}::text AS {}", ident, ident)
            } else {
                ident.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    if let (SyncMode::Incremental, Some(ci)) = (&stream.sync_mode, &stream.cursor_info) {
        let table_name = quote_identifier(&stream.stream_name);
        let cursor_field = quote_identifier(&ci.cursor_field);
        let cursor_column_arrow_type = columns
            .iter()
            .find(|c| c.name == ci.cursor_field)
            .map(|c| c.data_type.as_str())
            .unwrap_or("Utf8");

        if let Some(last_value) = ci.last_value.as_ref() {
            if matches!(last_value, CursorValue::Null) {
                ctx.log(
                    LogLevel::Info,
                    &format!(
                        "Incremental read (null prior cursor): {} ORDER BY {}",
                        stream.stream_name, cursor_field
                    ),
                );
                return Ok(CursorQuery {
                    sql: format!(
                        "SELECT {} FROM {} ORDER BY {}",
                        col_list, table_name, cursor_field
                    ),
                    bind: None,
                });
            }

            let resolved_cursor_type =
                effective_cursor_type(ci.cursor_type, cursor_column_arrow_type);
            if resolved_cursor_type != ci.cursor_type {
                ctx.log(
                    LogLevel::Debug,
                    &format!(
                        "Incremental cursor type adjusted: stream={} field={} declared={:?} inferred={} effective={:?}",
                        stream.stream_name,
                        ci.cursor_field,
                        ci.cursor_type,
                        cursor_column_arrow_type,
                        resolved_cursor_type
                    ),
                );
            }

            let (bind, cast) =
                cursor_bind_param(&resolved_cursor_type, last_value).map_err(|e| {
                    format!(
                        "Invalid incremental cursor value for stream '{}' field '{}': {e}",
                        stream.stream_name, ci.cursor_field
                    )
                })?;

            ctx.log(
                LogLevel::Info,
                &format!(
                    "Incremental read: {} WHERE {} > $1::{}",
                    stream.stream_name, cursor_field, cast
                ),
            );

            return Ok(CursorQuery {
                sql: format!(
                    "SELECT {} FROM {} WHERE {} > $1::{} ORDER BY {}",
                    col_list, table_name, cursor_field, cast, cursor_field
                ),
                bind: Some(bind),
            });
        }

        ctx.log(
            LogLevel::Info,
            &format!(
                "Incremental read (no prior cursor): {} ORDER BY {}",
                stream.stream_name, cursor_field
            ),
        );
        return Ok(CursorQuery {
            sql: format!(
                "SELECT {} FROM {} ORDER BY {}",
                col_list, table_name, cursor_field
            ),
            bind: None,
        });
    }

    Ok(CursorQuery {
        sql: format!(
            "SELECT {} FROM {}",
            col_list,
            quote_identifier(&stream.stream_name)
        ),
        bind: None,
    })
}

pub(crate) fn cursor_bind_param(
    cursor_type: &CursorType,
    value: &CursorValue,
) -> Result<(CursorBindParam, &'static str), String> {
    match cursor_type {
        CursorType::Int64 => {
            let n = match value {
                CursorValue::Int64(v) => *v,
                CursorValue::Utf8(v) => v
                    .parse::<i64>()
                    .map_err(|e| format!("failed to parse '{}' as i64: {e}", v))?,
                CursorValue::Decimal { value, .. } => value
                    .parse::<i64>()
                    .map_err(|e| format!("failed to parse decimal '{}' as i64: {e}", value))?,
                _ => return Err("cursor value is incompatible with int64 cursor type".to_string()),
            };
            Ok((CursorBindParam::Int64(n), "bigint"))
        }
        CursorType::Utf8 => {
            let text = match value {
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::Int64(v) => v.to_string(),
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Json(v) => v.to_string(),
                CursorValue::Lsn(v) => v.clone(),
                CursorValue::Null => return Err("null cursor cannot be used as a predicate".to_string()),
            };
            Ok((CursorBindParam::Text(text), "text"))
        }
        CursorType::TimestampMillis => {
            let ts = match value {
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::Int64(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                _ => return Err("cursor value is incompatible with timestamp_millis cursor type".to_string()),
            };
            // Double-cast: bind as text (tokio-postgres supports String->text),
            // then PG casts text->timestamp for the comparison.
            Ok((CursorBindParam::Text(ts), "text::timestamp"))
        }
        CursorType::TimestampMicros => {
            let ts = match value {
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Int64(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                _ => return Err("cursor value is incompatible with timestamp_micros cursor type".to_string()),
            };
            // Double-cast: bind as text (tokio-postgres supports String->text),
            // then PG casts text->timestamp for the comparison.
            Ok((CursorBindParam::Text(ts), "text::timestamp"))
        }
        CursorType::Decimal => {
            let decimal = match value {
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::Int64(v) => v.to_string(),
                _ => return Err("cursor value is incompatible with decimal cursor type".to_string()),
            };
            Ok((CursorBindParam::Text(decimal), "numeric"))
        }
        CursorType::Json => {
            let json = match value {
                CursorValue::Json(v) => v.clone(),
                CursorValue::Utf8(v) => serde_json::from_str::<serde_json::Value>(v)
                    .map_err(|e| format!("failed to parse '{}' as json: {e}", v))?,
                CursorValue::Null => serde_json::Value::Null,
                _ => return Err("cursor value is incompatible with json cursor type".to_string()),
            };
            Ok((CursorBindParam::Json(json), "jsonb"))
        }
        CursorType::Lsn => {
            // LSN cursors are used in CDC mode and are not applicable to
            // incremental queries. Treat as text if encountered here.
            let text = match value {
                CursorValue::Lsn(v) => v.clone(),
                CursorValue::Utf8(v) => v.clone(),
                _ => return Err("cursor value is incompatible with lsn cursor type".to_string()),
            };
            Ok((CursorBindParam::Text(text), "pg_lsn"))
        }
    }
}

pub(crate) fn timestamp_millis_to_rfc3339(ms: i64) -> Result<String, String> {
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| format!("invalid timestamp millis value: {}", ms))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(crate) fn timestamp_micros_to_rfc3339(us: i64) -> Result<String, String> {
    let secs = us.div_euclid(1_000_000);
    let micros = us.rem_euclid(1_000_000) as u32;
    let nanos = micros * 1_000;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| format!("invalid timestamp micros value: {}", us))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::protocol::{
        ArrowDataType, CursorInfo, StreamContext, StreamLimits, StreamPolicies, SyncMode,
    };

    fn columns_for_cursor() -> Vec<ColumnSchema> {
        vec![
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
        ]
    }

    fn base_context() -> StreamContext {
        StreamContext {
            stream_name: "users".to_string(),
            schema: rapidbyte_sdk::protocol::SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
        }
    }

    #[test]
    fn effective_cursor_type_promotes_numeric_utf8() {
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, "Int64"),
            CursorType::Int64
        );
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, "Utf8"),
            CursorType::Utf8
        );
    }

    #[test]
    fn effective_cursor_type_promotes_timestamp_utf8() {
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, "TimestampMicros"),
            CursorType::TimestampMicros
        );
    }

    #[test]
    fn cursor_bind_param_parses_int64_from_utf8() {
        let (bind, cast) = cursor_bind_param(&CursorType::Int64, &CursorValue::Utf8("42".into()))
            .expect("bind should parse");
        match bind {
            CursorBindParam::Int64(v) => assert_eq!(v, 42),
            _ => panic!("expected int64 bind"),
        }
        assert_eq!(cast, "bigint");
    }

    #[test]
    fn cursor_bind_param_rejects_bad_int64() {
        let err = cursor_bind_param(
            &CursorType::Int64,
            &CursorValue::Utf8("not_an_int".into()),
        )
        .expect_err("invalid int64 should fail");
        assert!(err.contains("failed to parse 'not_an_int' as i64"));
    }

    #[test]
    fn build_base_query_full_refresh_quotes_identifiers() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.stream_name = "User".to_string();
        let columns = vec![ColumnSchema {
            name: "select".to_string(),
            data_type: ArrowDataType::Utf8,
            nullable: true,
        }];
        let pg_types = vec!["text".to_string()];
        let query = build_base_query(&ctx, &stream, &columns, &pg_types).expect("query should build");
        assert_eq!(query.sql, "SELECT \"select\" FROM \"User\"");
        assert!(query.bind.is_none());
    }

    #[test]
    fn build_base_query_incremental_with_bind() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            cursor_type: CursorType::Int64,
            last_value: Some(CursorValue::Int64(7)),
        });

        let pg_types = vec!["bigint".to_string(), "text".to_string()];
        let query =
            build_base_query(&ctx, &stream, &columns_for_cursor(), &pg_types).expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT id, name FROM users WHERE id > $1::bigint ORDER BY id"
        );
        assert!(query.bind.is_some());
    }

    #[test]
    fn build_base_query_applies_text_cast_for_uuid() {
        let ctx = Context::new("source-postgres", "");
        let stream = base_context();
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "external_id".to_string(),
                data_type: ArrowDataType::Utf8,
                nullable: true,
            },
        ];
        let pg_types = vec!["bigint".to_string(), "uuid".to_string()];
        let query = build_base_query(&ctx, &stream, &columns, &pg_types).expect("query should build");
        // uuid needs ::text cast, bigint does not
        assert!(query.sql.contains("\"external_id\"::text AS \"external_id\""));
        assert!(!query.sql.contains("\"id\"::text"));
    }
}
