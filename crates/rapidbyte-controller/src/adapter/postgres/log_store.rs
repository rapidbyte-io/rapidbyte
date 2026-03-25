use std::fmt::Write as _;

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::ports::log_store::{
    LogError, LogFilter, LogStore, LogStreamFilter, StoredLogEntry,
};
use crate::traits::{EventStream, PaginatedList};

/// Parse a log cursor in the format `<rfc3339_timestamp>|<id>`.
fn parse_log_cursor(cursor: &str) -> Result<(chrono::DateTime<chrono::Utc>, i64), LogError> {
    let parts: Vec<&str> = cursor.splitn(2, '|').collect();
    if parts.len() != 2 {
        return Err(LogError::Database("invalid log cursor format".into()));
    }
    let ts = parts[0]
        .parse::<chrono::DateTime<chrono::Utc>>()
        .map_err(|_| LogError::Database("invalid cursor timestamp".into()))?;
    let id = parts[1]
        .parse::<i64>()
        .map_err(|_| LogError::Database("invalid cursor id".into()))?;
    Ok((ts, id))
}

pub struct PgLogStore {
    pool: PgPool,
}

impl PgLogStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LogStore for PgLogStore {
    async fn query(&self, filter: LogFilter) -> Result<PaginatedList<StoredLogEntry>, LogError> {
        // Cursor-based pagination using (timestamp, id) as the composite sort key.
        // Both ORDER BY and the cursor predicate use the same key to guarantee
        // stable paging even when timestamps are non-monotonic (backfills, clock skew).
        // Cursor format: "<rfc3339_timestamp>|<id>"
        let limit = filter.limit;

        let mut query = String::from(
            r#"SELECT id, run_id, pipeline, "timestamp", level, message, fields
               FROM run_logs WHERE pipeline = $1"#,
        );
        let mut param_idx = 2usize;

        if filter.run_id.is_some() {
            write!(query, " AND run_id = ${param_idx}").unwrap();
            param_idx += 1;
        }
        if filter.cursor.is_some() {
            write!(
                query,
                r#" AND ("timestamp", id) < (${}, ${})"#,
                param_idx,
                param_idx + 1
            )
            .unwrap();
            param_idx += 2;
        }

        write!(
            query,
            r#" ORDER BY "timestamp" DESC, id DESC LIMIT ${param_idx}"#
        )
        .unwrap();

        let mut q = sqlx::query(&query).bind(&filter.pipeline);
        if let Some(ref run_id) = filter.run_id {
            q = q.bind(run_id);
        }
        if let Some(ref cursor) = filter.cursor {
            let (cursor_ts, cursor_id) = parse_log_cursor(cursor)?;
            q = q.bind(cursor_ts);
            q = q.bind(cursor_id);
        }
        q = q.bind(i64::from(limit) + 1);

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| LogError::Database(e.to_string()))?;

        let has_more = rows.len() > limit as usize;
        let rows_to_take = rows.len().min(limit as usize);

        let mut entries: Vec<StoredLogEntry> = Vec::with_capacity(rows_to_take);
        let mut last_cursor_parts: Option<(chrono::DateTime<chrono::Utc>, i64)> = None;
        for row in rows.into_iter().take(rows_to_take) {
            let ts: chrono::DateTime<chrono::Utc> = row.get("timestamp");
            let id: i64 = row.get("id");
            last_cursor_parts = Some((ts, id));
            entries.push(StoredLogEntry {
                timestamp: ts,
                level: row.get("level"),
                pipeline: row.get("pipeline"),
                run_id: row.get("run_id"),
                message: row.get("message"),
                fields: row.get("fields"),
            });
        }

        let next_cursor = if has_more {
            last_cursor_parts.map(|(ts, id)| format!("{}|{id}", ts.to_rfc3339()))
        } else {
            None
        };

        Ok(PaginatedList {
            items: entries,
            next_cursor,
        })
    }

    async fn subscribe(
        &self,
        _filter: LogStreamFilter,
    ) -> Result<EventStream<StoredLogEntry>, LogError> {
        // Real-time log streaming via LISTEN/NOTIFY is not yet implemented.
        let stream = futures::stream::empty();
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_cursor() {
        let (ts, id) = parse_log_cursor("2025-03-19T14:30:00+00:00|42").unwrap();
        assert_eq!(id, 42);
        assert_eq!(ts.timestamp(), 1_742_394_600);
    }

    #[test]
    fn parse_cursor_missing_separator() {
        assert!(parse_log_cursor("no-separator").is_err());
    }

    #[test]
    fn parse_cursor_bad_id() {
        assert!(parse_log_cursor("2025-03-19T14:30:00+00:00|notanumber").is_err());
    }

    #[test]
    fn parse_cursor_bad_timestamp() {
        assert!(parse_log_cursor("not-a-date|42").is_err());
    }

    #[test]
    fn cursor_round_trip() {
        let ts = chrono::Utc::now();
        let id = 99i64;
        let cursor = format!("{}|{id}", ts.to_rfc3339());
        let (parsed_ts, parsed_id) = parse_log_cursor(&cursor).unwrap();
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_ts, ts);
    }
}
