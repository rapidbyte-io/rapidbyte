use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::ports::log_store::{
    LogError, LogFilter, LogStore, LogStreamFilter, StoredLogEntry,
};
use crate::traits::{EventStream, PaginatedList};

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
        let limit = filter.limit;

        let mut query = String::from(
            r#"SELECT id, run_id, pipeline, "timestamp", level, message, fields
               FROM run_logs WHERE pipeline = $1"#,
        );
        let mut param_idx = 2usize;

        if filter.run_id.is_some() {
            query.push_str(&format!(" AND run_id = ${param_idx}"));
            param_idx += 1;
        }
        if filter.cursor.is_some() {
            query.push_str(&format!(" AND id < ${param_idx}"));
            param_idx += 1;
        }

        query.push_str(&format!(r#" ORDER BY "timestamp" DESC LIMIT ${param_idx}"#));

        let mut q = sqlx::query(&query).bind(&filter.pipeline);
        if let Some(ref run_id) = filter.run_id {
            q = q.bind(run_id);
        }
        if let Some(ref cursor) = filter.cursor {
            let cursor_id: i64 = cursor
                .parse()
                .map_err(|_| LogError::Database("invalid cursor".into()))?;
            q = q.bind(cursor_id);
        }
        q = q.bind(i64::from(limit) + 1); // fetch one extra to detect next page

        let rows = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| LogError::Database(e.to_string()))?;

        let has_more = rows.len() > limit as usize;
        let rows_to_take = rows.len().min(limit as usize);

        let mut entries: Vec<StoredLogEntry> = Vec::with_capacity(rows_to_take);
        let mut last_id: Option<i64> = None;
        for row in rows.into_iter().take(rows_to_take) {
            last_id = Some(row.get::<i64, _>("id"));
            entries.push(StoredLogEntry {
                timestamp: row.get("timestamp"),
                level: row.get("level"),
                pipeline: row.get("pipeline"),
                run_id: row.get("run_id"),
                message: row.get("message"),
                fields: row.get("fields"),
            });
        }

        let next_cursor = if has_more {
            last_id.map(|id| id.to_string())
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
