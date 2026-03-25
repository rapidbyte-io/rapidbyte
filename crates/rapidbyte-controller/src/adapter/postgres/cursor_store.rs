use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::ports::cursor_store::{CursorError, CursorStore, StreamCursor, SyncTimestamp};

pub struct PgCursorStore {
    pool: PgPool,
}

impl PgCursorStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CursorStore for PgCursorStore {
    async fn get_cursors(&self, pipeline: &str) -> Result<Vec<StreamCursor>, CursorError> {
        let rows = sqlx::query(
            "SELECT stream, cursor_field, cursor_value, updated_at \
             FROM cursors \
             WHERE pipeline = $1 \
             ORDER BY stream",
        )
        .bind(pipeline)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CursorError::Database(e.to_string()))?;

        rows.iter()
            .map(|row| {
                let stream: String = row
                    .try_get("stream")
                    .map_err(|e| CursorError::Database(e.to_string()))?;
                let cursor_field: String = row
                    .try_get("cursor_field")
                    .map_err(|e| CursorError::Database(e.to_string()))?;
                let cursor_value: String = row
                    .try_get("cursor_value")
                    .map_err(|e| CursorError::Database(e.to_string()))?;
                let updated_at: chrono::DateTime<chrono::Utc> = row
                    .try_get("updated_at")
                    .map_err(|e| CursorError::Database(e.to_string()))?;
                Ok(StreamCursor {
                    stream,
                    cursor_field,
                    cursor_value,
                    updated_at,
                })
            })
            .collect()
    }

    async fn clear(&self, pipeline: &str, stream: Option<&str>) -> Result<u64, CursorError> {
        let result = if let Some(stream) = stream {
            sqlx::query("DELETE FROM cursors WHERE pipeline = $1 AND stream = $2")
                .bind(pipeline)
                .bind(stream)
                .execute(&self.pool)
                .await
        } else {
            sqlx::query("DELETE FROM cursors WHERE pipeline = $1")
                .bind(pipeline)
                .execute(&self.pool)
                .await
        };

        match result {
            Ok(r) => Ok(r.rows_affected()),
            Err(e) => Err(CursorError::Database(e.to_string())),
        }
    }

    async fn last_sync_times(
        &self,
        pipelines: &[String],
    ) -> Result<Vec<SyncTimestamp>, CursorError> {
        let mut results = Vec::with_capacity(pipelines.len());
        for pipeline in pipelines {
            let row = sqlx::query(
                "SELECT MAX(updated_at) AS last_sync_at \
                 FROM cursors \
                 WHERE pipeline = $1",
            )
            .bind(pipeline)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| CursorError::Database(e.to_string()))?;

            let last_sync_at = row
                .as_ref()
                .map(|r| {
                    r.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_sync_at")
                        .map_err(|e| CursorError::Database(e.to_string()))
                })
                .transpose()?
                .flatten();

            results.push(SyncTimestamp {
                pipeline: pipeline.clone(),
                last_sync_at,
            });
        }
        Ok(results)
    }
}
