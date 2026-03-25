use async_trait::async_trait;
use sqlx::PgPool;

use crate::domain::ports::cursor_store::{
    CursorError, CursorStore, PipelineState, StreamCursor, SyncTimestamp,
};

#[derive(sqlx::FromRow)]
struct CursorRow {
    stream: String,
    cursor_field: String,
    cursor_value: String,
    updated_at: chrono::DateTime<chrono::Utc>,
}

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
        let rows: Vec<CursorRow> = sqlx::query_as::<_, CursorRow>(
            "SELECT stream, cursor_field, cursor_value, updated_at \
             FROM cursors \
             WHERE pipeline = $1 \
             ORDER BY stream",
        )
        .bind(pipeline)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CursorError::Database(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| StreamCursor {
                stream: row.stream,
                cursor_field: row.cursor_field,
                cursor_value: row.cursor_value,
                updated_at: row.updated_at,
            })
            .collect())
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
        if pipelines.is_empty() {
            return Ok(vec![]);
        }

        let rows = sqlx::query(
            r#"SELECT pipeline, MAX(updated_at) as last_sync_at
               FROM cursors
               WHERE pipeline = ANY($1)
               GROUP BY pipeline"#,
        )
        .bind(pipelines)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CursorError::Database(e.to_string()))?;

        use sqlx::Row;
        let mut result_map: std::collections::HashMap<
            String,
            Option<chrono::DateTime<chrono::Utc>>,
        > = rows
            .into_iter()
            .map(|row| {
                let pipeline: String = row.get("pipeline");
                let last_sync_at: Option<chrono::DateTime<chrono::Utc>> = row.get("last_sync_at");
                (pipeline, last_sync_at)
            })
            .collect();

        // Ensure all requested pipelines are in the result (even if no cursors exist).
        Ok(pipelines
            .iter()
            .map(|p| SyncTimestamp {
                pipeline: p.clone(),
                last_sync_at: result_map.remove(p).flatten(),
            })
            .collect())
    }

    async fn get_pipeline_state(
        &self,
        pipeline: &str,
    ) -> Result<Option<PipelineState>, CursorError> {
        let row = sqlx::query("SELECT state FROM pipeline_states WHERE pipeline = $1")
            .bind(pipeline)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| CursorError::Database(e.to_string()))?;
        use sqlx::Row;
        use std::str::FromStr;
        Ok(row.and_then(|r| {
            let s: String = r.get("state");
            PipelineState::from_str(&s).ok()
        }))
    }

    async fn set_pipeline_state(
        &self,
        pipeline: &str,
        state: PipelineState,
    ) -> Result<(), CursorError> {
        sqlx::query(
            "INSERT INTO pipeline_states (pipeline, state, updated_at) VALUES ($1, $2, now()) \
             ON CONFLICT (pipeline) DO UPDATE SET state = EXCLUDED.state, updated_at = now()",
        )
        .bind(pipeline)
        .bind(state.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| CursorError::Database(e.to_string()))?;
        Ok(())
    }
}
