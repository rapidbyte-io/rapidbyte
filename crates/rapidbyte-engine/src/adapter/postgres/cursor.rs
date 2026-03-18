//! `CursorRepository` implementation for [`PgBackend`].

use async_trait::async_trait;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};

use crate::domain::ports::{CursorRepository, RepositoryError};

use super::PgBackend;

#[async_trait]
impl CursorRepository for PgBackend {
    async fn get(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<Option<CursorState>, RepositoryError> {
        let row = sqlx::query_as::<_, (Option<String>, Option<String>, String)>(
            super::queries::GET_CURSOR,
        )
        .bind(pipeline.as_str())
        .bind(stream.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::other)?;

        Ok(
            row.map(|(cursor_field, cursor_value, updated_at)| CursorState {
                cursor_field,
                cursor_value,
                updated_at,
            }),
        )
    }

    async fn set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (pipeline, stream) \
             DO UPDATE SET cursor_field = $3, cursor_value = $4, updated_at = now()",
        )
        .bind(pipeline.as_str())
        .bind(stream.as_str())
        .bind(&cursor.cursor_field)
        .bind(&cursor.cursor_value)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::other)?;

        Ok(())
    }

    async fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool, RepositoryError> {
        let rows_affected = match expected {
            Some(expected_val) => sqlx::query(
                "UPDATE sync_cursors SET cursor_value = $1, updated_at = now() \
                     WHERE pipeline = $2 AND stream = $3 AND cursor_value = $4",
            )
            .bind(new_value)
            .bind(pipeline.as_str())
            .bind(stream.as_str())
            .bind(expected_val)
            .execute(&self.pool)
            .await
            .map_err(RepositoryError::other)?
            .rows_affected(),
            None => sqlx::query(
                "INSERT INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
                     VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING",
            )
            .bind(pipeline.as_str())
            .bind(stream.as_str())
            .bind(new_value)
            .execute(&self.pool)
            .await
            .map_err(RepositoryError::other)?
            .rows_affected(),
        };

        Ok(rows_affected > 0)
    }
}
