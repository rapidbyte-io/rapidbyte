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
        sqlx::query(super::queries::SET_CURSOR)
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
            Some(expected_val) => sqlx::query(super::queries::CAS_UPDATE_CURSOR)
                .bind(new_value)
                .bind(pipeline.as_str())
                .bind(stream.as_str())
                .bind(expected_val)
                .execute(&self.pool)
                .await
                .map_err(RepositoryError::other)?
                .rows_affected(),
            None => sqlx::query(super::queries::CAS_INSERT_CURSOR)
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
