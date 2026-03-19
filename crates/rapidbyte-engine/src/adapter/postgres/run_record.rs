//! `RunRecordRepository` implementation for [`PgBackend`].

use async_trait::async_trait;
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};

use crate::domain::ports::{RepositoryError, RunRecordRepository};

use super::PgBackend;

#[async_trait]
impl RunRecordRepository for PgBackend {
    async fn start(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<i64, RepositoryError> {
        let (id,) = sqlx::query_as::<_, (i64,)>(super::queries::START_RUN)
            .bind(pipeline.as_str())
            .bind(stream.as_str())
            .bind(RunStatus::Running.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(RepositoryError::other)?;

        Ok(id)
    }

    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    async fn complete(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> Result<(), RepositoryError> {
        let result = sqlx::query(super::queries::COMPLETE_RUN)
            .bind(status.as_str())
            .bind(stats.records_read as i64)
            .bind(stats.records_written as i64)
            .bind(stats.bytes_read as i64)
            .bind(stats.bytes_written as i64)
            .bind(&stats.error_message)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .map_err(RepositoryError::other)?;

        if result.rows_affected() == 0 {
            return Err(RepositoryError::other(std::io::Error::other(format!(
                "run {run_id} not found"
            ))));
        }

        Ok(())
    }
}
