//! `DlqRepository` implementation for [`PgBackend`].

use async_trait::async_trait;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::PipelineId;

use crate::domain::ports::{DlqRepository, RepositoryError};

use super::PgBackend;

#[async_trait]
impl DlqRepository for PgBackend {
    async fn insert(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> Result<u64, RepositoryError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await.map_err(RepositoryError::other)?;

        for record in records {
            sqlx::query(super::queries::INSERT_DLQ_RECORD)
                .bind(pipeline.as_str())
                .bind(run_id)
                .bind(&record.stream_name)
                .bind(&record.record_json)
                .bind(&record.error_message)
                .bind(record.error_category.as_str())
                .bind(record.failed_at.as_str())
                .execute(&mut *tx)
                .await
                .map_err(RepositoryError::other)?;
        }

        tx.commit().await.map_err(RepositoryError::other)?;

        Ok(records.len() as u64)
    }
}
