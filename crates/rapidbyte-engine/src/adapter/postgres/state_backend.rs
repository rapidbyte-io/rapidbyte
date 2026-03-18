//! `StateBackend` (sync) implementation for [`PgBackend`].
//!
//! Delegates to the sync [`ClientPool`](super::ClientPool) so that the
//! legacy `StateBackend` trait (blocking I/O) works from any thread.

use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::state_error::StateError;

use super::PgBackend;

impl StateBackend for PgBackend {
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> rapidbyte_types::state_error::Result<Option<CursorState>> {
        let mut client = self.sync_pool.checkout()?;
        let rows = client
            .query(
                super::queries::GET_CURSOR,
                &[&pipeline.as_str(), &stream.as_str()],
            )
            .map_err(StateError::backend)?;

        match rows.first() {
            Some(row) => {
                let cursor_field: Option<String> = row.get(0);
                let cursor_value: Option<String> = row.get(1);
                let updated_at: String = row.get(2);
                Ok(Some(CursorState {
                    cursor_field,
                    cursor_value,
                    updated_at,
                }))
            }
            None => Ok(None),
        }
    }

    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> rapidbyte_types::state_error::Result<()> {
        let mut client = self.sync_pool.checkout()?;
        client
            .execute(
                "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
                 VALUES ($1, $2, $3, $4, now()) \
                 ON CONFLICT (pipeline, stream) \
                 DO UPDATE SET cursor_field = $3, cursor_value = $4, updated_at = now()",
                &[
                    &pipeline.as_str(),
                    &stream.as_str(),
                    &cursor.cursor_field,
                    &cursor.cursor_value,
                ],
            )
            .map_err(StateError::backend)?;
        Ok(())
    }

    fn start_run(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> rapidbyte_types::state_error::Result<i64> {
        let mut client = self.sync_pool.checkout()?;
        let row = client
            .query_one(
                super::queries::START_RUN,
                &[
                    &pipeline.as_str(),
                    &stream.as_str(),
                    &RunStatus::Running.as_str(),
                ],
            )
            .map_err(StateError::backend)?;
        Ok(row.get(0))
    }

    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    fn complete_run(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> rapidbyte_types::state_error::Result<()> {
        let mut client = self.sync_pool.checkout()?;
        client
            .execute(
                "UPDATE sync_runs SET status = $1, finished_at = now(), \
                 records_read = $2, records_written = $3, \
                 bytes_read = $4, bytes_written = $5, error_message = $6 \
                 WHERE id = $7",
                &[
                    &status.as_str(),
                    &(stats.records_read as i64),
                    &(stats.records_written as i64),
                    &(stats.bytes_read as i64),
                    &(stats.bytes_written as i64),
                    &stats.error_message,
                    &run_id,
                ],
            )
            .map_err(StateError::backend)?;
        Ok(())
    }

    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> rapidbyte_types::state_error::Result<bool> {
        let mut client = self.sync_pool.checkout()?;

        let rows_affected = match expected {
            Some(expected_val) => client
                .execute(
                    "UPDATE sync_cursors SET cursor_value = $1, updated_at = now() \
                     WHERE pipeline = $2 AND stream = $3 AND cursor_value = $4",
                    &[
                        &new_value,
                        &pipeline.as_str(),
                        &stream.as_str(),
                        &expected_val,
                    ],
                )
                .map_err(StateError::backend)?,
            None => client
                .execute(
                    "INSERT INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
                     VALUES ($1, $2, $3, now()) ON CONFLICT DO NOTHING",
                    &[&pipeline.as_str(), &stream.as_str(), &new_value],
                )
                .map_err(StateError::backend)?,
        };

        Ok(rows_affected > 0)
    }

    fn insert_dlq_records(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> rapidbyte_types::state_error::Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut client = self.sync_pool.checkout()?;
        let mut tx = client
            .transaction()
            .map_err(|e| StateError::backend_context("insert_dlq_records: begin tx", e))?;
        let query = "INSERT INTO dlq_records \
                 (pipeline, run_id, stream_name, record_json, \
                  error_message, error_category, failed_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7::text::timestamptz)";
        let stmt = tx
            .prepare(query)
            .map_err(|e| StateError::backend_context("insert_dlq_records: prepare", e))?;

        for record in records {
            tx.execute(
                &stmt,
                &[
                    &pipeline.as_str(),
                    &run_id,
                    &record.stream_name.as_str(),
                    &record.record_json.as_str(),
                    &record.error_message.as_str(),
                    &record.error_category.as_str(),
                    &record.failed_at.as_str(),
                ],
            )
            .map_err(|e| StateError::backend_context("insert_dlq_records: execute", e))?;
        }
        tx.commit()
            .map_err(|e| StateError::backend_context("insert_dlq_records: commit", e))?;

        Ok(records.len() as u64)
    }
}
