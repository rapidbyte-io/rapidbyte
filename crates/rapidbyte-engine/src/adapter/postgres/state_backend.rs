//! `StateBackend` (sync) implementation for [`PgBackend`].
//!
//! Each method sends a [`StateRequest`](super::StateRequest) through the
//! channel to the background async worker, which executes the query on the
//! shared `PgPool`. Responses come back via a `oneshot` channel.
//!
//! Uses `blocking_send` / `blocking_recv` so the sync `StateBackend` trait
//! methods block the calling thread without requiring a tokio runtime context.

use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::state_error::StateError;

use super::{PgBackend, StateRequest};

/// Channel-closed sentinel error.
fn channel_closed() -> StateError {
    StateError::backend(std::io::Error::other("state backend channel closed"))
}

/// Response-dropped sentinel error.
fn response_dropped() -> StateError {
    StateError::backend(std::io::Error::other("state backend response dropped"))
}

impl StateBackend for PgBackend {
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> rapidbyte_types::state_error::Result<Option<CursorState>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::GetCursor {
                pipeline: pipeline.to_string(),
                stream: stream.to_string(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
    }

    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> rapidbyte_types::state_error::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::SetCursor {
                pipeline: pipeline.to_string(),
                stream: stream.to_string(),
                cursor: cursor.clone(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
    }

    fn start_run(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> rapidbyte_types::state_error::Result<i64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::StartRun {
                pipeline: pipeline.to_string(),
                stream: stream.to_string(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
    }

    fn complete_run(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> rapidbyte_types::state_error::Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::CompleteRun {
                run_id,
                status,
                stats: stats.clone(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
    }

    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> rapidbyte_types::state_error::Result<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::CompareAndSet {
                pipeline: pipeline.to_string(),
                stream: stream.to_string(),
                expected: expected.map(String::from),
                new_value: new_value.to_string(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
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

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.state_tx
            .blocking_send(StateRequest::InsertDlqRecords {
                pipeline: pipeline.to_string(),
                run_id,
                records: records.to_vec(),
                reply: tx,
            })
            .map_err(|_| channel_closed())?;
        rx.blocking_recv().map_err(|_| response_dropped())?
    }
}
