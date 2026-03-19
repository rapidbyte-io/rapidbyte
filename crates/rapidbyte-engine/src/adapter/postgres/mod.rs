//! `PostgreSQL`-backed adapter implementing repository ports and `StateBackend`.
//!
//! [`PgBackend`] combines an async [`sqlx::PgPool`] for the hexagonal
//! repository traits and a channel-based bridge for the legacy
//! [`StateBackend`](rapidbyte_types::state_backend::StateBackend) trait.
//!
//! The bridge sends requests through an `mpsc` channel to a background
//! async task that executes sqlx queries on the shared `PgPool`, avoiding
//! the nested-runtime panics that the old sync `postgres` crate caused.
//!
//! | Sub-module        | Trait                  |
//! |-------------------|------------------------|
//! | `cursor`          | `CursorRepository`     |
//! | `run_record`      | `RunRecordRepository`  |
//! | `dlq`             | `DlqRepository`        |
//! | `state_backend`   | `StateBackend`         |

mod cursor;
mod dlq;
pub(crate) mod queries;
mod run_record;
mod state_backend;

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc;

use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, RunStats, RunStatus};
use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::state_error::StateError;

/// `PostgreSQL`-backed adapter combining async (sqlx) repository ports
/// and a channel-based bridge for the sync `StateBackend` trait.
///
/// Use [`PgBackend::connect`] to create an instance, then call
/// [`migrate`](PgBackend::migrate) to apply pending DDL migrations.
pub struct PgBackend {
    /// Async pool used by repository port implementations.
    pool: PgPool,
    /// Channel sender for the `StateBackend` bridge worker.
    state_tx: mpsc::Sender<StateRequest>,
}

impl PgBackend {
    /// Connect to a `PostgreSQL` database and spawn the state backend worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool fails to connect.
    pub async fn connect(connstr: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(connstr).await?;
        let (tx, rx) = mpsc::channel(64);
        let worker_pool = pool.clone();
        tokio::spawn(async move { state_backend_worker(worker_pool, rx).await });
        Ok(Self { pool, state_tx: tx })
    }

    /// Run pending SQL migrations against the database.
    ///
    /// # Errors
    ///
    /// Returns an error if any migration fails.
    pub async fn migrate(&self) -> Result<(), anyhow::Error> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Obtain an `Arc<dyn StateBackend>` view of this backend.
    #[must_use]
    pub fn as_state_backend(self: &Arc<Self>) -> Arc<dyn StateBackend> {
        Arc::clone(self) as Arc<dyn StateBackend>
    }
}

// ---------------------------------------------------------------------------
// Channel-based bridge: request enum + async worker
// ---------------------------------------------------------------------------

/// A request sent from a sync `StateBackend` method to the async worker.
enum StateRequest {
    GetCursor {
        pipeline: String,
        stream: String,
        reply:
            tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<Option<CursorState>>>,
    },
    SetCursor {
        pipeline: String,
        stream: String,
        cursor: CursorState,
        reply: tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<()>>,
    },
    StartRun {
        pipeline: String,
        stream: String,
        reply: tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<i64>>,
    },
    CompleteRun {
        run_id: i64,
        status: RunStatus,
        stats: RunStats,
        reply: tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<()>>,
    },
    CompareAndSet {
        pipeline: String,
        stream: String,
        expected: Option<String>,
        new_value: String,
        reply: tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<bool>>,
    },
    InsertDlqRecords {
        pipeline: String,
        run_id: i64,
        records: Vec<DlqRecord>,
        reply: tokio::sync::oneshot::Sender<rapidbyte_types::state_error::Result<u64>>,
    },
}

/// Background async task that processes [`StateRequest`]s using sqlx.
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::too_many_lines)]
async fn state_backend_worker(pool: PgPool, mut rx: mpsc::Receiver<StateRequest>) {
    while let Some(req) = rx.recv().await {
        match req {
            StateRequest::GetCursor {
                pipeline,
                stream,
                reply,
            } => {
                let result = sqlx::query_as::<_, (Option<String>, Option<String>, String)>(
                    queries::GET_CURSOR,
                )
                .bind(&pipeline)
                .bind(&stream)
                .fetch_optional(&pool)
                .await
                .map(|row| {
                    row.map(|(cursor_field, cursor_value, updated_at)| CursorState {
                        cursor_field,
                        cursor_value,
                        updated_at,
                    })
                })
                .map_err(StateError::backend);
                let _ = reply.send(result);
            }
            StateRequest::SetCursor {
                pipeline,
                stream,
                cursor,
                reply,
            } => {
                let result = sqlx::query(queries::SET_CURSOR)
                    .bind(&pipeline)
                    .bind(&stream)
                    .bind(&cursor.cursor_field)
                    .bind(&cursor.cursor_value)
                    .execute(&pool)
                    .await
                    .map(|_| ())
                    .map_err(StateError::backend);
                let _ = reply.send(result);
            }
            StateRequest::StartRun {
                pipeline,
                stream,
                reply,
            } => {
                let result = sqlx::query_as::<_, (i64,)>(queries::START_RUN)
                    .bind(&pipeline)
                    .bind(&stream)
                    .bind(RunStatus::Running.as_str())
                    .fetch_one(&pool)
                    .await
                    .map(|(id,)| id)
                    .map_err(StateError::backend);
                let _ = reply.send(result);
            }
            StateRequest::CompleteRun {
                run_id,
                status,
                stats,
                reply,
            } => {
                let result = sqlx::query(queries::COMPLETE_RUN)
                    .bind(status.as_str())
                    .bind(stats.records_read as i64)
                    .bind(stats.records_written as i64)
                    .bind(stats.bytes_read as i64)
                    .bind(stats.bytes_written as i64)
                    .bind(&stats.error_message)
                    .bind(run_id)
                    .execute(&pool)
                    .await
                    .map(|_| ())
                    .map_err(StateError::backend);
                let _ = reply.send(result);
            }
            StateRequest::CompareAndSet {
                pipeline,
                stream,
                expected,
                new_value,
                reply,
            } => {
                let result = match expected {
                    Some(expected_val) => sqlx::query(queries::CAS_UPDATE_CURSOR)
                        .bind(&new_value)
                        .bind(&pipeline)
                        .bind(&stream)
                        .bind(&expected_val)
                        .execute(&pool)
                        .await
                        .map(|r| r.rows_affected() > 0)
                        .map_err(StateError::backend),
                    None => sqlx::query(queries::CAS_INSERT_CURSOR)
                        .bind(&pipeline)
                        .bind(&stream)
                        .bind(&new_value)
                        .execute(&pool)
                        .await
                        .map(|r| r.rows_affected() > 0)
                        .map_err(StateError::backend),
                };
                let _ = reply.send(result);
            }
            StateRequest::InsertDlqRecords {
                pipeline,
                run_id,
                records,
                reply,
            } => {
                let result = async {
                    let mut tx = pool.begin().await.map_err(|e| {
                        StateError::backend_context("insert_dlq_records: begin tx", e)
                    })?;

                    for record in &records {
                        sqlx::query(queries::INSERT_DLQ_RECORD)
                            .bind(&pipeline)
                            .bind(run_id)
                            .bind(&record.stream_name)
                            .bind(&record.record_json)
                            .bind(&record.error_message)
                            .bind(record.error_category.as_str())
                            .bind(record.failed_at.as_str())
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| {
                                StateError::backend_context("insert_dlq_records: execute", e)
                            })?;
                    }

                    tx.commit().await.map_err(|e| {
                        StateError::backend_context("insert_dlq_records: commit", e)
                    })?;

                    Ok(records.len() as u64)
                }
                .await;
                let _ = reply.send(result);
            }
        }
    }
}
