use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use crate::domain::lease::Lease;
use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::repository::RepositoryError;
use crate::domain::run::Run;
use crate::domain::task::{Task, TaskOperation};

use super::error::box_err;
use super::run::{run_from_row, run_state_to_str};
use super::task::{task_from_row, task_state_to_str};

fn extract_run_error(run: &Run) -> (Option<&str>, Option<&str>) {
    match run.error() {
        Some(e) => (Some(e.code.as_str()), Some(e.message.as_str())),
        None => (None, None),
    }
}

type RunMetricsTuple = (
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

fn extract_run_metrics(run: &Run) -> RunMetricsTuple {
    match run.metrics() {
        Some(m) => (
            Some(m.rows_read.cast_signed()),
            Some(m.rows_written.cast_signed()),
            Some(m.bytes_read.cast_signed()),
            Some(m.bytes_written.cast_signed()),
            Some(m.duration_ms.cast_signed()),
        ),
        None => (None, None, None, None, None),
    }
}

fn extract_task_lease(task: &Task) -> (Option<i64>, Option<DateTime<Utc>>) {
    match task.lease() {
        Some(l) => (Some(l.epoch().cast_signed()), Some(l.expires_at())),
        None => (None, None),
    }
}

pub struct PgPipelineStore {
    pool: PgPool,
}

impl PgPipelineStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

async fn insert_run(conn: &mut PgConnection, run: &Run) -> Result<(), RepositoryError> {
    let (error_code, error_message) = extract_run_error(run);
    let (rows_read, rows_written, bytes_read, bytes_written, duration_ms) =
        extract_run_metrics(run);

    sqlx::query(
        "INSERT INTO runs (id, idempotency_key, pipeline_name, pipeline_yaml, state, cancel_requested, attempt, max_retries, timeout_seconds, error_code, error_message, rows_read, rows_written, bytes_read, bytes_written, duration_ms, metadata, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)",
    )
    .bind(run.id())
    .bind(run.idempotency_key())
    .bind(run.pipeline_name())
    .bind(run.pipeline_yaml())
    .bind(run_state_to_str(run.state()))
    .bind(run.is_cancel_requested())
    .bind(run.current_attempt().cast_signed())
    .bind(run.max_retries().cast_signed())
    .bind(run.timeout_seconds().map(u64::cast_signed))
    .bind(error_code)
    .bind(error_message)
    .bind(rows_read)
    .bind(rows_written)
    .bind(bytes_read)
    .bind(bytes_written)
    .bind(duration_ms)
    .bind(run.metadata())
    .bind(run.created_at())
    .bind(run.updated_at())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

async fn update_run(conn: &mut PgConnection, run: &Run) -> Result<(), RepositoryError> {
    let (error_code, error_message) = extract_run_error(run);
    let (rows_read, rows_written, bytes_read, bytes_written, duration_ms) =
        extract_run_metrics(run);

    sqlx::query(
        "UPDATE runs SET state = $1, cancel_requested = $2, attempt = $3, max_retries = $4, timeout_seconds = $5, error_code = $6, error_message = $7, rows_read = $8, rows_written = $9, bytes_read = $10, bytes_written = $11, duration_ms = $12, metadata = $13, updated_at = $14 WHERE id = $15",
    )
    .bind(run_state_to_str(run.state()))
    .bind(run.is_cancel_requested())
    .bind(run.current_attempt().cast_signed())
    .bind(run.max_retries().cast_signed())
    .bind(run.timeout_seconds().map(u64::cast_signed))
    .bind(error_code)
    .bind(error_message)
    .bind(rows_read)
    .bind(rows_written)
    .bind(bytes_read)
    .bind(bytes_written)
    .bind(duration_ms)
    .bind(run.metadata())
    .bind(run.updated_at())
    .bind(run.id())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

async fn insert_task(conn: &mut PgConnection, task: &Task) -> Result<(), RepositoryError> {
    let (lease_epoch, lease_expires_at) = extract_task_lease(task);

    sqlx::query(
        "INSERT INTO tasks (id, run_id, attempt, operation, state, agent_id, lease_epoch, lease_expires_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(task.id())
    .bind(task.run_id())
    .bind(task.attempt().cast_signed())
    .bind(task.operation().as_str())
    .bind(task_state_to_str(task.state()))
    .bind(task.agent_id())
    .bind(lease_epoch)
    .bind(lease_expires_at)
    .bind(task.created_at())
    .bind(task.updated_at())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

async fn update_task(conn: &mut PgConnection, task: &Task) -> Result<(), RepositoryError> {
    let (lease_epoch, lease_expires_at) = extract_task_lease(task);

    sqlx::query(
        "UPDATE tasks SET state = $1, agent_id = $2, lease_epoch = $3, lease_expires_at = $4, updated_at = $5 WHERE id = $6",
    )
    .bind(task_state_to_str(task.state()))
    .bind(task.agent_id())
    .bind(lease_epoch)
    .bind(lease_expires_at)
    .bind(task.updated_at())
    .bind(task.id())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

#[async_trait]
impl PipelineStore for PgPipelineStore {
    async fn submit_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        insert_run(&mut tx, run).await?;
        insert_task(&mut tx, task).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn assign_task(
        &self,
        agent_id: &str,
        max_concurrent_tasks: u32,
        lease: Lease,
        supported_operations: &[TaskOperation],
    ) -> Result<Option<(Task, Run)>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;

        // Serialize concurrent polls for the same agent using an advisory lock.
        // SELECT FOR UPDATE on running tasks doesn't lock when there are 0 rows,
        // so two concurrent transactions could both pass a COUNT-based check.
        // Advisory lock on a hash of the agent_id prevents this race.
        // Simple hash of agent_id for advisory lock key
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        agent_id.hash(&mut hasher);
        let lock_key = hasher.finish().cast_signed();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(box_err)?;

        // Check agent capacity (serialized by the advisory lock above)
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM tasks WHERE agent_id = $1 AND state = 'running'")
                .bind(agent_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(box_err)?;

        if count >= i64::from(max_concurrent_tasks) {
            tx.commit().await.map_err(box_err)?;
            return Ok(None);
        }

        // Build operation filter. The caller (poll_task) is responsible for
        // backwards-compat fallback (empty caps → sync-only). If an empty
        // slice reaches here, no tasks will match — this is correct behavior,
        // not a bug.
        let ops: Vec<String> = supported_operations
            .iter()
            .map(|op| op.as_str().to_string())
            .collect();

        // Poll next pending task filtered by supported operations
        let row = sqlx::query(
            "SELECT * FROM tasks WHERE state = 'pending' AND operation = ANY($1) ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
        )
        .bind(&ops)
        .fetch_optional(&mut *tx)
        .await
        .map_err(box_err)?;

        let Some(row) = row else {
            tx.commit().await.map_err(box_err)?;
            return Ok(None);
        };

        let task = task_from_row(&row)?;

        // Update task to running
        sqlx::query(
            "UPDATE tasks SET state = 'running', agent_id = $1, lease_epoch = $2, lease_expires_at = $3, updated_at = now() WHERE id = $4",
        )
        .bind(agent_id)
        .bind(lease.epoch().cast_signed())
        .bind(lease.expires_at())
        .bind(task.id())
        .execute(&mut *tx)
        .await
        .map_err(box_err)?;

        // Update run to running — check that it was actually pending.
        // If the run was concurrently cancelled/failed, this affects 0 rows.
        // Use RETURNING to avoid an extra SELECT round-trip.
        let run_row = sqlx::query(
            "UPDATE runs SET state = 'running', updated_at = now() WHERE id = $1 AND state = 'pending' RETURNING *",
        )
        .bind(task.run_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(box_err)?;

        let Some(run_row) = run_row else {
            // Run is no longer pending (cancelled/failed concurrently).
            // Rollback task assignment — put it back to pending.
            sqlx::query(
                "UPDATE tasks SET state = 'pending', agent_id = NULL, lease_epoch = NULL, lease_expires_at = NULL, updated_at = now() WHERE id = $1",
            )
            .bind(task.id())
            .execute(&mut *tx)
            .await
            .map_err(box_err)?;

            tx.commit().await.map_err(box_err)?;
            return Ok(None);
        };

        let run = run_from_row(&run_row)?;

        tx.commit().await.map_err(box_err)?;

        // Reconstruct task with assigned state
        let assigned_task = Task::from_row(
            task.id().to_string(),
            task.run_id().to_string(),
            task.attempt(),
            task.operation(),
            crate::domain::task::TaskState::Running,
            Some(agent_id.to_string()),
            Some(lease),
            task.created_at(),
            chrono::Utc::now(),
        );

        Ok(Some((assigned_task, run)))
    }

    async fn complete_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut tx, task).await?;
        update_run(&mut tx, run).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn fail_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut tx, task).await?;
        update_run(&mut tx, run).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn fail_and_retry(
        &self,
        failed_task: &Task,
        run: &Run,
        new_task: &Task,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut tx, failed_task).await?;
        update_run(&mut tx, run).await?;
        insert_task(&mut tx, new_task).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn cancel_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut tx, task).await?;
        update_run(&mut tx, run).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn cancel_pending_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;

        // Only cancel the run if it's still pending (prevents overwriting a concurrent assignment)
        let run_result = sqlx::query(
            "UPDATE runs SET state = 'cancelled', cancel_requested = TRUE, updated_at = now() WHERE id = $1 AND state = 'pending'",
        )
        .bind(run.id())
        .execute(&mut *tx)
        .await
        .map_err(box_err)?;

        if run_result.rows_affected() == 0 {
            // Run was concurrently assigned — cancel is no longer valid for pending path
            tx.commit().await.map_err(box_err)?;
            return Err(RepositoryError::Conflict(
                "run is no longer pending (concurrent assignment)".into(),
            ));
        }

        // Only cancel the task if it's still pending
        sqlx::query(
            "UPDATE tasks SET state = 'cancelled', updated_at = now() WHERE id = $1 AND state = 'pending'",
        )
        .bind(task.id())
        .execute(&mut *tx)
        .await
        .map_err(box_err)?;

        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn timeout_and_retry(
        &self,
        timed_out_task: &Task,
        run: &Run,
        new_task: Option<&Task>,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut tx, timed_out_task).await?;
        update_run(&mut tx, run).await?;
        if let Some(task) = new_task {
            insert_task(&mut tx, task).await?;
        }
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }
}
