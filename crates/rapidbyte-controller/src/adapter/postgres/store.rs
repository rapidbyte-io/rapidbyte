use async_trait::async_trait;
use sqlx::{PgConnection, PgPool};

use crate::domain::lease::Lease;
use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::repository::RepositoryError;
use crate::domain::run::Run;
use crate::domain::task::Task;

use super::run::run_from_row;
use super::task::task_from_row;

fn box_err(e: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError(Box::new(e))
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
    let (error_code, error_message) = match run.error() {
        Some(e) => (Some(e.code.as_str()), Some(e.message.as_str())),
        None => (None, None),
    };

    let (rows_read, rows_written, bytes_read, bytes_written, duration_ms) = match run.metrics() {
        Some(m) => (
            Some(m.rows_read.cast_signed()),
            Some(m.rows_written.cast_signed()),
            Some(m.bytes_read.cast_signed()),
            Some(m.bytes_written.cast_signed()),
            Some(m.duration_ms.cast_signed()),
        ),
        None => (None, None, None, None, None),
    };

    let state_str = match run.state() {
        crate::domain::run::RunState::Pending => "pending",
        crate::domain::run::RunState::Running => "running",
        crate::domain::run::RunState::Completed => "completed",
        crate::domain::run::RunState::Failed => "failed",
        crate::domain::run::RunState::Cancelled => "cancelled",
    };

    sqlx::query(
        "INSERT INTO runs (id, idempotency_key, pipeline_name, pipeline_yaml, state, cancel_requested, attempt, max_retries, timeout_seconds, error_code, error_message, rows_read, rows_written, bytes_read, bytes_written, duration_ms, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)",
    )
    .bind(run.id())
    .bind(run.idempotency_key())
    .bind(run.pipeline_name())
    .bind(run.pipeline_yaml())
    .bind(state_str)
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
    .bind(run.created_at())
    .bind(run.updated_at())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

async fn update_run(conn: &mut PgConnection, run: &Run) -> Result<(), RepositoryError> {
    let (error_code, error_message) = match run.error() {
        Some(e) => (Some(e.code.as_str()), Some(e.message.as_str())),
        None => (None, None),
    };

    let (rows_read, rows_written, bytes_read, bytes_written, duration_ms) = match run.metrics() {
        Some(m) => (
            Some(m.rows_read.cast_signed()),
            Some(m.rows_written.cast_signed()),
            Some(m.bytes_read.cast_signed()),
            Some(m.bytes_written.cast_signed()),
            Some(m.duration_ms.cast_signed()),
        ),
        None => (None, None, None, None, None),
    };

    let state_str = match run.state() {
        crate::domain::run::RunState::Pending => "pending",
        crate::domain::run::RunState::Running => "running",
        crate::domain::run::RunState::Completed => "completed",
        crate::domain::run::RunState::Failed => "failed",
        crate::domain::run::RunState::Cancelled => "cancelled",
    };

    sqlx::query(
        "UPDATE runs SET state = $1, cancel_requested = $2, attempt = $3, max_retries = $4, timeout_seconds = $5, error_code = $6, error_message = $7, rows_read = $8, rows_written = $9, bytes_read = $10, bytes_written = $11, duration_ms = $12, updated_at = $13 WHERE id = $14",
    )
    .bind(state_str)
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
    .bind(run.updated_at())
    .bind(run.id())
    .execute(&mut *conn)
    .await
    .map_err(box_err)?;

    Ok(())
}

async fn insert_task(conn: &mut PgConnection, task: &Task) -> Result<(), RepositoryError> {
    let (lease_epoch, lease_expires_at) = match task.lease() {
        Some(l) => (Some(l.epoch().cast_signed()), Some(l.expires_at())),
        None => (None, None),
    };

    let state_str = match task.state() {
        crate::domain::task::TaskState::Pending => "pending",
        crate::domain::task::TaskState::Running => "running",
        crate::domain::task::TaskState::Completed => "completed",
        crate::domain::task::TaskState::Failed => "failed",
        crate::domain::task::TaskState::Cancelled => "cancelled",
        crate::domain::task::TaskState::TimedOut => "timed_out",
    };

    sqlx::query(
        "INSERT INTO tasks (id, run_id, attempt, state, agent_id, lease_epoch, lease_expires_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(task.id())
    .bind(task.run_id())
    .bind(task.attempt().cast_signed())
    .bind(state_str)
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
    let (lease_epoch, lease_expires_at) = match task.lease() {
        Some(l) => (Some(l.epoch().cast_signed()), Some(l.expires_at())),
        None => (None, None),
    };

    let state_str = match task.state() {
        crate::domain::task::TaskState::Pending => "pending",
        crate::domain::task::TaskState::Running => "running",
        crate::domain::task::TaskState::Completed => "completed",
        crate::domain::task::TaskState::Failed => "failed",
        crate::domain::task::TaskState::Cancelled => "cancelled",
        crate::domain::task::TaskState::TimedOut => "timed_out",
    };

    sqlx::query(
        "UPDATE tasks SET state = $1, agent_id = $2, lease_epoch = $3, lease_expires_at = $4, updated_at = $5 WHERE id = $6",
    )
    .bind(state_str)
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
    ) -> Result<Option<(Task, Run)>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;

        // Check agent capacity — lock the agent's running tasks to prevent concurrent
        // polls from both seeing below-capacity and each assigning a task.
        let running_rows = sqlx::query(
            "SELECT id FROM tasks WHERE agent_id = $1 AND state = 'running' FOR UPDATE",
        )
        .bind(agent_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(box_err)?;

        if running_rows.len() >= max_concurrent_tasks as usize {
            tx.commit().await.map_err(box_err)?;
            return Ok(None);
        }

        // Poll next pending task
        let row = sqlx::query(
            "SELECT * FROM tasks WHERE state = 'pending' ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
        )
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
        let run_result = sqlx::query(
            "UPDATE runs SET state = 'running', updated_at = now() WHERE id = $1 AND state = 'pending'",
        )
        .bind(task.run_id())
        .execute(&mut *tx)
        .await
        .map_err(box_err)?;

        if run_result.rows_affected() == 0 {
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
        }

        // Select updated run
        let run_row = sqlx::query("SELECT * FROM runs WHERE id = $1")
            .bind(task.run_id())
            .fetch_one(&mut *tx)
            .await
            .map_err(box_err)?;

        let run = run_from_row(&run_row)?;

        tx.commit().await.map_err(box_err)?;

        // Reconstruct task with assigned state
        let assigned_task = Task::from_row(
            task.id().to_string(),
            task.run_id().to_string(),
            task.attempt(),
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
        update_run(&mut tx, run).await?;
        update_task(&mut tx, task).await?;
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
