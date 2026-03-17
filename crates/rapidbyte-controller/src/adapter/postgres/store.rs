use async_trait::async_trait;
use sqlx::{PgConnection, PgPool};

use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::repository::RepositoryError;
use crate::domain::run::Run;
use crate::domain::task::Task;

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
            Some(m.rows_read as i64),
            Some(m.rows_written as i64),
            Some(m.bytes_read as i64),
            Some(m.bytes_written as i64),
            Some(m.duration_ms as i64),
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
    .bind(run.current_attempt() as i32)
    .bind(run.max_retries() as i32)
    .bind(run.timeout_seconds().map(|t| t as i64))
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
            Some(m.rows_read as i64),
            Some(m.rows_written as i64),
            Some(m.bytes_read as i64),
            Some(m.bytes_written as i64),
            Some(m.duration_ms as i64),
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
    .bind(run.current_attempt() as i32)
    .bind(run.max_retries() as i32)
    .bind(run.timeout_seconds().map(|t| t as i64))
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
        Some(l) => (Some(l.epoch() as i64), Some(l.expires_at())),
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
    .bind(task.attempt() as i32)
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
        Some(l) => (Some(l.epoch() as i64), Some(l.expires_at())),
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
        insert_run(&mut *tx, run).await?;
        insert_task(&mut *tx, task).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn complete_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut *tx, task).await?;
        update_run(&mut *tx, run).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn fail_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut *tx, task).await?;
        update_run(&mut *tx, run).await?;
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
        update_task(&mut *tx, failed_task).await?;
        update_run(&mut *tx, run).await?;
        insert_task(&mut *tx, new_task).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn cancel_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_task(&mut *tx, task).await?;
        update_run(&mut *tx, run).await?;
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }

    async fn cancel_pending_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;
        update_run(&mut *tx, run).await?;
        update_task(&mut *tx, task).await?;
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
        update_task(&mut *tx, timed_out_task).await?;
        update_run(&mut *tx, run).await?;
        if let Some(task) = new_task {
            insert_task(&mut *tx, task).await?;
        }
        tx.commit().await.map_err(box_err)?;
        Ok(())
    }
}
