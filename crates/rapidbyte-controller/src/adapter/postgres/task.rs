use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use crate::domain::lease::Lease;
use crate::domain::ports::repository::{RepositoryError, TaskRepository};
use crate::domain::task::{Task, TaskState};

fn box_err(e: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError(Box::new(e))
}

fn parse_task_state(s: &str) -> Result<TaskState, RepositoryError> {
    match s {
        "pending" => Ok(TaskState::Pending),
        "running" => Ok(TaskState::Running),
        "completed" => Ok(TaskState::Completed),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        "timed_out" => Ok(TaskState::TimedOut),
        other => Err(RepositoryError(Box::from(format!(
            "unknown task state in database: {other}"
        )))),
    }
}

fn task_state_to_str(state: TaskState) -> &'static str {
    match state {
        TaskState::Pending => "pending",
        TaskState::Running => "running",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
        TaskState::TimedOut => "timed_out",
    }
}

fn task_from_row(row: &sqlx::postgres::PgRow) -> Result<Task, RepositoryError> {
    let id: String = row.try_get("id").map_err(box_err)?;
    let run_id: String = row.try_get("run_id").map_err(box_err)?;
    let attempt: i32 = row.try_get("attempt").map_err(box_err)?;
    let state_str: String = row.try_get("state").map_err(box_err)?;
    let agent_id: Option<String> = row.try_get("agent_id").map_err(box_err)?;
    let lease_epoch: Option<i64> = row.try_get("lease_epoch").map_err(box_err)?;
    let lease_expires_at: Option<DateTime<Utc>> =
        row.try_get("lease_expires_at").map_err(box_err)?;
    let created_at: DateTime<Utc> = row.try_get("created_at").map_err(box_err)?;
    let updated_at: DateTime<Utc> = row.try_get("updated_at").map_err(box_err)?;

    let state = parse_task_state(&state_str)?;

    let lease = match (lease_epoch, lease_expires_at) {
        (Some(epoch), Some(expires_at)) => Some(Lease::new(epoch.cast_unsigned(), expires_at)),
        _ => None,
    };

    Ok(Task::from_row(
        id,
        run_id,
        attempt.cast_unsigned(),
        state,
        agent_id,
        lease,
        created_at,
        updated_at,
    ))
}

pub struct PgTaskRepository {
    pool: PgPool,
}

impl PgTaskRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TaskRepository for PgTaskRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Task>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM tasks WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(box_err)?;

        row.as_ref().map(task_from_row).transpose()
    }

    async fn save(&self, task: &Task) -> Result<(), RepositoryError> {
        let (lease_epoch, lease_expires_at) = match task.lease() {
            Some(l) => (Some(l.epoch().cast_signed()), Some(l.expires_at())),
            None => (None, None),
        };

        sqlx::query(
            "INSERT INTO tasks (id, run_id, attempt, state, agent_id, lease_epoch, lease_expires_at, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (id) DO UPDATE SET
                state = EXCLUDED.state,
                agent_id = EXCLUDED.agent_id,
                lease_epoch = EXCLUDED.lease_epoch,
                lease_expires_at = EXCLUDED.lease_expires_at,
                updated_at = EXCLUDED.updated_at"
        )
        .bind(task.id())
        .bind(task.run_id())
        .bind(task.attempt().cast_signed())
        .bind(task_state_to_str(task.state()))
        .bind(task.agent_id())
        .bind(lease_epoch)
        .bind(lease_expires_at)
        .bind(task.created_at())
        .bind(task.updated_at())
        .execute(&self.pool)
        .await
        .map_err(box_err)?;

        Ok(())
    }

    async fn poll_and_assign(
        &self,
        agent_id: &str,
        lease: Lease,
    ) -> Result<Option<Task>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(box_err)?;

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

        tx.commit().await.map_err(box_err)?;

        // Reconstruct task with assigned state
        Ok(Some(Task::from_row(
            task.id().to_string(),
            task.run_id().to_string(),
            task.attempt(),
            TaskState::Running,
            Some(agent_id.to_string()),
            Some(lease),
            task.created_at(),
            Utc::now(),
        )))
    }

    async fn find_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Task>, RepositoryError> {
        let rows =
            sqlx::query("SELECT * FROM tasks WHERE state = 'running' AND lease_expires_at < $1")
                .bind(now)
                .fetch_all(&self.pool)
                .await
                .map_err(box_err)?;

        rows.iter().map(task_from_row).collect()
    }

    async fn find_by_run_id(&self, run_id: &str) -> Result<Vec<Task>, RepositoryError> {
        let rows = sqlx::query("SELECT * FROM tasks WHERE run_id = $1 ORDER BY attempt ASC")
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(box_err)?;

        rows.iter().map(task_from_row).collect()
    }

    async fn find_running_by_agent_id(&self, agent_id: &str) -> Result<Vec<Task>, RepositoryError> {
        let rows = sqlx::query("SELECT * FROM tasks WHERE state = 'running' AND agent_id = $1")
            .bind(agent_id)
            .fetch_all(&self.pool)
            .await
            .map_err(box_err)?;

        rows.iter().map(task_from_row).collect()
    }

    async fn next_lease_epoch(&self) -> Result<u64, RepositoryError> {
        let row: (i64,) = sqlx::query_as("SELECT nextval('lease_epoch_seq')")
            .fetch_one(&self.pool)
            .await
            .map_err(box_err)?;

        Ok(row.0.cast_unsigned())
    }
}
