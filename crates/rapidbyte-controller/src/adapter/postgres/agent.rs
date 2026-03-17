use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};

use crate::domain::agent::{Agent, AgentCapabilities};
use crate::domain::ports::repository::{AgentRepository, RepositoryError};

fn box_err(e: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError(Box::new(e))
}

fn agent_from_row(row: &sqlx::postgres::PgRow) -> Result<Agent, RepositoryError> {
    let id: String = row.try_get("id").map_err(box_err)?;
    let plugins: Vec<String> = row.try_get("plugins").map_err(box_err)?;
    let max_concurrent_tasks: i32 = row.try_get("max_concurrent_tasks").map_err(box_err)?;
    let last_seen_at: DateTime<Utc> = row.try_get("last_seen_at").map_err(box_err)?;
    let registered_at: DateTime<Utc> = row.try_get("registered_at").map_err(box_err)?;

    let capabilities = AgentCapabilities {
        plugins,
        max_concurrent_tasks: max_concurrent_tasks.cast_unsigned(),
    };

    Ok(Agent::from_row(
        id,
        capabilities,
        last_seen_at,
        registered_at,
    ))
}

pub struct PgAgentRepository {
    pool: PgPool,
}

impl PgAgentRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AgentRepository for PgAgentRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Agent>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM agents WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(box_err)?;

        row.as_ref().map(agent_from_row).transpose()
    }

    async fn save(&self, agent: &Agent) -> Result<(), RepositoryError> {
        sqlx::query(
            "INSERT INTO agents (id, plugins, max_concurrent_tasks, last_seen_at, registered_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE SET
                plugins = EXCLUDED.plugins,
                max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
                last_seen_at = EXCLUDED.last_seen_at",
        )
        .bind(agent.id())
        .bind(&agent.capabilities().plugins)
        .bind(agent.capabilities().max_concurrent_tasks.cast_signed())
        .bind(agent.last_seen_at())
        .bind(agent.registered_at())
        .execute(&self.pool)
        .await
        .map_err(box_err)?;

        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), RepositoryError> {
        sqlx::query("DELETE FROM agents WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(box_err)?;

        Ok(())
    }

    async fn find_stale(
        &self,
        timeout: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Agent>, RepositoryError> {
        let cutoff = now - timeout;

        let rows = sqlx::query("SELECT * FROM agents WHERE last_seen_at < $1")
            .bind(cutoff)
            .fetch_all(&self.pool)
            .await
            .map_err(box_err)?;

        rows.iter().map(agent_from_row).collect()
    }
}
