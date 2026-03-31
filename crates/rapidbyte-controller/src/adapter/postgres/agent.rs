use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};

use crate::domain::agent::{Agent, AgentCapabilities};
use crate::domain::ports::repository::{AgentRepository, RepositoryError};
use crate::domain::task::TaskOperation;

use super::error::box_err;

fn agent_from_row(row: &sqlx::postgres::PgRow) -> Result<Agent, RepositoryError> {
    let id: String = row.try_get("id").map_err(box_err)?;
    let plugins: Vec<String> = row.try_get("plugins").map_err(box_err)?;
    let max_concurrent_tasks: i32 = row.try_get("max_concurrent_tasks").map_err(box_err)?;
    let last_seen_at: DateTime<Utc> = row.try_get("last_seen_at").map_err(box_err)?;
    let registered_at: DateTime<Utc> = row.try_get("registered_at").map_err(box_err)?;
    let ops_strings: Vec<String> = row.try_get("supported_operations").map_err(box_err)?;
    let supported_operations: Vec<TaskOperation> =
        ops_strings.iter().filter_map(|s| s.parse().ok()).collect();

    let capabilities = AgentCapabilities {
        plugins,
        max_concurrent_tasks: max_concurrent_tasks.cast_unsigned(),
        supported_operations,
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
        let ops: Vec<String> = agent
            .capabilities()
            .supported_operations
            .iter()
            .map(|op| op.as_str().to_string())
            .collect();

        sqlx::query(
            "INSERT INTO agents (id, plugins, max_concurrent_tasks, supported_operations, last_seen_at, registered_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE SET
                plugins = EXCLUDED.plugins,
                max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
                supported_operations = EXCLUDED.supported_operations,
                last_seen_at = EXCLUDED.last_seen_at",
        )
        .bind(agent.id())
        .bind(&agent.capabilities().plugins)
        .bind(agent.capabilities().max_concurrent_tasks.cast_signed())
        .bind(&ops)
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
