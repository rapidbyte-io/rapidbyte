use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use crate::domain::agent::Agent;
use crate::domain::run::{Run, RunState};
use crate::domain::task::Task;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RepositoryError(pub Box<dyn std::error::Error + Send + Sync>);

pub struct RunFilter {
    pub state: Option<RunState>,
}

pub struct Pagination {
    pub page_size: u32,
    pub page_token: Option<String>,
}

pub struct RunPage {
    pub runs: Vec<Run>,
    pub next_page_token: Option<String>,
}

#[async_trait]
pub trait RunRepository: Send + Sync {
    async fn find_by_id(&self, id: &str) -> Result<Option<Run>, RepositoryError>;
    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<Run>, RepositoryError>;
    async fn save(&self, run: &Run) -> Result<(), RepositoryError>;
    async fn list(
        &self,
        filter: RunFilter,
        pagination: Pagination,
    ) -> Result<RunPage, RepositoryError>;
}

#[async_trait]
pub trait TaskRepository: Send + Sync {
    async fn find_by_id(&self, id: &str) -> Result<Option<Task>, RepositoryError>;
    async fn save(&self, task: &Task) -> Result<(), RepositoryError>;
    async fn find_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Task>, RepositoryError>;
    async fn find_by_run_id(&self, run_id: &str) -> Result<Vec<Task>, RepositoryError>;
    async fn find_running_by_agent_id(&self, agent_id: &str) -> Result<Vec<Task>, RepositoryError>;
    async fn next_lease_epoch(&self) -> Result<u64, RepositoryError>;
}

#[async_trait]
pub trait AgentRepository: Send + Sync {
    async fn find_by_id(&self, id: &str) -> Result<Option<Agent>, RepositoryError>;
    async fn save(&self, agent: &Agent) -> Result<(), RepositoryError>;
    async fn delete(&self, id: &str) -> Result<(), RepositoryError>;
    async fn find_stale(
        &self,
        timeout: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Agent>, RepositoryError>;
}
