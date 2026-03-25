use std::sync::Arc;
use std::time::Duration;

use crate::domain::ports::clock::Clock;
use crate::domain::ports::connection_tester::ConnectionTester;
use crate::domain::ports::cursor_store::CursorStore;
use crate::domain::ports::event_bus::EventBus;
use crate::domain::ports::log_store::LogStore;
use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::plugin_registry::PluginRegistry;
use crate::domain::ports::repository::{AgentRepository, RunRepository, TaskRepository};
use crate::domain::ports::secrets::SecretResolver;

use super::error::AppError;

pub struct RegistryConfig {
    pub url: String,
    pub insecure: bool,
}

pub struct AppConfig {
    pub default_lease_duration: Duration,
    pub lease_check_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub agent_reap_interval: Duration,
    pub default_max_retries: u32,
    pub registry: Option<RegistryConfig>,
    /// When `true`, requests without a valid bearer token are permitted.
    pub allow_unauthenticated: bool,
}

impl AppConfig {
    #[must_use]
    pub fn lease_duration_chrono(&self) -> chrono::Duration {
        chrono::Duration::from_std(self.default_lease_duration)
            .unwrap_or_else(|_| chrono::Duration::seconds(300))
    }
}

pub struct AppContext {
    pub runs: Arc<dyn RunRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub agents: Arc<dyn AgentRepository>,
    pub store: Arc<dyn PipelineStore>,
    pub event_bus: Arc<dyn EventBus>,
    pub secrets: Arc<dyn SecretResolver>,
    pub clock: Arc<dyn Clock>,
    pub cursor_store: Arc<dyn CursorStore>,
    pub log_store: Arc<dyn LogStore>,
    pub connection_tester: Arc<dyn ConnectionTester>,
    pub plugin_registry: Arc<dyn PluginRegistry>,
    pub config: AppConfig,
}

impl AppContext {
    /// Find an agent by id, returning `NotFound` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `AppError::NotFound` or a repository error.
    pub async fn find_agent(
        &self,
        agent_id: &str,
    ) -> Result<crate::domain::agent::Agent, AppError> {
        self.agents
            .find_by_id(agent_id)
            .await?
            .ok_or_else(|| AppError::NotFound {
                entity: "Agent",
                id: agent_id.to_string(),
            })
    }

    /// Find a run by id, returning `NotFound` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `AppError::NotFound` or a repository error.
    pub async fn find_run(&self, run_id: &str) -> Result<crate::domain::run::Run, AppError> {
        self.runs
            .find_by_id(run_id)
            .await?
            .ok_or_else(|| AppError::NotFound {
                entity: "Run",
                id: run_id.to_string(),
            })
    }

    /// Find a task by id, returning `NotFound` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `AppError::NotFound` or a repository error.
    pub async fn find_task(&self, task_id: &str) -> Result<crate::domain::task::Task, AppError> {
        self.tasks
            .find_by_id(task_id)
            .await?
            .ok_or_else(|| AppError::NotFound {
                entity: "Task",
                id: task_id.to_string(),
            })
    }
}
