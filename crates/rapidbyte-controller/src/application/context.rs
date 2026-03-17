use std::sync::Arc;
use std::time::Duration;

use crate::domain::ports::clock::Clock;
use crate::domain::ports::event_bus::EventBus;
use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::repository::{AgentRepository, RunRepository, TaskRepository};
use crate::domain::ports::secrets::SecretResolver;

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
}

pub struct AppContext {
    pub runs: Arc<dyn RunRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub agents: Arc<dyn AgentRepository>,
    pub store: Arc<dyn PipelineStore>,
    pub event_bus: Arc<dyn EventBus>,
    pub secrets: Arc<dyn SecretResolver>,
    pub clock: Arc<dyn Clock>,
    pub config: AppConfig,
}
