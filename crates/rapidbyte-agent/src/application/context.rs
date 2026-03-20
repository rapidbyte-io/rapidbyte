//! DI container and configuration for the agent application layer.

use std::sync::Arc;
use std::time::Duration;

use crate::domain::ports::{ControllerGateway, PipelineExecutor, ProgressCollector};

/// Application-level configuration (derived from infrastructure config at startup).
#[derive(Debug, Clone)]
pub struct AgentAppConfig {
    /// Maximum number of concurrent tasks.
    pub max_tasks: u32,
    /// Interval between heartbeat RPCs.
    pub heartbeat_interval: Duration,
    /// Delay between completion retry attempts on transient errors.
    pub completion_retry_delay: Duration,
}

impl Default for AgentAppConfig {
    fn default() -> Self {
        Self {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            completion_retry_delay: Duration::from_secs(1),
        }
    }
}

/// Application-level dependency container.
///
/// Holds `Arc`-wrapped trait objects for every external dependency the
/// agent needs. Constructed once at startup by the adapter factory.
#[derive(Clone)]
pub struct AgentContext {
    /// Controller communication (register, poll, heartbeat, complete).
    pub gateway: Arc<dyn ControllerGateway>,
    /// Pipeline execution.
    pub executor: Arc<dyn PipelineExecutor>,
    /// Progress snapshot for heartbeating.
    pub progress: Arc<dyn ProgressCollector>,
    /// Application configuration.
    pub config: AgentAppConfig,
}
