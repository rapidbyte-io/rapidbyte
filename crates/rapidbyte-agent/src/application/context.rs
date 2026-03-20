//! DI container and configuration for the agent application layer.

use std::sync::Arc;
use std::time::Duration;

use crate::domain::ports::{
    Clock, ControllerGateway, MetricsProvider, PipelineExecutor, ProgressCollector,
};

/// Application-level configuration (derived from infrastructure config at startup).
#[derive(Debug, Clone)]
pub struct AgentAppConfig {
    /// Maximum number of concurrent tasks.
    pub max_tasks: u32,
    /// Interval between heartbeat RPCs.
    pub heartbeat_interval: Duration,
    /// Seconds the server holds a long-poll before returning no-task.
    pub poll_wait_seconds: u32,
    /// Delay between completion retry attempts.
    pub completion_retry_delay: Duration,
    /// Maximum number of completion retry attempts before giving up.
    pub max_completion_retries: u32,
}

impl Default for AgentAppConfig {
    fn default() -> Self {
        Self {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
            completion_retry_delay: Duration::from_secs(1),
            max_completion_retries: 60,
        }
    }
}

/// Application-level dependency container.
///
/// Holds `Arc`-wrapped trait objects for every external dependency the
/// agent needs. Constructed once at startup by the adapter factory.
pub struct AgentContext {
    /// Controller communication (register, poll, heartbeat, complete).
    pub gateway: Arc<dyn ControllerGateway>,
    /// Pipeline execution.
    pub executor: Arc<dyn PipelineExecutor>,
    /// Progress snapshot for heartbeating.
    pub progress: Arc<dyn ProgressCollector>,
    /// OTel metrics handles.
    pub metrics: Arc<dyn MetricsProvider>,
    /// Monotonic clock for timing.
    pub clock: Arc<dyn Clock>,
    /// Application configuration.
    pub config: AgentAppConfig,
}
