//! Outbound port for communicating with the controller.

use async_trait::async_trait;

use crate::domain::error::AgentError;
use crate::domain::progress::ProgressSnapshot;
use crate::domain::task::TaskExecutionResult;

/// What the agent tells the controller at registration.
#[derive(Debug, Clone)]
pub struct RegistrationConfig {
    pub max_tasks: u32,
}

/// A task assigned by the controller.
#[derive(Debug, Clone)]
pub struct TaskAssignment {
    pub task_id: String,
    pub run_id: String,
    pub pipeline_yaml: String,
    pub lease_epoch: u64,
    pub attempt: u32,
}

/// Heartbeat request payload.
#[derive(Debug, Clone)]
pub struct HeartbeatPayload {
    pub agent_id: String,
    pub tasks: Vec<TaskHeartbeat>,
}

/// Per-task heartbeat data.
#[derive(Debug, Clone)]
pub struct TaskHeartbeat {
    pub task_id: String,
    pub lease_epoch: u64,
    pub progress: ProgressSnapshot,
}

/// Heartbeat response from the controller.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub directives: Vec<TaskDirective>,
}

/// Per-task directive from the controller.
#[derive(Debug, Clone)]
pub struct TaskDirective {
    pub task_id: String,
    pub cancel_requested: bool,
}

/// Task completion report.
#[derive(Debug, Clone)]
pub struct CompletionPayload {
    pub agent_id: String,
    pub task_id: String,
    pub lease_epoch: u64,
    pub result: TaskExecutionResult,
}

/// Registry configuration received from the controller on registration.
#[derive(Debug, Clone, Default)]
pub struct ControllerRegistryInfo {
    pub url: Option<String>,
    pub insecure: bool,
}

/// Registration response from the controller.
#[derive(Debug, Clone)]
pub struct RegistrationResponse {
    pub agent_id: String,
    pub registry: Option<ControllerRegistryInfo>,
}

/// Outbound port for all controller communication.
#[async_trait]
pub trait ControllerGateway: Send + Sync {
    /// Register this agent with the controller.
    async fn register(
        &self,
        config: &RegistrationConfig,
    ) -> Result<RegistrationResponse, AgentError>;

    /// Long-poll for a task assignment. Returns `None` if no task available.
    async fn poll(&self, agent_id: &str) -> Result<Option<TaskAssignment>, AgentError>;

    /// Send heartbeat with progress and active leases.
    async fn heartbeat(&self, request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError>;

    /// Report task completion (success, failure, or cancellation).
    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError>;
}
