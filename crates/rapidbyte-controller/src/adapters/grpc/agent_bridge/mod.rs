//! Agent-facing gRPC service handlers.

use std::time::Duration;

pub(crate) mod complete;
mod dispatch;
pub(crate) mod heartbeat;
pub(crate) mod poll;
pub(crate) mod register;
pub(crate) mod secret;

pub(crate) use dispatch::resolve_and_build_response;

use crate::state::ControllerState;

/// Default lease TTL for assigned tasks.
pub(crate) const LEASE_TTL: Duration = Duration::from_secs(300);

pub(crate) const ERROR_CODE_SECRET_RESOLUTION: &str = "SECRET_RESOLUTION_FAILED";

#[derive(Clone)]
pub struct AgentHandler {
    pub(crate) state: ControllerState,
    pub(crate) registry_url: String,
    pub(crate) registry_insecure: bool,
    pub(crate) trust_policy: String,
    pub(crate) trusted_key_pems: Vec<String>,
    #[cfg(test)]
    pub(crate) poll_barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
}

impl AgentHandler {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self {
            state,
            registry_url: String::new(),
            registry_insecure: false,
            trust_policy: "skip".to_owned(),
            trusted_key_pems: Vec::new(),
            #[cfg(test)]
            poll_barrier: None,
        }
    }

    #[must_use]
    pub fn with_trust_config(
        state: ControllerState,
        registry_url: String,
        registry_insecure: bool,
        trust_policy: String,
        trusted_key_pems: Vec<String>,
    ) -> Self {
        Self {
            state,
            registry_url,
            registry_insecure,
            trust_policy,
            trusted_key_pems,
            #[cfg(test)]
            poll_barrier: None,
        }
    }
}

use tonic::{Request, Response, Status};

use crate::proto::rapidbyte::v2::{
    agent_service_server::AgentService, CompleteTaskRequest, CompleteTaskResponse,
    HeartbeatRequest, HeartbeatResponse, PollTaskRequest, PollTaskResponse, RegisterAgentRequest,
    RegisterAgentResponse, ReportProgressRequest, ReportProgressResponse,
};

#[tonic::async_trait]
impl AgentService for AgentHandler {
    async fn register_agent(
        &self,
        request: Request<RegisterAgentRequest>,
    ) -> Result<Response<RegisterAgentResponse>, Status> {
        register::handle_register(self, request.into_inner()).await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        heartbeat::handle_heartbeat(self, request.into_inner()).await
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        poll::handle_poll(
            self,
            request.into_inner(),
            #[cfg(test)]
            self.poll_barrier.as_deref(),
        )
        .await
    }

    async fn report_progress(
        &self,
        request: Request<ReportProgressRequest>,
    ) -> Result<Response<ReportProgressResponse>, Status> {
        poll::handle_report_progress(self, request.into_inner()).await
    }

    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> Result<Response<CompleteTaskResponse>, Status> {
        complete::handle_complete(self, request.into_inner()).await
    }
}
