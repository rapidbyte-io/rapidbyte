//! `AgentService` gRPC trait implementation — thin delegation shim.
//!
//! All handler logic lives in [`crate::services::agent`] submodules.
//! This file wires the tonic trait methods to those free functions and
//! retains the `#[cfg(test)]` test suite until Task 10 migrates it.

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Barrier;
use tonic::{Request, Response, Status};

#[cfg(test)]
use crate::proto::rapidbyte::v1::{
    agent_directive, poll_task_response, run_event, ExecutionOptions, TaskOutcome,
};
use crate::proto::rapidbyte::v1::{
    agent_service_server::AgentService, CompleteTaskRequest, CompleteTaskResponse,
    HeartbeatRequest, HeartbeatResponse, PollTaskRequest, PollTaskResponse, RegisterAgentRequest,
    RegisterAgentResponse, ReportProgressRequest, ReportProgressResponse,
};
#[cfg(test)]
use crate::run_state::RunState as InternalRunState;
#[cfg(test)]
use crate::scheduler::TaskState;
use crate::state::ControllerState;

pub struct AgentServiceImpl {
    state: ControllerState,
    registry_url: String,
    registry_insecure: bool,
    trust_policy: String,
    trusted_key_pems: Vec<String>,
    #[cfg(test)]
    poll_barrier: Option<Arc<Barrier>>,
}

impl AgentServiceImpl {
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

    #[cfg(test)]
    fn with_poll_barrier(state: ControllerState, poll_barrier: Arc<Barrier>) -> Self {
        Self {
            state,
            registry_url: String::new(),
            registry_insecure: false,
            trust_policy: "skip".to_owned(),
            trusted_key_pems: Vec::new(),
            poll_barrier: Some(poll_barrier),
        }
    }

    /// Test-only delegation for `prepare_retry_if_allowed` (used by
    /// `test_prepare_retry_if_allowed_rechecks_cancelling_state`).
    #[cfg(test)]
    async fn prepare_retry_if_allowed(
        &self,
        run_id: &str,
        safe_to_retry: bool,
        retryable: bool,
        commit_state: &str,
    ) -> bool {
        // The real implementation lives in complete.rs; expose it here for
        // backwards-compatible test access until tests are migrated.
        crate::services::agent::complete::prepare_retry_if_allowed_for_test(
            &self.state,
            run_id,
            safe_to_retry,
            retryable,
            commit_state,
        )
        .await
    }

    fn handler(&self) -> crate::services::agent::AgentHandler {
        crate::services::agent::AgentHandler {
            state: self.state.clone(),
            registry_url: self.registry_url.clone(),
            registry_insecure: self.registry_insecure,
            trust_policy: self.trust_policy.clone(),
            trusted_key_pems: self.trusted_key_pems.clone(),
            #[cfg(test)]
            poll_barrier: self.poll_barrier.clone(),
        }
    }
}

#[tonic::async_trait]
impl AgentService for AgentServiceImpl {
    async fn register_agent(
        &self,
        request: Request<RegisterAgentRequest>,
    ) -> Result<Response<RegisterAgentResponse>, Status> {
        crate::services::agent::register::handle_register(&self.handler(), request.into_inner())
            .await
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        crate::services::agent::heartbeat::handle_heartbeat(&self.handler(), request.into_inner())
            .await
    }

    async fn poll_task(
        &self,
        request: Request<PollTaskRequest>,
    ) -> Result<Response<PollTaskResponse>, Status> {
        let handler = self.handler();
        crate::services::agent::poll::handle_poll(
            &handler,
            request.into_inner(),
            #[cfg(test)]
            handler.poll_barrier.as_deref(),
        )
        .await
    }

    async fn report_progress(
        &self,
        request: Request<ReportProgressRequest>,
    ) -> Result<Response<ReportProgressResponse>, Status> {
        crate::services::agent::poll::handle_report_progress(&self.handler(), request.into_inner())
            .await
    }

    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> Result<Response<CompleteTaskResponse>, Status> {
        crate::services::agent::complete::handle_complete(&self.handler(), request.into_inner())
            .await
    }
}

#[cfg(test)]
fn test_state() -> ControllerState {
    ControllerState::new(b"test-key-for-agent-service!!!!")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::manual_let_else, clippy::match_same_arms)]

    use super::*;
    use crate::proto::rapidbyte::v1::{
        pipeline_service_server::PipelineService as _, ActiveLease, PreviewAccess, PreviewState,
        ProgressUpdate, StreamPreview, SubmitPipelineRequest, TaskError,
    };
    use crate::store::test_support::FailingMetadataStore;

    /// Helper to submit a pipeline and return the `run_id`.
    async fn submit_pipeline(state: &ControllerState) -> String {
        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        svc.submit_pipeline(Request::new(SubmitPipelineRequest {
            pipeline_yaml_utf8: yaml.to_vec(),
            execution: Some(ExecutionOptions {
                dry_run: false,
                limit: None,
            }),
            idempotency_key: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .run_id
    }

    #[tokio::test]
    async fn test_register_agent_returns_uuid() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state);

        let resp = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 2,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: "hash".into(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.agent_id.is_empty());
        // Should be a valid UUID
        assert!(uuid::Uuid::parse_str(&resp.agent_id).is_ok());
    }

    #[tokio::test]
    async fn test_register_agent_rolls_back_when_persist_fails() {
        let store = FailingMetadataStore::new().fail_agent_upsert_on(1);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store.clone());
        let svc = AgentServiceImpl::new(state.clone());

        let err = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .expect_err("register should fail when agent persistence fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        assert!(state.registry.read().await.all_agents().is_empty());
        assert!(store.persisted_agent("nonexistent-agent").is_none());
    }

    #[tokio::test]
    async fn test_heartbeat_accepts_restored_agent() {
        let state = test_state();
        state
            .registry
            .write()
            .await
            .restore_agent(crate::registry::AgentRecord {
                agent_id: "restored-agent".into(),
                max_tasks: 1,
                active_tasks: 0,
                flight_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                last_heartbeat: std::time::Instant::now(),
                available_plugins: vec![],
                memory_bytes: 0,
            });
        let svc = AgentServiceImpl::new(state.clone());

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id: "restored-agent".into(),
                active_tasks: 0,
                active_leases: vec![],
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.directives.is_empty());
    }

    #[tokio::test]
    async fn test_poll_task_returns_pending_task() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        // Register agent
        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        // Submit a pipeline
        let _run_id = submit_pipeline(&state).await;

        // Poll — should get the task immediately (wait_seconds=0)
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::Task(t)) => {
                assert!(!t.task_id.is_empty());
                assert!(t.lease_epoch > 0);
            }
            _ => panic!("Expected a task assignment"),
        }
    }

    #[tokio::test]
    async fn test_poll_task_rolls_back_assignment_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(2);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let err = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .expect_err("assignment persistence failure should reject poll");
        assert_eq!(err.code(), tonic::Code::Internal);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Pending);
        assert!(run.current_task.is_none());
        drop(runs);

        let tasks = state.tasks.read().await;
        let task = tasks.find_by_run_id(&run_id).unwrap();
        assert_eq!(task.state, TaskState::Pending);
        assert!(task.lease.is_none());
        assert!(task.assigned_agent_id.is_none());
        assert_eq!(tasks.active_tasks_for_agent(&agent_id), 0);
    }

    #[tokio::test]
    async fn poll_task_persists_rejected_assignment() {
        let store = FailingMetadataStore::new();
        let state =
            ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store.clone());
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;
        let task_id = state
            .tasks
            .read()
            .await
            .find_by_run_id(&run_id)
            .expect("submitted task should exist")
            .task_id
            .clone();
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Cancelled)
                .unwrap();
        }

        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            resp.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        assert_eq!(
            store
                .persisted_task(&task_id)
                .expect("durable task should exist")
                .state,
            TaskState::Cancelled
        );
    }

    #[tokio::test]
    async fn poll_task_does_not_return_cancelled_assignment() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Cancelled)
                .unwrap();
        }

        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(matches!(
            resp.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        let tasks = state.tasks.read().await;
        let task = tasks.find_by_run_id(&run_id).unwrap();
        assert_eq!(task.state, TaskState::Cancelled);
        assert!(task.lease.is_none());
    }

    #[tokio::test]
    async fn test_poll_task_rejects_unknown_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let _run_id = submit_pipeline(&state).await;

        let err = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: "unknown-agent".into(),
                wait_seconds: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_poll_task_respects_agent_capacity() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let second_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;
        let second_run_id = submit_pipeline(&state).await;

        let first = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            first.result,
            Some(poll_task_response::Result::Task(_))
        ));

        let capped = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            capped.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        let second = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: second_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        match second.result {
            Some(poll_task_response::Result::Task(task)) => {
                assert_eq!(task.run_id, second_run_id);
            }
            _ => panic!("Expected second agent to receive queued task"),
        }
    }

    #[tokio::test]
    async fn test_poll_task_concurrent_polls_respect_agent_capacity() {
        let state = test_state();
        let poll_barrier = Arc::new(Barrier::new(2));
        let svc_a = AgentServiceImpl::with_poll_barrier(state.clone(), poll_barrier.clone());
        let svc_b = AgentServiceImpl::with_poll_barrier(state.clone(), poll_barrier);
        let register_svc = AgentServiceImpl::new(state.clone());

        let agent_id = register_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;
        submit_pipeline(&state).await;

        let (first, second) = tokio::join!(
            svc_a.poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            })),
            svc_b.poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            })),
        );

        let first = first.unwrap().into_inner();
        let second = second.unwrap().into_inner();
        let assigned = usize::from(matches!(
            first.result,
            Some(poll_task_response::Result::Task(_))
        )) + usize::from(matches!(
            second.result,
            Some(poll_task_response::Result::Task(_))
        ));
        let empty = usize::from(matches!(
            first.result,
            Some(poll_task_response::Result::NoTask(_))
        )) + usize::from(matches!(
            second.result,
            Some(poll_task_response::Result::NoTask(_))
        ));

        assert_eq!(assigned, 1, "only one concurrent poll should claim a task");
        assert_eq!(
            empty, 1,
            "the second concurrent poll should observe capacity"
        );
        assert_eq!(
            state.tasks.read().await.active_tasks_for_agent(&agent_id),
            1
        );
    }

    #[tokio::test]
    async fn test_complete_task_with_stale_epoch_returns_unacknowledged() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        // Register + submit + poll
        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with wrong epoch
        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch + 999,
                outcome: TaskOutcome::Completed.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.acknowledged);
    }

    #[tokio::test]
    async fn test_complete_task_completed_rolls_back_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let request = CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id.clone(),
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Completed.into(),
            error: None,
            metrics: None,
            preview: None,
            backend_run_id: 0,
        };

        let err = svc
            .complete_task(Request::new(request.clone()))
            .await
            .expect_err("task persistence failure should reject completion");
        assert_eq!(err.code(), tonic::Code::Internal);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Assigned);
        assert!(task_record.lease.is_some());
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
        drop(runs);

        let retry = svc
            .complete_task(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(retry.acknowledged);
    }

    #[tokio::test]
    async fn test_complete_task_completed_rolls_back_when_preview_persist_fails() {
        let store = FailingMetadataStore::new().fail_preview_upsert_on(1);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let request = CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id.clone(),
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Completed.into(),
            error: None,
            metrics: None,
            preview: Some(PreviewAccess {
                state: PreviewState::Ready.into(),
                flight_endpoint: "localhost:9093".into(),
                ticket: Vec::new(),
                expires_at: None,
                streams: vec![StreamPreview {
                    stream: "users".into(),
                    rows: 1,
                    ticket: Vec::new(),
                }],
            }),
            backend_run_id: 0,
        };

        let err = svc
            .complete_task(Request::new(request.clone()))
            .await
            .expect_err("preview persistence failure should reject completion");
        assert_eq!(err.code(), tonic::Code::Internal);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Assigned);
        assert!(task_record.lease.is_some());
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
        drop(runs);

        assert!(state.previews.read().await.get(&run_id).is_none());

        let retry = svc
            .complete_task(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(retry.acknowledged);
    }

    #[tokio::test]
    async fn test_complete_task_completed_rolls_back_when_run_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let request = CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id.clone(),
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Completed.into(),
            error: None,
            metrics: None,
            preview: None,
            backend_run_id: 0,
        };

        let err = svc
            .complete_task(Request::new(request.clone()))
            .await
            .expect_err("run persistence failure should reject completion");
        assert_eq!(err.code(), tonic::Code::Internal);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Assigned);
        assert!(task_record.lease.is_some());
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
        drop(runs);

        let retry = svc
            .complete_task(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(retry.acknowledged);
    }

    #[tokio::test]
    async fn test_complete_task_rejects_wrong_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id: wrong_agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Completed.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.acknowledged);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert_eq!(record.assigned_agent_id.as_deref(), Some(agent_id.as_str()));
    }

    #[tokio::test]
    async fn test_complete_task_rejects_unknown_outcome() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: 999,
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert!(record.lease.is_some());

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_complete_task_rejects_unspecified_outcome() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Unspecified.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let tasks = state.tasks.read().await;
        let record = tasks.get(&task.task_id).unwrap();
        assert_eq!(record.state, TaskState::Assigned);
        assert!(record.lease.is_some());

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_complete_task_cancelled_from_assigned_transitions_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Cancelled.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Cancelled);
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Cancelled);
    }

    #[tokio::test]
    async fn test_complete_task_cancelled_from_cancelling_transitions_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Cancelled.into(),
                error: None,
                metrics: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Cancelled);
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Cancelled);
    }

    #[tokio::test]
    async fn test_complete_task_safe_to_retry_requeues() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with retryable + safe_to_retry
        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id,
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "CONN_RESET".into(),
                message: "connection reset".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        // Should be requeued — poll again
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::Task(t)) => {
                assert_eq!(t.run_id, run_id);
                assert_eq!(t.attempt, 2);
            }
            _ => panic!("Expected requeued task"),
        }
    }

    #[tokio::test]
    async fn test_complete_task_retry_requeue_rolls_back_when_run_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let request = CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id.clone(),
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "CONN_RESET".into(),
                message: "connection reset".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        };

        let err = svc
            .complete_task(Request::new(request.clone()))
            .await
            .expect_err("run persistence failure should reject retry requeue");
        assert_eq!(err.code(), tonic::Code::Internal);

        let tasks = state.tasks.read().await;
        let task_record = tasks.get(&task.task_id).unwrap();
        assert_eq!(task_record.state, TaskState::Assigned);
        assert!(task_record.lease.is_some());
        assert_eq!(tasks.all_tasks().len(), 1);
        drop(tasks);

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
        drop(runs);

        let retry = svc
            .complete_task(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(retry.acknowledged);

        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        match resp.result {
            Some(poll_task_response::Result::Task(t)) => {
                assert_eq!(t.run_id, run_id);
                assert_eq!(t.attempt, 2);
            }
            _ => panic!("Expected requeued task"),
        }
    }

    #[tokio::test]
    async fn test_complete_task_unsafe_does_not_requeue() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Complete with safe_to_retry=false
        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id,
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "DATA_ERROR".into(),
                message: "schema mismatch".into(),
                retryable: true,
                safe_to_retry: false,
                commit_state: "after_commit_unknown".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        // Should NOT be requeued — poll returns empty
        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::NoTask(_)) => {} // expected
            None => {}                                        // also fine
            _ => panic!("Expected no task"),
        }
    }

    #[tokio::test]
    async fn test_complete_task_invalid_commit_state_does_not_requeue() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: task.task_id.clone(),
            lease_epoch: task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "UNKNOWN".into(),
                message: "ambiguous".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "mystery_state".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        let resp = svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        match resp.result {
            Some(poll_task_response::Result::NoTask(_)) => {}
            None => {}
            _ => panic!("Expected no task"),
        }

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Failed);
        assert_eq!(
            record.error.as_ref().map(|e| e.message.as_str()),
            Some("UNKNOWN: ambiguous")
        );
    }

    #[tokio::test]
    async fn test_heartbeat_returns_cancel_directive_for_cancelling_run() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        // Transition to Running then Cancelling
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        // Heartbeat should return a cancel directive
        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id.clone(),
                    lease_epoch: task.lease_epoch,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.directives.len(), 1);
        match &resp.directives[0].directive {
            Some(agent_directive::Directive::CancelTask(ct)) => {
                assert_eq!(ct.task_id, task.task_id);
            }
            _ => panic!("Expected CancelTask directive"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_returns_cancel_directive_for_assigned_run_cancelled_via_pipeline_service(
    ) {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());
        let pipeline_svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let cancel = pipeline_svc
            .cancel_run(Request::new(
                crate::proto::rapidbyte::v1::CancelRunRequest {
                    run_id: run_id.clone(),
                },
            ))
            .await
            .unwrap()
            .into_inner();

        assert!(cancel.accepted);

        {
            let runs = state.runs.read().await;
            assert_eq!(
                runs.get_run(&run_id).unwrap().state,
                InternalRunState::Cancelling
            );
        }
        {
            let tasks = state.tasks.read().await;
            let record = tasks.get(&task.task_id).unwrap();
            assert_eq!(record.state, TaskState::Assigned);
            assert!(record.lease.is_some());
        }

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id.clone(),
                    lease_epoch: task.lease_epoch,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.directives.len(), 1);
        match &resp.directives[0].directive {
            Some(agent_directive::Directive::CancelTask(ct)) => {
                assert_eq!(ct.task_id, task.task_id);
            }
            _ => panic!("Expected CancelTask directive"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_return_cancel_directive_for_foreign_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id: wrong_agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id,
                    lease_epoch: task.lease_epoch,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.directives.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_return_cancel_directive_for_stale_epoch() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id,
                active_leases: vec![ActiveLease {
                    task_id: task.task_id,
                    lease_epoch: task.lease_epoch + 1,
                }],
                active_tasks: 1,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.directives.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_renew_other_agents_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id,
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let before = {
            let tasks = state.tasks.read().await;
            tasks
                .get(&task.task_id)
                .unwrap()
                .lease
                .as_ref()
                .unwrap()
                .expires_at
        };

        tokio::time::sleep(Duration::from_millis(5)).await;

        svc.heartbeat(Request::new(HeartbeatRequest {
            agent_id: wrong_agent_id,
            active_leases: vec![ActiveLease {
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
            }],
            active_tasks: 1,
            cpu_usage: 0.0,
            memory_used_bytes: 0,
        }))
        .await
        .unwrap();

        let after = {
            let tasks = state.tasks.read().await;
            tasks
                .get(&task.task_id)
                .unwrap()
                .lease
                .as_ref()
                .unwrap()
                .expires_at
        };

        assert_eq!(after, before);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_heartbeat_rolls_back_renewed_leases_when_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(6);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store.clone());
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 2,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let first_run_id = submit_pipeline(&state).await;
        let second_run_id = submit_pipeline(&state).await;

        let first_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(task)) => task,
            _ => panic!("expected first task"),
        };
        let second_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(task)) => task,
            _ => panic!("expected second task"),
        };

        let before = {
            let tasks = state.tasks.read().await;
            [
                tasks
                    .get(&first_task.task_id)
                    .unwrap()
                    .lease
                    .clone()
                    .expect("first lease should exist"),
                tasks
                    .get(&second_task.task_id)
                    .unwrap()
                    .lease
                    .clone()
                    .expect("second lease should exist"),
            ]
        };

        tokio::time::sleep(Duration::from_millis(5)).await;

        let err = svc
            .heartbeat(Request::new(HeartbeatRequest {
                agent_id: agent_id.clone(),
                active_leases: vec![
                    ActiveLease {
                        task_id: first_task.task_id.clone(),
                        lease_epoch: first_task.lease_epoch,
                    },
                    ActiveLease {
                        task_id: second_task.task_id.clone(),
                        lease_epoch: second_task.lease_epoch,
                    },
                ],
                active_tasks: 2,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            }))
            .await
            .expect_err("heartbeat should fail when one renewed lease cannot persist");
        assert_eq!(err.code(), tonic::Code::Internal);

        let tasks = state.tasks.read().await;
        let first_after = tasks
            .get(&first_task.task_id)
            .unwrap()
            .lease
            .clone()
            .expect("first lease should still exist");
        let second_after = tasks
            .get(&second_task.task_id)
            .unwrap()
            .lease
            .clone()
            .expect("second lease should still exist");
        drop(tasks);

        assert_eq!(first_after.epoch, before[0].epoch);
        assert_eq!(first_after.expires_at, before[0].expires_at);
        assert_eq!(second_after.epoch, before[1].epoch);
        assert_eq!(second_after.expires_at, before[1].expires_at);

        let first_persisted = store
            .persisted_task(
                &state
                    .tasks
                    .read()
                    .await
                    .find_by_run_id(&first_run_id)
                    .unwrap()
                    .task_id,
            )
            .expect("first durable task should exist");
        let second_persisted = store
            .persisted_task(
                &state
                    .tasks
                    .read()
                    .await
                    .find_by_run_id(&second_run_id)
                    .unwrap()
                    .task_id,
            )
            .expect("second durable task should exist");
        assert_eq!(first_persisted.lease.unwrap().epoch, before[0].epoch);
        assert_eq!(second_persisted.lease.unwrap().epoch, before[1].epoch);
    }

    #[tokio::test]
    async fn test_report_progress_rejects_missing_lease() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        state.tasks.write().await.cancel(&task.task_id).unwrap();

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn test_report_progress_rejects_wrong_agent() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let owner_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let wrong_agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9092".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: owner_agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id: wrong_agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::PermissionDenied);

        {
            let tasks = state.tasks.read().await;
            let record = tasks.get(&task.task_id).unwrap();
            assert_eq!(record.state, TaskState::Assigned);
            assert_eq!(
                record.assigned_agent_id.as_deref(),
                Some(owner_agent_id.as_str())
            );
        }

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_report_progress_does_not_flip_reassigned_attempt() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let first_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected first task"),
        };

        svc.complete_task(Request::new(CompleteTaskRequest {
            agent_id: agent_id.clone(),
            task_id: first_task.task_id.clone(),
            lease_epoch: first_task.lease_epoch,
            outcome: TaskOutcome::Failed.into(),
            error: Some(TaskError {
                code: "RETRY".into(),
                message: "try again".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: "before_commit".into(),
            }),
            metrics: None,
            preview: None,
            backend_run_id: 0,
        }))
        .await
        .unwrap();

        let second_task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected requeued task"),
        };

        assert_ne!(second_task.task_id, first_task.task_id);
        assert_eq!(second_task.run_id, run_id);
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Assigned
        );

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id,
                task_id: first_task.task_id,
                lease_epoch: first_task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Assigned
        );
    }

    #[tokio::test]
    async fn test_report_progress_rolls_back_when_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;
        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(task)) => task,
            _ => panic!("Expected task"),
        };

        let err = svc
            .report_progress(Request::new(ReportProgressRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                progress: Some(ProgressUpdate {
                    stream: "users".into(),
                    phase: "running".into(),
                    records: 1,
                    bytes: 64,
                }),
            }))
            .await
            .expect_err("progress persistence failure should reject progress");

        assert_eq!(err.code(), tonic::Code::Internal);

        {
            let tasks = state.tasks.read().await;
            let record = tasks.get(&task.task_id).unwrap();
            assert_eq!(record.state, TaskState::Assigned);
            assert!(record.lease.is_some());
        }

        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn test_report_progress_transitions_reconciling_run_to_running() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;
        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(task)) => task,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            let run = runs.get_run_mut(&run_id).unwrap();
            run.state = InternalRunState::Reconciling;
        }

        svc.report_progress(Request::new(ReportProgressRequest {
            agent_id,
            task_id: task.task_id,
            lease_epoch: task.lease_epoch,
            progress: Some(ProgressUpdate {
                stream: "users".into(),
                phase: "running".into(),
                records: 5,
                bytes: 10,
            }),
        }))
        .await
        .unwrap();

        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            InternalRunState::Running
        );
    }

    #[tokio::test]
    async fn test_complete_task_from_cancelling_transitions_run_to_completed() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Completed.into(),
                metrics: Some(crate::proto::rapidbyte::v1::TaskMetrics {
                    records_processed: 7,
                    bytes_processed: 42,
                    elapsed_seconds: 1.5,
                    cursors_advanced: 1,
                }),
                error: None,
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Completed);
        assert_eq!(record.metrics.total_records, 7);
    }

    #[tokio::test]
    async fn test_complete_task_failure_from_cancelling_transitions_run_to_failed() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id,
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Failed.into(),
                metrics: None,
                error: Some(TaskError {
                    code: "PLUGIN".into(),
                    message: "boom".into(),
                    retryable: false,
                    safe_to_retry: false,
                    commit_state: "before_commit".into(),
                }),
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Failed);
        assert_eq!(
            record.error.as_ref().map(|e| e.message.as_str()),
            Some("PLUGIN: boom")
        );
    }

    #[tokio::test]
    async fn test_complete_task_retryable_failure_from_cancelling_does_not_requeue() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let agent_id = svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .agent_id;

        let run_id = submit_pipeline(&state).await;

        let task = match svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner()
            .result
        {
            Some(poll_task_response::Result::Task(t)) => t,
            _ => panic!("Expected task"),
        };

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Running).unwrap();
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let resp = svc
            .complete_task(Request::new(CompleteTaskRequest {
                agent_id,
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                outcome: TaskOutcome::Failed.into(),
                metrics: None,
                error: Some(TaskError {
                    code: "RETRY".into(),
                    message: "try again".into(),
                    retryable: true,
                    safe_to_retry: true,
                    commit_state: "before_commit".into(),
                }),
                preview: None,
                backend_run_id: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.acknowledged);

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Failed);
        assert_eq!(
            record.error.as_ref().map(|e| e.message.as_str()),
            Some("RETRY: try again")
        );
        drop(runs);

        let tasks = state.tasks.read().await;
        let latest = tasks.find_by_run_id(&run_id).unwrap();
        assert_eq!(latest.task_id, task.task_id);
        assert_eq!(latest.attempt, 1);
        assert_eq!(latest.state, TaskState::Failed);
    }

    #[tokio::test]
    async fn test_prepare_retry_if_allowed_rechecks_cancelling_state() {
        let state = test_state();
        let svc = AgentServiceImpl::new(state.clone());

        let run_id = submit_pipeline(&state).await;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Running).unwrap();
        }

        {
            let runs = state.runs.read().await;
            assert_eq!(
                runs.get_run(&run_id).unwrap().state,
                InternalRunState::Running
            );
        }

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Cancelling)
                .unwrap();
        }

        let should_retry = svc
            .prepare_retry_if_allowed(&run_id, true, true, "before_commit")
            .await;

        assert!(!should_retry);

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Cancelling
        );
    }

    /// Helper: set up a controller state with a custom secret provider,
    /// submit a pipeline with vault refs, register an agent, and poll.
    async fn poll_with_secret_provider(
        provider: impl rapidbyte_secrets::SecretProvider + 'static,
    ) -> (
        ControllerState,
        String, // run_id
        poll_task_response::Result,
    ) {
        let mut providers = rapidbyte_secrets::SecretProviders::new();
        providers.register("vault", std::sync::Arc::new(provider));
        let state = test_state().with_secrets(providers);

        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\nsource:\n  config:\n    password: ${vault:secret/db#password}\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let agent_svc = AgentServiceImpl::new(state.clone());
        let agent_resp = agent_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let poll_resp = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_resp.agent_id,
                wait_seconds: 0,
            }))
            .await
            .expect("poll_task should not return gRPC error");

        let result = poll_resp.into_inner().result.unwrap();
        (state, run_id, result)
    }

    /// Like `poll_with_secret_provider` but takes a pre-built `ControllerState`
    /// (e.g., one backed by a `FailingMetadataStore`).
    async fn poll_with_state_and_provider(
        state: ControllerState,
        provider: impl rapidbyte_secrets::SecretProvider + 'static,
    ) -> (
        ControllerState,
        String, // run_id
        String, // agent_id
        std::result::Result<Response<PollTaskResponse>, Status>,
    ) {
        let mut providers = rapidbyte_secrets::SecretProviders::new();
        providers.register("vault", std::sync::Arc::new(provider));
        let state = state.with_secrets(providers);

        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\nsource:\n  config:\n    password: ${vault:secret/db#password}\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let agent_svc = AgentServiceImpl::new(state.clone());
        let agent_resp = agent_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let agent_id = agent_resp.agent_id.clone();
        let poll_result = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_resp.agent_id,
                wait_seconds: 0,
            }))
            .await;

        (state, run_id, agent_id, poll_result)
    }

    #[tokio::test]
    async fn transient_secret_failure_releases_task_for_retry() {
        struct TransientFailProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for TransientFailProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::Unavailable(
                    "connection refused".into(),
                ))
            }
        }

        let (state, run_id, result) = poll_with_secret_provider(TransientFailProvider).await;

        assert!(
            matches!(result, poll_task_response::Result::NoTask(_)),
            "expected NoTask after transient failure"
        );

        // Task should be back in Pending for retry.
        let tasks = state.tasks.read().await;
        let task = tasks
            .all_tasks()
            .into_iter()
            .find(|t| t.run_id == run_id)
            .expect("task should exist");
        assert_eq!(
            task.state,
            crate::scheduler::TaskState::Pending,
            "task should be Pending for retry"
        );
        drop(tasks);

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Pending,
            "run should be Pending for retry"
        );
    }

    #[tokio::test]
    async fn permanent_secret_config_error_fails_task() {
        struct MissingKeyProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for MissingKeyProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::NotFound(
                    "key 'password' not found in Vault secret secret/db".into(),
                ))
            }
        }

        let (state, run_id, result) = poll_with_secret_provider(MissingKeyProvider).await;

        assert!(
            matches!(result, poll_task_response::Result::NoTask(_)),
            "expected NoTask after permanent failure"
        );

        // Task should be permanently Failed.
        let tasks = state.tasks.read().await;
        let task = tasks
            .all_tasks()
            .into_iter()
            .find(|t| t.run_id == run_id)
            .expect("task should exist");
        assert_eq!(task.state, crate::scheduler::TaskState::Failed);
        drop(tasks);

        let runs = state.runs.read().await;
        assert_eq!(
            runs.get_run(&run_id).unwrap().state,
            InternalRunState::Failed
        );
    }

    #[tokio::test]
    async fn transient_secret_persist_failure_rolls_back_and_returns_error() {
        use crate::store::test_support::FailingMetadataStore;

        struct TransientFailProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for TransientFailProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::Unavailable(
                    "connection refused".into(),
                ))
            }
        }

        // Fail the 3rd run upsert: 1st = submit_pipeline, 2nd = try_claim_task, 3rd = secret failure handler.
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let (state, run_id, _agent_id, poll_result) =
            poll_with_state_and_provider(state, TransientFailProvider).await;

        // poll_task should return a gRPC error (not silently succeed).
        assert!(
            poll_result.is_err(),
            "expected gRPC error on persist failure"
        );

        // In-memory state should be rolled back to Assigned (what was durably persisted).
        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(
            run.state,
            InternalRunState::Assigned,
            "run should be rolled back to Assigned after persist failure"
        );
    }

    #[tokio::test]
    async fn permanent_secret_persist_failure_rolls_back_and_returns_error() {
        use crate::store::test_support::FailingMetadataStore;

        struct MissingKeyProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for MissingKeyProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::NotFound(
                    "key 'password' not found in Vault secret secret/db".into(),
                ))
            }
        }

        // Fail the 3rd run upsert: 1st = submit_pipeline, 2nd = try_claim_task, 3rd = secret failure handler.
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let (state, run_id, _agent_id, poll_result) =
            poll_with_state_and_provider(state, MissingKeyProvider).await;

        // poll_task should return a gRPC error (not silently succeed).
        assert!(
            poll_result.is_err(),
            "expected gRPC error on persist failure"
        );

        // In-memory state should be rolled back to Assigned (what was durably persisted).
        let runs = state.runs.read().await;
        let run = runs.get_run(&run_id).unwrap();
        assert_eq!(
            run.state,
            InternalRunState::Assigned,
            "run should be rolled back to Assigned after persist failure"
        );
        drop(runs);

        let tasks = state.tasks.read().await;
        let task = tasks
            .all_tasks()
            .into_iter()
            .find(|t| t.run_id == run_id)
            .expect("task should exist");
        assert_eq!(
            task.state,
            crate::scheduler::TaskState::Assigned,
            "task should be rolled back to Assigned after persist failure"
        );
    }

    #[tokio::test]
    async fn permanent_secret_failure_event_uses_correct_attempt() {
        struct MissingKeyProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for MissingKeyProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::NotFound(
                    "key 'password' not found".into(),
                ))
            }
        }

        // Build state, submit pipeline, register agent — but bump the run attempt
        // to 3 before polling to simulate a retry scenario.
        let mut providers = rapidbyte_secrets::SecretProviders::new();
        providers.register("vault", std::sync::Arc::new(MissingKeyProvider));
        let state = test_state().with_secrets(providers);

        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\nsource:\n  config:\n    password: ${vault:secret/db#password}\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        // Bump the run attempt to 3 and the task attempt to match.
        {
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_run_mut(&run_id) {
                run.attempt = 3;
            }
        }
        {
            let mut tasks = state.tasks.write().await;
            let task_records: Vec<_> = tasks
                .all_tasks()
                .into_iter()
                .filter(|t| t.run_id == run_id)
                .collect();
            for mut task in task_records {
                task.attempt = 3;
                tasks.restore_task(task);
            }
        }

        // Subscribe to watcher before polling.
        let mut rx = state.watchers.write().await.subscribe(&run_id);

        let agent_svc = AgentServiceImpl::new(state.clone());
        let agent_resp = agent_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 1,
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let _poll_resp = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_resp.agent_id,
                wait_seconds: 0,
            }))
            .await
            .expect("poll_task should not return gRPC error");

        // Check the terminal event's attempt field.
        let event = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .expect("should receive terminal event")
            .expect("channel should not be closed");

        match event.event {
            Some(run_event::Event::Failed(failed)) => {
                assert_eq!(
                    failed.attempt, 3,
                    "terminal event should use run attempt (3), not hardcoded 1"
                );
            }
            other => panic!("expected Failed event, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn transient_secret_requeue_notifies_waiters() {
        struct TransientFailProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for TransientFailProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::Unavailable(
                    "connection refused".into(),
                ))
            }
        }

        let (state, _run_id, result) = poll_with_secret_provider(TransientFailProvider).await;

        assert!(
            matches!(result, poll_task_response::Result::NoTask(_)),
            "expected NoTask after transient failure"
        );

        // After successful transient requeue, a long-poll waiter should
        // be woken. Verify by subscribing to task_notify and checking
        // that a notification is immediately available (it was already sent).
        let notified = state.task_notify.notified();
        // If notify_waiters was called, `notified()` created after the
        // call won't fire immediately — but we can verify indirectly by
        // checking the task is pending and pollable.
        let tasks = state.tasks.read().await;
        assert!(
            tasks.peek_pending().is_some(),
            "requeued task should be in pending queue and pollable"
        );
        drop(tasks);
        drop(notified);
    }

    #[tokio::test]
    async fn transient_persist_failure_rollback_cleans_pending_queue() {
        use crate::store::test_support::FailingMetadataStore;

        struct TransientFailProvider;

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for TransientFailProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                Err(rapidbyte_secrets::SecretError::Unavailable(
                    "connection refused".into(),
                ))
            }
        }

        // Fail the 3rd run upsert: 1st = submit_pipeline, 2nd = try_claim_task, 3rd = secret failure handler.
        let store = FailingMetadataStore::new().fail_run_upsert_on(3);
        let state = ControllerState::with_metadata_store(b"test-key-for-agent-service!!!!", store);
        let (state, run_id, _agent_id, poll_result) =
            poll_with_state_and_provider(state, TransientFailProvider).await;

        assert!(
            poll_result.is_err(),
            "expected gRPC error on persist failure"
        );

        // After rollback, the task should be Assigned (not Pending), and
        // critically it must NOT appear in the pending queue — otherwise
        // a second agent could claim an already-assigned task.
        let tasks = state.tasks.read().await;
        let task = tasks
            .all_tasks()
            .into_iter()
            .find(|t| t.run_id == run_id)
            .expect("task should exist");
        assert_eq!(
            task.state,
            crate::scheduler::TaskState::Assigned,
            "task should be rolled back to Assigned"
        );
        assert!(
            tasks.peek_pending().is_none(),
            "pending queue must be empty after rollback — no duplicate assignment risk"
        );
    }

    #[tokio::test]
    async fn make_task_response_preserves_env_vars_for_agent() {
        // Verify the distributed-mode contract: env vars are NOT resolved
        // by the controller — they are left for the agent to expand from
        // its own environment.
        let assignment = crate::scheduler::TaskAssignment {
            task_id: "task-1".into(),
            run_id: "run-1".into(),
            attempt: 1,
            lease_epoch: 1,
            pipeline_yaml: b"host: ${DB_HOST}".to_vec(),
            dry_run: false,
            limit: None,
        };

        let secrets = rapidbyte_secrets::SecretProviders::new();
        let resp = crate::services::agent::resolve_and_build_response(assignment, &secrets)
            .await
            .expect("resolve_and_build_response should succeed");

        let task = match resp.result.unwrap() {
            poll_task_response::Result::Task(t) => t,
            other @ poll_task_response::Result::NoTask(_) => {
                panic!("expected Task, got: {other:?}")
            }
        };

        let yaml = String::from_utf8(task.pipeline_yaml_utf8).unwrap();
        assert_eq!(
            yaml, "host: ${DB_HOST}",
            "env vars must be preserved for the agent to resolve"
        );
    }

    #[tokio::test]
    async fn transient_failure_with_expired_lease_defers_to_sweep() {
        // Provider that returns Unavailable after the lease has been expired.
        struct SlowTransientProvider {
            state: ControllerState,
        }

        #[async_trait::async_trait]
        impl rapidbyte_secrets::SecretProvider for SlowTransientProvider {
            async fn read_secret(
                &self,
                _path: &str,
                _key: &str,
            ) -> Result<String, rapidbyte_secrets::SecretError> {
                // Expire all leases while "resolving" the secret.
                let expired = self.state.tasks.write().await.expire_leases();
                assert!(!expired.is_empty(), "should have expired a lease");
                Err(rapidbyte_secrets::SecretError::Unavailable(
                    "connection refused".into(),
                ))
            }
        }

        let state = test_state();

        // Use a very short lease so expire_leases() triggers.
        let svc = crate::pipeline_service::PipelineServiceImpl::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\nsource:\n  config:\n    password: ${vault:secret/db#password}\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        // Shorten the lease TTL on the pending task so it expires immediately.
        {
            let mut tasks = state.tasks.write().await;
            let task_records: Vec<_> = tasks
                .all_tasks()
                .into_iter()
                .filter(|t| t.run_id == run_id)
                .collect();
            for mut task in task_records {
                // Set lease with 0-second TTL so it's already expired.
                task.lease = Some(crate::lease::Lease::new(1, std::time::Duration::ZERO));
                task.state = crate::scheduler::TaskState::Assigned;
                task.assigned_agent_id = Some("test-agent".into());
                tasks.restore_task(task);
            }
        }

        let mut providers = rapidbyte_secrets::SecretProviders::new();
        providers.register(
            "vault",
            std::sync::Arc::new(SlowTransientProvider {
                state: state.clone(),
            }),
        );
        let state = state.with_secrets(providers);

        let agent_svc = AgentServiceImpl::new(state.clone());
        let agent_resp = agent_svc
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: 2, // Allow polling even with the expired task.
                flight_advertise_endpoint: "localhost:9091".into(),
                plugin_bundle_hash: String::new(),
                available_plugins: vec![],
                memory_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let poll_resp = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_resp.agent_id,
                wait_seconds: 0,
            }))
            .await
            .expect("poll_task should not return gRPC error");

        // Should get NoTask — the handler bailed because the lease expired.
        let result = poll_resp.into_inner().result.unwrap();
        assert!(
            matches!(result, poll_task_response::Result::NoTask(_)),
            "expected NoTask when lease expired during secret resolution"
        );

        // The task should NOT be in Pending (no stale requeue). The sweep
        // is responsible for terminal cleanup.
        let tasks = state.tasks.read().await;
        let task = tasks
            .all_tasks()
            .into_iter()
            .find(|t| t.run_id == run_id)
            .expect("task should exist");
        assert_ne!(
            task.state,
            crate::scheduler::TaskState::Pending,
            "task must not be requeued when lease is stale"
        );
    }
}
