//! Pipeline-facing gRPC service handlers.

mod cancel;
mod convert;
mod query;
mod submit;

#[allow(unused_imports)] // re-exported for callers outside the pipeline module
pub(crate) use convert::to_proto_state;

use std::pin::Pin;

use tonic::{Request, Response, Status};

use crate::proto::rapidbyte::v1::{
    pipeline_service_server::PipelineService, CancelRunRequest, CancelRunResponse, GetRunRequest,
    GetRunResponse, ListRunsRequest, ListRunsResponse, RunEvent, SubmitPipelineRequest,
    SubmitPipelineResponse, WatchRunRequest,
};
use crate::state::ControllerState;

pub struct PipelineHandler {
    pub(crate) state: ControllerState,
}

impl PipelineHandler {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }
}

type WatchRunStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<RunEvent, Status>> + Send>>;

#[tonic::async_trait]
impl PipelineService for PipelineHandler {
    type WatchRunStream = WatchRunStream;

    async fn submit_pipeline(
        &self,
        request: Request<SubmitPipelineRequest>,
    ) -> Result<Response<SubmitPipelineResponse>, Status> {
        submit::handle_submit(self, request.into_inner()).await
    }

    async fn get_run(
        &self,
        request: Request<GetRunRequest>,
    ) -> Result<Response<GetRunResponse>, Status> {
        query::handle_get_run(self, request.into_inner()).await
    }

    async fn watch_run(
        &self,
        request: Request<WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        query::handle_watch_run(self, request.into_inner()).await
    }

    async fn cancel_run(
        &self,
        request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        cancel::handle_cancel(self, request.into_inner()).await
    }

    async fn list_runs(
        &self,
        request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        query::handle_list_runs(self, request.into_inner()).await
    }
}

/// Helper to build a `ControllerState` for tests with a default signing key.
#[cfg(test)]
fn test_state() -> ControllerState {
    ControllerState::new(b"test-key-for-pipeline-service!!")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rapidbyte::v1::{
        agent_service_server::AgentService as _, ExecutionOptions, PollTaskRequest,
        RegisterAgentRequest,
    };
    use crate::run_state::RunState;
    use crate::scheduler::TaskState;
    use crate::store::test_support::FailingMetadataStore;
    use tokio_stream::StreamExt;

    use crate::proto::rapidbyte::v1::{run_event, RunState as ProtoRunState};

    #[tokio::test]
    async fn test_submit_pipeline_returns_run_id() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let yaml = b"pipeline: test\nsource:\n  use: src\n  config: {}\ndestination:\n  use: dst\n  config: {}\nstate:\n  backend: postgres\n";
        let resp = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: Some(ExecutionOptions {
                    dry_run: false,
                    limit: None,
                }),
                idempotency_key: String::new(),
            }))
            .await
            .unwrap();

        assert!(!resp.into_inner().run_id.is_empty());
    }

    #[tokio::test]
    async fn test_submit_pipeline_rejects_sqlite_backend() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let yaml = b"pipeline: test\nstate:\n  backend: sqlite\n";
        let result = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await;

        assert!(result.is_err());
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("postgres"));
    }

    #[tokio::test]
    async fn test_submit_pipeline_idempotency_key_dedup() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let resp1 = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "key-1".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp2 = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "key-1".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        assert_eq!(resp1, resp2);
    }

    #[tokio::test]
    async fn test_submit_pipeline_rolls_back_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(1);
        let state = ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store);
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let first = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .expect_err("persistence failure should reject submit");
        assert_eq!(first.code(), tonic::Code::Internal);
        assert!(state.runs.read().await.all_runs().is_empty());
        assert!(state.tasks.read().await.all_tasks().is_empty());
        assert!(state
            .runs
            .read()
            .await
            .find_by_idempotency_key("dedup-key")
            .is_none());

        let second = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .expect("second submit should succeed after rollback")
            .into_inner();

        assert!(!second.run_id.is_empty());
        assert_eq!(state.runs.read().await.all_runs().len(), 1);
        assert_eq!(state.tasks.read().await.all_tasks().len(), 1);
    }

    #[tokio::test]
    async fn test_get_run_returns_status() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let yaml = b"pipeline: mytest\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .get_run(Request::new(GetRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.run_id, run_id);
        assert_eq!(resp.state, ProtoRunState::Pending as i32);
        assert_eq!(resp.pipeline_name, "mytest");
    }

    #[tokio::test]
    async fn get_run_returns_real_metadata() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: mytest\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let pending = svc
            .get_run(Request::new(GetRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(pending.submitted_at.is_some());
        assert!(pending.started_at.is_none());
        assert!(pending.completed_at.is_none());
        assert!(pending.current_task.is_none());

        let agent_svc = crate::services::agent::AgentHandler::new(state.clone());
        let agent_id = agent_svc
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

        let task = agent_svc
            .poll_task(Request::new(PollTaskRequest {
                agent_id: agent_id.clone(),
                wait_seconds: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            task.result,
            Some(crate::proto::rapidbyte::v1::poll_task_response::Result::Task(_))
        ));

        let assigned = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();
        let current_task = assigned.current_task.expect("assigned task metadata");
        assert_eq!(current_task.agent_id, agent_id);
        assert_eq!(current_task.attempt, 1);
        assert!(current_task.assigned_at.is_some());
    }

    #[tokio::test]
    async fn list_runs_returns_most_recent_first() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let first = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: b"pipeline: first\nstate:\n  backend: postgres\n".to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let second = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: b"pipeline: second\nstate:\n  backend: postgres\n".to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 2,
                filter_state: None,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 2);
        assert_eq!(resp.runs[0].run_id, second);
        assert_eq!(resp.runs[1].run_id, first);
        assert!(resp.runs[0].submitted_at.is_some());
        assert!(resp.runs[1].submitted_at.is_some());
    }

    #[tokio::test]
    async fn list_runs_rejects_unknown_filter_state() {
        let state = test_state();
        let svc = PipelineHandler::new(state);

        let err = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(999),
            }))
            .await
            .unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Unknown filter_state"));
    }

    #[tokio::test]
    async fn test_cancel_pending_run_removes_from_queue() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);

        // Verify run is cancelled
        let get_resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(get_resp.state, ProtoRunState::Cancelled as i32);
    }

    #[tokio::test]
    async fn test_cancel_queued_run_rolls_back_when_task_persist_fails() {
        let store = FailingMetadataStore::new().fail_task_upsert_on(2);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store.clone());
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;
        let task_id = state
            .tasks
            .read()
            .await
            .find_by_run_id(&run_id)
            .expect("submitted task should exist")
            .task_id
            .clone();

        let err = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .expect_err("cancel should fail when task persistence fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        let runs = state.runs.read().await;
        assert_eq!(runs.get_run(&run_id).unwrap().state, RunState::Pending);
        drop(runs);

        let tasks = state.tasks.read().await;
        assert_eq!(tasks.get(&task_id).unwrap().state, TaskState::Pending);
        drop(tasks);

        assert_eq!(
            store
                .persisted_run(&run_id)
                .expect("durable run should exist")
                .state,
            RunState::Pending
        );
        assert_eq!(
            store
                .persisted_task(&task_id)
                .expect("durable task should exist")
                .state,
            TaskState::Pending
        );
    }

    #[tokio::test]
    async fn test_cancel_queued_run_rolls_back_when_run_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(2);
        let state =
            ControllerState::with_metadata_store(b"test-key-for-pipeline-service!!", store.clone());
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;
        let task_id = state
            .tasks
            .read()
            .await
            .find_by_run_id(&run_id)
            .expect("submitted task should exist")
            .task_id
            .clone();

        let err = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .expect_err("cancel should fail when run persistence fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        let runs = state.runs.read().await;
        assert_eq!(runs.get_run(&run_id).unwrap().state, RunState::Pending);
        drop(runs);

        let tasks = state.tasks.read().await;
        assert_eq!(tasks.get(&task_id).unwrap().state, TaskState::Pending);
        drop(tasks);

        assert_eq!(
            store
                .persisted_run(&run_id)
                .expect("durable run should exist")
                .state,
            RunState::Pending
        );
        assert_eq!(
            store
                .persisted_task(&task_id)
                .expect("durable task should exist")
                .state,
            TaskState::Pending
        );
    }

    #[tokio::test]
    async fn test_cancel_completed_run_returns_not_accepted() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        // Manually transition to Completed
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Running).unwrap();
            runs.transition(&run_id, RunState::Completed).unwrap();
        }

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.accepted);
    }

    #[tokio::test]
    async fn test_cancel_assigned_run_enters_cancelling_without_clearing_task() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let task_id = {
            let task = state.tasks.write().await.poll(
                "agent-1",
                std::time::Duration::from_secs(60),
                &state.epoch_gen,
            );
            let task = task.expect("expected assigned task");
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.set_current_task(
                &run_id,
                task.task_id.clone(),
                "agent-1".into(),
                task.attempt,
                task.lease_epoch,
            );
            task.task_id
        };

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);
        assert!(resp.message.contains("heartbeat"));

        let runs = state.runs.read().await;
        assert_eq!(runs.get_run(&run_id).unwrap().state, RunState::Cancelling);
        drop(runs);

        let tasks = state.tasks.read().await;
        let task = tasks.get(&task_id).unwrap();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
    }

    #[tokio::test]
    async fn test_cancel_reconciling_run_enters_cancelling() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Reconciling).unwrap();
        }

        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(resp.accepted);
        assert!(resp.message.contains("heartbeat"));
        assert_eq!(
            state.runs.read().await.get_run(&run_id).unwrap().state,
            RunState::Cancelling
        );
    }

    #[tokio::test]
    async fn test_cancel_run_with_stale_running_snapshot_returns_not_accepted() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Running).unwrap();
            runs.transition(&run_id, RunState::Completed).unwrap();
        }

        // Test the cancel handler through the public cancel_run RPC.
        // The handler reads the current state (Completed) and returns not_accepted.
        let resp = svc
            .cancel_run(Request::new(CancelRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(!resp.accepted);
        assert!(resp.message.contains("terminal state"));
    }

    #[tokio::test]
    async fn test_watch_run_rechecks_terminal_state_after_subscribe() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Running).unwrap();
            runs.transition(&run_id, RunState::Completed).unwrap();
            if let Some(record) = runs.get_run_mut(&run_id) {
                record.metrics.total_records = 11;
            }
        }

        let response = svc
            .watch_run(Request::new(WatchRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let event = stream.next().await.unwrap().unwrap();

        match event.event {
            Some(run_event::Event::Completed(completed)) => {
                assert_eq!(completed.total_records, 11);
            }
            other => panic!("Expected completed event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_get_run_returns_reconciling_state() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Reconciling).unwrap();
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, ProtoRunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_list_runs_returns_reconciling_state() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Reconciling).unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let run = resp
            .runs
            .into_iter()
            .find(|run| run.run_id == run_id)
            .unwrap();
        assert_eq!(run.state, ProtoRunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_list_runs_filters_reconciling_state() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let reconciling_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let running_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&reconciling_run_id, RunState::Assigned)
                .unwrap();
            runs.transition(&reconciling_run_id, RunState::Reconciling)
                .unwrap();
            runs.transition(&running_run_id, RunState::Assigned)
                .unwrap();
            runs.transition(&running_run_id, RunState::Running).unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(ProtoRunState::Reconciling as i32),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 1);
        assert_eq!(resp.runs[0].run_id, reconciling_run_id);
        assert_eq!(resp.runs[0].state, ProtoRunState::Reconciling as i32);
    }

    #[tokio::test]
    async fn test_get_run_returns_failed_state_for_timed_out_run() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::TimedOut).unwrap();
            runs.get_run_mut(&run_id).unwrap().error = Some(crate::run_state::RunError {
                code: String::new(),
                message: "Task task-1 lease expired (agent unresponsive)".into(),
                retryable: true,
                safe_to_retry: true,
                commit_state: String::new(),
            });
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, ProtoRunState::Failed as i32);
        let error = resp
            .last_error
            .expect("timed out runs expose recovery error");
        assert_eq!(error.code, "LEASE_EXPIRED");
    }

    #[tokio::test]
    async fn test_get_run_returns_execution_error_code_for_failed_run() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Running).unwrap();
            runs.transition(&run_id, RunState::Failed).unwrap();
            let run = runs.get_run_mut(&run_id).unwrap();
            run.error = Some(crate::run_state::RunError {
                code: "TEST_EXECUTION_FAILED".into(),
                message: "TEST_EXECUTION_FAILED: injected failure".into(),
                retryable: false,
                safe_to_retry: false,
                commit_state: "before_commit".into(),
            });
        }

        let resp = svc
            .get_run(Request::new(GetRunRequest { run_id }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.state, ProtoRunState::Failed as i32);
        let error = resp
            .last_error
            .expect("failed runs expose execution error metadata");
        assert_eq!(error.code, "TEST_EXECUTION_FAILED");
        assert_eq!(error.message, "TEST_EXECUTION_FAILED: injected failure");
        assert_eq!(error.commit_state, "before_commit");
    }

    #[tokio::test]
    async fn test_list_runs_filters_recovery_failed_state() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let recovery_failed_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        let failed_run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&recovery_failed_run_id, RunState::Assigned)
                .unwrap();
            runs.transition(&recovery_failed_run_id, RunState::RecoveryFailed)
                .unwrap();
            runs.transition(&failed_run_id, RunState::Assigned).unwrap();
            runs.transition(&failed_run_id, RunState::Running).unwrap();
            runs.transition(&failed_run_id, RunState::Failed).unwrap();
        }

        let resp = svc
            .list_runs(Request::new(ListRunsRequest {
                limit: 20,
                filter_state: Some(ProtoRunState::RecoveryFailed as i32),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 1);
        assert_eq!(resp.runs[0].run_id, recovery_failed_run_id);
        assert_eq!(resp.runs[0].state, ProtoRunState::RecoveryFailed as i32);
    }

    #[tokio::test]
    async fn test_watch_run_surfaces_reconciling_status() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::Reconciling).unwrap();
        }

        let response = svc
            .watch_run(Request::new(WatchRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let event = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
            .await
            .expect("expected a reconciling event")
            .expect("stream should yield an event")
            .unwrap();

        match event.event {
            Some(run_event::Event::Status(status)) => {
                assert_eq!(status.state, ProtoRunState::Reconciling as i32);
                assert!(status.message.contains("waiting to reconcile"));
            }
            other => panic!("expected reconciling status event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_watch_run_returns_recovery_failed_terminal_event_for_timed_out_run() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";
        let run_id = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .run_id;

        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, RunState::Assigned).unwrap();
            runs.transition(&run_id, RunState::RecoveryFailed).unwrap();
            runs.get_run_mut(&run_id).unwrap().error = Some(crate::run_state::RunError {
                code: String::new(),
                message: "Run recovery reconciliation timed out after controller restart".into(),
                retryable: false,
                safe_to_retry: false,
                commit_state: String::new(),
            });
        }

        let response = svc
            .watch_run(Request::new(WatchRunRequest {
                run_id: run_id.clone(),
            }))
            .await
            .unwrap();
        let mut stream = response.into_inner();
        let event = stream.next().await.unwrap().unwrap();

        match event.event {
            Some(run_event::Event::Failed(failed)) => {
                let error = failed
                    .error
                    .expect("timed out watch exposes recovery error");
                assert_eq!(error.code, "RECOVERY_TIMEOUT");
                assert!(error.message.contains("reconciliation timed out"));
            }
            other => panic!("expected failed terminal event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_watch_run_missing_run_cleans_up_subscription() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());

        let Err(err) = svc
            .watch_run(Request::new(WatchRunRequest {
                run_id: "missing-run".into(),
            }))
            .await
        else {
            panic!("expected missing run watch to fail");
        };

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(state.watchers.read().await.channel_count(), 0);
    }

    // Note: controller metric accounting for cancel_queued_run (runs_completed
    // and active_runs) is verified by code path coverage — the state transition
    // to Cancelled is confirmed by test_cancel_pending_run_removes_from_queue,
    // and the metric calls at the cancel path are on that same path.
    // A dedicated metric assertion test is not feasible because OnceLock-cached
    // instruments bind to the first provider and cannot be redirected to a
    // per-test provider.

    #[tokio::test]
    async fn concurrent_idempotent_submit_second_gets_aborted() {
        use crate::store::test_support::FailingMetadataStore;
        use std::sync::Arc;
        use tokio::sync::Notify;

        // Wire a store that blocks inside create_run_with_task until we say go.
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let store =
            FailingMetadataStore::new().pause_run_create(Arc::clone(&entered), Arc::clone(&resume));
        let state = ControllerState::with_metadata_store(b"test-signing-key", store);
        let svc = Arc::new(PipelineHandler::new(state.clone()));
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";

        // Task A: starts submit, blocks inside the durable persist.
        let svc_a = Arc::clone(&svc);
        let yaml_a = yaml.to_vec();
        let handle_a = tokio::spawn(async move {
            svc_a
                .submit_pipeline(Request::new(SubmitPipelineRequest {
                    pipeline_yaml_utf8: yaml_a,
                    execution: None,
                    idempotency_key: "race-key".into(),
                }))
                .await
        });

        // Wait until task A is inside create_run_with_task.
        entered.notified().await;

        // Task B: submits with same key while A is blocked — must get ABORTED.
        let err = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "race-key".into(),
            }))
            .await
            .expect_err("second submit should be aborted while first is in-flight");
        assert_eq!(err.code(), tonic::Code::Aborted);

        // Resume task A so it completes.
        resume.notify_one();
        let result_a = handle_a.await.unwrap().unwrap().into_inner();
        assert!(!result_a.run_id.is_empty());

        // Pending key must be cleared.
        assert!(
            state.pending_idempotency_keys.read().await.is_empty(),
            "pending key should be cleared after completion"
        );
    }

    #[tokio::test]
    async fn dedup_does_not_clear_inflight_pending_key() {
        let state = test_state();
        let svc = PipelineHandler::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";

        // First submission succeeds, creating the run and clearing pending.
        let first = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        // Simulate another request in-flight with a different key.
        state
            .pending_idempotency_keys
            .write()
            .await
            .insert("other-key".into());

        // Second submission with the same key deduplicates (is_new=false).
        let second = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "dedup-key".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(first.run_id, second.run_id);

        // The OTHER pending key must still be there — dedup must not clear it.
        assert!(
            state
                .pending_idempotency_keys
                .read()
                .await
                .contains("other-key"),
            "dedup path must not clear other pending keys"
        );
    }

    #[tokio::test]
    async fn pending_key_cleared_on_handler_cancellation() {
        use crate::store::test_support::FailingMetadataStore;
        use std::sync::Arc;
        use tokio::sync::Notify;

        // Wire a store that blocks inside create_run_with_task.
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let store =
            FailingMetadataStore::new().pause_run_create(Arc::clone(&entered), Arc::clone(&resume));
        let state = ControllerState::with_metadata_store(b"test-signing-key", store);
        let svc = Arc::new(PipelineHandler::new(state.clone()));
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";

        // Spawn a submit that will block inside persist.
        let svc_clone = Arc::clone(&svc);
        let yaml_clone = yaml.to_vec();
        let handle = tokio::spawn(async move {
            svc_clone
                .submit_pipeline(Request::new(SubmitPipelineRequest {
                    pipeline_yaml_utf8: yaml_clone,
                    execution: None,
                    idempotency_key: "cancel-key".into(),
                }))
                .await
        });

        // Wait until the handler is inside the persist call.
        entered.notified().await;

        // Key should be in pending now.
        assert!(
            state
                .pending_idempotency_keys
                .read()
                .await
                .contains("cancel-key"),
            "key should be pending while persist is in flight"
        );

        // Abort the handler (simulates client disconnect / cancellation).
        handle.abort();
        let _ = handle.await;

        // The drop guard should clean up the key via spawned task.
        // Give the spawned cleanup task a moment to run.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            !state
                .pending_idempotency_keys
                .read()
                .await
                .contains("cancel-key"),
            "pending key should be cleaned up by drop guard after cancellation"
        );
    }

    #[tokio::test]
    async fn pending_key_cleared_on_persist_failure() {
        use crate::store::test_support::FailingMetadataStore;

        let store = FailingMetadataStore::new().fail_run_upsert_on(1);
        let state = ControllerState::with_metadata_store(b"test-signing-key", store);
        let svc = PipelineHandler::new(state.clone());
        let yaml = b"pipeline: test\nstate:\n  backend: postgres\n";

        let err = svc
            .submit_pipeline(Request::new(SubmitPipelineRequest {
                pipeline_yaml_utf8: yaml.to_vec(),
                execution: None,
                idempotency_key: "fail-key".into(),
            }))
            .await
            .expect_err("persist failure should reject submit");

        assert_eq!(err.code(), tonic::Code::Internal);

        // The pending key must be cleared after failure so retries aren't blocked.
        assert!(
            !state
                .pending_idempotency_keys
                .read()
                .await
                .contains("fail-key"),
            "pending key should be cleared after persist failure"
        );
    }
}
