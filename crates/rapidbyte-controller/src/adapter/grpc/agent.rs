//! gRPC adapter for `AgentService`.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::adapter::grpc::convert;
use crate::application::context::AppContext;
use crate::application::heartbeat::TaskHeartbeatInput;
use crate::domain::run::CommitState;
use crate::proto::rapidbyte::v1 as pb;
use crate::proto::rapidbyte::v1::agent_service_server::AgentService;

pub struct AgentGrpcService {
    ctx: Arc<AppContext>,
}

impl AgentGrpcService {
    #[must_use]
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }
}

#[tonic::async_trait]
impl AgentService for AgentGrpcService {
    async fn register(
        &self,
        req: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        let req = req.into_inner();
        let caps = req.capabilities.unwrap_or_default();
        // Normalize: 0 means "use default" (1), prevents starvation
        let max_tasks = if caps.max_concurrent_tasks == 0 {
            1
        } else {
            caps.max_concurrent_tasks
        };

        crate::application::register::register(
            &self.ctx,
            &req.agent_id,
            crate::domain::agent::AgentCapabilities {
                plugins: caps.plugins,
                max_concurrent_tasks: max_tasks,
            },
        )
        .await
        .map_err(convert::app_error_to_status)?;

        let registry = self.ctx.config.registry.as_ref().map(|r| pb::RegistryInfo {
            url: r.url.clone(),
            insecure: r.insecure,
        });

        Ok(Response::new(pb::RegisterResponse { registry }))
    }

    async fn deregister(
        &self,
        req: Request<pb::DeregisterRequest>,
    ) -> Result<Response<pb::DeregisterResponse>, Status> {
        let req = req.into_inner();
        crate::application::register::deregister(&self.ctx, &req.agent_id)
            .await
            .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::DeregisterResponse {}))
    }

    async fn poll_task(
        &self,
        req: Request<pb::PollTaskRequest>,
    ) -> Result<Response<pb::PollTaskResponse>, Status> {
        let req = req.into_inner();
        let assignment = crate::application::poll::poll_task(&self.ctx, &req.agent_id)
            .await
            .map_err(convert::app_error_to_status)?;

        let result = match assignment {
            Some(a) => {
                let options = Some(pb::ExecutionOptions {
                    max_retries: a.max_retries,
                    timeout_seconds: a.timeout_seconds.unwrap_or(0),
                });
                let lease_expires_at = Some(convert::to_proto_timestamp(a.lease_expires_at));
                pb::poll_task_response::Result::Assignment(pb::TaskAssignment {
                    task_id: a.task_id,
                    run_id: a.run_id,
                    pipeline_yaml: a.pipeline_yaml,
                    options,
                    lease_epoch: a.lease_epoch,
                    lease_expires_at,
                    attempt: a.attempt,
                    operation: a.operation.as_str().to_string(),
                })
            }
            None => pb::poll_task_response::Result::NoTask(pb::NoTask {}),
        };

        Ok(Response::new(pb::PollTaskResponse {
            result: Some(result),
        }))
    }

    async fn heartbeat(
        &self,
        req: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        let req = req.into_inner();
        let inputs: Vec<TaskHeartbeatInput> = req
            .tasks
            .into_iter()
            .map(|t| TaskHeartbeatInput {
                task_id: t.task_id,
                lease_epoch: t.lease_epoch,
                progress_message: t.progress_message,
                progress_pct: t.progress_pct,
            })
            .collect();

        let directives = crate::application::heartbeat::heartbeat(&self.ctx, &req.agent_id, inputs)
            .await
            .map_err(convert::app_error_to_status)?;

        let proto_directives = directives
            .into_iter()
            .map(|d| pb::TaskDirective {
                task_id: d.task_id,
                acknowledged: d.acknowledged,
                cancel_requested: d.cancel_requested,
                lease_expires_at: Some(convert::to_proto_timestamp(d.lease_expires_at)),
            })
            .collect();

        Ok(Response::new(pb::HeartbeatResponse {
            directives: proto_directives,
        }))
    }

    async fn complete_task(
        &self,
        req: Request<pb::CompleteTaskRequest>,
    ) -> Result<Response<pb::CompleteTaskResponse>, Status> {
        let req = req.into_inner();
        let outcome = match req.outcome {
            Some(pb::complete_task_request::Outcome::Completed(c)) => {
                let metrics = c.metrics.unwrap_or_default();
                crate::application::complete::TaskOutcome::Completed {
                    metrics: crate::domain::run::RunMetrics {
                        rows_read: metrics.rows_read,
                        rows_written: metrics.rows_written,
                        bytes_read: metrics.bytes_read,
                        bytes_written: metrics.bytes_written,
                        duration_ms: metrics.duration_ms,
                    },
                }
            }
            Some(pb::complete_task_request::Outcome::Failed(f)) => {
                let commit_state = match pb::CommitState::try_from(f.commit_state) {
                    Ok(pb::CommitState::AfterCommitConfirmed) => CommitState::AfterCommitConfirmed,
                    Ok(pb::CommitState::AfterCommitUnknown) => CommitState::AfterCommitUnknown,
                    Ok(pb::CommitState::BeforeCommit) => CommitState::BeforeCommit,
                    Ok(pb::CommitState::Unspecified) | Err(_) => {
                        return Err(Status::invalid_argument(format!(
                            "invalid or missing commit_state: {}",
                            f.commit_state
                        )));
                    }
                };
                crate::application::complete::TaskOutcome::Failed {
                    error: crate::application::complete::TaskError {
                        code: f.error_code,
                        message: f.error_message,
                        retryable: f.retryable,
                        commit_state,
                    },
                }
            }
            Some(pb::complete_task_request::Outcome::Cancelled(_)) => {
                crate::application::complete::TaskOutcome::Cancelled
            }
            None => {
                return Err(Status::invalid_argument("outcome is required"));
            }
        };

        crate::application::complete::complete_task(
            &self.ctx,
            &req.agent_id,
            &req.task_id,
            req.lease_epoch,
            outcome,
        )
        .await
        .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::CompleteTaskResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::Request;

    use crate::application::testing::fake_context;
    use crate::proto::rapidbyte::v1 as pb;
    use crate::proto::rapidbyte::v1::agent_service_server::AgentService;

    use super::AgentGrpcService;

    /// Helper: register agent + submit pipeline + poll via the application layer,
    /// returning (`task_id`, `run_id`, `lease_epoch`).
    async fn setup_running_task(
        ctx: &crate::application::context::AppContext,
        agent_id: &str,
        max_concurrent_tasks: u32,
    ) -> (String, String, u64) {
        let now = ctx.clock.now();
        let agent = crate::domain::agent::Agent::new(
            agent_id.to_string(),
            crate::domain::agent::AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks,
            },
            now,
        );
        ctx.agents.save(&agent).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let _submit = crate::application::submit::submit_pipeline(
            ctx,
            None,
            yaml.to_string(),
            2,
            Some(60),
            crate::domain::task::TaskOperation::Sync,
        )
        .await
        .unwrap();

        let assignment = crate::application::poll::poll_task(ctx, agent_id)
            .await
            .unwrap()
            .unwrap();
        (
            assignment.task_id,
            assignment.run_id,
            assignment.lease_epoch,
        )
    }

    #[tokio::test]
    async fn register_missing_capabilities_defaults_to_one() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        // Register with capabilities = None
        svc.register(Request::new(pb::RegisterRequest {
            agent_id: "agent-1".to_string(),
            capabilities: None,
        }))
        .await
        .unwrap();

        // Submit a pipeline and poll — max_concurrent_tasks=1 should allow one task
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        crate::application::submit::submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            crate::domain::task::TaskOperation::Sync,
        )
        .await
        .unwrap();

        let resp = svc
            .poll_task(Request::new(pb::PollTaskRequest {
                agent_id: "agent-1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            matches!(
                resp.result,
                Some(pb::poll_task_response::Result::Assignment(_))
            ),
            "expected assignment, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn register_zero_capacity_normalized_to_one() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        // Register with max_concurrent_tasks = 0
        svc.register(Request::new(pb::RegisterRequest {
            agent_id: "agent-1".to_string(),
            capabilities: Some(pb::AgentCapabilities {
                max_concurrent_tasks: 0,
                plugins: vec![],
            }),
        }))
        .await
        .unwrap();

        // Submit and poll
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        crate::application::submit::submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            crate::domain::task::TaskOperation::Sync,
        )
        .await
        .unwrap();

        let resp = svc
            .poll_task(Request::new(pb::PollTaskRequest {
                agent_id: "agent-1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            matches!(
                resp.result,
                Some(pb::poll_task_response::Result::Assignment(_))
            ),
            "expected assignment, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn complete_task_missing_outcome_returns_invalid_argument() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let (task_id, _run_id, lease_epoch) = setup_running_task(&ctx, "agent-1", 4).await;
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        let status = svc
            .complete_task(Request::new(pb::CompleteTaskRequest {
                agent_id: "agent-1".to_string(),
                task_id,
                lease_epoch,
                outcome: None,
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn complete_task_unknown_commit_state_returns_invalid_argument() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let (task_id, _run_id, lease_epoch) = setup_running_task(&ctx, "agent-1", 4).await;
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        let status = svc
            .complete_task(Request::new(pb::CompleteTaskRequest {
                agent_id: "agent-1".to_string(),
                task_id,
                lease_epoch,
                outcome: Some(pb::complete_task_request::Outcome::Failed(pb::TaskFailed {
                    error_code: "E1".to_string(),
                    error_message: "msg".to_string(),
                    retryable: false,
                    commit_state: 99,
                })),
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn complete_task_completed_maps_metrics() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let (task_id, run_id, lease_epoch) = setup_running_task(&ctx, "agent-1", 4).await;
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        svc.complete_task(Request::new(pb::CompleteTaskRequest {
            agent_id: "agent-1".to_string(),
            task_id,
            lease_epoch,
            outcome: Some(pb::complete_task_request::Outcome::Completed(
                pb::TaskCompleted {
                    metrics: Some(pb::RunMetrics {
                        rows_read: 10,
                        rows_written: 5,
                        bytes_read: 100,
                        bytes_written: 50,
                        duration_ms: 1000,
                    }),
                },
            )),
        }))
        .await
        .unwrap();

        // Verify via app query
        let run = crate::application::query::get_run(&ctx, &run_id)
            .await
            .unwrap();
        assert_eq!(run.state(), crate::domain::run::RunState::Completed);
        let m = run.metrics().unwrap();
        assert_eq!(m.rows_read, 10);
        assert_eq!(m.rows_written, 5);
    }

    #[tokio::test]
    async fn complete_task_failed_maps_error_fields() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let (task_id, run_id, lease_epoch) = setup_running_task(&ctx, "agent-1", 4).await;
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        svc.complete_task(Request::new(pb::CompleteTaskRequest {
            agent_id: "agent-1".to_string(),
            task_id,
            lease_epoch,
            outcome: Some(pb::complete_task_request::Outcome::Failed(pb::TaskFailed {
                error_code: "E1".to_string(),
                error_message: "msg".to_string(),
                retryable: false,
                commit_state: pb::CommitState::BeforeCommit.into(),
            })),
        }))
        .await
        .unwrap();

        // Verify run is Failed
        let run = crate::application::query::get_run(&ctx, &run_id)
            .await
            .unwrap();
        assert_eq!(run.state(), crate::domain::run::RunState::Failed);
        let e = run.error().unwrap();
        assert_eq!(e.code, "E1");
        assert_eq!(e.message, "msg");
    }

    #[tokio::test]
    async fn heartbeat_maps_multiple_tasks_to_directives() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);

        // Register agent with max=2 via app layer
        let now = ctx.clock.now();
        let agent = crate::domain::agent::Agent::new(
            "agent-1".to_string(),
            crate::domain::agent::AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: 2,
            },
            now,
        );
        ctx.agents.save(&agent).await.unwrap();

        // Submit and poll 2 tasks
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        crate::application::submit::submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            crate::domain::task::TaskOperation::Sync,
        )
        .await
        .unwrap();
        crate::application::submit::submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            crate::domain::task::TaskOperation::Sync,
        )
        .await
        .unwrap();

        let a1 = crate::application::poll::poll_task(&ctx, "agent-1")
            .await
            .unwrap()
            .unwrap();
        let a2 = crate::application::poll::poll_task(&ctx, "agent-1")
            .await
            .unwrap()
            .unwrap();

        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        let resp = svc
            .heartbeat(Request::new(pb::HeartbeatRequest {
                agent_id: "agent-1".to_string(),
                tasks: vec![
                    pb::TaskHeartbeat {
                        task_id: a1.task_id.clone(),
                        lease_epoch: a1.lease_epoch,
                        progress_message: None,
                        progress_pct: None,
                    },
                    pb::TaskHeartbeat {
                        task_id: a2.task_id.clone(),
                        lease_epoch: a2.lease_epoch,
                        progress_message: None,
                        progress_pct: None,
                    },
                ],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.directives.len(), 2);
        assert!(resp.directives[0].acknowledged);
        assert!(resp.directives[1].acknowledged);
    }

    #[tokio::test]
    async fn poll_task_no_task_returns_no_task_variant() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let svc = AgentGrpcService::new(Arc::clone(&ctx));

        // Register agent but don't submit any pipeline
        svc.register(Request::new(pb::RegisterRequest {
            agent_id: "agent-1".to_string(),
            capabilities: Some(pb::AgentCapabilities {
                max_concurrent_tasks: 4,
                plugins: vec![],
            }),
        }))
        .await
        .unwrap();

        let resp = svc
            .poll_task(Request::new(pb::PollTaskRequest {
                agent_id: "agent-1".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(
            matches!(resp.result, Some(pb::poll_task_response::Result::NoTask(_))),
            "expected NoTask, got {resp:?}"
        );
    }
}
