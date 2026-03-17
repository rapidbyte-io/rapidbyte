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
                let lease_expires_at = Some(prost_types::Timestamp {
                    seconds: a.lease_expires_at.timestamp(),
                    nanos: a.lease_expires_at.timestamp_subsec_nanos().cast_signed(),
                });
                pb::poll_task_response::Result::Assignment(pb::TaskAssignment {
                    task_id: a.task_id,
                    run_id: a.run_id,
                    pipeline_yaml: a.pipeline_yaml,
                    options,
                    lease_epoch: a.lease_epoch,
                    lease_expires_at,
                    attempt: a.attempt,
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
                lease_expires_at: Some(prost_types::Timestamp {
                    seconds: d.lease_expires_at.timestamp(),
                    nanos: d.lease_expires_at.timestamp_subsec_nanos().cast_signed(),
                }),
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
