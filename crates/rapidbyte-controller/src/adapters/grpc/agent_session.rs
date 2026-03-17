//! v2 `AgentSession` gRPC handler.

use std::pin::Pin;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Request;
use tracing::warn;

use crate::adapters::grpc::agent_bridge::AgentHandler;
use crate::proto::rapidbyte::v2::agent_service_server::AgentService as _;
use crate::proto::rapidbyte::v2::agent_session_server::AgentSession;
use crate::proto::rapidbyte::v2::{
    agent_directive, poll_task_response, ActiveLease, CompleteTaskRequest, HeartbeatRequest,
    PollTaskRequest, ProgressUpdate, RegisterAgentRequest, ReportProgressRequest, TaskError,
    TaskMetrics, TaskOutcome,
};
use crate::proto::rapidbyte::v2::{
    agent_message, controller_message, AgentMessage, ControllerMessage, SessionAccepted,
    TaskAssignment, TaskDirective,
};

type ControllerMessageStream =
    Pin<Box<dyn Stream<Item = Result<ControllerMessage, tonic::Status>> + Send>>;

#[derive(Clone, Default)]
pub struct AgentSessionHandler {
    bridge: Option<AgentHandler>,
}

impl AgentSessionHandler {
    #[must_use]
    pub fn new(bridge: AgentHandler) -> Self {
        Self {
            bridge: Some(bridge),
        }
    }

    /// Build outbound session messages from an inbound agent stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream is malformed or when bridged
    /// agent operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn open_session_from_stream<S>(
        &self,
        mut inbound: S,
    ) -> Result<ControllerMessageStream, tonic::Status>
    where
        S: Stream<Item = Result<AgentMessage, tonic::Status>> + Unpin + Send + 'static,
    {
        let first = inbound
            .next()
            .await
            .transpose()?
            .ok_or_else(|| tonic::Status::invalid_argument("session stream is empty"))?;

        let Some(agent_message::Payload::Hello(hello)) = first.payload else {
            return Err(tonic::Status::invalid_argument(
                "expected hello payload first",
            ));
        };

        let Some(handler) = self.bridge.clone() else {
            return Err(tonic::Status::failed_precondition(
                "agent session bridge is not configured",
            ));
        };

        let registration = handler
            .register_agent(Request::new(RegisterAgentRequest {
                max_tasks: hello.max_tasks.max(1),
                flight_advertise_endpoint: hello.flight_advertise_endpoint,
                plugin_bundle_hash: hello.plugin_bundle_hash,
                available_plugins: hello.available_plugins,
                memory_bytes: hello.memory_bytes,
            }))
            .await?
            .into_inner();
        let registered_agent_id = registration.agent_id.clone();

        let (out_tx, out_rx) = mpsc::channel(8);
        out_tx
            .send(Ok(ControllerMessage {
                payload: Some(controller_message::Payload::Accepted(SessionAccepted {
                    session_id: format!("session-{registered_agent_id}"),
                    agent_id: registration.agent_id,
                    registry_url: registration.registry_url,
                    registry_insecure: registration.registry_insecure,
                    trust_policy: registration.trust_policy,
                    trusted_key_pems: registration.trusted_key_pems,
                })),
            }))
            .await
            .map_err(|_| tonic::Status::internal("failed to publish session accepted"))?;

        let assignment_handler = handler.clone();
        let assignment_agent_id = registered_agent_id.clone();
        let assignment_tx = out_tx.clone();
        tokio::spawn(async move {
            loop {
                let response = assignment_handler
                    .poll_task(Request::new(PollTaskRequest {
                        agent_id: assignment_agent_id.clone(),
                        wait_seconds: 5,
                    }))
                    .await;

                let assignment = match response {
                    Ok(response) => match response.into_inner().result {
                        Some(poll_task_response::Result::Task(task)) => task,
                        _ => continue,
                    },
                    Err(error) => {
                        warn!(error = %error, "failed to poll assignment for v2 session");
                        break;
                    }
                };

                let execution = assignment.execution;
                let outbound = ControllerMessage {
                    payload: Some(controller_message::Payload::Assignment(TaskAssignment {
                        run_id: assignment.run_id,
                        task_id: assignment.task_id,
                        lease_epoch: assignment.lease_epoch,
                        attempt: assignment.attempt,
                        pipeline_yaml_utf8: assignment.pipeline_yaml_utf8,
                        dry_run: execution.as_ref().is_some_and(|item| item.dry_run),
                        limit: execution.and_then(|item| item.limit),
                        lease_expires_at: assignment.lease_expires_at,
                        execution,
                    })),
                };

                if assignment_tx.send(Ok(outbound)).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            while let Some(incoming) = inbound.next().await {
                let Ok(message) = incoming else {
                    break;
                };

                if !message.agent_id.is_empty() && message.agent_id != registered_agent_id {
                    warn!(
                        registered_agent_id = %registered_agent_id,
                        reported_agent_id = %message.agent_id,
                        "ignoring mismatched agent_id from session message"
                    );
                }
                let agent_id = registered_agent_id.clone();

                match message.payload {
                    Some(agent_message::Payload::Heartbeat(heartbeat)) => {
                        let request = HeartbeatRequest {
                            agent_id,
                            active_leases: heartbeat
                                .active_leases
                                .into_iter()
                                .map(|lease| ActiveLease {
                                    task_id: lease.task_id,
                                    lease_epoch: lease.lease_epoch,
                                })
                                .collect(),
                            active_tasks: heartbeat.active_tasks,
                            cpu_usage: heartbeat.cpu_usage,
                            memory_used_bytes: heartbeat.memory_used_bytes,
                        };
                        match handler.heartbeat(Request::new(request)).await {
                            Ok(response) => {
                                for directive in response.into_inner().directives {
                                    if let Some(agent_directive::Directive::CancelTask(cancel)) =
                                        directive.directive
                                    {
                                        let _ = out_tx
                                            .send(Ok(ControllerMessage {
                                                payload: Some(
                                                    controller_message::Payload::Directive(
                                                        TaskDirective {
                                                            task_id: cancel.task_id,
                                                            action: "cancel".to_owned(),
                                                            lease_epoch: cancel.lease_epoch,
                                                        },
                                                    ),
                                                ),
                                            }))
                                            .await;
                                    }
                                }
                            }
                            Err(error) => {
                                warn!(error = %error, "failed to bridge v2 heartbeat message");
                            }
                        }
                    }
                    Some(agent_message::Payload::Progress(progress)) => {
                        let request = ReportProgressRequest {
                            agent_id,
                            task_id: progress.task_id,
                            lease_epoch: progress.lease_epoch,
                            progress: Some(ProgressUpdate {
                                stream: String::new(),
                                phase: "running".to_owned(),
                                records: progress.records,
                                bytes: progress.bytes,
                            }),
                        };
                        if let Err(error) = handler.report_progress(Request::new(request)).await {
                            warn!(error = %error, "failed to bridge v2 progress message");
                        }
                    }
                    Some(agent_message::Payload::Completion(completion)) => {
                        let failed_error = (!completion.success).then(|| TaskError {
                            code: "AGENT_SESSION_FAILURE".to_owned(),
                            message: "task failed via v2 agent session".to_owned(),
                            retryable: false,
                            safe_to_retry: false,
                            commit_state: "before_commit".to_owned(),
                        });
                        let request = CompleteTaskRequest {
                            agent_id,
                            task_id: completion.task_id,
                            lease_epoch: completion.lease_epoch,
                            outcome: if completion.success {
                                TaskOutcome::Completed as i32
                            } else {
                                TaskOutcome::Failed as i32
                            },
                            error: failed_error,
                            metrics: Some(TaskMetrics {
                                records_processed: completion.records_processed,
                                bytes_processed: completion.bytes_processed,
                                elapsed_seconds: completion.elapsed_seconds,
                                cursors_advanced: completion.cursors_advanced,
                            }),
                            preview: None,
                            backend_run_id: 0,
                        };
                        if let Err(error) = complete_task_with_retry(&handler, request).await {
                            warn!(error = %error, "failed to bridge v2 completion message");
                        }
                    }
                    _ => {}
                }
            }

            drop(out_tx);
        });

        Ok(Box::pin(ReceiverStream::new(out_rx)))
    }
}

async fn complete_task_with_retry(
    handler: &AgentHandler,
    request: CompleteTaskRequest,
) -> Result<(), tonic::Status> {
    const MAX_ATTEMPTS: usize = 3;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_error: Option<tonic::Status> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match handler.complete_task(Request::new(request.clone())).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| tonic::Status::internal("complete_task failed")))
}

#[tonic::async_trait]
impl AgentSession for AgentSessionHandler {
    type OpenSessionStream = ControllerMessageStream;

    async fn open_session(
        &self,
        request: tonic::Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<tonic::Response<Self::OpenSessionStream>, tonic::Status> {
        let stream = self.open_session_from_stream(request.into_inner()).await?;
        Ok(tonic::Response::new(stream))
    }
}
