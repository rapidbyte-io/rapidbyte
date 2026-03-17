//! v2 `AgentSession` gRPC handler.

use std::pin::Pin;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request};
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
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

        let close_watch_tx = out_tx.clone();
        let close_watch_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            close_watch_tx.closed().await;
            let _ = close_watch_shutdown_tx.send(true);
        });

        let assignment_handler = handler.clone();
        let assignment_agent_id = registered_agent_id.clone();
        let assignment_tx = out_tx.clone();
        let mut assignment_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                if *assignment_shutdown_rx.borrow() {
                    break;
                }

                let response = tokio::select! {
                    changed = assignment_shutdown_rx.changed() => {
                        if changed.is_err() || *assignment_shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    response = assignment_handler.poll_task(Request::new(PollTaskRequest {
                        agent_id: assignment_agent_id.clone(),
                        wait_seconds: 5,
                    })) => response,
                };

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

        let shutdown_on_inbound_end = shutdown_tx;
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
                        let request = completion_to_complete_task_request(agent_id, completion);
                        if let Err(error) = complete_task_with_retry(&handler, request).await {
                            warn!(error = %error, "failed to bridge v2 completion message");
                        }
                    }
                    _ => {}
                }
            }

            let _ = shutdown_on_inbound_end.send(true);
            drop(out_tx);
        });

        Ok(Box::pin(ReceiverStream::new(out_rx)))
    }
}

fn completion_to_complete_task_request(
    agent_id: String,
    completion: crate::proto::rapidbyte::v2::TaskCompletion,
) -> CompleteTaskRequest {
    let outcome = TaskOutcome::try_from(completion.outcome)
        .ok()
        .filter(|outcome| *outcome != TaskOutcome::Unspecified)
        .unwrap_or({
            if completion.success {
                TaskOutcome::Completed
            } else {
                TaskOutcome::Failed
            }
        });

    let error = match outcome {
        TaskOutcome::Completed | TaskOutcome::Unspecified => None,
        TaskOutcome::Cancelled => completion.error,
        TaskOutcome::Failed => completion.error.or_else(|| {
            Some(TaskError {
                code: "AGENT_SESSION_FAILURE".to_owned(),
                message: "task failed via v2 agent session".to_owned(),
                retryable: false,
                safe_to_retry: false,
                commit_state: "before_commit".to_owned(),
            })
        }),
    };

    CompleteTaskRequest {
        agent_id,
        task_id: completion.task_id,
        lease_epoch: completion.lease_epoch,
        outcome: outcome as i32,
        error,
        metrics: Some(TaskMetrics {
            records_processed: completion.records_processed,
            bytes_processed: completion.bytes_processed,
            elapsed_seconds: completion.elapsed_seconds,
            cursors_advanced: completion.cursors_advanced,
        }),
        preview: None,
        backend_run_id: 0,
    }
}

fn is_retryable_complete_task_status(error: &tonic::Status) -> bool {
    match error.code() {
        Code::Unavailable | Code::ResourceExhausted | Code::DeadlineExceeded => true,
        Code::Unknown => {
            let message = error.message().to_ascii_lowercase();
            message.contains("transport")
                || message.contains("connection")
                || message.contains("timed out")
                || message.contains("broken pipe")
                || message.contains("reset by peer")
        }
        _ => false,
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
                if is_retryable_complete_task_status(&error) {
                    last_error = Some(error);
                    if attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                    break;
                }
                // Non-transient errors should fail fast instead of retrying.
                return Err(error);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_mapping_preserves_failed_error_details() {
        let request = completion_to_complete_task_request(
            "agent-1".to_owned(),
            crate::proto::rapidbyte::v2::TaskCompletion {
                task_id: "task-1".to_owned(),
                lease_epoch: 10,
                success: false,
                records_processed: 0,
                bytes_processed: 0,
                elapsed_seconds: 1.0,
                cursors_advanced: 0,
                outcome: TaskOutcome::Failed as i32,
                error: Some(TaskError {
                    code: "PLUGIN_FAILURE".to_owned(),
                    message: "transform exploded".to_owned(),
                    retryable: true,
                    safe_to_retry: true,
                    commit_state: "before_commit".to_owned(),
                }),
            },
        );

        assert_eq!(request.outcome, TaskOutcome::Failed as i32);
        let error = request.error.expect("failed outcome should include error");
        assert_eq!(error.code, "PLUGIN_FAILURE");
        assert!(error.retryable);
        assert!(error.safe_to_retry);
        assert_eq!(error.commit_state, "before_commit");
    }

    #[test]
    fn completion_mapping_uses_cancelled_outcome() {
        let request = completion_to_complete_task_request(
            "agent-1".to_owned(),
            crate::proto::rapidbyte::v2::TaskCompletion {
                task_id: "task-1".to_owned(),
                lease_epoch: 11,
                success: false,
                records_processed: 0,
                bytes_processed: 0,
                elapsed_seconds: 0.5,
                cursors_advanced: 0,
                outcome: TaskOutcome::Cancelled as i32,
                error: None,
            },
        );

        assert_eq!(request.outcome, TaskOutcome::Cancelled as i32);
        assert!(request.error.is_none());
    }

    #[test]
    fn retry_policy_only_retries_transient_statuses() {
        assert!(is_retryable_complete_task_status(
            &tonic::Status::unavailable("controller unavailable")
        ));
        assert!(is_retryable_complete_task_status(
            &tonic::Status::resource_exhausted("backpressure")
        ));
        assert!(!is_retryable_complete_task_status(
            &tonic::Status::invalid_argument("bad payload")
        ));
        assert!(!is_retryable_complete_task_status(
            &tonic::Status::internal("state transition failed")
        ));
    }
}
