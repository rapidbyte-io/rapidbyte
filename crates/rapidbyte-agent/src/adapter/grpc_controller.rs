//! gRPC adapter for the `ControllerGateway` port.

use async_trait::async_trait;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint};

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{
    CompletionPayload, ControllerGateway, ControllerRegistryInfo, HeartbeatPayload,
    HeartbeatResponse, RegistrationConfig, RegistrationResponse, TaskAssignment, TaskDirective,
};
use crate::domain::task::{CommitState, TaskOutcomeKind};

mod proto {
    pub mod rapidbyte {
        pub mod v1 {
            #![allow(
                clippy::doc_markdown,
                clippy::trivially_copy_pass_by_ref,
                clippy::default_trait_access
            )]
            tonic::include_proto!("rapidbyte.v1");
        }
    }
}

use proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use proto::rapidbyte::v1::{
    self as pb, complete_task_request, poll_task_response, AgentCapabilities, CompleteTaskRequest,
    HeartbeatRequest, PollTaskRequest, RegisterRequest, RunMetrics, TaskCancelled, TaskCompleted,
    TaskFailed,
};

/// TLS configuration for connecting to the controller.
#[derive(Clone)]
pub struct ClientTlsConfig {
    pub ca_cert_pem: Vec<u8>,
    pub domain_name: Option<String>,
}

/// gRPC implementation of [`ControllerGateway`].
pub struct GrpcControllerGateway {
    client: AgentServiceClient<Channel>,
    bearer: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl GrpcControllerGateway {
    /// Connect to the controller at the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC channel cannot be established or the
    /// bearer token is not valid ASCII.
    pub async fn connect(
        url: &str,
        tls: Option<&ClientTlsConfig>,
        auth_token: Option<&str>,
    ) -> Result<Self, anyhow::Error> {
        let channel = connect_channel(url, tls).await?;
        let client = AgentServiceClient::new(channel);
        let bearer = auth_token
            .map(|token| format!("Bearer {token}").parse())
            .transpose()
            .map_err(|_| anyhow::anyhow!("invalid bearer token"))?;
        Ok(Self { client, bearer })
    }

    fn authed_request<T>(&self, body: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(body);
        if let Some(bearer) = &self.bearer {
            req.metadata_mut().insert("authorization", bearer.clone());
        }
        req
    }
}

#[async_trait]
impl ControllerGateway for GrpcControllerGateway {
    async fn register(
        &self,
        config: &RegistrationConfig,
    ) -> Result<RegistrationResponse, AgentError> {
        let agent_id = uuid::Uuid::new_v4().to_string();
        let mut client = self.client.clone();
        let request = RegisterRequest {
            agent_id: agent_id.clone(),
            capabilities: Some(AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: config.max_tasks,
            }),
        };
        let response = client
            .register(self.authed_request(request))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();

        let registry = response.registry.map(|r| ControllerRegistryInfo {
            url: if r.url.is_empty() { None } else { Some(r.url) },
            insecure: r.insecure,
        });

        Ok(RegistrationResponse { agent_id, registry })
    }

    async fn poll(&self, agent_id: &str) -> Result<Option<TaskAssignment>, AgentError> {
        let mut client = self.client.clone();
        let request = PollTaskRequest {
            agent_id: agent_id.to_owned(),
        };
        let response = client
            .poll_task(self.authed_request(request))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();

        Ok(match response.result {
            Some(poll_task_response::Result::Assignment(task)) => Some(TaskAssignment {
                task_id: task.task_id,
                run_id: task.run_id,
                pipeline_yaml: task.pipeline_yaml,
                lease_epoch: task.lease_epoch,
                attempt: task.attempt,
            }),
            Some(poll_task_response::Result::NoTask(_)) | None => None,
        })
    }

    async fn heartbeat(&self, request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError> {
        let mut client = self.client.clone();
        let proto_tasks: Vec<pb::TaskHeartbeat> = request
            .tasks
            .into_iter()
            .map(|t| pb::TaskHeartbeat {
                task_id: t.task_id,
                lease_epoch: t.lease_epoch,
                progress_message: t.progress.message,
                progress_pct: t.progress.progress_pct,
            })
            .collect();

        let proto_request = HeartbeatRequest {
            agent_id: request.agent_id,
            tasks: proto_tasks,
        };

        let response = client
            .heartbeat(self.authed_request(proto_request))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();

        Ok(HeartbeatResponse {
            directives: response
                .directives
                .into_iter()
                .map(|d| TaskDirective {
                    task_id: d.task_id,
                    cancel_requested: d.cancel_requested,
                })
                .collect(),
        })
    }

    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError> {
        let mut client = self.client.clone();

        let outcome = match &request.result.outcome {
            TaskOutcomeKind::Completed => {
                let m = &request.result.metrics;
                complete_task_request::Outcome::Completed(TaskCompleted {
                    metrics: Some(RunMetrics {
                        rows_read: m.records_read,
                        rows_written: m.records_written,
                        bytes_read: m.bytes_read,
                        bytes_written: m.bytes_written,
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        duration_ms: (m.elapsed_seconds * 1000.0).max(0.0) as u64,
                    }),
                })
            }
            TaskOutcomeKind::Failed(info) => {
                let commit_state = match info.commit_state {
                    CommitState::BeforeCommit => pb::CommitState::BeforeCommit as i32,
                    CommitState::AfterCommitUnknown => pb::CommitState::AfterCommitUnknown as i32,
                    CommitState::AfterCommitConfirmed => {
                        pb::CommitState::AfterCommitConfirmed as i32
                    }
                };
                complete_task_request::Outcome::Failed(TaskFailed {
                    error_code: info.code.clone(),
                    error_message: info.message.clone(),
                    retryable: info.retryable,
                    commit_state,
                })
            }
            TaskOutcomeKind::Cancelled => {
                complete_task_request::Outcome::Cancelled(TaskCancelled {})
            }
        };

        let proto_request = CompleteTaskRequest {
            agent_id: request.agent_id,
            task_id: request.task_id,
            lease_epoch: request.lease_epoch,
            outcome: Some(outcome),
        };

        client
            .complete_task(self.authed_request(proto_request))
            .await
            .map_err(|s| map_status(&s))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn connect_channel(
    url: &str,
    tls: Option<&ClientTlsConfig>,
) -> Result<Channel, anyhow::Error> {
    let mut endpoint = Endpoint::from_shared(url.to_string())?;
    if url.starts_with("https://") || tls.is_some() {
        let mut tls_config = TonicClientTlsConfig::new();
        if let Some(tls) = tls {
            if !tls.ca_cert_pem.is_empty() {
                tls_config =
                    tls_config.ca_certificate(Certificate::from_pem(tls.ca_cert_pem.clone()));
            }
            if let Some(domain_name) = &tls.domain_name {
                tls_config = tls_config.domain_name(domain_name.clone());
            }
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint.connect().await?)
}

/// Map a tonic `Status` to typed `AgentError` — non-retryable codes get their own variant.
fn map_status(status: &tonic::Status) -> AgentError {
    let msg = status.message().to_string();
    match status.code() {
        tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied
        | tonic::Code::NotFound
        | tonic::Code::InvalidArgument
        | tonic::Code::Aborted
        | tonic::Code::FailedPrecondition => AgentError::ControllerNonRetryable(msg),
        _ => AgentError::Controller(msg),
    }
}
