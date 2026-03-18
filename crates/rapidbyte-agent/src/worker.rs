//! Main agent loop: register, poll tasks, execute, report.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig as TonicClientTlsConfig, Endpoint, Identity,
    ServerTlsConfig as TonicServerTlsConfig,
};
use tracing::{error, info, warn};

use opentelemetry::KeyValue;

use crate::auth::request_with_bearer;
use crate::executor::{self, TaskOutcomeKind};
use crate::flight::PreviewFlightService;
use crate::progress::ProgressSnapshot;
use crate::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use crate::proto::rapidbyte::v1::{
    complete_task_request, poll_task_response, AgentCapabilities, CompleteTaskRequest,
    HeartbeatRequest, PollTaskRequest, RegisterRequest, RunMetrics, TaskCancelled, TaskCompleted,
    TaskFailed, TaskHeartbeat,
};
use crate::spool::{PreviewKey, PreviewSpool};

/// Configuration for the agent worker.
#[derive(Clone)]
pub struct ClientTlsConfig {
    pub ca_cert_pem: Vec<u8>,
    pub domain_name: Option<String>,
}

#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

#[derive(Clone)]
pub struct AgentConfig {
    pub controller_url: String,
    pub flight_listen: String,
    pub flight_advertise: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub poll_wait_seconds: u32,
    pub signing_key: Vec<u8>,
    pub preview_ttl: Duration,
    pub auth_token: Option<String>,
    pub allow_insecure_default_signing_key: bool,
    pub controller_tls: Option<ClientTlsConfig>,
    pub flight_tls: Option<ServerTlsConfig>,
    /// Optional Prometheus metrics listen address (e.g. `127.0.0.1:9191`).
    /// Prometheus endpoint is only started when this is set.
    pub metrics_listen: Option<String>,
    /// OCI registry URL for plugin pulls, received from the controller on registration.
    /// Empty means no registry is configured; plugins are resolved locally.
    pub registry_url: Option<String>,
    /// Use HTTP instead of HTTPS when pulling from the registry.
    pub registry_insecure: bool,
    /// Plugin signature trust policy, received from the controller on registration.
    pub trust_policy: String,
    /// Trusted Ed25519 public key PEM contents, received from the controller on registration.
    pub trusted_key_pems: Vec<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller_url: "http://[::]:9090".into(),
            flight_listen: "[::]:9091".into(),
            flight_advertise: "localhost:9091".into(),
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            poll_wait_seconds: 30,
            signing_key: b"rapidbyte-dev-signing-key-not-for-production".to_vec(),
            preview_ttl: Duration::from_secs(300),
            auth_token: None,
            allow_insecure_default_signing_key: false,
            controller_tls: None,
            flight_tls: None,
            metrics_listen: None,
            registry_url: None,
            registry_insecure: false,
            trust_policy: "skip".to_owned(),
            trusted_key_pems: Vec::new(),
        }
    }
}

/// Shared state for tracking active leases across worker and heartbeat.
/// Maps `task_id` to (`lease_epoch`, `cancellation_token`, `progress_snapshot`).
type ActiveLeaseMap = Arc<RwLock<HashMap<String, LeaseEntry>>>;

struct LeaseEntry {
    lease_epoch: u64,
    cancel_token: CancellationToken,
    progress: Arc<RwLock<ProgressSnapshot>>,
}

const COMPLETE_TASK_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_SIGNING_KEY: &[u8] = b"rapidbyte-dev-signing-key-not-for-production";

#[derive(Clone)]
struct TaskExecutionContext {
    channel: Channel,
    agent_id: String,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    config: AgentConfig,
    shutdown: CancellationToken,
}

enum WorkerPoll<T> {
    Task(T),
    Idle,
    Stop,
}

/// Run the agent worker loop.
///
/// # Errors
///
/// Returns an error if the controller connection fails, agent registration
/// is rejected, or the Flight server address cannot be parsed.
///
#[allow(clippy::too_many_lines)]
pub async fn run(
    config: AgentConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
) -> anyhow::Result<()> {
    validate_signing_key_config(&config)?;

    if let Some(ref metrics_addr) = config.metrics_listen {
        info!("Prometheus metrics endpoint at {metrics_addr}");
        let metrics_listener = rapidbyte_metrics::bind_prometheus(metrics_addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            otel_guard.clone(),
            metrics_listener,
        ));
    }

    // Bind Flight first so startup fails fast before the agent registers
    // itself as preview-capable.
    let flight_listener = tokio::net::TcpListener::bind(&config.flight_listen).await?;
    let flight_addr = flight_listener.local_addr()?;

    let channel = connect_channel(&config.controller_url, config.controller_tls.as_ref()).await?;
    let mut client = AgentServiceClient::new(channel.clone());

    // Set up preview spool and Flight server
    let spool = Arc::new(RwLock::new(PreviewSpool::new(config.preview_ttl)));
    let flight_svc = PreviewFlightService::new(spool.clone(), &config.signing_key);

    info!(addr = %flight_addr, "Starting Flight server");
    let mut flight_server =
        tonic::transport::Server::builder().layer(rapidbyte_metrics::grpc_layer::GrpcMetricsLayer);
    if let Some(tls) = &config.flight_tls {
        flight_server = flight_server.tls_config(TonicServerTlsConfig::new().identity(
            Identity::from_pem(tls.cert_pem.clone(), tls.key_pem.clone()),
        ))?;
    }
    tokio::spawn(async move {
        if let Err(e) = flight_server
            .add_service(flight_svc.into_server())
            .serve_with_incoming(TcpListenerStream::new(flight_listener))
            .await
        {
            error!(error = %e, "Flight server failed");
        }
    });

    // Register with controller
    let agent_id = uuid::Uuid::new_v4().to_string();
    let resp = client
        .register(
            request_with_bearer(
                RegisterRequest {
                    agent_id: agent_id.clone(),
                    capabilities: Some(AgentCapabilities {
                        plugins: vec![],
                        max_concurrent_tasks: config.max_tasks,
                    }),
                },
                config.auth_token.as_deref(),
            )
            .map_err(|_| anyhow::anyhow!("Invalid bearer token"))?,
        )
        .await?;
    let registration = resp.into_inner();
    // Controller response is authoritative for registry config.
    let mut config = config;
    if let Some(registry) = registration.registry {
        if registry.url.is_empty() {
            config.registry_url = None;
            config.registry_insecure = false;
            info!(agent_id, "Registered with controller");
        } else {
            info!(
                agent_id,
                registry_url = registry.url,
                registry_insecure = registry.insecure,
                "Registered with controller (registry configured)"
            );
            config.registry_url = Some(registry.url);
            config.registry_insecure = registry.insecure;
        }
    } else {
        config.registry_url = None;
        config.registry_insecure = false;
        info!(agent_id, "Registered with controller (no registry)");
    }

    // Active lease tracking shared between worker and heartbeat
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
    let shutdown_token = CancellationToken::new();

    // Spawn heartbeat loop
    let hb_client = AgentServiceClient::new(channel.clone());
    let hb_agent_id = agent_id.clone();
    let hb_interval = config.heartbeat_interval;
    let hb_leases = active_leases.clone();
    let hb_spool = spool.clone();
    let hb_shutdown = shutdown_token.clone();
    let hb_auth_token = config.auth_token.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            hb_client,
            hb_agent_id,
            hb_interval,
            hb_leases,
            hb_spool,
            hb_shutdown,
            hb_auth_token,
        )
        .await;
    });

    let worker_pool = run_worker_pool(config.max_tasks, {
        let channel = channel.clone();
        let agent_id = agent_id.clone();
        let active_leases = active_leases.clone();
        let spool = spool.clone();
        let otel_guard = otel_guard.clone();
        let config = config.clone();
        let shutdown = shutdown_token.clone();
        move || {
            worker_runner_loop(
                channel.clone(),
                agent_id.clone(),
                active_leases.clone(),
                spool.clone(),
                otel_guard.clone(),
                config.clone(),
                shutdown.clone(),
            )
        }
    });
    tokio::pin!(worker_pool);

    // Main coordinator loop with graceful shutdown.
    // Handle both SIGINT (Ctrl-C) and SIGTERM (container orchestrators).
    let signal_token = shutdown_token.clone();
    tokio::spawn(async move {
        let sigint = tokio::signal::ctrl_c();

        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    tokio::select! {
                        result = sigint => {
                            if result.is_ok() { info!("SIGINT received, stopping agent..."); }
                        }
                        _ = sigterm.recv() => { info!("SIGTERM received, stopping agent..."); }
                    }
                }
                Err(e) => {
                    warn!("failed to install SIGTERM handler: {e}; falling back to SIGINT only");
                    if sigint.await.is_ok() {
                        info!("SIGINT received, stopping agent...");
                    }
                }
            }
        }

        #[cfg(not(unix))]
        {
            if sigint.await.is_ok() {
                info!("SIGINT received, stopping agent...");
            }
        }

        signal_token.cancel();
    });

    let pool_result = worker_pool.await;
    shutdown_token.cancel();

    let _ = heartbeat_handle.await;
    pool_result?;

    info!("Agent stopped");
    Ok(())
}

fn validate_signing_key_config(config: &AgentConfig) -> anyhow::Result<()> {
    if config.signing_key == DEFAULT_SIGNING_KEY && !config.allow_insecure_default_signing_key {
        anyhow::bail!(
            "Agent preview signing key must be set explicitly. Pass --signing-key / RAPIDBYTE_SIGNING_KEY or --allow-insecure-default-signing-key for local development."
        );
    }
    Ok(())
}

async fn connect_channel(
    controller_url: &str,
    tls: Option<&ClientTlsConfig>,
) -> anyhow::Result<Channel> {
    let mut endpoint = Endpoint::from_shared(controller_url.to_string())?;
    if controller_url.starts_with("https://") || tls.is_some() {
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

async fn worker_runner_loop(
    channel: Channel,
    agent_id: String,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    config: AgentConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let client = AgentServiceClient::new(channel.clone());
    let poll_auth_token = config.auth_token.clone();

    worker_loop(
        || {
            let shutdown = shutdown.clone();
            let mut poll_client = client.clone();
            let agent_id = agent_id.clone();
            let auth_token = poll_auth_token.clone();
            async move {
                if shutdown.is_cancelled() {
                    return Ok(WorkerPoll::Stop);
                }

                let resp = poll_client
                    .poll_task(
                        request_with_bearer(PollTaskRequest { agent_id }, auth_token.as_deref())
                            .map_err(|_| anyhow::anyhow!("Invalid bearer token"))?,
                    )
                    .await?
                    .into_inner();

                Ok(match resp.result {
                    Some(poll_task_response::Result::Assignment(task)) => WorkerPoll::Task(task),
                    Some(poll_task_response::Result::NoTask(_)) | None => {
                        if shutdown.is_cancelled() {
                            WorkerPoll::Stop
                        } else {
                            WorkerPoll::Idle
                        }
                    }
                })
            }
        },
        |task| {
            process_task(
                TaskExecutionContext {
                    channel: channel.clone(),
                    agent_id: agent_id.clone(),
                    active_leases: active_leases.clone(),
                    spool: spool.clone(),
                    otel_guard: otel_guard.clone(),
                    config: config.clone(),
                    shutdown: shutdown.clone(),
                },
                task,
            )
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn process_task(
    ctx: TaskExecutionContext,
    task: crate::proto::rapidbyte::v1::TaskAssignment,
) -> anyhow::Result<()> {
    rapidbyte_metrics::instruments::agent::tasks_received().add(1, &[]);
    rapidbyte_metrics::instruments::agent::active_tasks().add(1, &[]);
    let task_start = Instant::now();

    info!(
        task_id = task.task_id,
        run_id = task.run_id,
        attempt = task.attempt,
        lease_epoch = task.lease_epoch,
        "Received task"
    );

    let cancel_token = CancellationToken::new();
    let progress_snapshot = Arc::new(RwLock::new(ProgressSnapshot::default()));
    ctx.active_leases.write().await.insert(
        task.task_id.clone(),
        LeaseEntry {
            lease_epoch: task.lease_epoch,
            cancel_token: cancel_token.clone(),
            progress: progress_snapshot.clone(),
        },
    );

    let dry_run = false;
    let limit = None;

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_handle = tokio::spawn(crate::progress::collect_progress(
        progress_rx,
        progress_snapshot,
    ));

    let trust_policy = if ctx.config.trust_policy.is_empty() {
        rapidbyte_registry::TrustPolicy::Skip
    } else if let Ok(p) = rapidbyte_registry::TrustPolicy::from_str_name(&ctx.config.trust_policy) {
        p
    } else {
        // Invalid policy defaults to Verify (most restrictive) so the task
        // fails safely on plugin resolution rather than crashing the worker.
        tracing::error!(
            policy = %ctx.config.trust_policy,
            "invalid trust policy from controller, defaulting to 'verify'"
        );
        rapidbyte_registry::TrustPolicy::Verify
    };
    let registry_config = rapidbyte_registry::RegistryConfig {
        insecure: ctx.config.registry_insecure,
        default_registry: rapidbyte_registry::normalize_registry_url_option(
            ctx.config.registry_url.as_deref(),
        ),
        trust_policy,
        trusted_key_pems: ctx.config.trusted_key_pems.clone(),
        ..Default::default()
    };
    let result = executor::execute_task(
        executor::TaskConfig {
            pipeline_yaml: task.pipeline_yaml.as_bytes(),
            dry_run,
            limit,
            progress_tx: Some(progress_tx),
            cancel_token,
            snapshot_reader: ctx.otel_guard.snapshot_reader(),
            meter_provider: ctx.otel_guard.meter_provider(),
            registry_config: &registry_config,
        },
        |config, options, progress_tx, cancel_token, metrics_runtime, registry_config| {
            Box::pin(rapidbyte_engine::orchestrator::run_pipeline(
                config,
                options,
                progress_tx,
                cancel_token,
                metrics_runtime.snapshot_reader,
                metrics_runtime.meter_provider,
                registry_config,
            ))
        },
    )
    .await;

    let _ = progress_handle.await;

    // Store preview data in spool if dry-run produced results
    if let Some(dr) = result.dry_run_result {
        ctx.spool.write().await.store(
            PreviewKey {
                run_id: task.run_id.clone(),
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
            },
            dr,
        );
    }

    let proto_outcome = match &result.outcome {
        TaskOutcomeKind::Completed => complete_task_request::Outcome::Completed(TaskCompleted {
            metrics: Some(RunMetrics {
                rows_read: result.metrics.records_read,
                rows_written: result.metrics.records_written,
                bytes_read: result.metrics.bytes_read,
                bytes_written: result.metrics.bytes_written,
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                duration_ms: { (result.metrics.elapsed_seconds * 1000.0).max(0.0) as u64 },
            }),
        }),
        TaskOutcomeKind::Failed(info) => {
            use crate::proto::rapidbyte::v1::CommitState as ProtoCommitState;
            let commit_state = match info.commit_state {
                rapidbyte_types::prelude::CommitState::BeforeCommit => {
                    ProtoCommitState::BeforeCommit as i32
                }
                rapidbyte_types::prelude::CommitState::AfterCommitUnknown => {
                    ProtoCommitState::AfterCommitUnknown as i32
                }
                rapidbyte_types::prelude::CommitState::AfterCommitConfirmed => {
                    ProtoCommitState::AfterCommitConfirmed as i32
                }
            };
            complete_task_request::Outcome::Failed(TaskFailed {
                error_code: info.code.clone(),
                error_message: info.message.clone(),
                retryable: info.retryable,
                commit_state,
            })
        }
        TaskOutcomeKind::Cancelled => complete_task_request::Outcome::Cancelled(TaskCancelled {}),
    };

    let complete_request = CompleteTaskRequest {
        agent_id: ctx.agent_id.clone(),
        task_id: task.task_id.clone(),
        lease_epoch: task.lease_epoch,
        outcome: Some(proto_outcome),
    };

    let status_label = match &result.outcome {
        TaskOutcomeKind::Completed => rapidbyte_metrics::labels::STATUS_OK,
        TaskOutcomeKind::Failed(_) => rapidbyte_metrics::labels::STATUS_ERROR,
        TaskOutcomeKind::Cancelled => rapidbyte_metrics::labels::STATUS_CANCELLED,
    };
    rapidbyte_metrics::instruments::agent::tasks_completed().add(
        1,
        &[KeyValue::new(
            rapidbyte_metrics::labels::STATUS,
            status_label,
        )],
    );
    rapidbyte_metrics::instruments::agent::active_tasks().add(-1, &[]);
    rapidbyte_metrics::instruments::agent::task_duration()
        .record(task_start.elapsed().as_secs_f64(), &[]);

    let _ = report_completion_until_terminal(
        &ctx.active_leases,
        complete_request,
        COMPLETE_TASK_RETRY_DELAY,
        |req| {
            let mut completion_client = AgentServiceClient::new(ctx.channel.clone());
            let auth_token = ctx.config.auth_token.clone();
            async move {
                completion_client
                    .complete_task(
                        request_with_bearer(req, auth_token.as_deref())
                            .map_err(|_| tonic::Status::unauthenticated("Invalid bearer token"))?,
                    )
                    .await
                    .map(tonic::Response::into_inner)
            }
        },
        ctx.shutdown,
    )
    .await;

    Ok(())
}

async fn run_worker_pool<F, Fut>(max_tasks: u32, mut make_worker: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let mut workers = JoinSet::new();
    for _ in 0..max_tasks.max(1) {
        workers.spawn(make_worker());
    }

    while let Some(result) = workers.join_next().await {
        result??;
    }

    Ok(())
}

async fn worker_loop<P, PFut, H, HFut, T>(mut poll: P, mut handle_task: H) -> anyhow::Result<()>
where
    P: FnMut() -> PFut,
    PFut: Future<Output = anyhow::Result<WorkerPoll<T>>>,
    H: FnMut(T) -> HFut,
    HFut: Future<Output = anyhow::Result<()>>,
{
    loop {
        match poll().await? {
            WorkerPoll::Task(task) => handle_task(task).await?,
            WorkerPoll::Idle => {}
            WorkerPoll::Stop => return Ok(()),
        }
    }
}

async fn heartbeat_loop(
    mut client: AgentServiceClient<Channel>,
    agent_id: String,
    interval: Duration,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    shutdown: CancellationToken,
    auth_token: Option<String>,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _tick = ticker.tick() => {}
        }

        let removed = spool.write().await.cleanup_expired();
        if removed > 0 {
            tracing::debug!(removed, "Evicted expired preview entries");
        }

        // Build TaskHeartbeat for each active lease, including latest progress
        let tasks: Vec<TaskHeartbeat> = {
            let leases = active_leases.read().await;
            let mut tasks = Vec::with_capacity(leases.len());
            for (task_id, entry) in leases.iter() {
                let snap = entry.progress.read().await;
                tasks.push(TaskHeartbeat {
                    task_id: task_id.clone(),
                    lease_epoch: entry.lease_epoch,
                    progress_message: snap.message.clone(),
                    progress_pct: snap.progress_pct,
                });
            }
            tasks
        };

        let Ok(request) = request_with_bearer(
            HeartbeatRequest {
                agent_id: agent_id.clone(),
                tasks,
            },
            auth_token.as_deref(),
        ) else {
            warn!("Failed to build authenticated heartbeat request: invalid bearer token");
            break;
        };
        let resp = client.heartbeat(request).await;
        match resp {
            Ok(resp) => {
                for directive in resp.into_inner().directives {
                    if directive.cancel_requested {
                        warn!(task_id = directive.task_id, "Received cancel directive");
                        // Signal the executor to cancel via the token
                        let leases = active_leases.read().await;
                        if let Some(entry) = leases.get(&directive.task_id) {
                            entry.cancel_token.cancel();
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Heartbeat failed");
            }
        }
    }
}

async fn report_completion_until_terminal<F, Fut>(
    active_leases: &ActiveLeaseMap,
    request: CompleteTaskRequest,
    retry_delay: Duration,
    mut send_completion: F,
    shutdown: CancellationToken,
) -> bool
where
    F: FnMut(CompleteTaskRequest) -> Fut,
    Fut: Future<Output = Result<crate::proto::rapidbyte::v1::CompleteTaskResponse, tonic::Status>>,
{
    fn is_non_retryable(code: tonic::Code) -> bool {
        matches!(
            code,
            tonic::Code::Unauthenticated
                | tonic::Code::PermissionDenied
                | tonic::Code::Aborted        // lease mismatch/expired
                | tonic::Code::FailedPrecondition // invalid state transition
                | tonic::Code::NotFound        // task/run no longer exists
                | tonic::Code::InvalidArgument // malformed request
        )
    }

    loop {
        if shutdown.is_cancelled() {
            warn!(
                task_id = request.task_id,
                "Stopping completion retries because the agent is shutting down"
            );
            return false;
        }

        match send_completion(request.clone()).await {
            Ok(_resp) => {
                {
                    let mut leases = active_leases.write().await;
                    if leases
                        .get(&request.task_id)
                        .is_some_and(|e| e.lease_epoch == request.lease_epoch)
                    {
                        leases.remove(&request.task_id);
                    }
                }
                info!(task_id = request.task_id, "Task completed");
                return true;
            }
            Err(e) => {
                if is_non_retryable(e.code()) {
                    warn!(
                        task_id = request.task_id,
                        code = ?e.code(),
                        error = %e,
                        "Stopping completion retries — non-retryable error"
                    );
                    return false;
                }
                warn!(
                    task_id = request.task_id,
                    error = %e,
                    "Failed to report completion, retrying while lease stays active"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {
                        warn!(
                            task_id = request.task_id,
                            "Stopping completion retries because the agent is shutting down"
                        );
                        return false;
                    }
                    () = tokio::time::sleep(retry_delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    fn test_lease_entry() -> LeaseEntry {
        LeaseEntry {
            lease_epoch: 42,
            cancel_token: CancellationToken::new(),
            progress: Arc::new(RwLock::new(ProgressSnapshot::default())),
        }
    }

    fn test_complete_request() -> CompleteTaskRequest {
        CompleteTaskRequest {
            agent_id: "agent-1".into(),
            task_id: "task-1".into(),
            lease_epoch: 42,
            outcome: Some(complete_task_request::Outcome::Completed(TaskCompleted {
                metrics: Some(RunMetrics {
                    rows_read: 1,
                    rows_written: 1,
                    bytes_read: 1,
                    bytes_written: 1,
                    duration_ms: 100,
                }),
            })),
        }
    }

    #[tokio::test]
    async fn complete_task_transport_failure_keeps_lease_active() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), test_lease_entry());

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let active_for_closure = active_leases.clone();
        let shutdown = CancellationToken::new();
        let acknowledged = report_completion_until_terminal(
            &active_leases,
            test_complete_request(),
            Duration::from_millis(1),
            move |_req| {
                let attempts = attempts_for_closure.clone();
                let active_leases = active_for_closure.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    assert!(active_leases.read().await.contains_key("task-1"));
                    if attempt == 0 {
                        Err(tonic::Status::unavailable("controller unavailable"))
                    } else {
                        Ok(crate::proto::rapidbyte::v1::CompleteTaskResponse {})
                    }
                }
            },
            shutdown,
        )
        .await;

        assert!(acknowledged);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert!(!active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn completion_retries_stop_on_shutdown() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), test_lease_entry());
        let shutdown = CancellationToken::new();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let completion = tokio::spawn({
            let active_leases = active_leases.clone();
            let shutdown = shutdown.clone();
            async move {
                report_completion_until_terminal(
                    &active_leases,
                    test_complete_request(),
                    Duration::from_millis(1),
                    move |_req| {
                        let attempts = attempts_for_closure.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(tonic::Status::unavailable("controller unavailable"))
                        }
                    },
                    shutdown,
                )
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.cancel();

        let acknowledged = tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("completion retry loop should stop on shutdown")
            .unwrap();

        assert!(!acknowledged);
        assert!(attempts.load(Ordering::SeqCst) > 0);
        assert!(active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn completion_retries_stop_on_auth_failures() {
        for code in [tonic::Code::Unauthenticated, tonic::Code::PermissionDenied] {
            let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
            active_leases
                .write()
                .await
                .insert("task-1".into(), test_lease_entry());
            let shutdown = CancellationToken::new();

            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_for_closure = attempts.clone();
            let acknowledged = report_completion_until_terminal(
                &active_leases,
                test_complete_request(),
                Duration::from_millis(1),
                move |_req| {
                    let attempts = attempts_for_closure.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err(tonic::Status::new(code, "auth failure"))
                    }
                },
                shutdown,
            )
            .await;

            assert!(!acknowledged, "expected {code:?} to stop retries");
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert!(active_leases.read().await.contains_key("task-1"));
        }
    }

    #[tokio::test]
    async fn completion_invalid_argument_stops_immediately() {
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        active_leases
            .write()
            .await
            .insert("task-1".into(), test_lease_entry());
        let shutdown = CancellationToken::new();

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();
        let completion = tokio::spawn({
            let active_leases = active_leases.clone();
            let shutdown = shutdown.clone();
            async move {
                report_completion_until_terminal(
                    &active_leases,
                    test_complete_request(),
                    Duration::from_millis(1),
                    move |_req| {
                        let attempts = attempts_for_closure.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            Err(tonic::Status::invalid_argument("non-auth validation error"))
                        }
                    },
                    shutdown,
                )
                .await
            }
        });

        let acknowledged = tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("should stop immediately without retrying")
            .unwrap();

        // InvalidArgument is non-retryable: stops after 1 attempt, not acknowledged
        assert!(!acknowledged);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        // Lease is NOT removed on non-retryable errors (agent doesn't know if it's safe)
        assert!(active_leases.read().await.contains_key("task-1"));
    }

    #[tokio::test]
    async fn max_tasks_allows_parallel_execution() {
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let queue = Arc::new(RwLock::new(VecDeque::from([
            "task-1".to_string(),
            "task-2".to_string(),
        ])));

        let pool = tokio::spawn(run_worker_pool(2, {
            let queue = queue.clone();
            let started = started.clone();
            let release = release.clone();
            move || {
                let queue = queue.clone();
                let started = started.clone();
                let release = release.clone();
                async move {
                    worker_loop(
                        || {
                            let queue = queue.clone();
                            async move {
                                Ok(match queue.write().await.pop_front() {
                                    Some(task_id) => WorkerPoll::Task(task_id),
                                    None => WorkerPoll::Stop,
                                })
                            }
                        },
                        |_task_id| {
                            let started = started.clone();
                            let release = release.clone();
                            async move {
                                started.fetch_add(1, Ordering::SeqCst);
                                release.notified().await;
                                Ok(())
                            }
                        },
                    )
                    .await?;
                    Ok(())
                }
            }
        }));

        for _ in 0..100 {
            if started.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(started.load(Ordering::SeqCst), 2);
        release.notify_waiters();
        pool.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn run_fails_when_flight_listener_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let guard =
            Arc::new(rapidbyte_metrics::init("test-agent").expect("otel init should succeed"));
        let err = run(
            AgentConfig {
                controller_url: "http://127.0.0.1:1".into(),
                flight_listen: addr.to_string(),
                flight_advertise: addr.to_string(),
                signing_key: b"test-signing-key".to_vec(),
                ..Default::default()
            },
            guard,
        )
        .await
        .unwrap_err();

        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("address already in use") || msg.contains("addrinuse"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn run_fails_when_metrics_listener_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let guard =
            Arc::new(rapidbyte_metrics::init("test-agent").expect("otel init should succeed"));
        let err = run(
            AgentConfig {
                controller_url: "http://127.0.0.1:1".into(),
                flight_listen: "127.0.0.1:0".into(),
                flight_advertise: "127.0.0.1:0".into(),
                metrics_listen: Some(addr.to_string()),
                signing_key: b"test-signing-key".to_vec(),
                ..Default::default()
            },
            guard,
        )
        .await
        .unwrap_err();

        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("address already in use") || msg.contains("addrinuse"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn default_signing_key_requires_explicit_override() {
        let config = AgentConfig {
            controller_url: "http://127.0.0.1:9090".into(),
            flight_listen: "127.0.0.1:9091".into(),
            flight_advertise: "127.0.0.1:9091".into(),
            ..Default::default()
        };
        let err = validate_signing_key_config(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("preview signing key must be set explicitly"));
    }

    #[test]
    fn allow_insecure_default_signing_key_permits_dev_default() {
        let config = AgentConfig {
            controller_url: "http://127.0.0.1:9090".into(),
            flight_listen: "127.0.0.1:9091".into(),
            flight_advertise: "127.0.0.1:9091".into(),
            allow_insecure_default_signing_key: true,
            ..Default::default()
        };
        validate_signing_key_config(&config).unwrap();
    }
}
