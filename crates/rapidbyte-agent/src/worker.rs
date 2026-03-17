//! Main agent loop: register, poll tasks, execute, report.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::wrappers::UnboundedReceiverStream;
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
use crate::proto::rapidbyte::v2::agent_message;
use crate::proto::rapidbyte::v2::agent_session_client::AgentSessionClient;
use crate::proto::rapidbyte::v2::controller_message;
use crate::proto::rapidbyte::v2::{
    AgentMessage, ControllerMessage, LeaseStatus, SessionAccepted, SessionHeartbeat, SessionHello,
    TaskAssignment, TaskCompletion,
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
/// Maps `task_id` to (`lease_epoch`, `cancellation_token`).
type ActiveLeaseMap = Arc<RwLock<HashMap<String, (u64, CancellationToken)>>>;

const DEFAULT_SIGNING_KEY: &[u8] = b"rapidbyte-dev-signing-key-not-for-production";

#[derive(Clone)]
struct TaskExecutionContext {
    agent_id: String,
    session_outbound: mpsc::UnboundedSender<AgentMessage>,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    config: AgentConfig,
}

enum WorkerPoll<T> {
    Task(T),
    Stop,
}

/// Bidirectional v2 session handle used for agent-controller communication.
pub struct V2Session {
    outbound: mpsc::UnboundedSender<AgentMessage>,
    inbound: tonic::Streaming<ControllerMessage>,
}

impl V2Session {
    /// Send a message to the controller over the open session stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the session outbound channel is closed.
    #[allow(clippy::result_large_err)]
    pub fn send_message(
        &self,
        message: AgentMessage,
    ) -> Result<(), mpsc::error::SendError<AgentMessage>> {
        self.outbound.send(message)
    }

    /// Read the next controller message from the stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the transport stream reports one.
    pub async fn next_controller_message(
        &mut self,
    ) -> Result<Option<ControllerMessage>, tonic::Status> {
        self.inbound.message().await
    }
}

/// Open a v2 bidirectional session and complete the hello/accepted handshake.
///
/// # Errors
///
/// Returns an error when the stream cannot be opened, the auth token is
/// invalid, or the server does not acknowledge the hello message.
pub async fn open_v2_session(
    channel: Channel,
    hello: SessionHello,
    auth_token: Option<&str>,
) -> anyhow::Result<(V2Session, SessionAccepted)> {
    let mut client = AgentSessionClient::new(channel);
    let (outbound, outbound_rx) = mpsc::unbounded_channel();
    let hello_agent_id = hello.agent_id.clone();
    outbound
        .send(AgentMessage {
            agent_id: hello_agent_id,
            payload: Some(agent_message::Payload::Hello(hello)),
        })
        .map_err(|_| anyhow::anyhow!("failed to queue session hello"))?;

    let inbound = client
        .open_session(
            request_with_bearer(UnboundedReceiverStream::new(outbound_rx), auth_token)
                .map_err(|_| anyhow::anyhow!("Invalid bearer token"))?,
        )
        .await?
        .into_inner();

    let mut session = V2Session { outbound, inbound };

    let accepted = session
        .next_controller_message()
        .await?
        .ok_or_else(|| anyhow::anyhow!("session closed before acceptance"))?;
    let Some(controller_message::Payload::Accepted(accepted)) = accepted.payload else {
        anyhow::bail!("session did not return accepted payload")
    };

    Ok((session, accepted))
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

    let mut config = config;
    let (mut session, accepted) = open_v2_session(
        channel.clone(),
        SessionHello {
            agent_id: String::new(),
            max_tasks: config.max_tasks,
            flight_advertise_endpoint: config.flight_advertise.clone(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        },
        config.auth_token.as_deref(),
    )
    .await?;
    let agent_id = accepted.agent_id;
    // Controller response is authoritative for registry and trust config.
    // Empty values explicitly mean "not configured" and override any local settings.
    if accepted.registry_url.is_empty() {
        config.registry_url = None;
        config.registry_insecure = false;
        info!(agent_id, "Registered with controller");
    } else {
        info!(
            agent_id,
            registry_url = accepted.registry_url,
            registry_insecure = accepted.registry_insecure,
            "Registered with controller (registry configured)"
        );
        config.registry_url = Some(accepted.registry_url);
        config.registry_insecure = accepted.registry_insecure;
    }
    config.trust_policy = if accepted.trust_policy.is_empty() {
        "skip".to_owned()
    } else {
        accepted.trust_policy
    };
    config.trusted_key_pems = accepted.trusted_key_pems;

    // Active lease tracking shared between worker and heartbeat
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
    let shutdown_token = CancellationToken::new();

    let session_outbound = session.outbound.clone();
    let (assignment_tx, assignment_rx) = mpsc::channel::<TaskAssignment>(128);
    let assignment_rx = Arc::new(Mutex::new(assignment_rx));
    let session_shutdown = shutdown_token.clone();
    let session_agent_id = agent_id.clone();
    let session_leases = active_leases.clone();
    let session_handle = tokio::spawn(async move {
        loop {
            if session_shutdown.is_cancelled() {
                return;
            }
            match session.next_controller_message().await {
                Ok(Some(message)) => match message.payload {
                    Some(controller_message::Payload::Assignment(assignment)) => {
                        if assignment_tx.send(assignment).await.is_err() {
                            return;
                        }
                    }
                    Some(controller_message::Payload::Directive(directive)) => {
                        if directive.action.eq_ignore_ascii_case("cancel") {
                            let leases = session_leases.read().await;
                            if let Some((epoch, token)) = leases.get(&directive.task_id) {
                                if directive.lease_epoch == 0 || *epoch == directive.lease_epoch {
                                    token.cancel();
                                }
                            }
                        }
                    }
                    Some(controller_message::Payload::Accepted(_)) | None => {}
                },
                Ok(None) => return,
                Err(error) => {
                    warn!(error = %error, agent_id = %session_agent_id, "Agent session stream ended with error");
                    return;
                }
            }
        }
    });

    // Spawn heartbeat loop
    let hb_session_outbound = session_outbound.clone();
    let hb_agent_id = agent_id.clone();
    let hb_interval = config.heartbeat_interval;
    let hb_leases = active_leases.clone();
    let hb_spool = spool.clone();
    let hb_shutdown = shutdown_token.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            hb_session_outbound,
            hb_agent_id,
            hb_interval,
            hb_leases,
            hb_spool,
            hb_shutdown,
        )
        .await;
    });

    let worker_pool = run_worker_pool(config.max_tasks, {
        let agent_id = agent_id.clone();
        let assignment_rx = assignment_rx.clone();
        let active_leases = active_leases.clone();
        let session_outbound = session_outbound.clone();
        let spool = spool.clone();
        let otel_guard = otel_guard.clone();
        let config = config.clone();
        let shutdown = shutdown_token.clone();
        move || {
            worker_runner_loop(
                agent_id.clone(),
                assignment_rx.clone(),
                active_leases.clone(),
                session_outbound.clone(),
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
    let _ = session_handle.await;
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

#[allow(clippy::too_many_arguments)]
async fn worker_runner_loop(
    agent_id: String,
    assignment_rx: Arc<Mutex<mpsc::Receiver<TaskAssignment>>>,
    active_leases: ActiveLeaseMap,
    session_outbound: mpsc::UnboundedSender<AgentMessage>,
    spool: Arc<RwLock<PreviewSpool>>,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    config: AgentConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    worker_loop(
        || {
            let shutdown = shutdown.clone();
            let assignment_rx = assignment_rx.clone();
            async move {
                if shutdown.is_cancelled() {
                    return Ok(WorkerPoll::Stop);
                }

                let mut rx = assignment_rx.lock().await;
                tokio::select! {
                    () = shutdown.cancelled() => Ok(WorkerPoll::Stop),
                    assignment = rx.recv() => Ok(match assignment {
                        Some(task) => WorkerPoll::Task(task),
                        None => WorkerPoll::Stop,
                    }),
                }
            }
        },
        |task| {
            process_task(
                TaskExecutionContext {
                    agent_id: agent_id.clone(),
                    active_leases: active_leases.clone(),
                    session_outbound: session_outbound.clone(),
                    spool: spool.clone(),
                    otel_guard: otel_guard.clone(),
                    config: config.clone(),
                },
                task,
            )
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn process_task(ctx: TaskExecutionContext, task: TaskAssignment) -> anyhow::Result<()> {
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
    ctx.active_leases.write().await.insert(
        task.task_id.clone(),
        (task.lease_epoch, cancel_token.clone()),
    );

    let dry_run = task.dry_run;
    let limit = task.limit;

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let progress_handle = tokio::spawn(crate::progress::forward_progress(
        progress_rx,
        ctx.session_outbound.clone(),
        ctx.agent_id.clone(),
        task.task_id.clone(),
        task.lease_epoch,
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
            pipeline_yaml: &task.pipeline_yaml_utf8,
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

    let preview_prepared = if let Some(dr) = result.dry_run_result {
        ctx.spool.write().await.store(
            PreviewKey {
                run_id: task.run_id.clone(),
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
            },
            dr,
        );
        true
    } else {
        false
    };

    if ctx
        .session_outbound
        .send(AgentMessage {
            agent_id: ctx.agent_id.clone(),
            payload: Some(agent_message::Payload::Completion(TaskCompletion {
                task_id: task.task_id.clone(),
                lease_epoch: task.lease_epoch,
                success: matches!(result.outcome, TaskOutcomeKind::Completed),
                records_processed: result.metrics.records_processed,
                bytes_processed: result.metrics.bytes_processed,
                elapsed_seconds: result.metrics.elapsed_seconds,
                cursors_advanced: result.metrics.cursors_advanced,
            })),
        })
        .is_err()
    {
        warn!(
            task_id = task.task_id,
            "failed to send completion on v2 session"
        );
    }

    if preview_prepared {
        tracing::debug!(
            task_id = task.task_id,
            "preview metadata prepared for v2 completion"
        );
    }

    ctx.active_leases.write().await.remove(&task.task_id);

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
            WorkerPoll::Stop => return Ok(()),
        }
    }
}

async fn heartbeat_loop(
    session_outbound: mpsc::UnboundedSender<AgentMessage>,
    agent_id: String,
    interval: Duration,
    active_leases: ActiveLeaseMap,
    spool: Arc<RwLock<PreviewSpool>>,
    shutdown: CancellationToken,
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

        let leases: Vec<LeaseStatus> = active_leases
            .read()
            .await
            .iter()
            .map(|(task_id, (epoch, _))| LeaseStatus {
                task_id: task_id.clone(),
                lease_epoch: *epoch,
            })
            .collect();
        let active_count = u32::try_from(leases.len()).unwrap_or(u32::MAX);
        let message = AgentMessage {
            agent_id: agent_id.clone(),
            payload: Some(agent_message::Payload::Heartbeat(SessionHeartbeat {
                active_leases: leases,
                active_tasks: active_count,
                cpu_usage: 0.0,
                memory_used_bytes: 0,
            })),
        };
        if session_outbound.send(message).is_err() {
            warn!("Heartbeat failed: session channel closed");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

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
