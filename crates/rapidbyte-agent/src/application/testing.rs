//! In-memory fakes for all agent port traits and a [`TestContext`] factory.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use async_trait::async_trait;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::clock::Clock;
use crate::domain::ports::controller::{
    CompletionPayload, ControllerGateway, HeartbeatPayload, HeartbeatResponse, RegistrationConfig,
    RegistrationResponse, TaskAssignment,
};
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::ports::metrics::MetricsProvider;
use crate::domain::ports::progress::ProgressCollector;
use crate::domain::progress::ProgressSnapshot;
use crate::domain::task::TaskExecutionResult;

use super::context::{AgentAppConfig, AgentContext};

// ---------------------------------------------------------------------------
// FakeControllerGateway
// ---------------------------------------------------------------------------

pub struct FakeControllerGateway {
    register_results: Mutex<VecDeque<Result<RegistrationResponse, AgentError>>>,
    poll_results: Mutex<VecDeque<Result<Option<TaskAssignment>, AgentError>>>,
    heartbeat_results: Mutex<VecDeque<Result<HeartbeatResponse, AgentError>>>,
    complete_results: Mutex<VecDeque<Result<(), AgentError>>>,
    completed_payloads: Mutex<Vec<CompletionPayload>>,
}

impl FakeControllerGateway {
    #[must_use]
    pub fn new() -> Self {
        Self {
            register_results: Mutex::new(VecDeque::new()),
            poll_results: Mutex::new(VecDeque::new()),
            heartbeat_results: Mutex::new(VecDeque::new()),
            complete_results: Mutex::new(VecDeque::new()),
            completed_payloads: Mutex::new(Vec::new()),
        }
    }

    pub fn enqueue_register(&self, result: Result<RegistrationResponse, AgentError>) {
        self.register_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_poll(&self, result: Result<Option<TaskAssignment>, AgentError>) {
        self.poll_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_heartbeat(&self, result: Result<HeartbeatResponse, AgentError>) {
        self.heartbeat_results.lock().unwrap().push_back(result);
    }

    pub fn enqueue_complete(&self, result: Result<(), AgentError>) {
        self.complete_results.lock().unwrap().push_back(result);
    }

    /// Returns all completion payloads that were reported.
    pub fn completed_payloads(&self) -> Vec<CompletionPayload> {
        self.completed_payloads.lock().unwrap().clone()
    }
}

#[async_trait]
impl ControllerGateway for FakeControllerGateway {
    async fn register(
        &self,
        _config: &RegistrationConfig,
    ) -> Result<RegistrationResponse, AgentError> {
        self.register_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no register result enqueued")
    }

    async fn poll(&self, _agent_id: &str) -> Result<Option<TaskAssignment>, AgentError> {
        self.poll_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no poll result enqueued")
    }

    async fn heartbeat(&self, _request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError> {
        self.heartbeat_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no heartbeat result enqueued")
    }

    async fn complete(&self, request: CompletionPayload) -> Result<(), AgentError> {
        self.completed_payloads.lock().unwrap().push(request);
        self.complete_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeControllerGateway: no complete result enqueued")
    }
}

// ---------------------------------------------------------------------------
// FakePipelineExecutor
// ---------------------------------------------------------------------------

pub struct FakePipelineExecutor {
    results: Mutex<VecDeque<Result<TaskExecutionResult, AgentError>>>,
}

impl FakePipelineExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            results: Mutex::new(VecDeque::new()),
        }
    }

    pub fn enqueue(&self, result: Result<TaskExecutionResult, AgentError>) {
        self.results.lock().unwrap().push_back(result);
    }
}

#[async_trait]
impl PipelineExecutor for FakePipelineExecutor {
    async fn execute(
        &self,
        _config: &PipelineConfig,
        _cancel: CancellationToken,
        _progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError> {
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakePipelineExecutor: no result enqueued")
    }
}

// ---------------------------------------------------------------------------
// FakeProgressCollector
// ---------------------------------------------------------------------------

pub struct FakeProgressCollector {
    snapshot: RwLock<ProgressSnapshot>,
}

impl FakeProgressCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(ProgressSnapshot::default()),
        }
    }

    pub fn set(&self, snapshot: ProgressSnapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }
}

impl ProgressCollector for FakeProgressCollector {
    fn latest(&self) -> ProgressSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    fn reset(&self) {
        *self.snapshot.write().unwrap() = ProgressSnapshot::default();
    }
}

// ---------------------------------------------------------------------------
// FakeMetricsProvider
// ---------------------------------------------------------------------------

pub struct FakeMetricsProvider {
    reader: rapidbyte_metrics::snapshot::SnapshotReader,
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl FakeMetricsProvider {
    #[must_use]
    pub fn new() -> Self {
        let reader = rapidbyte_metrics::snapshot::SnapshotReader::new();
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader.build_reader())
            .build();
        Self { reader, provider }
    }
}

impl MetricsProvider for FakeMetricsProvider {
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader {
        &self.reader
    }

    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider {
        &self.provider
    }
}

// ---------------------------------------------------------------------------
// FakeClock
// ---------------------------------------------------------------------------

pub struct FakeClock {
    now: Mutex<Instant>,
}

impl FakeClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            now: Mutex::new(Instant::now()),
        }
    }

    pub fn advance(&self, duration: std::time::Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// TestContext
// ---------------------------------------------------------------------------

/// Bundles `AgentContext` with typed references to each fake for test assertions.
pub struct TestContext {
    pub ctx: AgentContext,
    pub gateway: Arc<FakeControllerGateway>,
    pub executor: Arc<FakePipelineExecutor>,
    pub progress: Arc<FakeProgressCollector>,
    pub clock: Arc<FakeClock>,
}

/// Create a fully-wired test context with in-memory fakes.
#[must_use]
pub fn fake_context() -> TestContext {
    let gateway = Arc::new(FakeControllerGateway::new());
    let executor = Arc::new(FakePipelineExecutor::new());
    let progress = Arc::new(FakeProgressCollector::new());
    let clock = Arc::new(FakeClock::new());

    let ctx = AgentContext {
        gateway: Arc::clone(&gateway) as Arc<dyn ControllerGateway>,
        executor: Arc::clone(&executor) as Arc<dyn PipelineExecutor>,
        progress: Arc::clone(&progress) as Arc<dyn ProgressCollector>,
        metrics: Arc::new(FakeMetricsProvider::new()),
        clock: Arc::clone(&clock) as Arc<dyn Clock>,
        config: AgentAppConfig::default(),
    };

    TestContext {
        ctx,
        gateway,
        executor,
        progress,
        clock,
    }
}
