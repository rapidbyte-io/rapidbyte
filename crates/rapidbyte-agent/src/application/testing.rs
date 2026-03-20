//! In-memory fakes for all agent port traits and a [`TestContext`] factory.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

/// Minimal valid pipeline YAML for tests that need to reach the executor.
pub const VALID_YAML: &str = "version: '1.0'\npipeline: test\nsource:\n  use: postgres\n  config:\n    host: localhost\n  streams:\n    - name: users\n      sync_mode: full_refresh\ndestination:\n  use: postgres\n  config:\n    host: localhost\n  write_mode: append\n";

use async_trait::async_trait;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{
    CompletionPayload, ControllerGateway, HeartbeatPayload, HeartbeatResponse, RegistrationConfig,
    RegistrationResponse, TaskAssignment,
};
use crate::domain::ports::executor::PipelineExecutor;
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
    heartbeat_payloads: Mutex<Vec<HeartbeatPayload>>,
}

impl Default for FakeControllerGateway {
    fn default() -> Self {
        Self::new()
    }
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
            heartbeat_payloads: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a registration result.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_register(&self, result: Result<RegistrationResponse, AgentError>) {
        self.register_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a poll result.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_poll(&self, result: Result<Option<TaskAssignment>, AgentError>) {
        self.poll_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a heartbeat result.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_heartbeat(&self, result: Result<HeartbeatResponse, AgentError>) {
        self.heartbeat_results.lock().unwrap().push_back(result);
    }

    /// Enqueue a completion result.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn enqueue_complete(&self, result: Result<(), AgentError>) {
        self.complete_results.lock().unwrap().push_back(result);
    }

    /// Returns all completion payloads that were reported.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn completed_payloads(&self) -> Vec<CompletionPayload> {
        self.completed_payloads.lock().unwrap().clone()
    }

    /// Returns all heartbeat payloads that were sent.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn heartbeat_payloads(&self) -> Vec<HeartbeatPayload> {
        self.heartbeat_payloads.lock().unwrap().clone()
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

    async fn heartbeat(&self, request: HeartbeatPayload) -> Result<HeartbeatResponse, AgentError> {
        self.heartbeat_payloads.lock().unwrap().push(request);
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

impl Default for FakePipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl FakePipelineExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            results: Mutex::new(VecDeque::new()),
        }
    }

    /// Enqueue an execution result.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
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

impl Default for FakeProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProgressCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(ProgressSnapshot::default()),
        }
    }

    /// Set a progress message for testing.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn set_message(&self, message: &str) {
        self.snapshot.write().unwrap().message = Some(message.to_owned());
    }
}

impl ProgressCollector for FakeProgressCollector {
    fn latest(&self) -> ProgressSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    fn take(&self) -> ProgressSnapshot {
        std::mem::take(&mut *self.snapshot.write().unwrap())
    }

    fn update(&self, snapshot: ProgressSnapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }

    fn reset(&self) {
        *self.snapshot.write().unwrap() = ProgressSnapshot::default();
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
}

/// Create a fully-wired test context with in-memory fakes.
#[must_use]
pub fn fake_context() -> TestContext {
    let gateway = Arc::new(FakeControllerGateway::new());
    let executor = Arc::new(FakePipelineExecutor::new());
    let progress = Arc::new(FakeProgressCollector::new());

    let ctx = AgentContext {
        gateway: Arc::clone(&gateway) as Arc<dyn ControllerGateway>,
        executor: Arc::clone(&executor) as Arc<dyn PipelineExecutor>,
        progress: Arc::clone(&progress) as Arc<dyn ProgressCollector>,
        config: AgentAppConfig::default(),
    };

    TestContext {
        ctx,
        gateway,
        executor,
    }
}
