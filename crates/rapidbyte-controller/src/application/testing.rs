use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::application::context::{AppConfig, AppContext};
use crate::domain::agent::Agent;
use crate::domain::event::DomainEvent;
use crate::domain::lease::Lease;
use crate::domain::ports::clock::Clock;
use crate::domain::ports::event_bus::{EventBus, EventBusError, EventStream};
use crate::domain::ports::pipeline_store::PipelineStore;
use crate::domain::ports::repository::{
    AgentRepository, Pagination, RepositoryError, RunFilter, RunPage, RunRepository, TaskRepository,
};
use crate::domain::ports::secrets::{SecretError, SecretResolver};
use crate::domain::run::Run;
use crate::domain::task::{Task, TaskState};

// ---------------------------------------------------------------------------
// Shared backing store
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FakeStorage {
    pub runs: Arc<Mutex<HashMap<String, Run>>>,
    pub tasks: Arc<Mutex<HashMap<String, Task>>>,
    pub agents: Arc<Mutex<HashMap<String, Agent>>>,
}

impl FakeStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            runs: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            agents: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeStorage {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// FakeRunRepository
// ---------------------------------------------------------------------------

pub struct FakeRunRepository {
    storage: FakeStorage,
}

impl FakeRunRepository {
    #[must_use]
    pub fn new(storage: FakeStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl RunRepository for FakeRunRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Run>, RepositoryError> {
        let map = self.storage.runs.lock().unwrap();
        Ok(map.get(id).cloned())
    }

    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<Run>, RepositoryError> {
        let map = self.storage.runs.lock().unwrap();
        Ok(map
            .values()
            .find(|r| r.idempotency_key() == Some(key))
            .cloned())
    }

    async fn save(&self, run: &Run) -> Result<(), RepositoryError> {
        let mut map = self.storage.runs.lock().unwrap();
        map.insert(run.id().to_string(), run.clone());
        Ok(())
    }

    async fn list(
        &self,
        filter: RunFilter,
        pagination: Pagination,
    ) -> Result<RunPage, RepositoryError> {
        let map = self.storage.runs.lock().unwrap();
        let mut runs: Vec<Run> = map
            .values()
            .filter(|r| {
                filter
                    .state
                    .as_ref()
                    .is_none_or(|state| r.state() == *state)
            })
            .cloned()
            .collect();
        runs.sort_by_key(Run::created_at);

        let page_size = pagination.page_size as usize;
        let offset = pagination
            .page_token
            .as_deref()
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);

        let page: Vec<Run> = runs.into_iter().skip(offset).take(page_size).collect();
        let next_page_token = if page.len() == page_size {
            Some((offset + page_size).to_string())
        } else {
            None
        };
        Ok(RunPage {
            runs: page,
            next_page_token,
        })
    }
}

// ---------------------------------------------------------------------------
// FakeTaskRepository
// ---------------------------------------------------------------------------

pub struct FakeTaskRepository {
    storage: FakeStorage,
    lease_epoch: AtomicU64,
}

impl FakeTaskRepository {
    #[must_use]
    pub fn new(storage: FakeStorage) -> Self {
        Self {
            storage,
            lease_epoch: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl TaskRepository for FakeTaskRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Task>, RepositoryError> {
        let map = self.storage.tasks.lock().unwrap();
        Ok(map.get(id).cloned())
    }

    async fn save(&self, task: &Task) -> Result<(), RepositoryError> {
        let mut map = self.storage.tasks.lock().unwrap();
        map.insert(task.id().to_string(), task.clone());
        Ok(())
    }

    async fn find_expired_leases(&self, now: DateTime<Utc>) -> Result<Vec<Task>, RepositoryError> {
        let map = self.storage.tasks.lock().unwrap();
        Ok(map
            .values()
            .filter(|t| {
                t.state() == TaskState::Running
                    && t.lease().is_some_and(|lease| lease.is_expired(now))
            })
            .cloned()
            .collect())
    }

    async fn find_by_run_id(&self, run_id: &str) -> Result<Vec<Task>, RepositoryError> {
        let map = self.storage.tasks.lock().unwrap();
        Ok(map
            .values()
            .filter(|t| t.run_id() == run_id)
            .cloned()
            .collect())
    }

    async fn find_running_by_agent_id(&self, agent_id: &str) -> Result<Vec<Task>, RepositoryError> {
        let map = self.storage.tasks.lock().unwrap();
        Ok(map
            .values()
            .filter(|t| t.state() == TaskState::Running && t.agent_id() == Some(agent_id))
            .cloned()
            .collect())
    }

    async fn next_lease_epoch(&self) -> Result<u64, RepositoryError> {
        Ok(self.lease_epoch.fetch_add(1, Ordering::SeqCst) + 1)
    }
}

// ---------------------------------------------------------------------------
// FakeAgentRepository
// ---------------------------------------------------------------------------

pub struct FakeAgentRepository {
    storage: FakeStorage,
}

impl FakeAgentRepository {
    #[must_use]
    pub fn new(storage: FakeStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl AgentRepository for FakeAgentRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Agent>, RepositoryError> {
        let map = self.storage.agents.lock().unwrap();
        Ok(map.get(id).cloned())
    }

    async fn save(&self, agent: &Agent) -> Result<(), RepositoryError> {
        let mut map = self.storage.agents.lock().unwrap();
        map.insert(agent.id().to_string(), agent.clone());
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<(), RepositoryError> {
        let mut map = self.storage.agents.lock().unwrap();
        map.remove(id);
        Ok(())
    }

    async fn find_stale(
        &self,
        timeout: chrono::Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Agent>, RepositoryError> {
        let map = self.storage.agents.lock().unwrap();
        Ok(map
            .values()
            .filter(|a| !a.is_alive(now, timeout))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// FakePipelineStore
// ---------------------------------------------------------------------------

pub struct FakePipelineStore {
    storage: FakeStorage,
}

impl FakePipelineStore {
    #[must_use]
    pub fn new(storage: FakeStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl PipelineStore for FakePipelineStore {
    async fn submit_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        runs.insert(run.id().to_string(), run.clone());
        tasks.insert(task.id().to_string(), task.clone());
        Ok(())
    }

    async fn complete_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        tasks.insert(task.id().to_string(), task.clone());
        runs.insert(run.id().to_string(), run.clone());
        Ok(())
    }

    async fn fail_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        tasks.insert(task.id().to_string(), task.clone());
        runs.insert(run.id().to_string(), run.clone());
        Ok(())
    }

    async fn fail_and_retry(
        &self,
        failed_task: &Task,
        run: &Run,
        new_task: &Task,
    ) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        tasks.insert(failed_task.id().to_string(), failed_task.clone());
        runs.insert(run.id().to_string(), run.clone());
        tasks.insert(new_task.id().to_string(), new_task.clone());
        Ok(())
    }

    async fn cancel_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        tasks.insert(task.id().to_string(), task.clone());
        runs.insert(run.id().to_string(), run.clone());
        Ok(())
    }

    async fn cancel_pending_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        runs.insert(run.id().to_string(), run.clone());
        tasks.insert(task.id().to_string(), task.clone());
        Ok(())
    }

    async fn assign_task(
        &self,
        agent_id: &str,
        max_concurrent_tasks: u32,
        lease: Lease,
    ) -> Result<Option<(Task, Run)>, RepositoryError> {
        let mut tasks = self.storage.tasks.lock().unwrap();
        let mut runs = self.storage.runs.lock().unwrap();

        // 1. Check agent capacity
        let running_count = tasks
            .values()
            .filter(|t| t.state() == TaskState::Running && t.agent_id() == Some(agent_id))
            .count();
        if running_count >= max_concurrent_tasks as usize {
            return Ok(None);
        }

        // 2. Find first pending task by created_at
        let pending_id = tasks
            .values()
            .filter(|t| t.state() == TaskState::Pending)
            .min_by_key(|t| t.created_at())
            .map(|t| t.id().to_string());

        let Some(id) = pending_id else {
            return Ok(None);
        };

        // 3. Assign task (set state=Running, agent_id, lease)
        let task = tasks.get(&id).unwrap();
        let assigned_task = Task::from_row(
            task.id().to_string(),
            task.run_id().to_string(),
            task.attempt(),
            TaskState::Running,
            Some(agent_id.to_string()),
            Some(lease),
            task.created_at(),
            task.updated_at(),
        );
        let run_id = assigned_task.run_id().to_string();
        tasks.insert(id, assigned_task.clone());

        // 4. Find and update the run (set state=Running via run.start())
        let Some(run) = runs.get(&run_id) else {
            return Ok(None);
        };
        let mut run = run.clone();
        run.start().map_err(|e| RepositoryError(Box::new(e)))?;

        // 5. Save run
        runs.insert(run_id, run.clone());

        // 6. Return Some((task, run))
        Ok(Some((assigned_task, run)))
    }

    async fn timeout_and_retry(
        &self,
        timed_out_task: &Task,
        run: &Run,
        new_task: Option<&Task>,
    ) -> Result<(), RepositoryError> {
        let mut runs = self.storage.runs.lock().unwrap();
        let mut tasks = self.storage.tasks.lock().unwrap();
        tasks.insert(timed_out_task.id().to_string(), timed_out_task.clone());
        runs.insert(run.id().to_string(), run.clone());
        if let Some(new) = new_task {
            tasks.insert(new.id().to_string(), new.clone());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeEventBus
// ---------------------------------------------------------------------------

pub struct FakeEventBus {
    events: Arc<Mutex<Vec<DomainEvent>>>,
    sender: broadcast::Sender<DomainEvent>,
}

impl FakeEventBus {
    #[must_use]
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            sender,
        }
    }

    /// Return all published events for test assertions.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn published_events(&self) -> Vec<DomainEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl Default for FakeEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventBus for FakeEventBus {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventBusError> {
        self.events.lock().unwrap().push(event.clone());
        // Ignore send error (no active subscribers is OK)
        let _ = self.sender.send(event);
        Ok(())
    }

    async fn subscribe(&self, run_id: &str) -> Result<EventStream, EventBusError> {
        let receiver = self.sender.subscribe();
        let run_id = run_id.to_string();
        let stream = BroadcastStream::new(receiver).filter_map(move |item| {
            match item {
                Ok(event) => {
                    let matches = match &event {
                        DomainEvent::RunStateChanged { run_id: id, .. }
                        | DomainEvent::ProgressReported { run_id: id, .. }
                        | DomainEvent::RunCompleted { run_id: id, .. }
                        | DomainEvent::RunFailed { run_id: id, .. }
                        | DomainEvent::RunCancelled { run_id: id } => *id == run_id,
                    };
                    if matches {
                        Some(event)
                    } else {
                        None
                    }
                }
                Err(_) => None, // lagged receiver, skip
            }
        });
        Ok(Box::pin(stream))
    }

    async fn cleanup(&self, _run_id: &str) {
        // No-op for test fakes
    }
}

// ---------------------------------------------------------------------------
// FakeSecretResolver
// ---------------------------------------------------------------------------

pub struct FakeSecretResolver;

#[async_trait]
impl SecretResolver for FakeSecretResolver {
    async fn resolve(&self, yaml: &str) -> Result<String, SecretError> {
        Ok(yaml.to_string())
    }
}

// ---------------------------------------------------------------------------
// FakeClock
// ---------------------------------------------------------------------------

pub struct FakeClock {
    now: Mutex<DateTime<Utc>>,
}

impl FakeClock {
    #[must_use]
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Advance the fake clock by the given duration.
    ///
    /// # Panics
    /// Panics if the internal mutex is poisoned.
    pub fn advance(&self, duration: chrono::Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

// ---------------------------------------------------------------------------
// TestContext + factory
// ---------------------------------------------------------------------------

pub struct TestContext {
    pub ctx: AppContext,
    pub storage: FakeStorage,
    pub event_bus: Arc<FakeEventBus>,
    pub clock: Arc<FakeClock>,
}

/// Build an `AppContext` wired with all in-memory fakes.
///
/// The returned `TestContext` exposes the shared storage and typed fakes
/// so tests can inspect state and control time.
#[must_use]
pub fn fake_context() -> TestContext {
    let storage = FakeStorage::new();
    let event_bus = Arc::new(FakeEventBus::new());
    let clock = Arc::new(FakeClock::new(Utc::now()));

    let ctx = AppContext {
        runs: Arc::new(FakeRunRepository::new(storage.clone())),
        tasks: Arc::new(FakeTaskRepository::new(storage.clone())),
        agents: Arc::new(FakeAgentRepository::new(storage.clone())),
        store: Arc::new(FakePipelineStore::new(storage.clone())),
        event_bus: Arc::clone(&event_bus) as Arc<dyn EventBus>,
        secrets: Arc::new(FakeSecretResolver),
        clock: Arc::clone(&clock) as Arc<dyn Clock>,
        config: AppConfig {
            default_lease_duration: Duration::from_secs(300),
            lease_check_interval: Duration::from_secs(30),
            agent_reap_timeout: Duration::from_secs(60),
            agent_reap_interval: Duration::from_secs(30),
            default_max_retries: 0,
            registry: None,
        },
    };

    TestContext {
        ctx,
        storage,
        event_bus,
        clock,
    }
}
