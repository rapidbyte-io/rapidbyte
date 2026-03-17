use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rapidbyte_controller::app::cancel_run::{CancelRunCommand, CancelRunUseCase};
use rapidbyte_controller::app::get_run::{GetRunCommand, GetRunUseCase};
use rapidbyte_controller::app::list_runs::{ListRunsCommand, ListRunsUseCase};
use rapidbyte_controller::app::retry_run::{RetryRunCommand, RetryRunUseCase};
use rapidbyte_controller::app::submit_run::{SubmitRunCommand, SubmitRunUseCase};
use rapidbyte_controller::domain::run::{Run, RunId, RunState};
use rapidbyte_controller::ports::clock::Clock;
use rapidbyte_controller::ports::event_bus::EventBus;
use rapidbyte_controller::ports::id_generator::IdGenerator;
use rapidbyte_controller::ports::repositories::{RunRepository, StoredRun};
use rapidbyte_controller::ports::unit_of_work::UnitOfWork;

#[derive(Default)]
struct FakeRunRepository {
    runs: Mutex<HashMap<String, StoredRun>>,
    idempotency: Mutex<HashMap<String, String>>,
}

#[async_trait::async_trait]
impl RunRepository for FakeRunRepository {
    async fn create_or_get_by_idempotency(
        &self,
        run: Run,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<StoredRun> {
        if let Some(key) = idempotency_key {
            if let Some(run_id) = self.idempotency.lock().expect("lock").get(key) {
                return self
                    .runs
                    .lock()
                    .expect("lock")
                    .get(run_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing run"));
            }
        }

        let stored = StoredRun {
            run,
            retryable: false,
        };
        let run_id = stored.run.id.as_str().to_owned();
        self.runs
            .lock()
            .expect("lock")
            .insert(run_id.clone(), stored.clone());
        if let Some(key) = idempotency_key {
            self.idempotency
                .lock()
                .expect("lock")
                .insert(key.to_owned(), run_id);
        }
        Ok(stored)
    }

    async fn get(&self, run_id: &RunId) -> anyhow::Result<Option<StoredRun>> {
        Ok(self
            .runs
            .lock()
            .expect("lock")
            .get(run_id.as_str())
            .cloned())
    }

    async fn queue_retry(&self, run_id: &RunId) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().expect("lock");
        let record = runs
            .get_mut(run_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("run not found"))?;
        record.run.transition(RunState::Queued)?;
        Ok(())
    }

    async fn list(&self, limit: usize, state: Option<RunState>) -> anyhow::Result<Vec<StoredRun>> {
        let mut values = self
            .runs
            .lock()
            .expect("lock")
            .values()
            .cloned()
            .filter(|stored| state.is_none_or(|filter| stored.run.state == filter))
            .collect::<Vec<_>>();
        values.truncate(limit);
        Ok(values)
    }

    async fn mark_cancelled(&self, run_id: &RunId) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().expect("lock");
        let record = runs
            .get_mut(run_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("run not found"))?;
        record.run.transition(RunState::CancelRequested)?;
        record.run.transition(RunState::Cancelled)?;
        Ok(())
    }
}

struct FakeEventBus;

impl EventBus for FakeEventBus {}

struct FakeClock;

impl Clock for FakeClock {}

struct FakeIdGenerator;

impl IdGenerator for FakeIdGenerator {
    fn new_run_id(&self) -> RunId {
        RunId::new("run-1")
    }
}

struct FakeUnitOfWork;

#[async_trait::async_trait]
impl UnitOfWork for FakeUnitOfWork {
    async fn in_transaction<T, F, Fut>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        T: Send,
    {
        f().await
    }
}

#[tokio::test]
async fn submit_is_idempotent_inside_single_transaction_boundary() {
    let repo = Arc::new(FakeRunRepository::default());
    let use_case = SubmitRunUseCase::new(
        repo.clone(),
        Arc::new(FakeEventBus),
        Arc::new(FakeClock),
        Arc::new(FakeIdGenerator),
        Arc::new(FakeUnitOfWork),
    );

    let first = use_case
        .execute(SubmitRunCommand {
            idempotency_key: Some("key-1".to_owned()),
        })
        .await
        .expect("first submit should pass");
    let second = use_case
        .execute(SubmitRunCommand {
            idempotency_key: Some("key-1".to_owned()),
        })
        .await
        .expect("second submit should pass");
    assert_eq!(first.run_id, second.run_id);
}

#[tokio::test]
async fn retry_requires_terminal_retryable_failure() {
    let repo = Arc::new(FakeRunRepository::default());
    let retry_use_case = RetryRunUseCase::new(repo.clone(), Arc::new(FakeUnitOfWork));

    let result = retry_use_case
        .execute(RetryRunCommand {
            run_id: RunId::new("run-1"),
        })
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn get_returns_submitted_run() {
    let repo = Arc::new(FakeRunRepository::default());
    let submit = SubmitRunUseCase::new(
        repo.clone(),
        Arc::new(FakeEventBus),
        Arc::new(FakeClock),
        Arc::new(FakeIdGenerator),
        Arc::new(FakeUnitOfWork),
    );
    let get = GetRunUseCase::new(repo.clone());

    let submitted = submit
        .execute(SubmitRunCommand {
            idempotency_key: Some("key-get".to_owned()),
        })
        .await
        .expect("submit should pass");

    let loaded = get
        .execute(GetRunCommand {
            run_id: RunId::new(submitted.run_id.clone()),
        })
        .await
        .expect("get should pass");

    assert_eq!(loaded.run_id, submitted.run_id);
}

#[tokio::test]
async fn list_returns_submitted_run() {
    let repo = Arc::new(FakeRunRepository::default());
    let submit = SubmitRunUseCase::new(
        repo.clone(),
        Arc::new(FakeEventBus),
        Arc::new(FakeClock),
        Arc::new(FakeIdGenerator),
        Arc::new(FakeUnitOfWork),
    );
    let list = ListRunsUseCase::new(repo.clone());

    submit
        .execute(SubmitRunCommand {
            idempotency_key: Some("key-list".to_owned()),
        })
        .await
        .expect("submit should pass");

    let runs = list
        .execute(ListRunsCommand {
            limit: Some(20),
            state: None,
        })
        .await
        .expect("list should pass");
    assert_eq!(runs.runs.len(), 1);
}

#[tokio::test]
async fn cancel_marks_run_cancelled() {
    let repo = Arc::new(FakeRunRepository::default());
    let submit = SubmitRunUseCase::new(
        repo.clone(),
        Arc::new(FakeEventBus),
        Arc::new(FakeClock),
        Arc::new(FakeIdGenerator),
        Arc::new(FakeUnitOfWork),
    );
    let cancel = CancelRunUseCase::new(repo.clone(), Arc::new(FakeUnitOfWork));
    let get = GetRunUseCase::new(repo.clone());

    let submitted = submit
        .execute(SubmitRunCommand {
            idempotency_key: Some("key-cancel".to_owned()),
        })
        .await
        .expect("submit should pass");

    cancel
        .execute(CancelRunCommand {
            run_id: RunId::new(submitted.run_id.clone()),
        })
        .await
        .expect("cancel should pass");

    let loaded = get
        .execute(GetRunCommand {
            run_id: RunId::new(submitted.run_id),
        })
        .await
        .expect("get should pass");
    assert_eq!(loaded.state, RunState::Cancelled);
}
