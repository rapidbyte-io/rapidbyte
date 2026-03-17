use std::sync::Arc;

use rapidbyte_controller::app::submit_run::SubmitRunUseCase;
use rapidbyte_controller::domain::run::{Run, RunId};
use rapidbyte_controller::ports::clock::Clock;
use rapidbyte_controller::ports::event_bus::EventBus;
use rapidbyte_controller::ports::id_generator::IdGenerator;
use rapidbyte_controller::ports::repositories::{RunRepository, StoredRun};
use rapidbyte_controller::ports::unit_of_work::UnitOfWork;

struct FakeRunRepository;

#[async_trait::async_trait]
impl RunRepository for FakeRunRepository {
    async fn create_or_get_by_idempotency(
        &self,
        run: Run,
        _idempotency_key: Option<&str>,
    ) -> anyhow::Result<StoredRun> {
        Ok(StoredRun {
            run,
            retryable: false,
        })
    }

    async fn get(&self, _run_id: &RunId) -> anyhow::Result<Option<StoredRun>> {
        Ok(None)
    }

    async fn queue_retry(&self, _run_id: &RunId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list(&self, _limit: usize) -> anyhow::Result<Vec<StoredRun>> {
        Ok(Vec::new())
    }

    async fn mark_cancelled(&self, _run_id: &RunId) -> anyhow::Result<()> {
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

#[test]
fn submit_use_case_is_constructible_from_ports() {
    let _use_case = SubmitRunUseCase::new(
        Arc::new(FakeRunRepository),
        Arc::new(FakeEventBus),
        Arc::new(FakeClock),
        Arc::new(FakeIdGenerator),
        Arc::new(FakeUnitOfWork),
    );
}
