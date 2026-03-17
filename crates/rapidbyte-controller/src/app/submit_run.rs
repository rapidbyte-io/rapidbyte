//! Submit-run use-case scaffold.

use std::sync::Arc;

use crate::domain::run::{Run, RunState};
use crate::ports::clock::Clock;
use crate::ports::event_bus::EventBus;
use crate::ports::id_generator::IdGenerator;
use crate::ports::repositories::RunRepository;
use crate::ports::unit_of_work::UnitOfWork;

#[derive(Debug, Clone)]
pub struct SubmitRunCommand {
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitRunOutput {
    pub run_id: String,
}

pub struct SubmitRunUseCase<R, E, C, I, U>
where
    R: RunRepository,
    E: EventBus,
    C: Clock,
    I: IdGenerator,
    U: UnitOfWork,
{
    run_repository: Arc<R>,
    _event_bus: Arc<E>,
    _clock: Arc<C>,
    id_generator: Arc<I>,
    unit_of_work: Arc<U>,
}

impl<R, E, C, I, U> SubmitRunUseCase<R, E, C, I, U>
where
    R: RunRepository,
    E: EventBus,
    C: Clock,
    I: IdGenerator,
    U: UnitOfWork,
{
    #[must_use]
    pub fn new(
        run_repository: Arc<R>,
        event_bus: Arc<E>,
        clock: Arc<C>,
        id_generator: Arc<I>,
        unit_of_work: Arc<U>,
    ) -> Self {
        Self {
            run_repository,
            _event_bus: event_bus,
            _clock: clock,
            id_generator,
            unit_of_work,
        }
    }

    /// # Errors
    ///
    /// Returns an error when the transaction cannot persist the run.
    pub async fn execute(&self, command: SubmitRunCommand) -> anyhow::Result<SubmitRunOutput> {
        let run_repository = Arc::clone(&self.run_repository);
        let id_generator = Arc::clone(&self.id_generator);
        let idempotency_key = command.idempotency_key;

        self.unit_of_work
            .in_transaction(move || async move {
                let mut run = Run::new(id_generator.new_run_id());
                run.transition(RunState::Queued)?;

                let stored = run_repository
                    .create_or_get_by_idempotency(run, idempotency_key.as_deref())
                    .await?;
                Ok(SubmitRunOutput {
                    run_id: stored.run.id.as_str().to_owned(),
                })
            })
            .await
    }
}
