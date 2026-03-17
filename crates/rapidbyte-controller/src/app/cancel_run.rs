//! Cancel-run use-case.

use std::sync::Arc;

use crate::domain::run::RunId;
use crate::ports::repositories::RunRepository;
use crate::ports::unit_of_work::UnitOfWork;

#[derive(Debug, Clone)]
pub struct CancelRunCommand {
    pub run_id: RunId,
}

pub struct CancelRunUseCase<R, U>
where
    R: RunRepository,
    U: UnitOfWork,
{
    run_repository: Arc<R>,
    unit_of_work: Arc<U>,
}

impl<R, U> CancelRunUseCase<R, U>
where
    R: RunRepository,
    U: UnitOfWork,
{
    #[must_use]
    pub fn new(run_repository: Arc<R>, unit_of_work: Arc<U>) -> Self {
        Self {
            run_repository,
            unit_of_work,
        }
    }

    /// # Errors
    ///
    /// Returns an error when cancellation cannot be persisted.
    pub async fn execute(&self, command: CancelRunCommand) -> anyhow::Result<()> {
        let run_repository = Arc::clone(&self.run_repository);
        let run_id = command.run_id;
        self.unit_of_work
            .in_transaction(move || async move { run_repository.mark_cancelled(&run_id).await })
            .await
    }
}
