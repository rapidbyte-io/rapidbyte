//! Retry-run use-case scaffold.

use std::sync::Arc;

use thiserror::Error;

use crate::domain::run::{RunId, RunState};
use crate::ports::repositories::RunRepository;
use crate::ports::unit_of_work::UnitOfWork;

#[derive(Debug, Clone)]
pub struct RetryRunCommand {
    pub run_id: RunId,
}

#[derive(Debug, Error)]
pub enum RetryRunError {
    #[error("run not found: {0}")]
    NotFound(String),
    #[error("run is not in retryable terminal state: {0}")]
    NotRetryable(String),
    #[error(transparent)]
    Repository(#[from] anyhow::Error),
}

pub struct RetryRunUseCase<R, U>
where
    R: RunRepository,
    U: UnitOfWork,
{
    run_repository: Arc<R>,
    unit_of_work: Arc<U>,
}

impl<R, U> RetryRunUseCase<R, U>
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
    /// Returns an error when the run is not retryable or persistence fails.
    pub async fn execute(&self, command: RetryRunCommand) -> anyhow::Result<()> {
        let run_repository = Arc::clone(&self.run_repository);
        let run_id = command.run_id;

        self.unit_of_work
            .in_transaction(move || async move {
                let retry_result: Result<(), RetryRunError> = async {
                    let run = run_repository
                        .get(&run_id)
                        .await?
                        .ok_or_else(|| RetryRunError::NotFound(run_id.as_str().to_owned()))?;

                    let is_retryable_terminal =
                        run.retryable && run.run.state == RunState::FailedRetryable;
                    if !is_retryable_terminal {
                        return Err(RetryRunError::NotRetryable(run_id.as_str().to_owned()));
                    }

                    run_repository.queue_retry(&run_id).await?;
                    Ok(())
                }
                .await;

                retry_result.map_err(anyhow::Error::from)
            })
            .await
    }
}
