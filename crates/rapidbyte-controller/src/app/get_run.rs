//! Get-run use-case.

use std::sync::Arc;

use crate::domain::run::{RunId, RunState};
use crate::ports::repositories::RunRepository;

#[derive(Debug, Clone)]
pub struct GetRunCommand {
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetRunOutput {
    pub run_id: String,
    pub state: RunState,
}

pub struct GetRunUseCase<R>
where
    R: RunRepository,
{
    run_repository: Arc<R>,
}

impl<R> GetRunUseCase<R>
where
    R: RunRepository,
{
    #[must_use]
    pub fn new(run_repository: Arc<R>) -> Self {
        Self { run_repository }
    }

    /// # Errors
    ///
    /// Returns an error when run lookup fails or run does not exist.
    pub async fn execute(&self, command: GetRunCommand) -> anyhow::Result<GetRunOutput> {
        let stored = self
            .run_repository
            .get(&command.run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run not found: {}", command.run_id.as_str()))?;

        Ok(GetRunOutput {
            run_id: stored.run.id.as_str().to_owned(),
            state: stored.run.state,
        })
    }
}
