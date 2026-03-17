//! List-runs use-case.

use std::sync::Arc;

use crate::domain::run::RunState;
use crate::ports::repositories::RunRepository;

#[derive(Debug, Clone)]
pub struct ListRunsCommand {
    pub limit: Option<usize>,
    pub state: Option<RunState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunListItem {
    pub run_id: String,
    pub state: RunState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRunsOutput {
    pub runs: Vec<RunListItem>,
}

pub struct ListRunsUseCase<R>
where
    R: RunRepository,
{
    run_repository: Arc<R>,
}

impl<R> ListRunsUseCase<R>
where
    R: RunRepository,
{
    #[must_use]
    pub fn new(run_repository: Arc<R>) -> Self {
        Self { run_repository }
    }

    /// # Errors
    ///
    /// Returns an error when repository listing fails.
    pub async fn execute(&self, command: ListRunsCommand) -> anyhow::Result<ListRunsOutput> {
        let limit = command.limit.unwrap_or(20);
        let runs = self
            .run_repository
            .list(limit, command.state)
            .await?
            .into_iter()
            .map(|stored| RunListItem {
                run_id: stored.run.id.as_str().to_owned(),
                state: stored.run.state,
            })
            .collect();
        Ok(ListRunsOutput { runs })
    }
}
