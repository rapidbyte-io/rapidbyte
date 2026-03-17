use async_trait::async_trait;

use crate::domain::run::Run;
use crate::domain::task::Task;

use super::repository::RepositoryError;

#[async_trait]
pub trait PipelineStore: Send + Sync {
    async fn submit_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError>;
    async fn complete_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError>;
    async fn fail_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError>;
    async fn fail_and_retry(
        &self,
        failed_task: &Task,
        run: &Run,
        new_task: &Task,
    ) -> Result<(), RepositoryError>;
    async fn cancel_run(&self, task: &Task, run: &Run) -> Result<(), RepositoryError>;
    async fn cancel_pending_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError>;
    async fn timeout_and_retry(
        &self,
        timed_out_task: &Task,
        run: &Run,
        new_task: Option<&Task>,
    ) -> Result<(), RepositoryError>;
}
