use async_trait::async_trait;

use crate::domain::lease::Lease;
use crate::domain::run::Run;
use crate::domain::task::Task;

use super::repository::RepositoryError;

#[async_trait]
pub trait PipelineStore: Send + Sync {
    async fn submit_run(&self, run: &Run, task: &Task) -> Result<(), RepositoryError>;
    /// Atomically assign a pending task to an agent and transition the run to Running.
    /// Returns the assigned task and updated run if one was available and the agent has
    /// capacity, `None` otherwise. Combines capacity check, task assignment, and run
    /// state transition in a single transaction to prevent races.
    async fn assign_task(
        &self,
        agent_id: &str,
        max_concurrent_tasks: u32,
        lease: Lease,
    ) -> Result<Option<(Task, Run)>, RepositoryError>;
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
