use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::event::DomainEvent;
use crate::domain::run::{Run, RunError, RunState};
use crate::domain::task::Task;

/// Handle a timed-out task: cancel if requested, retry if possible, otherwise fail.
///
/// # Errors
///
/// Returns a repository or event-bus error on failure.
pub async fn handle_task_timeout(
    ctx: &AppContext,
    task: &Task,
    run: &mut Run,
    error_code: &str,
    error_message: &str,
) -> Result<(), AppError> {
    let now = ctx.clock.now();

    if run.is_cancel_requested() {
        run.cancel()?;
        ctx.store.timeout_and_retry(task, run, None).await?;
        ctx.event_bus
            .publish(DomainEvent::RunCancelled {
                run_id: run.id().to_string(),
            })
            .await?;
    } else if run.can_retry_after_timeout() {
        let new_attempt = run.retry()?;
        let new_task_id = uuid::Uuid::new_v4().to_string();
        let new_task = Task::new(new_task_id, run.id().to_string(), new_attempt, now);
        ctx.store
            .timeout_and_retry(task, run, Some(&new_task))
            .await?;
        ctx.event_bus
            .publish(DomainEvent::RunStateChanged {
                run_id: run.id().to_string(),
                state: RunState::Pending,
                attempt: new_attempt,
            })
            .await?;
    } else {
        let error = RunError {
            code: error_code.to_string(),
            message: error_message.to_string(),
        };
        run.fail(error.clone())?;
        ctx.store.timeout_and_retry(task, run, None).await?;
        ctx.event_bus
            .publish(DomainEvent::RunFailed {
                run_id: run.id().to_string(),
                error,
            })
            .await?;
    }

    Ok(())
}
