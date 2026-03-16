//! Secret resolution at dispatch time and error handling.

use tonic::Status;

use crate::proto::rapidbyte::v1::{poll_task_response, NoTask, PollTaskResponse};
use crate::run_state::{RunError, RunState as InternalRunState};
use crate::scheduler::TerminalTaskOutcome;

use super::ERROR_CODE_SECRET_RESOLUTION;

/// Resolve secrets in the pipeline YAML and build the response, or handle
/// transient/permanent secret-resolution errors.
pub(crate) async fn dispatch_task(
    handler: &super::AgentHandler,
    assignment: crate::scheduler::TaskAssignment,
    agent_id: &str,
) -> Result<PollTaskResponse, Status> {
    let task_id = assignment.task_id.clone();
    let run_id = assignment.run_id.clone();
    let lease_epoch = assignment.lease_epoch;
    let attempt = assignment.attempt;
    match super::resolve_and_build_response(assignment, &handler.state.secrets).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            if e.code() == tonic::Code::Unavailable {
                requeue_on_transient_failure(handler, &task_id, &run_id, lease_epoch, &e).await
            } else {
                fail_on_permanent_error(
                    handler,
                    &task_id,
                    &run_id,
                    agent_id,
                    lease_epoch,
                    attempt,
                    &e,
                )
                .await
            }
        }
    }
}

async fn requeue_on_transient_failure(
    handler: &super::AgentHandler,
    task_id: &str,
    run_id: &str,
    lease_epoch: u64,
    error: &Status,
) -> Result<PollTaskResponse, Status> {
    let state = &handler.state;

    tracing::warn!(
        task_id,
        "transient secret resolution failure, requeueing: {error}"
    );

    // Snapshot the pre-modification state so we can rollback on persist failure.
    let previous_task = state.tasks.read().await.get(task_id).cloned();
    let previous_run = state.runs.read().await.get_run(run_id).cloned();

    // Release the task assignment. If the lease expired during secret
    // resolution, the background sweep owns cleanup — bail and let it.
    let released_task = {
        let mut tasks = state.tasks.write().await;
        if let Err(release_err) = tasks.release_assignment(task_id, lease_epoch) {
            tracing::warn!(
                task_id,
                "cannot release task (lease likely expired): {release_err}"
            );
            return Ok(PollTaskResponse {
                result: Some(poll_task_response::Result::NoTask(NoTask {})),
            });
        }
        tasks.get(task_id).cloned()
    };

    let released_run = {
        let mut runs = state.runs.write().await;
        if let Some(record) = runs.get_run_mut(run_id) {
            record.state = InternalRunState::Pending;
            record.current_task = None;
        }
        runs.get_run(run_id).cloned()
    };

    // Persist rollback so restarts see consistent state.
    if let (Some(task), Some(run)) = (&released_task, &released_run) {
        if let Err(persist_err) = state.persist_assignment_records(run, task).await {
            tracing::error!(
                task_id,
                "failed to persist transient rollback, restoring previous state: {persist_err}"
            );
            // Restore in-memory state to match what is durably persisted.
            if let (Some(prev_task), Some(prev_run)) = (previous_task, previous_run) {
                super::poll::rollback_assignment(state, prev_run, prev_task).await;
            }
            return Err(Status::internal(format!(
                "failed to persist secret-resolution rollback: {persist_err}"
            )));
        }
    }

    // Wake long-poll waiters so the requeued task is picked up promptly.
    state.task_notify.notify_waiters();

    Ok(PollTaskResponse {
        result: Some(poll_task_response::Result::NoTask(NoTask {})),
    })
}

async fn fail_on_permanent_error(
    handler: &super::AgentHandler,
    task_id: &str,
    run_id: &str,
    agent_id: &str,
    lease_epoch: u64,
    attempt: u32,
    error: &Status,
) -> Result<PollTaskResponse, Status> {
    let state = &handler.state;

    tracing::error!(
        task_id,
        "secret resolution failed during dispatch, failing task: {error}"
    );

    // Snapshot the pre-modification state so we can rollback on persist failure.
    let previous_task = state.tasks.read().await.get(task_id).cloned();
    let previous_run = state.runs.read().await.get_run(run_id).cloned();

    // Complete the task as Failed. If the lease expired during secret
    // resolution, `complete` returns None — the background sweep owns
    // cleanup, so bail and let it handle the terminal transition.
    let mut tasks = state.tasks.write().await;
    match tasks.complete(task_id, agent_id, lease_epoch, TerminalTaskOutcome::Failed) {
        Ok(None) | Err(_) => {
            tracing::warn!(
                task_id,
                "cannot complete task (lease likely expired), deferring to sweep"
            );
            return Ok(PollTaskResponse {
                result: Some(poll_task_response::Result::NoTask(NoTask {})),
            });
        }
        Ok(Some(_)) => {}
    }
    let failed_task = tasks.get(task_id).cloned();
    drop(tasks);

    let mut runs = state.runs.write().await;
    let _ = runs.transition(run_id, InternalRunState::Failed);
    if let Some(record) = runs.get_run_mut(run_id) {
        record.error = Some(RunError {
            code: ERROR_CODE_SECRET_RESOLUTION.into(),
            message: format!("{ERROR_CODE_SECRET_RESOLUTION}: {}", error.message()),
            retryable: false,
            safe_to_retry: false,
            commit_state: rapidbyte_types::error::CommitState::BeforeCommit
                .as_str()
                .into(),
        });
    }
    let failed_run = runs.get_run(run_id).cloned();
    drop(runs);

    if let (Some(task), Some(run)) = (&failed_task, &failed_run) {
        if let Err(persist_err) = state.persist_assignment_records(run, task).await {
            tracing::error!(
                task_id,
                "failed to persist secret-resolution failure, restoring previous state: {persist_err}"
            );
            if let (Some(prev_task), Some(prev_run)) = (previous_task, previous_run) {
                super::poll::rollback_assignment(state, prev_run, prev_task).await;
            }
            return Err(Status::internal(format!(
                "failed to persist secret-resolution failure: {persist_err}"
            )));
        }
    }

    crate::terminal::finalize_terminal(
        state,
        run_id,
        crate::terminal::TerminalOutcome::Failed {
            error: Some(RunError {
                code: ERROR_CODE_SECRET_RESOLUTION.into(),
                message: format!("{ERROR_CODE_SECRET_RESOLUTION}: {}", error.message()),
                retryable: false,
                safe_to_retry: false,
                commit_state: rapidbyte_types::error::CommitState::BeforeCommit
                    .as_str()
                    .into(),
            }),
            attempt,
        },
    )
    .await;

    Ok(PollTaskResponse {
        result: Some(poll_task_response::Result::NoTask(NoTask {})),
    })
}
