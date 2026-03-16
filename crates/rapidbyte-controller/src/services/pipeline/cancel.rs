//! `cancel_run` handler and cancel state machine.

use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{CancelRunRequest, CancelRunResponse};
use crate::run_state::RunState;

pub(crate) async fn handle_cancel(
    handler: &super::PipelineHandler,
    req: CancelRunRequest,
) -> Result<Response<CancelRunResponse>, Status> {
    let run_id = req.run_id;

    // Read the current state and transition under a short-lived lock.
    // IMPORTANT: drop runs lock before acquiring tasks lock to maintain
    // consistent lock ordering (tasks -> runs) with poll_task/lease-expiry.
    let current_state = {
        let runs = handler.state.runs.read().await;
        let record = runs
            .get_run(&run_id)
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
        record.state
    };

    Ok(Response::new(
        cancel_run_for_state(handler, &run_id, current_state).await?,
    ))
}

async fn cancel_run_for_state(
    handler: &super::PipelineHandler,
    run_id: &str,
    snapshot_state: RunState,
) -> Result<CancelRunResponse, Status> {
    match snapshot_state {
        RunState::Pending => cancel_queued_run(handler, run_id, snapshot_state).await,
        RunState::Assigned | RunState::Reconciling | RunState::Running => {
            cancel_inflight_run(handler, run_id, snapshot_state).await
        }
        RunState::Cancelling => Ok(CancelRunResponse {
            accepted: true,
            message: "Run is already being cancelled".into(),
        }),
        _ if snapshot_state.is_terminal() => Ok(CancelRunResponse {
            accepted: false,
            message: format!("Run is already in terminal state: {snapshot_state:?}"),
        }),
        _ => Ok(CancelRunResponse {
            accepted: false,
            message: format!("Cannot cancel run in state: {snapshot_state:?}"),
        }),
    }
}

async fn cancel_queued_run(
    handler: &super::PipelineHandler,
    run_id: &str,
    snapshot_state: RunState,
) -> Result<CancelRunResponse, Status> {
    let snapshot_run = {
        handler
            .state
            .runs
            .read()
            .await
            .get_run(run_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
    };
    if let Some(response) =
        transition_or_retry(handler, run_id, snapshot_state, RunState::Cancelled).await?
    {
        return Ok(response);
    }
    let (snapshot_task, cancelled_task) = cancel_latest_task(handler, run_id).await;
    if let Some(cancelled_task) = cancelled_task.as_ref() {
        if let Err(error) = handler.state.persist_task_record(cancelled_task).await {
            rollback_queued_cancel(handler, snapshot_run, snapshot_task).await;
            return Err(Status::internal(error.to_string()));
        }
    }
    let cancelled_run = handler
        .state
        .runs
        .read()
        .await
        .get_run(run_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
    if let Err(error) = handler.state.persist_run_record(&cancelled_run).await {
        if let Some(snapshot_task_record) = snapshot_task.clone() {
            let rollback_task_id = snapshot_task_record.task_id.clone();
            let rollback_error = handler
                .state
                .persist_task_record(&snapshot_task_record)
                .await
                .err();
            rollback_queued_cancel(handler, snapshot_run, Some(snapshot_task_record)).await;
            return Err(Status::internal(match rollback_error {
                Some(rollback_error) => {
                    format!(
                        "{error}; durable rollback for task {rollback_task_id} also failed: {rollback_error}"
                    )
                }
                None => error.to_string(),
            }));
        }
        rollback_queued_cancel(handler, snapshot_run, None).await;
        return Err(Status::internal(error.to_string()));
    }
    crate::terminal::finalize_terminal(
        &handler.state,
        run_id,
        crate::terminal::TerminalOutcome::Cancelled,
    )
    .await;
    Ok(CancelRunResponse {
        accepted: true,
        message: "Queued run cancelled".into(),
    })
}

/// Cancel an in-flight run (Assigned, Running, or Reconciling).
///
/// The only difference between these states is the human-readable response
/// message, which is derived from `snapshot_state`.
async fn cancel_inflight_run(
    handler: &super::PipelineHandler,
    run_id: &str,
    snapshot_state: RunState,
) -> Result<CancelRunResponse, Status> {
    if let Some(response) =
        transition_or_retry(handler, run_id, snapshot_state, RunState::Cancelling).await?
    {
        return Ok(response);
    }
    handler
        .state
        .persist_run(run_id)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

    let label = match snapshot_state {
        RunState::Assigned => "Assigned",
        _ => "Running",
    };
    Ok(CancelRunResponse {
        accepted: true,
        message: format!("{label} — cancel will be delivered via heartbeat"),
    })
}

async fn transition_or_retry(
    handler: &super::PipelineHandler,
    run_id: &str,
    snapshot_state: RunState,
    target_state: RunState,
) -> Result<Option<CancelRunResponse>, Status> {
    let actual_state = {
        let mut runs = handler.state.runs.write().await;
        match runs.transition(run_id, target_state) {
            Ok(()) => return Ok(None),
            Err(err) => {
                let actual_state = runs
                    .get_run(run_id)
                    .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
                    .state;
                if actual_state == snapshot_state {
                    return Err(Status::internal(err.to_string()));
                }
                Some(actual_state)
            }
        }
    };

    let Some(actual_state) = actual_state else {
        unreachable!("transition_or_retry only falls through with a refreshed run state");
    };
    Ok(Some(
        Box::pin(cancel_run_for_state(handler, run_id, actual_state)).await?,
    ))
}

async fn cancel_latest_task(
    handler: &super::PipelineHandler,
    run_id: &str,
) -> (
    Option<crate::scheduler::TaskRecord>,
    Option<crate::scheduler::TaskRecord>,
) {
    let mut tasks = handler.state.tasks.write().await;
    if let Some(task) = tasks.find_by_run_id(run_id) {
        let snapshot_task = task.clone();
        let task_id = snapshot_task.task_id.clone();
        let _ = tasks.cancel(&task_id);
        let cancelled_task = tasks.get(&task_id).cloned();
        return (Some(snapshot_task), cancelled_task);
    }
    (None, None)
}

async fn rollback_queued_cancel(
    handler: &super::PipelineHandler,
    snapshot_run: crate::run_state::RunRecord,
    snapshot_task: Option<crate::scheduler::TaskRecord>,
) {
    if let Some(snapshot_task) = snapshot_task {
        let mut tasks = handler.state.tasks.write().await;
        tasks.restore_task(snapshot_task);
    }
    let mut runs = handler.state.runs.write().await;
    runs.restore_run(snapshot_run);
}
