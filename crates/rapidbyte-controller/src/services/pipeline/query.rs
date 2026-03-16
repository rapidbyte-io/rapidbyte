//! `get_run`, `list_runs`, and `watch_run` handlers.

use std::time::UNIX_EPOCH;

use prost_types::Timestamp;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{
    run_event, GetRunRequest, GetRunResponse, ListRunsRequest, ListRunsResponse, PreviewAccess,
    PreviewState, RunCancelled, RunCompleted, RunEvent, RunFailed, RunState, RunStatus, RunSummary,
    StreamPreview, TaskError, TaskRef, WatchRunRequest,
};
use crate::run_state::{
    RunState as InternalRunState, ERROR_CODE_LEASE_EXPIRED, ERROR_CODE_RECOVERY_TIMEOUT,
};

use super::convert::to_proto_state;

pub(crate) async fn handle_get_run(
    handler: &super::PipelineHandler,
    req: GetRunRequest,
) -> Result<Response<GetRunResponse>, Status> {
    let run_id = req.run_id;

    // Extract run data and drop the runs lock before touching previews
    let (
        run_id_out,
        state,
        pipeline_name,
        submitted_at,
        started_at,
        completed_at,
        current_task,
        last_error,
    ) = {
        let runs = handler.state.runs.read().await;
        let record = runs
            .get_run(&run_id)
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
        (
            record.run_id.clone(),
            to_proto_state(record.state),
            record.pipeline_name.clone(),
            Some(to_timestamp(record.created_at)),
            record.started_at.map(to_timestamp),
            record.completed_at.map(to_timestamp),
            record.current_task.as_ref().map(|task| TaskRef {
                task_id: task.task_id.clone(),
                agent_id: task.agent_id.clone(),
                attempt: task.attempt,
                lease_epoch: task.lease_epoch,
                assigned_at: Some(to_timestamp(task.assigned_at)),
            }),
            terminal_error_for_run(record),
        )
    };

    // Preview lookup with runs lock already released
    let preview = {
        let previews = handler.state.previews.read().await;
        previews.get(&run_id).map(|p| PreviewAccess {
            state: PreviewState::Ready.into(),
            flight_endpoint: p.flight_endpoint.clone(),
            ticket: p.ticket.to_vec(),
            expires_at: None,
            streams: p
                .streams
                .iter()
                .map(|stream| StreamPreview {
                    stream: stream.stream.clone(),
                    rows: stream.rows,
                    ticket: stream.ticket.to_vec(),
                })
                .collect(),
        })
    };

    Ok(Response::new(GetRunResponse {
        run_id: run_id_out,
        state,
        pipeline_name,
        submitted_at,
        started_at,
        completed_at,
        current_task,
        preview,
        last_error,
    }))
}

pub(crate) async fn handle_list_runs(
    handler: &super::PipelineHandler,
    req: ListRunsRequest,
) -> Result<Response<ListRunsResponse>, Status> {
    let state_filter = match req.filter_state {
        Some(state) => Some(
            from_proto_states(state)
                .ok_or_else(|| Status::invalid_argument("Unknown filter_state"))?,
        ),
        None => None,
    };

    let runs = handler.state.runs.read().await;
    let mut records = runs.list_runs(state_filter.as_deref());
    records.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    // Clamp limit to a positive value; default to 20 if unset or zero
    let limit = if req.limit > 0 {
        req.limit.unsigned_abs() as usize
    } else {
        20
    };
    records.truncate(limit);

    let summaries = records
        .into_iter()
        .map(|r| RunSummary {
            run_id: r.run_id.clone(),
            pipeline_name: r.pipeline_name.clone(),
            state: to_proto_state(r.state),
            submitted_at: Some(to_timestamp(r.created_at)),
        })
        .collect();

    Ok(Response::new(ListRunsResponse { runs: summaries }))
}

pub(crate) async fn handle_watch_run(
    handler: &super::PipelineHandler,
    req: WatchRunRequest,
) -> Result<Response<super::WatchRunStream>, Status> {
    let run_id = req.run_id;
    stream_events_from(handler, &run_id).await
}

pub(crate) async fn stream_events_from(
    handler: &super::PipelineHandler,
    run_id: &str,
) -> Result<Response<super::WatchRunStream>, Status> {
    let rx = {
        let mut watchers = handler.state.watchers.write().await;
        watchers.subscribe(run_id)
    };

    let terminal_event = match terminal_event_for_existing_run(handler, run_id).await {
        Ok(event) => event,
        Err(err) => {
            handler.state.watchers.write().await.remove(run_id);
            return Err(err);
        }
    };

    if let Some(event) = terminal_event {
        handler.state.watchers.write().await.remove(run_id);
        let stream: super::WatchRunStream = Box::pin(tokio_stream::once(Ok(event)));
        return Ok(Response::new(stream));
    }

    let live_stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(event) => Some(Ok(event)),
        Err(_) => None, // Lag or closed — skip
    });

    let status_event = match status_event_for_existing_run(handler, run_id).await {
        Ok(event) => event,
        Err(err) => {
            handler.state.watchers.write().await.remove(run_id);
            return Err(err);
        }
    };

    let stream: super::WatchRunStream = match status_event {
        Some(event) => Box::pin(tokio_stream::once(Ok(event)).chain(live_stream)),
        None => Box::pin(live_stream),
    };

    Ok(Response::new(stream))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn terminal_event_for_existing_run(
    handler: &super::PipelineHandler,
    run_id: &str,
) -> Result<Option<RunEvent>, Status> {
    let runs = handler.state.runs.read().await;
    let record = runs
        .get_run(run_id)
        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
    Ok(record
        .state
        .is_terminal()
        .then(|| terminal_event_for_run(record)))
}

async fn status_event_for_existing_run(
    handler: &super::PipelineHandler,
    run_id: &str,
) -> Result<Option<RunEvent>, Status> {
    let runs = handler.state.runs.read().await;
    let record = runs
        .get_run(run_id)
        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
    Ok(status_event_for_run(record))
}

fn terminal_error_for_run(record: &crate::run_state::RunRecord) -> Option<TaskError> {
    let error = record.error.as_ref();
    let message = error.map(|e| e.message.clone())?;
    let (code, retryable, safe_to_retry) = match record.state {
        InternalRunState::RecoveryFailed => (ERROR_CODE_RECOVERY_TIMEOUT.into(), false, false),
        InternalRunState::TimedOut => (ERROR_CODE_LEASE_EXPIRED.into(), true, true),
        _ => (
            error.map_or_else(String::new, |e| e.code.clone()),
            error.is_some_and(|e| e.retryable),
            error.is_some_and(|e| e.safe_to_retry),
        ),
    };
    Some(TaskError {
        code,
        message,
        retryable,
        safe_to_retry,
        commit_state: error.map_or_else(String::new, |e| e.commit_state.clone()),
    })
}

/// Build a terminal `RunEvent` for a run that is already in a terminal state,
/// using real data from the run record instead of placeholder values.
fn terminal_event_for_run(record: &crate::run_state::RunRecord) -> RunEvent {
    let event = match record.state {
        InternalRunState::Completed => run_event::Event::Completed(RunCompleted {
            total_records: record.metrics.total_records,
            total_bytes: record.metrics.total_bytes,
            elapsed_seconds: record.metrics.elapsed_seconds,
            cursors_advanced: record.metrics.cursors_advanced,
        }),
        InternalRunState::Cancelled => run_event::Event::Cancelled(RunCancelled {}),
        _ => run_event::Event::Failed(RunFailed {
            error: terminal_error_for_run(record),
            attempt: record.attempt,
        }),
    };
    RunEvent {
        run_id: record.run_id.clone(),
        event: Some(event),
    }
}

fn status_event_for_run(record: &crate::run_state::RunRecord) -> Option<RunEvent> {
    match record.state {
        InternalRunState::Reconciling => Some(RunEvent {
            run_id: record.run_id.clone(),
            event: Some(run_event::Event::Status(RunStatus {
                state: RunState::Reconciling.into(),
                message: "Controller restarted while this run was in flight; waiting to reconcile the active lease.".into(),
            })),
        }),
        _ => None,
    }
}

fn to_timestamp(time: std::time::SystemTime) -> Timestamp {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    Timestamp {
        seconds: duration.as_secs().cast_signed(),
        nanos: duration.subsec_nanos().cast_signed(),
    }
}

/// Maps proto `RunState` i32 back to internal `RunState`(s) for filtering.
/// `RUNNING` maps to both `Running` and `Cancelling` (externally both appear as `RUNNING`).
/// `FAILED` includes normal failure plus ordinary lease timeouts.
/// `RECOVERY_FAILED` is reserved for reconciliation-specific terminal failure.
fn from_proto_states(v: i32) -> Option<Vec<InternalRunState>> {
    match RunState::try_from(v) {
        Ok(RunState::Pending) => Some(vec![InternalRunState::Pending]),
        Ok(RunState::Assigned) => Some(vec![InternalRunState::Assigned]),
        Ok(RunState::Running) => Some(vec![
            InternalRunState::Running,
            InternalRunState::Cancelling,
        ]),
        Ok(RunState::Reconciling) => Some(vec![InternalRunState::Reconciling]),
        Ok(RunState::PreviewReady) => Some(vec![InternalRunState::PreviewReady]),
        Ok(RunState::Completed) => Some(vec![InternalRunState::Completed]),
        Ok(RunState::Failed) => Some(vec![InternalRunState::Failed, InternalRunState::TimedOut]),
        Ok(RunState::RecoveryFailed) => Some(vec![InternalRunState::RecoveryFailed]),
        Ok(RunState::Cancelled) => Some(vec![InternalRunState::Cancelled]),
        _ => None,
    }
}
