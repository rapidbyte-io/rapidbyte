//! Handler for the `complete_task` RPC.

use std::time::Duration;

use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{CompleteTaskRequest, CompleteTaskResponse, TaskOutcome};
use crate::run_state::{RunError, RunMetrics, RunState};
use crate::scheduler::TerminalTaskOutcome;
use crate::state::ControllerState;

pub(crate) async fn handle_complete(
    handler: &super::AgentHandler,
    req: CompleteTaskRequest,
) -> Result<Response<CompleteTaskResponse>, Status> {
    let state = &handler.state;

    let outcome = match TaskOutcome::try_from(req.outcome) {
        Ok(TaskOutcome::Unspecified) | Err(_) => {
            return Err(Status::invalid_argument("Unknown task outcome"));
        }
        Ok(outcome) => outcome,
    };

    // Complete the task in the scheduler (validates lease epoch).
    let scheduler_outcome = match outcome {
        TaskOutcome::Completed => TerminalTaskOutcome::Completed,
        TaskOutcome::Failed => TerminalTaskOutcome::Failed,
        TaskOutcome::Cancelled => TerminalTaskOutcome::Cancelled,
        TaskOutcome::Unspecified => unreachable!("invalid task outcome rejected above"),
    };

    let (snapshot_task, snapshot_run) = state.snapshot_task_and_run(&req.task_id).await?;

    let (run_id, attempt) = {
        let mut tasks = state.tasks.write().await;
        match tasks
            .complete(
                &req.task_id,
                &req.agent_id,
                req.lease_epoch,
                scheduler_outcome,
            )
            .map_err(|e| Status::not_found(e.to_string()))?
        {
            Some(info) => info,
            None => {
                return Ok(Response::new(CompleteTaskResponse {
                    acknowledged: false,
                }));
            }
        }
    };

    let completed_task = state
        .tasks
        .read()
        .await
        .get(&req.task_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("unknown task: {}", req.task_id)))?;
    if let Err(error) = state.persist_task_record(&completed_task).await {
        rollback_completion_state(
            state,
            &run_id,
            snapshot_task.clone(),
            snapshot_run.clone(),
            None,
            None,
        )
        .await;
        return Err(Status::internal(error.to_string()));
    }

    // Transition run state and publish events
    match outcome {
        TaskOutcome::Completed => {
            handle_completed(
                handler,
                &req,
                &run_id,
                attempt,
                &snapshot_task,
                &snapshot_run,
            )
            .await?;
        }
        TaskOutcome::Failed => {
            handle_failed(
                handler,
                &req,
                &run_id,
                attempt,
                &snapshot_task,
                &snapshot_run,
            )
            .await?;
        }
        TaskOutcome::Cancelled => {
            handle_cancelled(handler, &req, &run_id, &snapshot_task, &snapshot_run).await?;
        }
        TaskOutcome::Unspecified => unreachable!("invalid task outcome rejected above"),
    }

    rapidbyte_metrics::instruments::controller::tasks_completed().add(1, &[]);
    Ok(Response::new(CompleteTaskResponse { acknowledged: true }))
}

#[allow(clippy::too_many_lines)]
async fn handle_completed(
    handler: &super::AgentHandler,
    req: &CompleteTaskRequest,
    run_id: &str,
    _attempt: u32,
    snapshot_task: &crate::scheduler::TaskRecord,
    snapshot_run: &crate::run_state::RunRecord,
) -> Result<(), Status> {
    let state = &handler.state;
    let has_preview = req.preview.is_some();
    let snapshot_preview = { state.previews.read().await.get(run_id).cloned() };
    {
        let mut runs = state.runs.write().await;
        runs.ensure_running(run_id);
        // Dry-run tasks with preview data pass through PreviewReady
        // so clients can discover previews via GetRun/ListRuns.
        if has_preview {
            let _ = runs.transition(run_id, RunState::PreviewReady);
        }
        let _ = runs.transition(run_id, RunState::Completed);
        if let Some(record) = runs.get_run_mut(run_id) {
            let metrics = req.metrics.as_ref();
            record.metrics = RunMetrics {
                total_records: metrics.map_or(0, |m| m.records_processed),
                total_bytes: metrics.map_or(0, |m| m.bytes_processed),
                elapsed_seconds: metrics.map_or(0.0, |m| m.elapsed_seconds),
                cursors_advanced: metrics.map_or(0, |m| m.cursors_advanced),
            };
        }
    }

    // Store preview if provided (runs lock dropped first).
    // Controller signs the ticket — agent sends flight_endpoint only.
    if let Some(preview) = &req.preview {
        let expires_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 300;
        let signed_streams = preview
            .streams
            .iter()
            .map(|stream| {
                let payload = crate::preview::TicketPayload {
                    run_id: run_id.to_owned(),
                    task_id: req.task_id.clone(),
                    stream_name: stream.stream.clone(),
                    lease_epoch: req.lease_epoch,
                    expires_at_unix,
                };
                crate::preview::PreviewStreamEntry {
                    stream: stream.stream.clone(),
                    rows: stream.rows,
                    ticket: state.ticket_signer.sign(&payload),
                }
            })
            .collect::<Vec<_>>();
        let signed_ticket = signed_streams
            .first()
            .map_or_else(bytes::Bytes::new, |stream| stream.ticket.clone());

        let mut previews = state.previews.write().await;
        previews.store(crate::preview::PreviewEntry {
            run_id: run_id.to_owned(),
            task_id: req.task_id.clone(),
            flight_endpoint: preview.flight_endpoint.clone(),
            ticket: signed_ticket,
            streams: signed_streams,
            created_at: std::time::Instant::now(),
            ttl: Duration::from_secs(300),
        });
    }
    let new_preview = if req.preview.is_some() {
        state.previews.read().await.get(run_id).cloned()
    } else {
        None
    };
    if let Some(preview) = new_preview.as_ref() {
        if let Err(error) = state.persist_preview_record(preview).await {
            let rollback_error = state.persist_task_record(snapshot_task).await.err();
            rollback_completion_state(
                state,
                run_id,
                snapshot_task.clone(),
                snapshot_run.clone(),
                snapshot_preview,
                None,
            )
            .await;
            return Err(Status::internal(match rollback_error {
                Some(rollback_error) => format!(
                    "{error}; durable rollback for task {} also failed: {rollback_error}",
                    req.task_id
                ),
                None => error.to_string(),
            }));
        }
    }
    let completed_run = state
        .runs
        .read()
        .await
        .get_run(run_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("unknown run: {run_id}")))?;
    if let Err(error) = state.persist_run_record(&completed_run).await {
        let task_rollback_error = state.persist_task_record(snapshot_task).await.err();
        let preview_rollback_error = if new_preview.is_some() {
            rollback_preview_durable(state, run_id, snapshot_preview.as_ref()).await
        } else {
            None
        };
        rollback_completion_state(
            state,
            run_id,
            snapshot_task.clone(),
            snapshot_run.clone(),
            snapshot_preview,
            None,
        )
        .await;
        let mut details = Vec::new();
        if let Some(task_rollback_error) = task_rollback_error {
            details.push(format!(
                "durable rollback for task {} also failed: {task_rollback_error}",
                req.task_id
            ));
        }
        if let Some(preview_rollback_error) = preview_rollback_error {
            details.push(format!(
                "durable rollback for preview {run_id} also failed: {preview_rollback_error}"
            ));
        }
        let message = if details.is_empty() {
            error.to_string()
        } else {
            format!("{error}; {}", details.join("; "))
        };
        return Err(Status::internal(message));
    }

    let metrics = req.metrics.as_ref();
    crate::terminal::finalize_terminal(
        state,
        run_id,
        crate::terminal::TerminalOutcome::Completed {
            metrics: RunMetrics {
                total_records: metrics.map_or(0, |m| m.records_processed),
                total_bytes: metrics.map_or(0, |m| m.bytes_processed),
                elapsed_seconds: metrics.map_or(0.0, |m| m.elapsed_seconds),
                cursors_advanced: metrics.map_or(0, |m| m.cursors_advanced),
            },
        },
    )
    .await;

    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_failed(
    handler: &super::AgentHandler,
    req: &CompleteTaskRequest,
    run_id: &str,
    attempt: u32,
    snapshot_task: &crate::scheduler::TaskRecord,
    snapshot_run: &crate::run_state::RunRecord,
) -> Result<(), Status> {
    let state = &handler.state;
    let error = req.error.as_ref();
    let safe_to_retry = error.is_some_and(|e| e.safe_to_retry);
    let retryable = error.is_some_and(|e| e.retryable);
    let commit_state = error.map_or("", |e| e.commit_state.as_str());
    let is_cancelling = state
        .runs
        .read()
        .await
        .get_run(run_id)
        .is_some_and(|run| run.state == RunState::Cancelling);

    // Retry safety policy: only auto-requeue if safe_to_retry AND retryable
    // AND the commit state is explicitly before_commit.
    let should_retry = !is_cancelling
        && prepare_retry_if_allowed(state, run_id, safe_to_retry, retryable, commit_state).await;

    if should_retry {
        // Extract retry-specific task data (only cloned when needed)
        let (yaml, dry_run, limit) = {
            let tasks = state.tasks.read().await;
            let task = tasks.get(&req.task_id).unwrap();
            (task.pipeline_yaml.clone(), task.dry_run, task.limit)
        };

        let next_task_id = {
            let mut tasks = state.tasks.write().await;
            tasks.enqueue(run_id.to_owned(), yaml, dry_run, limit, attempt + 1)
        };
        let next_task = state
            .tasks
            .read()
            .await
            .get(&next_task_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown task: {next_task_id}")))?;
        if let Err(error) = state.persist_task_record(&next_task).await {
            let rollback_error = state.persist_task_record(snapshot_task).await.err();
            rollback_completion_state(
                state,
                run_id,
                snapshot_task.clone(),
                snapshot_run.clone(),
                None,
                Some(&next_task_id),
            )
            .await;
            return Err(Status::internal(match rollback_error {
                Some(rollback_error) => format!(
                    "{error}; durable rollback for task {} also failed: {rollback_error}",
                    req.task_id
                ),
                None => error.to_string(),
            }));
        }
        let retried_run = state
            .runs
            .read()
            .await
            .get_run(run_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown run: {run_id}")))?;
        if let Err(error) = state.persist_run_record(&retried_run).await {
            let task_rollback_error = state.persist_task_record(snapshot_task).await.err();
            let next_task_rollback_error = state.delete_task(&next_task_id).await.err();
            rollback_completion_state(
                state,
                run_id,
                snapshot_task.clone(),
                snapshot_run.clone(),
                None,
                Some(&next_task_id),
            )
            .await;
            let mut details = Vec::new();
            if let Some(task_rollback_error) = task_rollback_error {
                details.push(format!(
                    "durable rollback for task {} also failed: {task_rollback_error}",
                    req.task_id
                ));
            }
            if let Some(next_task_rollback_error) = next_task_rollback_error {
                details.push(format!(
                    "durable rollback for queued task {next_task_id} also failed: {next_task_rollback_error}"
                ));
            }
            let message = if details.is_empty() {
                error.to_string()
            } else {
                format!("{error}; {}", details.join("; "))
            };
            return Err(Status::internal(message));
        }
        state.task_notify.notify_waiters();

        tracing::info!(run_id, attempt = attempt + 1, "Auto-requeued failed task");
    } else {
        {
            let mut runs = state.runs.write().await;
            runs.ensure_running(run_id);
            let _ = runs.transition(run_id, RunState::Failed);
            if let Some(record) = runs.get_run_mut(run_id) {
                record.error = error.map(|e| RunError {
                    code: e.code.clone(),
                    message: format!("{}: {}", e.code, e.message),
                    retryable: e.retryable,
                    safe_to_retry: e.safe_to_retry,
                    commit_state: e.commit_state.clone(),
                });
            }
        }
        let failed_run = state
            .runs
            .read()
            .await
            .get_run(run_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("unknown run: {run_id}")))?;
        if let Err(error) = state.persist_run_record(&failed_run).await {
            let rollback_error = state.persist_task_record(snapshot_task).await.err();
            rollback_completion_state(
                state,
                run_id,
                snapshot_task.clone(),
                snapshot_run.clone(),
                None,
                None,
            )
            .await;
            return Err(Status::internal(match rollback_error {
                Some(rollback_error) => format!(
                    "{error}; durable rollback for task {} also failed: {rollback_error}",
                    req.task_id
                ),
                None => error.to_string(),
            }));
        }

        crate::terminal::finalize_terminal(
            state,
            run_id,
            crate::terminal::TerminalOutcome::Failed {
                error: error.map(|e| RunError {
                    code: e.code.clone(),
                    message: format!("{}: {}", e.code, e.message),
                    retryable: e.retryable,
                    safe_to_retry: e.safe_to_retry,
                    commit_state: e.commit_state.clone(),
                }),
                attempt,
            },
        )
        .await;
    }

    Ok(())
}

async fn handle_cancelled(
    handler: &super::AgentHandler,
    req: &CompleteTaskRequest,
    run_id: &str,
    snapshot_task: &crate::scheduler::TaskRecord,
    snapshot_run: &crate::run_state::RunRecord,
) -> Result<(), Status> {
    let state = &handler.state;

    {
        let mut runs = state.runs.write().await;
        runs.ensure_running(run_id);
        let _ = runs.transition(run_id, RunState::Cancelled);
    }
    let cancelled_run = state
        .runs
        .read()
        .await
        .get_run(run_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("unknown run: {run_id}")))?;
    if let Err(error) = state.persist_run_record(&cancelled_run).await {
        let rollback_error = state.persist_task_record(snapshot_task).await.err();
        rollback_completion_state(
            state,
            run_id,
            snapshot_task.clone(),
            snapshot_run.clone(),
            None,
            None,
        )
        .await;
        return Err(Status::internal(match rollback_error {
            Some(rollback_error) => format!(
                "{error}; durable rollback for task {} also failed: {rollback_error}",
                req.task_id
            ),
            None => error.to_string(),
        }));
    }

    crate::terminal::finalize_terminal(state, run_id, crate::terminal::TerminalOutcome::Cancelled)
        .await;

    Ok(())
}

async fn rollback_completion_state(
    state: &ControllerState,
    run_id: &str,
    snapshot_task: crate::scheduler::TaskRecord,
    snapshot_run: crate::run_state::RunRecord,
    snapshot_preview: Option<crate::preview::PreviewEntry>,
    next_task_id: Option<&str>,
) {
    {
        let mut tasks = state.tasks.write().await;
        if let Some(next_task_id) = next_task_id {
            let _ = tasks.remove_task(next_task_id);
        }
        tasks.restore_task(snapshot_task);
    }
    {
        let mut runs = state.runs.write().await;
        runs.restore_run(snapshot_run);
    }
    {
        let mut previews = state.previews.write().await;
        let _ = previews.remove(run_id);
        if let Some(snapshot_preview) = snapshot_preview {
            previews.restore(snapshot_preview);
        }
    }
}

async fn rollback_preview_durable(
    state: &ControllerState,
    run_id: &str,
    snapshot_preview: Option<&crate::preview::PreviewEntry>,
) -> Option<anyhow::Error> {
    match snapshot_preview {
        Some(snapshot_preview) => state.persist_preview_record(snapshot_preview).await.err(),
        None => state.delete_preview(run_id).await.err(),
    }
}

async fn prepare_retry_if_allowed(
    state: &ControllerState,
    run_id: &str,
    safe_to_retry: bool,
    retryable: bool,
    commit_state: &str,
) -> bool {
    if !safe_to_retry
        || !retryable
        || commit_state != rapidbyte_types::error::CommitState::BeforeCommit.as_str()
    {
        return false;
    }

    let mut runs = state.runs.write().await;
    if runs
        .get_run(run_id)
        .is_some_and(|run| run.state == RunState::Cancelling)
    {
        return false;
    }

    let _ = runs.transition(run_id, RunState::Failed);
    if let Some(record) = runs.get_run_mut(run_id) {
        record.attempt += 1;
    }
    runs.prepare_retry(run_id);
    true
}

/// Test-only re-export of [`prepare_retry_if_allowed`] so that tests
/// in `services/agent/mod.rs` can call it via `AgentHandler`.
#[cfg(test)]
pub(crate) async fn prepare_retry_if_allowed_for_test(
    state: &ControllerState,
    run_id: &str,
    safe_to_retry: bool,
    retryable: bool,
    commit_state: &str,
) -> bool {
    prepare_retry_if_allowed(state, run_id, safe_to_retry, retryable, commit_state).await
}
