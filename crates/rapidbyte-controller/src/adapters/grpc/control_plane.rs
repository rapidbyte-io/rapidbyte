//! v2 `ControlPlane` gRPC handler backed by `ControllerState`.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use opentelemetry::KeyValue;
use tokio::sync::RwLock;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Response, Status};

use crate::proto::rapidbyte::v2::control_plane_server::ControlPlane;
use crate::proto::rapidbyte::v2::{
    run_event, CancelRunRequest, CancelRunResponse, GetRunRequest, GetRunResponse, ListRunsRequest,
    ListRunsResponse, RetryRunRequest, RetryRunResponse, RunCancelled, RunCompleted, RunEvent,
    RunFailed, RunState as ProtoRunState, RunStatus, RunSummary, StreamRunRequest,
    SubmitRunRequest, SubmitRunResponse, TaskError,
};
use crate::run_state::{RunError, RunRecord, RunState};
use crate::state::ControllerState;

type RunEventStream = Pin<Box<dyn Stream<Item = Result<RunEvent, Status>> + Send>>;

/// RAII guard that removes an idempotency key from the pending set on drop.
struct PendingKeyGuard {
    pending: Arc<RwLock<HashSet<String>>>,
    key: Option<String>,
}

impl PendingKeyGuard {
    fn new(pending: Arc<RwLock<HashSet<String>>>, key: String) -> Self {
        Self {
            pending,
            key: Some(key),
        }
    }

    fn disarm(&mut self) {
        self.key = None;
    }
}

impl Drop for PendingKeyGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let pending = Arc::clone(&self.pending);
            tokio::spawn(async move {
                pending.write().await.remove(&key);
            });
        }
    }
}

pub struct ControlPlaneHandler {
    state: ControllerState,
}

impl ControlPlaneHandler {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneHandler {
    type StreamRunStream = RunEventStream;

    async fn submit_run(
        &self,
        request: tonic::Request<SubmitRunRequest>,
    ) -> Result<tonic::Response<SubmitRunResponse>, tonic::Status> {
        let req = request.into_inner();
        let pipeline_name = validate_pipeline_yaml(&req.pipeline_yaml_utf8)?;

        let idempotency_key = req.idempotency_key.filter(|key| !key.trim().is_empty());

        let mut pending_guard: Option<PendingKeyGuard> = None;
        if let Some(key) = &idempotency_key {
            let mut pending = self.state.pending_idempotency_keys.write().await;
            if !pending.insert(key.clone()) {
                return Err(Status::aborted(
                    "A submission with this idempotency key is already in progress. Retry shortly.",
                ));
            }
            pending_guard = Some(PendingKeyGuard::new(
                Arc::clone(&self.state.pending_idempotency_keys),
                key.clone(),
            ));
        }

        let run_id = uuid::Uuid::new_v4().to_string();
        let (actual_run_id, is_new) = {
            let mut runs = self.state.runs.write().await;
            runs.create_run(run_id, pipeline_name, idempotency_key.clone())
        };

        if is_new {
            let dry_run = req
                .execution
                .as_ref()
                .is_some_and(|execution| execution.dry_run);
            let limit = req.execution.as_ref().and_then(|execution| execution.limit);
            let task_id = {
                let mut tasks = self.state.tasks.write().await;
                tasks.enqueue(
                    actual_run_id.clone(),
                    req.pipeline_yaml_utf8,
                    dry_run,
                    limit,
                    1,
                )
            };

            let run_snapshot = {
                self.state
                    .runs
                    .read()
                    .await
                    .get_run(&actual_run_id)
                    .cloned()
                    .expect("newly created run should exist")
            };
            let task_snapshot = {
                self.state
                    .tasks
                    .read()
                    .await
                    .get(&task_id)
                    .cloned()
                    .expect("newly created task should exist")
            };

            if let Err(error) = self
                .state
                .create_run_with_task_records(&run_snapshot, &task_snapshot)
                .await
            {
                rollback_new_submission(&self.state, &actual_run_id, &task_id).await;
                if let Some(guard) = &mut pending_guard {
                    if let Some(key) = &idempotency_key {
                        self.state
                            .pending_idempotency_keys
                            .write()
                            .await
                            .remove(key);
                    }
                    guard.disarm();
                }
                return Err(Status::internal(error.to_string()));
            }

            self.state.task_notify.notify_waiters();
            rapidbyte_metrics::instruments::controller::runs_submitted().add(
                1,
                &[KeyValue::new(rapidbyte_metrics::labels::STATUS, "accepted")],
            );
            rapidbyte_metrics::instruments::controller::active_runs().add(1, &[]);
        }

        if let Some(guard) = &mut pending_guard {
            if let Some(key) = &idempotency_key {
                self.state
                    .pending_idempotency_keys
                    .write()
                    .await
                    .remove(key);
            }
            guard.disarm();
        }

        Ok(Response::new(SubmitRunResponse {
            run_id: actual_run_id,
        }))
    }

    async fn get_run(
        &self,
        request: tonic::Request<GetRunRequest>,
    ) -> Result<tonic::Response<GetRunResponse>, tonic::Status> {
        let run_id = request.into_inner().run_id;
        let runs = self.state.runs.read().await;
        let record = runs
            .get_run(&run_id)
            .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;

        Ok(Response::new(GetRunResponse {
            run_id: record.run_id.clone(),
            state: format!("{:?}", record.state),
        }))
    }

    async fn list_runs(
        &self,
        request: tonic::Request<ListRunsRequest>,
    ) -> Result<tonic::Response<ListRunsResponse>, tonic::Status> {
        let request = request.into_inner();
        let limit = request
            .limit
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(20)
            .max(1);
        let state_filter = request
            .state
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(parse_list_runs_state)
            .transpose()?;
        let state_slice = state_filter.map(|state| [state]);

        let runs = self.state.runs.read().await;
        let mut records = runs.list_runs(state_slice.as_ref().map(|slice| &slice[..]));
        records.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        records.truncate(limit);

        let summaries = records
            .into_iter()
            .map(|record| RunSummary {
                run_id: record.run_id.clone(),
                state: format!("{:?}", record.state),
            })
            .collect();

        Ok(Response::new(ListRunsResponse { runs: summaries }))
    }

    async fn cancel_run(
        &self,
        request: tonic::Request<CancelRunRequest>,
    ) -> Result<tonic::Response<CancelRunResponse>, tonic::Status> {
        let run_id = request.into_inner().run_id;
        let current_state = {
            let runs = self.state.runs.read().await;
            let record = runs
                .get_run(&run_id)
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
            record.state
        };

        let response = match current_state {
            RunState::Pending => {
                let snapshot_run = {
                    self.state
                        .runs
                        .read()
                        .await
                        .get_run(&run_id)
                        .cloned()
                        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
                };

                {
                    let mut runs = self.state.runs.write().await;
                    runs.transition(&run_id, RunState::Cancelled)
                        .map_err(|error| Status::internal(error.to_string()))?;
                }

                let (snapshot_task, cancelled_task) =
                    cancel_latest_task(&self.state, &run_id).await;
                if let Some(cancelled_task) = cancelled_task.as_ref() {
                    if let Err(error) = self.state.persist_task_record(cancelled_task).await {
                        rollback_queued_cancel(&self.state, snapshot_run, snapshot_task).await;
                        return Err(Status::internal(error.to_string()));
                    }
                }

                let cancelled_run = {
                    self.state
                        .runs
                        .read()
                        .await
                        .get_run(&run_id)
                        .cloned()
                        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
                };
                if let Err(error) = self.state.persist_run_record(&cancelled_run).await {
                    rollback_queued_cancel(&self.state, snapshot_run, snapshot_task).await;
                    return Err(Status::internal(error.to_string()));
                }

                crate::terminal::finalize_terminal(
                    &self.state,
                    &run_id,
                    crate::terminal::TerminalOutcome::Cancelled,
                )
                .await;

                CancelRunResponse {
                    accepted: true,
                    message: "Queued run cancelled".to_owned(),
                }
            }
            RunState::Assigned | RunState::Reconciling | RunState::Running => {
                {
                    let mut runs = self.state.runs.write().await;
                    runs.transition(&run_id, RunState::Cancelling)
                        .map_err(|error| Status::internal(error.to_string()))?;
                }
                self.state
                    .persist_run(&run_id)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;

                let label = if current_state == RunState::Assigned {
                    "Assigned"
                } else {
                    "Running"
                };
                CancelRunResponse {
                    accepted: true,
                    message: format!("{label} — cancel will be delivered via heartbeat"),
                }
            }
            RunState::Cancelling => CancelRunResponse {
                accepted: true,
                message: "Run is already being cancelled".to_owned(),
            },
            _ if current_state.is_terminal() => CancelRunResponse {
                accepted: false,
                message: format!("Run is already in terminal state: {current_state:?}"),
            },
            _ => CancelRunResponse {
                accepted: false,
                message: format!("Cannot cancel run in state: {current_state:?}"),
            },
        };

        Ok(Response::new(response))
    }

    async fn retry_run(
        &self,
        request: tonic::Request<RetryRunRequest>,
    ) -> Result<tonic::Response<RetryRunResponse>, tonic::Status> {
        let run_id = request.into_inner().run_id;

        let snapshot_run = {
            self.state
                .runs
                .read()
                .await
                .get_run(&run_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
        };

        if !matches!(
            snapshot_run.state,
            RunState::Failed | RunState::TimedOut | RunState::RecoveryFailed
        ) {
            return Err(Status::failed_precondition(format!(
                "Run {run_id} is not retryable from state {:?}",
                snapshot_run.state
            )));
        }

        let previous_task = {
            self.state
                .tasks
                .read()
                .await
                .find_by_run_id(&run_id)
                .cloned()
                .ok_or_else(|| {
                    Status::failed_precondition("Cannot retry run without prior task payload")
                })?
        };

        {
            let mut runs = self.state.runs.write().await;
            if let Some(record) = runs.get_run_mut(&run_id) {
                record.attempt += 1;
                record.error = None;
            }
            runs.prepare_retry(&run_id);
        }

        let retried_task_id = {
            let mut tasks = self.state.tasks.write().await;
            tasks.enqueue(
                run_id.clone(),
                previous_task.pipeline_yaml.clone(),
                previous_task.dry_run,
                previous_task.limit,
                previous_task.attempt.saturating_add(1),
            )
        };

        let retried_run = {
            self.state
                .runs
                .read()
                .await
                .get_run(&run_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?
        };
        let retried_task = {
            self.state
                .tasks
                .read()
                .await
                .get(&retried_task_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Task {retried_task_id} not found")))?
        };

        if let Err(error) = self.state.persist_run_record(&retried_run).await {
            rollback_new_task_only(&self.state, &retried_task_id).await;
            return Err(Status::internal(error.to_string()));
        }
        if let Err(error) = self.state.persist_task_record(&retried_task).await {
            rollback_new_task_only(&self.state, &retried_task_id).await;
            return Err(Status::internal(error.to_string()));
        }

        self.state.task_notify.notify_waiters();
        Ok(Response::new(RetryRunResponse { accepted: true }))
    }

    async fn stream_run(
        &self,
        request: tonic::Request<StreamRunRequest>,
    ) -> Result<tonic::Response<Self::StreamRunStream>, tonic::Status> {
        let run_id = request.into_inner().run_id;

        let rx = {
            let mut watchers = self.state.watchers.write().await;
            watchers.subscribe(&run_id)
        };

        if let Some(event) = terminal_event_for_existing_run(&self.state, &run_id).await? {
            self.state.watchers.write().await.remove(&run_id);
            let stream: RunEventStream = Box::pin(tokio_stream::once(Ok(event)));
            return Ok(Response::new(stream));
        }

        let live_stream = BroadcastStream::new(rx).filter_map(|result| match result {
            Ok(event) => Some(Ok(event)),
            Err(_) => None,
        });

        let stream: RunEventStream = match status_event_for_existing_run(&self.state, &run_id)
            .await?
        {
            Some(status_event) => Box::pin(tokio_stream::once(Ok(status_event)).chain(live_stream)),
            None => Box::pin(live_stream),
        };

        Ok(Response::new(stream))
    }
}

#[allow(clippy::result_large_err)]
fn validate_pipeline_yaml(raw: &[u8]) -> Result<String, Status> {
    let yaml = std::str::from_utf8(raw).map_err(|error| {
        Status::invalid_argument(format!("Pipeline YAML is not valid UTF-8: {error}"))
    })?;
    let config: serde_yaml::Value = serde_yaml::from_str(yaml)
        .map_err(|error| Status::invalid_argument(format!("Invalid YAML: {error}")))?;

    let backend = config
        .get("state")
        .and_then(|state| state.get("backend"))
        .and_then(serde_yaml::Value::as_str);
    if backend != Some("postgres") {
        return Err(Status::invalid_argument(
            "Distributed mode requires state.backend: postgres.",
        ));
    }

    Ok(config
        .get("pipeline")
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or("unknown")
        .to_owned())
}

#[allow(clippy::result_large_err)]
fn parse_list_runs_state(raw: &str) -> Result<RunState, Status> {
    let normalized = raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "PENDING" => Ok(RunState::Pending),
        "ASSIGNED" => Ok(RunState::Assigned),
        "RECONCILING" => Ok(RunState::Reconciling),
        "RUNNING" => Ok(RunState::Running),
        "PREVIEW_READY" | "PREVIEWREADY" => Ok(RunState::PreviewReady),
        "COMPLETED" | "SUCCEEDED" => Ok(RunState::Completed),
        "FAILED" => Ok(RunState::Failed),
        "RECOVERY_FAILED" | "RECOVERYFAILED" => Ok(RunState::RecoveryFailed),
        "CANCELLING" => Ok(RunState::Cancelling),
        "CANCELLED" => Ok(RunState::Cancelled),
        "TIMED_OUT" | "TIMEDOUT" => Ok(RunState::TimedOut),
        _ => Err(Status::invalid_argument(format!(
            "Unsupported run state filter: {raw}"
        ))),
    }
}

async fn rollback_new_submission(state: &ControllerState, run_id: &str, task_id: &str) {
    {
        let mut tasks = state.tasks.write().await;
        let _ = tasks.remove_task(task_id);
    }
    {
        let mut runs = state.runs.write().await;
        let _ = runs.remove_run(run_id);
    }
}

async fn rollback_new_task_only(state: &ControllerState, task_id: &str) {
    let mut tasks = state.tasks.write().await;
    let _ = tasks.remove_task(task_id);
}

async fn cancel_latest_task(
    state: &ControllerState,
    run_id: &str,
) -> (
    Option<crate::scheduler::TaskRecord>,
    Option<crate::scheduler::TaskRecord>,
) {
    let mut tasks = state.tasks.write().await;
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
    state: &ControllerState,
    snapshot_run: RunRecord,
    snapshot_task: Option<crate::scheduler::TaskRecord>,
) {
    if let Some(snapshot_task) = snapshot_task {
        let mut tasks = state.tasks.write().await;
        tasks.restore_task(snapshot_task);
    }
    let mut runs = state.runs.write().await;
    runs.restore_run(snapshot_run);
}

async fn terminal_event_for_existing_run(
    state: &ControllerState,
    run_id: &str,
) -> Result<Option<RunEvent>, Status> {
    let runs = state.runs.read().await;
    let record = runs
        .get_run(run_id)
        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
    Ok(record
        .state
        .is_terminal()
        .then(|| terminal_event_for_run(record)))
}

async fn status_event_for_existing_run(
    state: &ControllerState,
    run_id: &str,
) -> Result<Option<RunEvent>, Status> {
    let runs = state.runs.read().await;
    let record = runs
        .get_run(run_id)
        .ok_or_else(|| Status::not_found(format!("Run {run_id} not found")))?;
    Ok(status_event_for_run(record))
}

fn terminal_event_for_run(record: &RunRecord) -> RunEvent {
    let event = match record.state {
        RunState::Completed => run_event::Event::Completed(RunCompleted {
            total_records: record.metrics.total_records,
            total_bytes: record.metrics.total_bytes,
            elapsed_seconds: record.metrics.elapsed_seconds,
            cursors_advanced: record.metrics.cursors_advanced,
        }),
        RunState::Cancelled => run_event::Event::Cancelled(RunCancelled {}),
        _ => run_event::Event::Failed(RunFailed {
            error: terminal_error_for_run(record),
            attempt: record.attempt,
        }),
    };

    RunEvent {
        run_id: record.run_id.clone(),
        detail: String::new(),
        event: Some(event),
    }
}

fn status_event_for_run(record: &RunRecord) -> Option<RunEvent> {
    if record.state != RunState::Reconciling {
        return None;
    }

    Some(RunEvent {
        run_id: record.run_id.clone(),
        detail: "Run is reconciling after controller restart".to_owned(),
        event: Some(run_event::Event::Status(RunStatus {
            state: ProtoRunState::Reconciling.into(),
            message:
                "Controller restarted while this run was in flight; waiting to reconcile active lease."
                    .to_owned(),
        })),
    })
}

fn terminal_error_for_run(record: &RunRecord) -> Option<TaskError> {
    let error = record.error.as_ref()?;
    Some(map_run_error(error))
}

fn map_run_error(error: &RunError) -> TaskError {
    TaskError {
        code: error.code.clone(),
        message: error.message.clone(),
        retryable: error.retryable,
        safe_to_retry: error.safe_to_retry,
        commit_state: error.commit_state.clone(),
    }
}
