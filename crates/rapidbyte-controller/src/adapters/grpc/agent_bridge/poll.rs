//! Handlers for the `poll_task` and `report_progress` RPCs.

use std::time::Duration;

use tonic::{Response, Status};

use crate::proto::rapidbyte::v2::{
    poll_task_response, run_event, NoTask, PollTaskRequest, PollTaskResponse,
    ReportProgressRequest, ReportProgressResponse, RunEvent,
};
use crate::run_state::RunState;
use crate::scheduler::TaskState;
use crate::state::ControllerState;

use super::LEASE_TTL;

pub(crate) async fn handle_poll(
    handler: &super::AgentHandler,
    req: PollTaskRequest,
    #[cfg(test)] poll_barrier: Option<&tokio::sync::Barrier>,
) -> Result<Response<PollTaskResponse>, Status> {
    let state = &handler.state;
    let wait = Duration::from_secs(u64::from(req.wait_seconds).min(60));
    let max_tasks = {
        let registry = state.registry.read().await;
        registry
            .get(&req.agent_id)
            .ok_or_else(|| Status::not_found("Unknown agent"))?
            .max_tasks
    };

    // Try immediate poll
    {
        let tasks = state.tasks.read().await;
        if tasks.active_tasks_for_agent(&req.agent_id)
            >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
        {
            return Ok(Response::new(PollTaskResponse {
                result: Some(poll_task_response::Result::NoTask(NoTask {})),
            }));
        }
    }
    #[cfg(test)]
    if let Some(poll_barrier) = poll_barrier {
        poll_barrier.wait().await;
    }
    if let Some(assignment) = claim_task(handler, &req.agent_id, max_tasks).await? {
        let resp = super::secret::dispatch_task(handler, assignment, &req.agent_id).await?;
        return Ok(Response::new(resp));
    }

    // Long-poll: wait for notification or timeout
    let notified = state.task_notify.notified();
    tokio::select! {
        () = notified => {},
        () = tokio::time::sleep(wait) => {},
    }

    // Try again after wakeup
    {
        let registry = state.registry.read().await;
        if registry.get(&req.agent_id).is_none() {
            return Err(Status::not_found("Unknown agent"));
        }
    }
    let tasks = state.tasks.read().await;
    if tasks.active_tasks_for_agent(&req.agent_id)
        >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
    {
        return Ok(Response::new(PollTaskResponse {
            result: Some(poll_task_response::Result::NoTask(NoTask {})),
        }));
    }
    drop(tasks);
    if let Some(assignment) = claim_task(handler, &req.agent_id, max_tasks).await? {
        let resp = super::secret::dispatch_task(handler, assignment, &req.agent_id).await?;
        return Ok(Response::new(resp));
    }

    Ok(Response::new(PollTaskResponse {
        result: Some(poll_task_response::Result::NoTask(NoTask {})),
    }))
}

pub(crate) async fn handle_report_progress(
    handler: &super::AgentHandler,
    req: ReportProgressRequest,
) -> Result<Response<ReportProgressResponse>, Status> {
    let state = &handler.state;

    let (snapshot_task, snapshot_run) = state.snapshot_task_and_run(&req.task_id).await?;

    // Validate lease and update scheduler state while holding the task lock.
    let run_id = {
        let mut tasks = state.tasks.write().await;
        let task = tasks
            .get(&req.task_id)
            .ok_or_else(|| Status::not_found("Task not found"))?;

        crate::state::validate_lease(task, &req.agent_id, req.lease_epoch)?;

        let run_id = task.run_id.clone();
        let task_state = task.state;
        if task_state == TaskState::Assigned {
            tasks
                .report_running(&req.task_id, &req.agent_id, req.lease_epoch)
                .map_err(|err| match err {
                    crate::scheduler::SchedulerError::AgentMismatch(_, _) => {
                        Status::permission_denied("Task lease belongs to a different agent")
                    }
                    _ => Status::failed_precondition("Stale lease epoch"),
                })?;
        }
        run_id
    };

    state.runs.write().await.ensure_running(&run_id);
    let running_task = state
        .tasks
        .read()
        .await
        .get(&req.task_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("unknown task: {}", req.task_id)))?;
    let running_run = state
        .runs
        .read()
        .await
        .get_run(&run_id)
        .cloned()
        .ok_or_else(|| Status::not_found(format!("unknown run: {run_id}")))?;
    if let Err(error) = state
        .persist_running_records(&running_run, &running_task)
        .await
    {
        rollback_assignment(state, snapshot_run, snapshot_task).await;
        return Err(Status::internal(error.to_string()));
    }

    if let Some(progress) = req.progress {
        let watchers = state.watchers.read().await;
        watchers.publish(
            &run_id,
            RunEvent {
                run_id: run_id.clone(),
                detail: String::new(),
                event: Some(run_event::Event::Progress(progress)),
            },
        );
    }

    Ok(Response::new(ReportProgressResponse {}))
}

async fn claim_task(
    handler: &super::AgentHandler,
    agent_id: &str,
    max_tasks: u32,
) -> Result<Option<crate::scheduler::TaskAssignment>, Status> {
    let state = &handler.state;

    let claimed = {
        let mut tasks = state.tasks.write().await;
        if tasks.active_tasks_for_agent(agent_id)
            >= usize::try_from(max_tasks).unwrap_or(usize::MAX)
        {
            return Ok(None);
        }
        let Some(snapshot_task) = tasks.peek_pending().cloned() else {
            return Ok(None);
        };
        let Some(assignment) = tasks.poll(agent_id, LEASE_TTL, &state.epoch_gen) else {
            return Ok(None);
        };
        let assigned_task = tasks
            .get(&assignment.task_id)
            .cloned()
            .expect("claimed task should still exist");
        let mut runs = state.runs.write().await;
        let snapshot_run = runs
            .get_run(&assignment.run_id)
            .cloned()
            .expect("claimed run should exist");
        if runs
            .transition(&assignment.run_id, RunState::Assigned)
            .is_err()
        {
            drop(runs);
            let _ = tasks.reject_assignment(&assignment.task_id, assignment.lease_epoch);
            let rejected_task = tasks
                .get(&assignment.task_id)
                .cloned()
                .expect("rejected task should still exist");
            drop(tasks);
            state
                .persist_task_record(&rejected_task)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            return Ok(None);
        }
        runs.set_current_task(
            &assignment.run_id,
            assignment.task_id.clone(),
            agent_id.to_string(),
            assignment.attempt,
            assignment.lease_epoch,
        );
        let assigned_run = runs
            .get_run(&assignment.run_id)
            .cloned()
            .expect("assigned run should exist");
        (
            assignment,
            snapshot_task,
            snapshot_run,
            assigned_task,
            assigned_run,
        )
    };

    let (assignment, snapshot_task, snapshot_run, assigned_task, assigned_run) = claimed;

    if let Err(error) = state
        .persist_assignment_records(&assigned_run, &assigned_task)
        .await
    {
        rollback_assignment(state, snapshot_run, snapshot_task).await;
        return Err(Status::internal(error.to_string()));
    }

    rapidbyte_metrics::instruments::controller::lease_grants().add(1, &[]);
    Ok(Some(assignment))
}

pub(super) async fn rollback_assignment(
    state: &ControllerState,
    snapshot_run: crate::run_state::RunRecord,
    snapshot_task: crate::scheduler::TaskRecord,
) {
    {
        let mut runs = state.runs.write().await;
        runs.restore_run(snapshot_run);
    }
    {
        let mut tasks = state.tasks.write().await;
        tasks.restore_task(snapshot_task);
    }
}
