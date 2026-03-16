//! Handler for the `heartbeat` RPC.

use opentelemetry::KeyValue;
use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{
    agent_directive, AgentDirective, CancelTask, HeartbeatRequest, HeartbeatResponse,
};
use crate::run_state::RunState;
use crate::scheduler::TaskRecord;
use crate::state::ControllerState;

use super::LEASE_TTL;

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_heartbeat(
    handler: &super::AgentHandler,
    req: HeartbeatRequest,
) -> Result<Response<HeartbeatResponse>, Status> {
    let state = &handler.state;

    // Update heartbeat in registry
    {
        let mut registry = state.registry.write().await;
        registry
            .heartbeat(&req.agent_id, req.active_tasks)
            .map_err(|e| Status::not_found(e.to_string()))?;
    }
    state
        .persist_agent(&req.agent_id)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

    // Renew leases for active tasks reported by the agent
    if !req.active_leases.is_empty() {
        let mut renewed = Vec::new();
        let mut tasks = state.tasks.write().await;
        for active_lease in &req.active_leases {
            let snapshot_task = tasks.get(&active_lease.task_id).cloned();
            if tasks.renew_lease(
                &active_lease.task_id,
                &req.agent_id,
                active_lease.lease_epoch,
                LEASE_TTL,
            ) {
                let renewed_task = tasks
                    .get(&active_lease.task_id)
                    .cloned()
                    .expect("renewed task should still exist");
                renewed.push((
                    snapshot_task.expect("renewed task should have previous snapshot"),
                    renewed_task,
                ));
            }
        }
        drop(tasks);

        let mut persisted_previous = Vec::new();
        for (snapshot_task, renewed_task) in &renewed {
            if let Err(error) = state.persist_task_record(renewed_task).await {
                let rollback_error = if persisted_previous.is_empty() {
                    None
                } else {
                    let mut first_error = None;
                    for snapshot_task in &persisted_previous {
                        if let Err(rollback_error) = state.persist_task_record(snapshot_task).await
                        {
                            if first_error.is_none() {
                                first_error = Some(rollback_error);
                            }
                        }
                    }
                    first_error
                };
                let snapshot_tasks = renewed
                    .into_iter()
                    .map(|(snapshot_task, _)| snapshot_task)
                    .collect();
                rollback_renewed_tasks(state, snapshot_tasks).await;
                return Err(Status::internal(match rollback_error {
                    Some(rollback_error) => format!(
                        "{error}; durable rollback for renewed leases also failed: {rollback_error}"
                    ),
                    None => error.to_string(),
                }));
            }
            persisted_previous.push(snapshot_task.clone());
        }
    }

    // Check for cancel directives — look up active leases and see if any
    // of their runs are in Cancelling state
    let mut directives = Vec::new();
    {
        let runs = state.runs.read().await;
        let tasks = state.tasks.read().await;

        for active_lease in &req.active_leases {
            if let Some(task) = tasks.get(&active_lease.task_id) {
                let lease_matches = task.lease.as_ref().is_some_and(|lease| {
                    lease.is_valid(active_lease.lease_epoch)
                        && task.assigned_agent_id.as_deref() == Some(req.agent_id.as_str())
                });
                if !lease_matches {
                    continue;
                }

                if let Some(run) = runs.get_run(&task.run_id) {
                    if run.state == RunState::Cancelling {
                        directives.push(AgentDirective {
                            directive: Some(agent_directive::Directive::CancelTask(CancelTask {
                                task_id: active_lease.task_id.clone(),
                                lease_epoch: active_lease.lease_epoch,
                            })),
                        });
                    }
                }
            }
        }
    }

    rapidbyte_metrics::instruments::controller::heartbeat_received().add(
        1,
        &[KeyValue::new(
            rapidbyte_metrics::labels::AGENT_ID,
            req.agent_id.clone(),
        )],
    );
    Ok(Response::new(HeartbeatResponse { directives }))
}

async fn rollback_renewed_tasks(state: &ControllerState, snapshot_tasks: Vec<TaskRecord>) {
    let mut tasks = state.tasks.write().await;
    for snapshot_task in snapshot_tasks {
        tasks.restore_task(snapshot_task);
    }
}
