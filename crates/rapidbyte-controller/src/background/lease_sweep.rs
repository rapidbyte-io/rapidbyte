//! Lease sweep and reconciliation timeout background task.

use std::time::{Duration, Instant};

use tracing::info;

use crate::run_state::{
    RunError, RunState as InternalRunState, ERROR_CODE_LEASE_EXPIRED, ERROR_CODE_RECOVERY_TIMEOUT,
};
use crate::state::ControllerState;

async fn rollback_background_timeout(
    state: &ControllerState,
    previous_run: crate::run_state::RunRecord,
    previous_task: Option<crate::scheduler::TaskRecord>,
) {
    {
        let mut runs = state.runs.write().await;
        runs.restore_run(previous_run);
    }
    if let Some(previous_task) = previous_task {
        let mut tasks = state.tasks.write().await;
        tasks.restore_task(previous_task);
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_expired_lease(
    state: &ControllerState,
    previous_task: crate::scheduler::TaskRecord,
) {
    let task_id = previous_task.task_id.clone();
    let run_id = previous_task.run_id.clone();
    let previous_run = state
        .runs
        .read()
        .await
        .get_run(&run_id)
        .cloned()
        .expect("run should exist for expired lease");
    let timeout_outcome = {
        let mut runs = state.runs.write().await;
        let target_state = if runs
            .get_run(&run_id)
            .is_some_and(|record| record.state == InternalRunState::Reconciling)
        {
            InternalRunState::RecoveryFailed
        } else {
            InternalRunState::TimedOut
        };

        if let Ok(()) = runs.transition(&run_id, target_state) {
            let record = runs.get_run_mut(&run_id);
            if let Some(r) = record {
                let error_info = if target_state == InternalRunState::RecoveryFailed {
                    RunError {
                        code: ERROR_CODE_RECOVERY_TIMEOUT.into(),
                        message: format!(
                            "Run recovery reconciliation timed out after controller restart for task {task_id}"
                        ),
                        retryable: false,
                        safe_to_retry: false,
                        commit_state: String::new(),
                    }
                } else {
                    RunError {
                        code: ERROR_CODE_LEASE_EXPIRED.into(),
                        message: format!("Task {task_id} lease expired (agent unresponsive)"),
                        retryable: true,
                        safe_to_retry: true,
                        commit_state: String::new(),
                    }
                };
                r.error = Some(error_info.clone());
                Some((target_state, error_info, r.attempt))
            } else {
                None
            }
        } else {
            let actual_state = runs.get_run(&run_id).map(|r| r.state);
            if let Some(actual_state) = actual_state {
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %run_id,
                    ?actual_state,
                    "Skipping lease-expiry terminal event because run could not transition to TimedOut"
                );
            }
            None
        }
    };
    let Some((target_state, error_info, attempt)) = timeout_outcome else {
        return;
    };
    let timed_out_run = state
        .runs
        .read()
        .await
        .get_run(&run_id)
        .cloned()
        .expect("timed-out run should exist");
    let timed_out_task = state
        .tasks
        .read()
        .await
        .get(&task_id)
        .cloned()
        .expect("timed-out task should exist");
    let persist_start = Instant::now();
    let durable = match state
        .persist_timeout_records(&timed_out_run, Some(&timed_out_task))
        .await
    {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(
                task_id = %task_id,
                run_id = %run_id,
                ?error,
                "failed to persist lease-expiry timeout transition"
            );
            false
        }
    };
    rapidbyte_metrics::instruments::controller::state_persist_duration()
        .record(persist_start.elapsed().as_secs_f64(), &[]);

    if !durable {
        rollback_background_timeout(state, previous_run, Some(previous_task)).await;
    }

    if durable {
        let outcome = if target_state == InternalRunState::RecoveryFailed {
            crate::terminal::TerminalOutcome::RecoveryFailed {
                error: error_info,
                attempt,
            }
        } else {
            crate::terminal::TerminalOutcome::TimedOut {
                error: error_info,
                attempt,
            }
        };
        crate::terminal::finalize_terminal(state, &run_id, outcome).await;
    } else {
        tracing::warn!(
            task_id = %task_id,
            run_id = %run_id,
            "skipping lease-expiry terminal publish because durable persistence failed"
        );
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn sweep_reconciliation_timeouts(
    state: &ControllerState,
    reconciliation_timeout: Duration,
) {
    let now = std::time::SystemTime::now();
    let stale_run_ids = {
        let runs = state.runs.read().await;
        runs.list_runs(Some(&[InternalRunState::Reconciling]))
            .into_iter()
            .filter(|run| {
                run.recovery_started_at
                    .and_then(|started_at| now.duration_since(started_at).ok())
                    .is_some_and(|elapsed| elapsed >= reconciliation_timeout)
            })
            .map(|run| run.run_id.clone())
            .collect::<Vec<_>>()
    };

    for run_id in stale_run_ids {
        let previous_run = state
            .runs
            .read()
            .await
            .get_run(&run_id)
            .cloned()
            .expect("reconciling run should exist");
        let task_id = previous_run
            .current_task
            .as_ref()
            .map(|task| task.task_id.clone());
        let previous_task = if let Some(task_id) = task_id.as_deref() {
            state.tasks.read().await.get(task_id).cloned()
        } else {
            None
        };
        let error_info = RunError {
            code: ERROR_CODE_RECOVERY_TIMEOUT.into(),
            message: "Run recovery reconciliation timed out after controller restart".into(),
            retryable: false,
            safe_to_retry: false,
            commit_state: String::new(),
        };

        let attempt = {
            let mut runs = state.runs.write().await;
            let Some(run) = runs.get_run(&run_id) else {
                continue;
            };
            if run.state != InternalRunState::Reconciling {
                continue;
            }
            let attempt = run.attempt;
            if runs
                .transition(&run_id, InternalRunState::RecoveryFailed)
                .is_err()
            {
                continue;
            }
            if let Some(run) = runs.get_run_mut(&run_id) {
                run.error = Some(error_info.clone());
            }
            attempt
        };

        let timed_out_task = if let Some(task_id) = task_id.as_deref() {
            let timed_out = {
                let mut tasks = state.tasks.write().await;
                tasks.mark_timed_out(task_id)
            };
            if timed_out {
                state.tasks.read().await.get(task_id).cloned()
            } else {
                None
            }
        } else {
            None
        };
        let recovery_failed_run = state
            .runs
            .read()
            .await
            .get_run(&run_id)
            .cloned()
            .expect("recovery-failed run should exist");
        let durable = match state
            .persist_timeout_records(&recovery_failed_run, timed_out_task.as_ref())
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(
                    run_id,
                    ?error,
                    "failed to persist reconciliation-timeout transition"
                );
                false
            }
        };

        if !durable {
            rollback_background_timeout(state, previous_run, previous_task).await;
        }

        if durable {
            crate::terminal::finalize_terminal(
                state,
                &run_id,
                crate::terminal::TerminalOutcome::RecoveryFailed {
                    error: error_info,
                    attempt,
                },
            )
            .await;
        } else {
            tracing::warn!(run_id, "skipping reconciliation-timeout terminal publish because durable persistence failed");
        }
    }
}

pub fn spawn_lease_sweep(
    state: ControllerState,
    lease_check_interval: Duration,
    reconciliation_timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(lease_check_interval);
        loop {
            interval.tick().await;
            let expired = state.tasks.write().await.expire_leases();
            for previous_task in expired {
                info!(task_id = %previous_task.task_id, run_id = %previous_task.run_id, "Task lease expired");
                handle_expired_lease(&state, previous_task).await;
            }
            sweep_reconciliation_timeouts(&state, reconciliation_timeout).await;
            rapidbyte_metrics::instruments::controller::reconciliation_sweeps().add(1, &[]);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::FailingMetadataStore;
    use std::time::Duration;

    #[tokio::test]
    async fn handle_expired_lease_transitions_assigned_run_to_timed_out() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
        }

        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::TimedOut);
        assert_eq!(
            record.error.as_ref().map(|e| e.message.as_str()),
            Some(format!("Task {task_id} lease expired (agent unresponsive)").as_str())
        );
    }

    #[tokio::test]
    async fn handle_expired_lease_transitions_reconciling_run_to_recovery_failed() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::RecoveryFailed);
        assert!(record
            .error
            .as_ref()
            .map(|e| e.message.as_str())
            .is_some_and(|message| message.contains("reconciliation timed out")));
    }

    #[tokio::test]
    async fn handle_expired_lease_rolls_back_when_persist_fails() {
        let state = ControllerState::with_metadata_store(
            b"test-signing-key",
            FailingMetadataStore::new().fail_task_upsert_on(1),
        );
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
        }

        let mut rx = state.watchers.write().await.subscribe(&run_id);
        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(
            recv.is_err(),
            "terminal event should not publish on persist failure"
        );
        assert_eq!(state.watchers.read().await.channel_count(), 1);
        let task = state.tasks.read().await.get(&task_id).unwrap().clone();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
        let run = state.runs.read().await.get_run(&run_id).unwrap().clone();
        assert_eq!(run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn handle_expired_lease_keeps_durable_state_consistent_when_timeout_persist_fails() {
        let store = FailingMetadataStore::new().fail_run_upsert_on(2);
        let state = ControllerState::with_metadata_store(b"test-signing-key", store.clone());
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
        }

        let previous_run = state.runs.read().await.get_run(&run_id).unwrap().clone();
        let previous_task = state.tasks.read().await.get(&task_id).unwrap().clone();
        state
            .persist_run_record(&previous_run)
            .await
            .expect("initial run persistence should succeed");
        state
            .persist_task_record(&previous_task)
            .await
            .expect("initial task persistence should succeed");

        {
            let mut tasks = state.tasks.write().await;
            assert!(tasks.mark_timed_out(&task_id));
        }
        handle_expired_lease(&state, previous_task).await;

        let durable_task = store
            .persisted_task(&task_id)
            .expect("durable task snapshot should exist");
        assert_eq!(durable_task.state, crate::scheduler::TaskState::Assigned);
        assert!(durable_task.lease.is_some());
        let durable_run = store
            .persisted_run(&run_id)
            .expect("durable run snapshot should exist");
        assert_eq!(durable_run.state, InternalRunState::Assigned);
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_fails_stale_reconciling_run() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().recovery_started_at = Some(
                std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(120))
                    .unwrap(),
            );
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(30)).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::RecoveryFailed);
        assert!(record
            .error
            .as_ref()
            .map(|e| e.message.as_str())
            .is_some_and(|message| message.contains("reconciliation timed out")));
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_rolls_back_when_persist_fails() {
        let state = ControllerState::with_metadata_store(
            b"test-signing-key",
            FailingMetadataStore::new().fail_run_upsert_on(1),
        );
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        let task_id = {
            let mut tasks = state.tasks.write().await;
            let task_id = tasks.enqueue(run_id.clone(), b"yaml".to_vec(), false, None, 1);
            let assignment = tasks
                .poll("agent-1", Duration::from_secs(60), &state.epoch_gen)
                .expect("task should be assigned");
            assert_eq!(assignment.task_id, task_id);
            task_id
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
            runs.get_run_mut(&run_id).unwrap().recovery_started_at = Some(
                std::time::SystemTime::now()
                    .checked_sub(Duration::from_secs(120))
                    .unwrap(),
            );
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(30)).await;

        let run = state.runs.read().await.get_run(&run_id).unwrap().clone();
        assert_eq!(run.state, InternalRunState::Reconciling);
        let task = state.tasks.read().await.get(&task_id).unwrap().clone();
        assert_eq!(task.state, crate::scheduler::TaskState::Assigned);
        assert!(task.lease.is_some());
    }

    #[tokio::test]
    async fn handle_reconciliation_timeout_leaves_fresh_reconciling_run_unchanged() {
        let state = ControllerState::new(b"test-signing-key");
        let (run_id, _) = {
            let mut runs = state.runs.write().await;
            runs.create_run("run-1".into(), "pipe".into(), None)
        };
        {
            let mut runs = state.runs.write().await;
            runs.transition(&run_id, InternalRunState::Assigned)
                .unwrap();
            runs.transition(&run_id, InternalRunState::Reconciling)
                .unwrap();
        }

        sweep_reconciliation_timeouts(&state, Duration::from_secs(300)).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, InternalRunState::Reconciling);
    }
}
