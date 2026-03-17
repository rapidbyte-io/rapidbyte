//! Terminal state transition helper.
//!
//! Consolidates the repeated pattern of transitioning a run to a terminal
//! state, setting error/metrics on the run record, publishing a terminal
//! event to watchers, and recording completion metrics.

use opentelemetry::KeyValue;

use crate::proto::rapidbyte::v2::{
    run_event, RunCancelled, RunCompleted, RunEvent, RunFailed, TaskError,
};
use crate::run_state::{RunError, RunMetrics, RunState};
use crate::state::ControllerState;

/// Describes how a run reached its terminal state.
pub enum TerminalOutcome {
    Completed {
        metrics: RunMetrics,
    },
    Failed {
        error: Option<RunError>,
        attempt: u32,
    },
    Cancelled,
    TimedOut {
        error: RunError,
        attempt: u32,
    },
    RecoveryFailed {
        error: RunError,
        attempt: u32,
    },
}

impl TerminalOutcome {
    /// The internal run state this outcome maps to.
    fn target_state(&self) -> RunState {
        match self {
            Self::Completed { .. } => RunState::Completed,
            Self::Failed { .. } => RunState::Failed,
            Self::Cancelled => RunState::Cancelled,
            Self::TimedOut { .. } => RunState::TimedOut,
            Self::RecoveryFailed { .. } => RunState::RecoveryFailed,
        }
    }

    /// Whether `ensure_running` should be called before the transition.
    ///
    /// Agent-initiated completions (`Completed`, `Failed`, `Cancelled`)
    /// call `ensure_running` because the run may still be in `Assigned`
    /// or `Reconciling` state. Background tasks (`TimedOut`,
    /// `RecoveryFailed`) skip this because the run may be in `Assigned`
    /// or `Reconciling` state and the transition is valid from those
    /// states directly.
    fn needs_ensure_running(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled
        )
    }

    /// Metric status label for the `runs_completed` counter.
    fn status_label(&self) -> &'static str {
        match self {
            Self::Completed { .. } => rapidbyte_metrics::labels::STATUS_OK,
            Self::Failed { .. } | Self::TimedOut { .. } | Self::RecoveryFailed { .. } => {
                rapidbyte_metrics::labels::STATUS_ERROR
            }
            Self::Cancelled => rapidbyte_metrics::labels::STATUS_CANCELLED,
        }
    }
}

/// Transition a run to its terminal state, set error/metrics, publish
/// the terminal event to watchers, and record completion metrics.
///
/// The transition is idempotent — callers that need to persist the
/// terminal state durably before publishing MAY pre-transition the run.
/// In that case, `finalize_terminal` handles event publishing and
/// metric recording only (the redundant transition is a harmless no-op).
pub async fn finalize_terminal(state: &ControllerState, run_id: &str, outcome: TerminalOutcome) {
    let target = outcome.target_state();

    // Perform the state transition and set error/metrics on the record.
    {
        let mut runs = state.runs.write().await;
        if outcome.needs_ensure_running() {
            runs.ensure_running(run_id);
        }
        let _ = runs.transition(run_id, target);
        if let Some(record) = runs.get_run_mut(run_id) {
            match &outcome {
                TerminalOutcome::Completed { metrics } => {
                    record.metrics = metrics.clone();
                }
                TerminalOutcome::Failed { error, .. } => {
                    record.error.clone_from(error);
                }
                TerminalOutcome::Cancelled => {}
                TerminalOutcome::TimedOut { error, .. }
                | TerminalOutcome::RecoveryFailed { error, .. } => {
                    record.error = Some(error.clone());
                }
            }
        }
    }

    // Build the proto event.
    let event = match &outcome {
        TerminalOutcome::Completed { metrics } => run_event::Event::Completed(RunCompleted {
            total_records: metrics.total_records,
            total_bytes: metrics.total_bytes,
            elapsed_seconds: metrics.elapsed_seconds,
            cursors_advanced: metrics.cursors_advanced,
        }),
        TerminalOutcome::Failed { error, attempt } => run_event::Event::Failed(RunFailed {
            error: error.as_ref().map(|e| TaskError {
                code: e.code.clone(),
                message: e.message.clone(),
                retryable: e.retryable,
                safe_to_retry: e.safe_to_retry,
                commit_state: e.commit_state.clone(),
            }),
            attempt: *attempt,
        }),
        TerminalOutcome::Cancelled => run_event::Event::Cancelled(RunCancelled {}),
        TerminalOutcome::TimedOut { error, attempt }
        | TerminalOutcome::RecoveryFailed { error, attempt } => {
            run_event::Event::Failed(RunFailed {
                error: Some(TaskError {
                    code: error.code.clone(),
                    message: error.message.clone(),
                    retryable: error.retryable,
                    safe_to_retry: error.safe_to_retry,
                    commit_state: error.commit_state.clone(),
                }),
                attempt: *attempt,
            })
        }
    };

    // Publish terminal event and remove watcher channel.
    state.watchers.write().await.publish_terminal(
        run_id,
        RunEvent {
            run_id: run_id.to_owned(),
            detail: String::new(),
            event: Some(event),
        },
    );

    // Record metrics.
    let status = outcome.status_label();
    rapidbyte_metrics::instruments::controller::runs_completed().add(
        1,
        &[KeyValue::new(rapidbyte_metrics::labels::STATUS, status)],
    );
    rapidbyte_metrics::instruments::controller::active_runs().add(-1, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_state::RunState;

    #[tokio::test]
    async fn finalize_completed_transitions_and_publishes() {
        let state = ControllerState::new(b"test-signing-key");
        let run_id = {
            let mut runs = state.runs.write().await;
            let (id, _) = runs.create_run("run-1".into(), "pipe".into(), None);
            runs.transition(&id, RunState::Assigned).unwrap();
            runs.transition(&id, RunState::Running).unwrap();
            id
        };
        let mut rx = state.watchers.write().await.subscribe(&run_id);

        let metrics = RunMetrics {
            total_records: 42,
            total_bytes: 1024,
            elapsed_seconds: 1.5,
            cursors_advanced: 2,
        };
        finalize_terminal(&state, &run_id, TerminalOutcome::Completed { metrics }).await;

        // Assert state transitioned
        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, RunState::Completed);
        assert_eq!(record.metrics.total_records, 42);
        assert_eq!(record.metrics.total_bytes, 1024);
        drop(runs);

        // Assert terminal event was published
        let event = rx.try_recv().expect("should have received terminal event");
        assert_eq!(event.run_id, run_id);
        match event.event {
            Some(run_event::Event::Completed(c)) => {
                assert_eq!(c.total_records, 42);
                assert_eq!(c.total_bytes, 1024);
            }
            other => panic!("expected Completed event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn finalize_failed_sets_error_on_record() {
        let state = ControllerState::new(b"test-signing-key");
        let run_id = {
            let mut runs = state.runs.write().await;
            let (id, _) = runs.create_run("run-1".into(), "pipe".into(), None);
            runs.transition(&id, RunState::Assigned).unwrap();
            runs.transition(&id, RunState::Running).unwrap();
            id
        };
        let mut rx = state.watchers.write().await.subscribe(&run_id);

        let error = RunError {
            code: "PLUGIN_CRASH".into(),
            message: "segfault in source".into(),
            retryable: false,
            safe_to_retry: false,
            commit_state: "before_commit".into(),
        };
        finalize_terminal(
            &state,
            &run_id,
            TerminalOutcome::Failed {
                error: Some(error),
                attempt: 2,
            },
        )
        .await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, RunState::Failed);
        let err = record.error.as_ref().expect("error should be set");
        assert_eq!(err.code, "PLUGIN_CRASH");
        assert_eq!(err.message, "segfault in source");
        drop(runs);

        // Assert terminal event was published with error
        let event = rx.try_recv().expect("should have received terminal event");
        match event.event {
            Some(run_event::Event::Failed(f)) => {
                assert_eq!(f.attempt, 2);
                let te = f.error.expect("proto error should be set");
                assert_eq!(te.code, "PLUGIN_CRASH");
            }
            other => panic!("expected Failed event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn finalize_cancelled_transitions_and_publishes() {
        let state = ControllerState::new(b"test-signing-key");
        let run_id = {
            let mut runs = state.runs.write().await;
            let (id, _) = runs.create_run("run-1".into(), "pipe".into(), None);
            runs.transition(&id, RunState::Assigned).unwrap();
            runs.transition(&id, RunState::Running).unwrap();
            id
        };
        let mut rx = state.watchers.write().await.subscribe(&run_id);

        finalize_terminal(&state, &run_id, TerminalOutcome::Cancelled).await;

        let runs = state.runs.read().await;
        let record = runs.get_run(&run_id).unwrap();
        assert_eq!(record.state, RunState::Cancelled);
        drop(runs);

        // Assert terminal event was published
        let event = rx.try_recv().expect("should have received terminal event");
        assert_eq!(event.run_id, run_id);
        assert!(matches!(event.event, Some(run_event::Event::Cancelled(_))));
    }
}
