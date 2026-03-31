//! Pure conversion functions between domain types and proto types.

use chrono::{DateTime, Utc};
use tonic::Status;

use crate::application::error::AppError;
use crate::domain::error::DomainError;
use crate::domain::event::DomainEvent;
use crate::domain::run::{Run, RunState};
use crate::proto::rapidbyte::v1 as pb;
use crate::traits::ServiceError;

/// Map an `AppError` to a gRPC `Status` with the appropriate code.
#[allow(clippy::needless_pass_by_value, clippy::must_use_candidate)]
pub fn app_error_to_status(err: AppError) -> Status {
    match &err {
        AppError::Domain(
            DomainError::InvalidTransition { .. } | DomainError::RetriesExhausted { .. },
        ) => Status::failed_precondition(err.to_string()),
        AppError::Domain(DomainError::LeaseMismatch { .. } | DomainError::LeaseExpired) => {
            Status::aborted(err.to_string())
        }
        AppError::Domain(DomainError::AgentMismatch { .. }) => {
            Status::permission_denied(err.to_string())
        }
        AppError::Domain(DomainError::InvalidPipeline(_)) => {
            Status::invalid_argument(err.to_string())
        }
        AppError::NotFound { .. } => Status::not_found(err.to_string()),
        AppError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        AppError::Repository(_) | AppError::EventBus(_) => Status::internal("internal error"),
        AppError::SecretResolution(_) => Status::internal("secret resolution failed"),
    }
}

/// Map a `ServiceError` to a gRPC `Status` with the appropriate code.
#[allow(clippy::needless_pass_by_value, clippy::must_use_candidate)]
pub fn service_error_to_status(err: ServiceError) -> Status {
    match err {
        ServiceError::NotFound { resource, id } => {
            Status::not_found(format!("{resource} '{id}' not found"))
        }
        ServiceError::Conflict { message } => Status::already_exists(message),
        ServiceError::ValidationFailed { details } => {
            let msg = details
                .iter()
                .map(|d| format!("{}: {}", d.field, d.reason))
                .collect::<Vec<_>>()
                .join(", ");
            Status::invalid_argument(msg)
        }
        ServiceError::Unauthorized => Status::unauthenticated("unauthorized"),
        ServiceError::Internal { message } => Status::internal(message),
        ServiceError::NotImplemented { feature } => Status::unimplemented(feature),
    }
}

#[must_use]
pub fn to_proto_timestamp(dt: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos().cast_signed(),
    }
}

/// Convert a domain `RunState` to the proto enum integer value.
#[must_use]
pub fn run_state_to_proto(state: RunState) -> i32 {
    match state {
        RunState::Pending => pb::RunState::Pending.into(),
        RunState::Running => pb::RunState::Running.into(),
        RunState::Completed => pb::RunState::Completed.into(),
        RunState::Failed => pb::RunState::Failed.into(),
        RunState::Cancelled => pb::RunState::Cancelled.into(),
    }
}

/// Convert a proto `RunState` integer value to a domain `RunState`, returning
/// `None` for unspecified or unknown values.
#[must_use]
pub fn proto_to_run_state_filter(state: i32) -> Option<RunState> {
    match pb::RunState::try_from(state) {
        Ok(pb::RunState::Pending) => Some(RunState::Pending),
        Ok(pb::RunState::Running) => Some(RunState::Running),
        Ok(pb::RunState::Completed) => Some(RunState::Completed),
        Ok(pb::RunState::Failed) => Some(RunState::Failed),
        Ok(pb::RunState::Cancelled) => Some(RunState::Cancelled),
        Ok(pb::RunState::Unspecified) | Err(_) => None,
    }
}

/// Build a `RunDetail` proto message from a domain `Run`.
#[must_use]
pub fn run_to_detail(run: &Run) -> pb::RunDetail {
    pb::RunDetail {
        run_id: run.id().to_string(),
        pipeline_name: run.pipeline_name().to_string(),
        state: run_state_to_proto(run.state()),
        cancel_requested: run.is_cancel_requested(),
        attempt: run.current_attempt(),
        max_retries: run.max_retries(),
        metrics: run.metrics().map(|m| pb::RunMetrics {
            rows_read: m.rows_read,
            rows_written: m.rows_written,
            bytes_read: m.bytes_read,
            bytes_written: m.bytes_written,
            duration_ms: m.duration_ms,
        }),
        error: run.error().map(|e| pb::RunError {
            code: e.code.clone(),
            message: e.message.clone(),
        }),
        created_at: Some(to_proto_timestamp(run.created_at())),
        updated_at: Some(to_proto_timestamp(run.updated_at())),
    }
}

/// Build a `RunSummary` proto message from a domain `Run`.
#[must_use]
pub fn run_to_summary(run: &Run) -> pb::RunSummary {
    pb::RunSummary {
        run_id: run.id().to_string(),
        pipeline_name: run.pipeline_name().to_string(),
        state: run_state_to_proto(run.state()),
        attempt: run.current_attempt(),
        created_at: Some(to_proto_timestamp(run.created_at())),
    }
}

/// Convert a `DomainEvent` to a `RunEvent` proto message.
#[must_use]
pub fn domain_event_to_run_event(event: &DomainEvent) -> pb::RunEvent {
    let now = Utc::now();
    match event {
        DomainEvent::RunStateChanged {
            run_id,
            state,
            attempt,
        } => pb::RunEvent {
            run_id: run_id.clone(),
            timestamp: Some(to_proto_timestamp(now)),
            state: run_state_to_proto(*state),
            attempt: *attempt,
            detail: None,
        },
        DomainEvent::ProgressReported {
            run_id,
            message,
            pct,
        } => pb::RunEvent {
            run_id: run_id.clone(),
            timestamp: Some(to_proto_timestamp(now)),
            state: pb::RunState::Running.into(),
            attempt: 0,
            detail: Some(pb::run_event::Detail::Progress(pb::ProgressDetail {
                message: message.clone(),
                progress_pct: *pct,
            })),
        },
        DomainEvent::RunCompleted { run_id, metrics } => pb::RunEvent {
            run_id: run_id.clone(),
            timestamp: Some(to_proto_timestamp(now)),
            state: pb::RunState::Completed.into(),
            attempt: 0,
            detail: Some(pb::run_event::Detail::Completed(pb::CompletionDetail {
                metrics: Some(pb::RunMetrics {
                    rows_read: metrics.rows_read,
                    rows_written: metrics.rows_written,
                    bytes_read: metrics.bytes_read,
                    bytes_written: metrics.bytes_written,
                    duration_ms: metrics.duration_ms,
                }),
            })),
        },
        DomainEvent::RunFailed { run_id, error } => pb::RunEvent {
            run_id: run_id.clone(),
            timestamp: Some(to_proto_timestamp(now)),
            state: pb::RunState::Failed.into(),
            attempt: 0,
            detail: Some(pb::run_event::Detail::Failed(pb::FailureDetail {
                error_code: error.code.clone(),
                error_message: error.message.clone(),
                retryable: false,
                commit_state: pb::CommitState::Unspecified.into(),
            })),
        },
        DomainEvent::RunCancelled { run_id } => pb::RunEvent {
            run_id: run_id.clone(),
            timestamp: Some(to_proto_timestamp(now)),
            state: pb::RunState::Cancelled.into(),
            attempt: 0,
            detail: None,
        },
    }
}

/// Build a `RunEvent` representing the current state of a run (for the initial
/// event in a watch stream).
#[must_use]
pub fn run_state_to_event(run: &Run) -> pb::RunEvent {
    let now = Utc::now();
    let detail = match run.state() {
        RunState::Completed => run.metrics().map(|m| {
            pb::run_event::Detail::Completed(pb::CompletionDetail {
                metrics: Some(pb::RunMetrics {
                    rows_read: m.rows_read,
                    rows_written: m.rows_written,
                    bytes_read: m.bytes_read,
                    bytes_written: m.bytes_written,
                    duration_ms: m.duration_ms,
                }),
            })
        }),
        RunState::Failed => run.error().map(|e| {
            pb::run_event::Detail::Failed(pb::FailureDetail {
                error_code: e.code.clone(),
                error_message: e.message.clone(),
                retryable: false,
                commit_state: pb::CommitState::Unspecified.into(),
            })
        }),
        _ => None,
    };

    pb::RunEvent {
        run_id: run.id().to_string(),
        timestamp: Some(to_proto_timestamp(now)),
        state: run_state_to_proto(run.state()),
        attempt: run.current_attempt(),
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::run::{RunError, RunMetrics};
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_000_000, 0).unwrap()
    }

    fn make_run(state: RunState) -> Run {
        Run::from_row(
            "run-1".into(),
            Some("idem-1".into()),
            "my-pipeline".into(),
            "yaml: true".into(),
            state,
            2,
            3,
            Some(60),
            false,
            if state == RunState::Failed {
                Some(RunError {
                    code: "E001".into(),
                    message: "boom".into(),
                })
            } else {
                None
            },
            if state == RunState::Completed {
                Some(RunMetrics {
                    rows_read: 100,
                    rows_written: 50,
                    bytes_read: 1024,
                    bytes_written: 512,
                    duration_ms: 5000,
                })
            } else {
                None
            },
            None,
            now(),
            now(),
        )
    }

    // --- app_error_to_status tests ---

    #[test]
    fn invalid_transition_maps_to_failed_precondition() {
        let err = AppError::Domain(DomainError::InvalidTransition {
            from: "Pending",
            to: "Completed",
        });
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn lease_mismatch_maps_to_aborted() {
        let err = AppError::Domain(DomainError::LeaseMismatch {
            expected: 5,
            got: 3,
        });
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Aborted);
    }

    #[test]
    fn agent_mismatch_maps_to_permission_denied() {
        let err = AppError::Domain(DomainError::AgentMismatch {
            expected: "a".into(),
            got: "b".into(),
        });
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn lease_expired_maps_to_aborted() {
        let err = AppError::Domain(DomainError::LeaseExpired);
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Aborted);
    }

    #[test]
    fn retries_exhausted_maps_to_failed_precondition() {
        let err = AppError::Domain(DomainError::RetriesExhausted { attempt: 3, max: 3 });
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn invalid_pipeline_maps_to_invalid_argument() {
        let err = AppError::Domain(DomainError::InvalidPipeline("bad".into()));
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn not_found_maps_to_not_found() {
        let err = AppError::NotFound {
            entity: "Run",
            id: "x".into(),
        };
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[test]
    fn already_exists_maps_to_already_exists() {
        let err = AppError::AlreadyExists { run_id: "x".into() };
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    #[test]
    fn repository_error_maps_to_internal() {
        use crate::domain::ports::repository::RepositoryError;
        let err = AppError::Repository(RepositoryError::Other(Box::new(std::io::Error::other(
            "db down",
        ))));
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
    }

    #[test]
    fn secret_resolution_maps_to_internal() {
        let err = AppError::SecretResolution("vault down".into());
        let status = app_error_to_status(err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "secret resolution failed");
    }

    // --- run_state_to_proto / proto_to_run_state_filter tests ---

    #[test]
    fn run_state_roundtrip() {
        let states = [
            RunState::Pending,
            RunState::Running,
            RunState::Completed,
            RunState::Failed,
            RunState::Cancelled,
        ];
        for state in states {
            let proto_val = run_state_to_proto(state);
            let back = proto_to_run_state_filter(proto_val);
            assert_eq!(back, Some(state), "roundtrip failed for {state:?}");
        }
    }

    #[test]
    fn unspecified_run_state_returns_none() {
        assert_eq!(proto_to_run_state_filter(0), None);
    }

    #[test]
    fn unknown_run_state_returns_none() {
        assert_eq!(proto_to_run_state_filter(999), None);
    }

    // --- run_to_detail tests ---

    #[test]
    fn run_to_detail_pending() {
        let run = make_run(RunState::Pending);
        let detail = run_to_detail(&run);
        assert_eq!(detail.run_id, "run-1");
        assert_eq!(detail.pipeline_name, "my-pipeline");
        assert_eq!(detail.state, run_state_to_proto(RunState::Pending));
        assert_eq!(detail.attempt, 2);
        assert_eq!(detail.max_retries, 3);
        assert!(detail.metrics.is_none());
        assert!(detail.error.is_none());
        assert!(detail.created_at.is_some());
        assert!(detail.updated_at.is_some());
    }

    #[test]
    fn run_to_detail_completed_has_metrics() {
        let run = make_run(RunState::Completed);
        let detail = run_to_detail(&run);
        assert_eq!(detail.state, run_state_to_proto(RunState::Completed));
        let m = detail.metrics.unwrap();
        assert_eq!(m.rows_read, 100);
        assert_eq!(m.rows_written, 50);
    }

    #[test]
    fn run_to_detail_failed_has_error() {
        let run = make_run(RunState::Failed);
        let detail = run_to_detail(&run);
        assert_eq!(detail.state, run_state_to_proto(RunState::Failed));
        let e = detail.error.unwrap();
        assert_eq!(e.code, "E001");
        assert_eq!(e.message, "boom");
    }

    // --- run_to_summary tests ---

    #[test]
    fn run_to_summary_fields() {
        let run = make_run(RunState::Running);
        let summary = run_to_summary(&run);
        assert_eq!(summary.run_id, "run-1");
        assert_eq!(summary.pipeline_name, "my-pipeline");
        assert_eq!(summary.state, run_state_to_proto(RunState::Running));
        assert_eq!(summary.attempt, 2);
        assert!(summary.created_at.is_some());
    }

    // --- domain_event_to_run_event tests ---

    #[test]
    fn state_changed_event() {
        let event = DomainEvent::RunStateChanged {
            run_id: "r1".into(),
            state: RunState::Running,
            attempt: 1,
        };
        let proto = domain_event_to_run_event(&event);
        assert_eq!(proto.run_id, "r1");
        assert_eq!(proto.state, run_state_to_proto(RunState::Running));
        assert_eq!(proto.attempt, 1);
        assert!(proto.detail.is_none());
    }

    #[test]
    fn progress_event() {
        let event = DomainEvent::ProgressReported {
            run_id: "r1".into(),
            message: "50% done".into(),
            pct: Some(50.0),
        };
        let proto = domain_event_to_run_event(&event);
        assert_eq!(proto.run_id, "r1");
        match proto.detail {
            Some(pb::run_event::Detail::Progress(p)) => {
                assert_eq!(p.message, "50% done");
                assert_eq!(p.progress_pct, Some(50.0));
            }
            other => panic!("expected Progress, got {other:?}"),
        }
    }

    #[test]
    fn completed_event() {
        let event = DomainEvent::RunCompleted {
            run_id: "r1".into(),
            metrics: crate::domain::run::RunMetrics {
                rows_read: 10,
                rows_written: 5,
                bytes_read: 100,
                bytes_written: 50,
                duration_ms: 1000,
            },
        };
        let proto = domain_event_to_run_event(&event);
        assert_eq!(proto.state, run_state_to_proto(RunState::Completed));
        match proto.detail {
            Some(pb::run_event::Detail::Completed(c)) => {
                let m = c.metrics.unwrap();
                assert_eq!(m.rows_read, 10);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[test]
    fn failed_event() {
        let event = DomainEvent::RunFailed {
            run_id: "r1".into(),
            error: crate::domain::run::RunError {
                code: "E001".into(),
                message: "boom".into(),
            },
        };
        let proto = domain_event_to_run_event(&event);
        assert_eq!(proto.state, run_state_to_proto(RunState::Failed));
        match proto.detail {
            Some(pb::run_event::Detail::Failed(f)) => {
                assert_eq!(f.error_code, "E001");
                assert_eq!(f.error_message, "boom");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn cancelled_event() {
        let event = DomainEvent::RunCancelled {
            run_id: "r1".into(),
        };
        let proto = domain_event_to_run_event(&event);
        assert_eq!(proto.state, run_state_to_proto(RunState::Cancelled));
        assert!(proto.detail.is_none());
    }

    // --- run_state_to_event tests ---

    #[test]
    fn run_state_to_event_for_completed_run() {
        let run = make_run(RunState::Completed);
        let event = run_state_to_event(&run);
        assert_eq!(event.run_id, "run-1");
        assert_eq!(event.state, run_state_to_proto(RunState::Completed));
        assert!(matches!(
            event.detail,
            Some(pb::run_event::Detail::Completed(_))
        ));
    }

    #[test]
    fn run_state_to_event_for_failed_run() {
        let run = make_run(RunState::Failed);
        let event = run_state_to_event(&run);
        assert_eq!(event.state, run_state_to_proto(RunState::Failed));
        assert!(matches!(
            event.detail,
            Some(pb::run_event::Detail::Failed(_))
        ));
    }

    #[test]
    fn run_state_to_event_for_pending_run() {
        let run = make_run(RunState::Pending);
        let event = run_state_to_event(&run);
        assert_eq!(event.state, run_state_to_proto(RunState::Pending));
        assert!(event.detail.is_none());
    }
}
