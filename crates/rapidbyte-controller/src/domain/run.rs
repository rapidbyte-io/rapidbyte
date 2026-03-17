use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::error::DomainError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    BeforeCommit,
    AfterCommitUnknown,
    AfterCommitConfirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetrics {
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct Run {
    id: String,
    idempotency_key: Option<String>,
    pipeline_name: String,
    pipeline_yaml: String,
    state: RunState,
    current_attempt: u32,
    max_retries: u32,
    timeout_seconds: Option<u64>,
    cancel_requested: bool,
    error: Option<RunError>,
    metrics: Option<RunMetrics>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Run {
    #[must_use]
    pub fn new(
        id: String,
        idempotency_key: Option<String>,
        pipeline_name: String,
        pipeline_yaml: String,
        max_retries: u32,
        timeout_seconds: Option<u64>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            idempotency_key,
            pipeline_name,
            pipeline_yaml,
            state: RunState::Pending,
            current_attempt: 1,
            max_retries,
            timeout_seconds,
            cancel_requested: false,
            error: None,
            metrics: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Rebuild a `Run` from a database row. No invariant checks are performed.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_row(
        id: String,
        idempotency_key: Option<String>,
        pipeline_name: String,
        pipeline_yaml: String,
        state: RunState,
        current_attempt: u32,
        max_retries: u32,
        timeout_seconds: Option<u64>,
        cancel_requested: bool,
        error: Option<RunError>,
        metrics: Option<RunMetrics>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            idempotency_key,
            pipeline_name,
            pipeline_yaml,
            state,
            current_attempt,
            max_retries,
            timeout_seconds,
            cancel_requested,
            error,
            metrics,
            created_at,
            updated_at,
        }
    }

    // --- State transitions ---

    /// Pending -> Running
    pub fn start(&mut self) -> Result<(), DomainError> {
        if self.state != RunState::Pending {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Running",
            });
        }
        self.state = RunState::Running;
        Ok(())
    }

    /// Running -> Completed
    pub fn complete(&mut self, metrics: RunMetrics) -> Result<(), DomainError> {
        if self.state != RunState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Completed",
            });
        }
        self.state = RunState::Completed;
        self.metrics = Some(metrics);
        Ok(())
    }

    /// Running -> Failed
    pub fn fail(&mut self, error: RunError) -> Result<(), DomainError> {
        if self.state != RunState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Failed",
            });
        }
        self.state = RunState::Failed;
        self.error = Some(error);
        Ok(())
    }

    /// Pending|Running -> Cancelled
    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.state != RunState::Pending && self.state != RunState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Cancelled",
            });
        }
        self.state = RunState::Cancelled;
        Ok(())
    }

    /// Running -> Pending (increments attempt, returns new attempt number)
    pub fn retry(&mut self) -> Result<u32, DomainError> {
        if self.state != RunState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Pending",
            });
        }
        self.state = RunState::Pending;
        self.current_attempt += 1;
        self.error = None;
        Ok(self.current_attempt)
    }

    // --- Decision methods ---

    /// Sets `cancel_requested` without changing state.
    pub fn request_cancel(&mut self) {
        self.cancel_requested = true;
    }

    /// Whether this run can be retried after an error.
    #[must_use]
    pub fn can_retry_after_error(&self, retryable: bool, commit_state: &CommitState) -> bool {
        retryable
            && *commit_state == CommitState::BeforeCommit
            && self.current_attempt < self.max_retries + 1
    }

    /// Whether this run can be retried after a timeout.
    #[must_use]
    pub fn can_retry_after_timeout(&self) -> bool {
        self.current_attempt < self.max_retries + 1
    }

    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    // --- Getters ---

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    #[must_use]
    pub fn pipeline_name(&self) -> &str {
        &self.pipeline_name
    }

    #[must_use]
    pub fn pipeline_yaml(&self) -> &str {
        &self.pipeline_yaml
    }

    #[must_use]
    pub fn state(&self) -> RunState {
        self.state
    }

    #[must_use]
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    #[must_use]
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    #[must_use]
    pub fn timeout_seconds(&self) -> Option<u64> {
        self.timeout_seconds
    }

    #[must_use]
    pub fn error(&self) -> Option<&RunError> {
        self.error.as_ref()
    }

    #[must_use]
    pub fn metrics(&self) -> Option<&RunMetrics> {
        self.metrics.as_ref()
    }

    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn set_updated_at(&mut self, now: DateTime<Utc>) {
        self.updated_at = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_000_000, 0).unwrap()
    }

    fn make_run() -> Run {
        Run::new(
            "run-1".into(),
            Some("idem-1".into()),
            "my-pipeline".into(),
            "yaml: true".into(),
            2,
            Some(60),
            now(),
        )
    }

    fn sample_metrics() -> RunMetrics {
        RunMetrics {
            rows_read: 100,
            rows_written: 50,
            bytes_read: 1024,
            bytes_written: 512,
            duration_ms: 5000,
        }
    }

    fn sample_error() -> RunError {
        RunError {
            code: "E001".into(),
            message: "something broke".into(),
        }
    }

    // --- Constructor tests ---

    #[test]
    fn new_run_starts_pending_attempt_1() {
        let run = make_run();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 1);
        assert_eq!(run.id(), "run-1");
        assert_eq!(run.idempotency_key(), Some("idem-1"));
        assert_eq!(run.pipeline_name(), "my-pipeline");
        assert_eq!(run.pipeline_yaml(), "yaml: true");
        assert_eq!(run.max_retries(), 2);
        assert_eq!(run.timeout_seconds(), Some(60));
        assert!(!run.is_cancel_requested());
        assert!(run.error().is_none());
        assert!(run.metrics().is_none());
    }

    // --- Valid transitions ---

    #[test]
    fn pending_to_running() {
        let mut run = make_run();
        assert!(run.start().is_ok());
        assert_eq!(run.state(), RunState::Running);
    }

    #[test]
    fn running_to_completed() {
        let mut run = make_run();
        run.start().unwrap();
        assert!(run.complete(sample_metrics()).is_ok());
        assert_eq!(run.state(), RunState::Completed);
        assert!(run.metrics().is_some());
        assert_eq!(run.metrics().unwrap().rows_read, 100);
    }

    #[test]
    fn running_to_failed() {
        let mut run = make_run();
        run.start().unwrap();
        assert!(run.fail(sample_error()).is_ok());
        assert_eq!(run.state(), RunState::Failed);
        assert_eq!(run.error().unwrap().code, "E001");
    }

    #[test]
    fn pending_to_cancelled() {
        let mut run = make_run();
        assert!(run.cancel().is_ok());
        assert_eq!(run.state(), RunState::Cancelled);
    }

    #[test]
    fn running_to_cancelled() {
        let mut run = make_run();
        run.start().unwrap();
        assert!(run.cancel().is_ok());
        assert_eq!(run.state(), RunState::Cancelled);
    }

    #[test]
    fn running_to_pending_via_retry() {
        let mut run = make_run();
        run.start().unwrap();
        let attempt = run.retry().unwrap();
        assert_eq!(attempt, 2);
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);
    }

    // --- Invalid transitions ---

    #[test]
    fn start_from_running_fails() {
        let mut run = make_run();
        run.start().unwrap();
        let err = run.start().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Running",
                to: "Running"
            }
        ));
    }

    #[test]
    fn start_from_completed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.complete(sample_metrics()).unwrap();
        let err = run.start().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Completed",
                ..
            }
        ));
    }

    #[test]
    fn start_from_failed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.fail(sample_error()).unwrap();
        let err = run.start().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition { from: "Failed", .. }
        ));
    }

    #[test]
    fn start_from_cancelled_fails() {
        let mut run = make_run();
        run.cancel().unwrap();
        let err = run.start().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Cancelled",
                ..
            }
        ));
    }

    #[test]
    fn complete_from_pending_fails() {
        let mut run = make_run();
        let err = run.complete(sample_metrics()).unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Completed"
            }
        ));
    }

    #[test]
    fn complete_from_failed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.fail(sample_error()).unwrap();
        let err = run.complete(sample_metrics()).unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Failed",
                to: "Completed"
            }
        ));
    }

    #[test]
    fn fail_from_pending_fails() {
        let mut run = make_run();
        let err = run.fail(sample_error()).unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Failed"
            }
        ));
    }

    #[test]
    fn fail_from_completed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.complete(sample_metrics()).unwrap();
        let err = run.fail(sample_error()).unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Completed",
                to: "Failed"
            }
        ));
    }

    #[test]
    fn cancel_from_completed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.complete(sample_metrics()).unwrap();
        let err = run.cancel().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Completed",
                to: "Cancelled"
            }
        ));
    }

    #[test]
    fn cancel_from_failed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.fail(sample_error()).unwrap();
        let err = run.cancel().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Failed",
                to: "Cancelled"
            }
        ));
    }

    #[test]
    fn cancel_from_cancelled_fails() {
        let mut run = make_run();
        run.cancel().unwrap();
        let err = run.cancel().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Cancelled",
                to: "Cancelled"
            }
        ));
    }

    #[test]
    fn retry_from_pending_fails() {
        let mut run = make_run();
        let err = run.retry().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Pending"
            }
        ));
    }

    #[test]
    fn retry_from_completed_fails() {
        let mut run = make_run();
        run.start().unwrap();
        run.complete(sample_metrics()).unwrap();
        let err = run.retry().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Completed",
                ..
            }
        ));
    }

    // --- Decision method tests ---

    #[test]
    fn can_retry_after_error_all_conditions_met() {
        let run = make_run(); // attempt=1, max_retries=2
        assert!(run.can_retry_after_error(true, &CommitState::BeforeCommit));
    }

    #[test]
    fn can_retry_after_error_not_retryable() {
        let run = make_run();
        assert!(!run.can_retry_after_error(false, &CommitState::BeforeCommit));
    }

    #[test]
    fn can_retry_after_error_after_commit_unknown() {
        let run = make_run();
        assert!(!run.can_retry_after_error(true, &CommitState::AfterCommitUnknown));
    }

    #[test]
    fn can_retry_after_error_after_commit_confirmed() {
        let run = make_run();
        assert!(!run.can_retry_after_error(true, &CommitState::AfterCommitConfirmed));
    }

    #[test]
    fn can_retry_after_error_attempts_exhausted() {
        // max_retries=2 means max 3 attempts total (1 original + 2 retries)
        let run = Run::from_row(
            "r".into(),
            None,
            "p".into(),
            "y".into(),
            RunState::Running,
            3, // attempt 3 of max_retries=2 -> 3 >= 2+1 -> exhausted
            2,
            None,
            false,
            None,
            None,
            now(),
            now(),
        );
        assert!(!run.can_retry_after_error(true, &CommitState::BeforeCommit));
    }

    #[test]
    fn can_retry_after_timeout_true() {
        let run = make_run(); // attempt=1, max_retries=2
        assert!(run.can_retry_after_timeout());
    }

    #[test]
    fn can_retry_after_timeout_exhausted() {
        let run = Run::from_row(
            "r".into(),
            None,
            "p".into(),
            "y".into(),
            RunState::Running,
            3,
            2,
            None,
            false,
            None,
            None,
            now(),
            now(),
        );
        assert!(!run.can_retry_after_timeout());
    }

    #[test]
    fn request_cancel_sets_flag() {
        let mut run = make_run();
        assert!(!run.is_cancel_requested());
        run.request_cancel();
        assert!(run.is_cancel_requested());
        // State should remain Pending
        assert_eq!(run.state(), RunState::Pending);
    }

    #[test]
    fn set_updated_at() {
        let mut run = make_run();
        let later = Utc.timestamp_opt(2_000_000, 0).unwrap();
        run.set_updated_at(later);
        assert_eq!(run.updated_at(), later);
    }

    #[test]
    fn from_row_preserves_all_fields() {
        let run = Run::from_row(
            "id-x".into(),
            Some("key-x".into()),
            "pipe-x".into(),
            "yaml-x".into(),
            RunState::Failed,
            3,
            5,
            Some(120),
            true,
            Some(RunError {
                code: "E".into(),
                message: "m".into(),
            }),
            Some(RunMetrics {
                rows_read: 1,
                rows_written: 2,
                bytes_read: 3,
                bytes_written: 4,
                duration_ms: 5,
            }),
            now(),
            now(),
        );
        assert_eq!(run.id(), "id-x");
        assert_eq!(run.idempotency_key(), Some("key-x"));
        assert_eq!(run.state(), RunState::Failed);
        assert_eq!(run.current_attempt(), 3);
        assert_eq!(run.max_retries(), 5);
        assert_eq!(run.timeout_seconds(), Some(120));
        assert!(run.is_cancel_requested());
        assert!(run.error().is_some());
        assert!(run.metrics().is_some());
    }

    #[test]
    fn retry_clears_previous_error() {
        let mut run = make_run();
        run.start().unwrap();
        // Simulate that an error was set on the run before retry
        run.error = Some(sample_error());
        let _ = run.retry().unwrap();
        assert!(run.error().is_none());
    }
}
