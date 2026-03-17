use chrono::{DateTime, Utc};

use super::error::DomainError;
use super::lease::Lease;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

impl TaskState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::TimedOut => "TimedOut",
        }
    }
}

#[derive(Clone)]
pub struct Task {
    id: String,
    run_id: String,
    attempt: u32,
    state: TaskState,
    agent_id: Option<String>,
    lease: Option<Lease>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Task {
    #[must_use]
    pub fn new(id: String, run_id: String, attempt: u32, now: DateTime<Utc>) -> Self {
        Self {
            id,
            run_id,
            attempt,
            state: TaskState::Pending,
            agent_id: None,
            lease: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Rebuild a `Task` from a database row. No invariant checks are performed.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn from_row(
        id: String,
        run_id: String,
        attempt: u32,
        state: TaskState,
        agent_id: Option<String>,
        lease: Option<Lease>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            run_id,
            attempt,
            state,
            agent_id,
            lease,
            created_at,
            updated_at,
        }
    }

    // --- State transitions ---

    /// Pending -> Running
    pub fn assign(&mut self, agent_id: String, lease: Lease) -> Result<(), DomainError> {
        if self.state != TaskState::Pending {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Running",
            });
        }
        self.state = TaskState::Running;
        self.agent_id = Some(agent_id);
        self.lease = Some(lease);
        Ok(())
    }

    /// Running -> Completed
    pub fn complete(&mut self) -> Result<(), DomainError> {
        if self.state != TaskState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Completed",
            });
        }
        self.state = TaskState::Completed;
        Ok(())
    }

    /// Running -> Failed
    pub fn fail(&mut self) -> Result<(), DomainError> {
        if self.state != TaskState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Failed",
            });
        }
        self.state = TaskState::Failed;
        Ok(())
    }

    /// Running -> Cancelled
    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.state != TaskState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "Cancelled",
            });
        }
        self.state = TaskState::Cancelled;
        Ok(())
    }

    /// Running -> TimedOut
    pub fn timeout(&mut self) -> Result<(), DomainError> {
        if self.state != TaskState::Running {
            return Err(DomainError::InvalidTransition {
                from: self.state.as_str(),
                to: "TimedOut",
            });
        }
        self.state = TaskState::TimedOut;
        Ok(())
    }

    // --- Validation ---

    /// Validates that the given agent and lease epoch match the task's assigned values.
    pub fn validate_lease(&self, agent_id: &str, lease_epoch: u64) -> Result<(), DomainError> {
        if let Some(ref assigned_agent) = self.agent_id {
            if assigned_agent != agent_id {
                return Err(DomainError::AgentMismatch {
                    expected: assigned_agent.clone(),
                    got: agent_id.to_string(),
                });
            }
        }
        if let Some(ref lease) = self.lease {
            if lease.epoch() != lease_epoch {
                return Err(DomainError::LeaseMismatch {
                    expected: lease.epoch(),
                    got: lease_epoch,
                });
            }
        }
        Ok(())
    }

    // --- Getters ---

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    #[must_use]
    pub fn state(&self) -> TaskState {
        self.state
    }

    #[must_use]
    pub fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_deref()
    }

    #[must_use]
    pub fn lease(&self) -> Option<&Lease> {
        self.lease.as_ref()
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

    fn make_task() -> Task {
        Task::new("task-1".into(), "run-1".into(), 1, now())
    }

    fn make_lease() -> Lease {
        Lease::new(1, Utc.timestamp_opt(2_000_000, 0).unwrap())
    }

    // --- Constructor ---

    #[test]
    fn new_task_starts_pending() {
        let task = make_task();
        assert_eq!(task.state(), TaskState::Pending);
        assert_eq!(task.id(), "task-1");
        assert_eq!(task.run_id(), "run-1");
        assert_eq!(task.attempt(), 1);
        assert!(task.agent_id().is_none());
        assert!(task.lease().is_none());
    }

    // --- Valid transitions ---

    #[test]
    fn assign_pending_to_running() {
        let mut task = make_task();
        assert!(task.assign("agent-1".into(), make_lease()).is_ok());
        assert_eq!(task.state(), TaskState::Running);
        assert_eq!(task.agent_id(), Some("agent-1"));
        assert!(task.lease().is_some());
    }

    #[test]
    fn complete_running_to_completed() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        assert!(task.complete().is_ok());
        assert_eq!(task.state(), TaskState::Completed);
    }

    #[test]
    fn fail_running_to_failed() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        assert!(task.fail().is_ok());
        assert_eq!(task.state(), TaskState::Failed);
    }

    #[test]
    fn cancel_running_to_cancelled() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        assert!(task.cancel().is_ok());
        assert_eq!(task.state(), TaskState::Cancelled);
    }

    #[test]
    fn timeout_running_to_timed_out() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        assert!(task.timeout().is_ok());
        assert_eq!(task.state(), TaskState::TimedOut);
    }

    // --- Invalid transitions ---

    #[test]
    fn assign_from_running_fails() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        let err = task.assign("b".into(), make_lease()).unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Running",
                to: "Running"
            }
        ));
    }

    #[test]
    fn complete_from_pending_fails() {
        let mut task = make_task();
        let err = task.complete().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Completed"
            }
        ));
    }

    #[test]
    fn fail_from_pending_fails() {
        let mut task = make_task();
        let err = task.fail().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Failed"
            }
        ));
    }

    #[test]
    fn cancel_from_pending_fails() {
        let mut task = make_task();
        let err = task.cancel().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "Cancelled"
            }
        ));
    }

    #[test]
    fn timeout_from_pending_fails() {
        let mut task = make_task();
        let err = task.timeout().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Pending",
                to: "TimedOut"
            }
        ));
    }

    #[test]
    fn complete_from_completed_fails() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        task.complete().unwrap();
        let err = task.complete().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Completed",
                to: "Completed"
            }
        ));
    }

    #[test]
    fn fail_from_failed_fails() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        task.fail().unwrap();
        let err = task.fail().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Failed",
                to: "Failed"
            }
        ));
    }

    #[test]
    fn cancel_from_cancelled_fails() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        task.cancel().unwrap();
        let err = task.cancel().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "Cancelled",
                to: "Cancelled"
            }
        ));
    }

    #[test]
    fn timeout_from_timed_out_fails() {
        let mut task = make_task();
        task.assign("a".into(), make_lease()).unwrap();
        task.timeout().unwrap();
        let err = task.timeout().unwrap_err();
        assert!(matches!(
            err,
            DomainError::InvalidTransition {
                from: "TimedOut",
                to: "TimedOut"
            }
        ));
    }

    // --- validate_lease ---

    #[test]
    fn validate_lease_success() {
        let mut task = make_task();
        task.assign("agent-1".into(), Lease::new(5, now())).unwrap();
        assert!(task.validate_lease("agent-1", 5).is_ok());
    }

    #[test]
    fn validate_lease_agent_mismatch() {
        let mut task = make_task();
        task.assign("agent-1".into(), make_lease()).unwrap();
        let err = task.validate_lease("agent-2", 1).unwrap_err();
        assert!(matches!(err, DomainError::AgentMismatch { .. }));
        if let DomainError::AgentMismatch { expected, got } = err {
            assert_eq!(expected, "agent-1");
            assert_eq!(got, "agent-2");
        }
    }

    #[test]
    fn validate_lease_epoch_mismatch() {
        let mut task = make_task();
        task.assign("agent-1".into(), Lease::new(5, now())).unwrap();
        let err = task.validate_lease("agent-1", 99).unwrap_err();
        assert!(matches!(err, DomainError::LeaseMismatch { .. }));
        if let DomainError::LeaseMismatch { expected, got } = err {
            assert_eq!(expected, 5);
            assert_eq!(got, 99);
        }
    }

    #[test]
    fn validate_lease_no_assignment_succeeds() {
        // A task with no agent/lease assigned should pass validation
        let task = make_task();
        assert!(task.validate_lease("any-agent", 999).is_ok());
    }

    // --- from_row ---

    #[test]
    fn from_row_preserves_all_fields() {
        let lease = make_lease();
        let task = Task::from_row(
            "t-1".into(),
            "r-1".into(),
            3,
            TaskState::Running,
            Some("agent-x".into()),
            Some(lease.clone()),
            now(),
            now(),
        );
        assert_eq!(task.id(), "t-1");
        assert_eq!(task.run_id(), "r-1");
        assert_eq!(task.attempt(), 3);
        assert_eq!(task.state(), TaskState::Running);
        assert_eq!(task.agent_id(), Some("agent-x"));
        assert_eq!(task.lease().unwrap(), &lease);
    }

    // --- set_updated_at ---

    #[test]
    fn set_updated_at_changes_timestamp() {
        let mut task = make_task();
        let later = Utc.timestamp_opt(9_000_000, 0).unwrap();
        task.set_updated_at(later);
        assert_eq!(task.updated_at(), later);
    }
}
