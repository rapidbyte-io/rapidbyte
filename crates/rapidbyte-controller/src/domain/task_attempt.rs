//! Task attempt aggregate and lifecycle states.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAttemptState {
    Created,
    Leased,
    Running,
    Completed,
    Failed,
    Cancelled,
    LeaseExpired,
    ReconciliationExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAttempt {
    pub run_id: String,
    pub attempt: u32,
    pub state: TaskAttemptState,
}

impl TaskAttempt {
    #[must_use]
    pub fn new(run_id: impl Into<String>, attempt: u32) -> Self {
        Self {
            run_id: run_id.into(),
            attempt,
            state: TaskAttemptState::Created,
        }
    }
}
