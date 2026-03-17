//! Task attempt aggregate and lifecycle states.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TaskAttemptState {
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
#[allow(dead_code)]
pub(crate) struct TaskAttempt {
    pub(crate) run_id: String,
    pub(crate) attempt: u32,
    pub(crate) state: TaskAttemptState,
}

#[allow(dead_code)]
impl TaskAttempt {
    #[must_use]
    pub(crate) fn new(run_id: impl Into<String>, attempt: u32) -> Self {
        Self {
            run_id: run_id.into(),
            attempt,
            state: TaskAttemptState::Created,
        }
    }
}
