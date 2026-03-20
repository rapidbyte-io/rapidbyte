//! Task execution result types.

pub use rapidbyte_types::prelude::CommitState;

/// Terminal outcome of a task execution.
#[derive(Debug, Clone)]
pub enum TaskOutcomeKind {
    Completed,
    Failed(TaskErrorInfo),
    Cancelled,
}

/// Error details for a failed task.
#[derive(Debug, Clone)]
pub struct TaskErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    /// Domain-internal only — used for commit-state decisions, not sent over the wire.
    pub safe_to_retry: bool,
    pub commit_state: CommitState,
}

/// Execution metrics collected during a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct TaskMetrics {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub elapsed_seconds: f64,
    pub cursors_advanced: u64,
}

/// Combined result of executing a single task.
#[derive(Debug, Clone)]
pub struct TaskExecutionResult {
    pub outcome: TaskOutcomeKind,
    pub metrics: TaskMetrics,
}
