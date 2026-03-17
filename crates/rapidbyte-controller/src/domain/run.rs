//! Run aggregate and run lifecycle transitions.

use crate::domain::error::{DomainError, StateViolation};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunId(String);

impl RunId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Accepted,
    Queued,
    Dispatched,
    Executing,
    PreviewReady,
    FailedRetryable,
    FailedTerminal,
    CancelRequested,
    Cancelled,
    Reconciliation,
    Succeeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub id: RunId,
    pub state: RunState,
}

impl Run {
    #[must_use]
    pub fn new(id: RunId) -> Self {
        Self {
            id,
            state: RunState::Accepted,
        }
    }

    /// Test hook for setting an explicit state before transition assertions.
    pub fn force_state_for_test(&mut self, state: RunState) {
        self.state = state;
    }

    /// # Errors
    ///
    /// Returns [`DomainError::StateViolation`] when the transition is invalid.
    pub fn transition(&mut self, to: RunState) -> Result<(), DomainError> {
        if !is_valid_transition(self.state, to) {
            return Err(StateViolation {
                from: self.state,
                to,
            }
            .into());
        }

        self.state = to;
        Ok(())
    }
}

fn is_valid_transition(from: RunState, to: RunState) -> bool {
    matches!(
        (from, to),
        (
            RunState::Accepted | RunState::FailedRetryable,
            RunState::Queued
        ) | (
            RunState::Queued,
            RunState::Dispatched | RunState::CancelRequested
        ) | (
            RunState::Dispatched,
            RunState::Executing | RunState::Reconciliation | RunState::CancelRequested
        ) | (
            RunState::Executing,
            RunState::PreviewReady
                | RunState::FailedRetryable
                | RunState::FailedTerminal
                | RunState::CancelRequested
                | RunState::Succeeded
        ) | (
            RunState::PreviewReady,
            RunState::Succeeded | RunState::CancelRequested
        ) | (
            RunState::Reconciliation,
            RunState::Executing | RunState::FailedTerminal | RunState::CancelRequested
        ) | (RunState::CancelRequested, RunState::Cancelled)
    )
}
