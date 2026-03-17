//! Domain-level error types.

use thiserror::Error;

use crate::domain::run::RunState;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid run transition from {from:?} to {to:?}")]
pub(crate) struct StateViolation {
    pub from: RunState,
    pub to: RunState,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid run transition from {from:?} to {to:?}")]
    StateViolation { from: RunState, to: RunState },
    #[error("stale lease: expected owner {expected_owner} at epoch {expected_epoch}")]
    LeaseStale {
        expected_owner: String,
        expected_epoch: u64,
    },
}

impl From<StateViolation> for DomainError {
    fn from(value: StateViolation) -> Self {
        Self::StateViolation {
            from: value.from,
            to: value.to,
        }
    }
}
