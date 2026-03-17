use thiserror::Error;

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("invalid transition from {from} to {to}")]
    InvalidTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("lease mismatch: expected epoch {expected}, got {got}")]
    LeaseMismatch { expected: u64, got: u64 },
    #[error("agent mismatch: expected {expected}, got {got}")]
    AgentMismatch { expected: String, got: String },
    #[error("lease expired")]
    LeaseExpired,
    #[error("retries exhausted: attempt {attempt} of {max}")]
    RetriesExhausted { attempt: u32, max: u32 },
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_invalid_transition() {
        let err = DomainError::InvalidTransition {
            from: "Pending",
            to: "Completed",
        };
        assert_eq!(
            err.to_string(),
            "invalid transition from Pending to Completed"
        );
    }

    #[test]
    fn display_lease_mismatch() {
        let err = DomainError::LeaseMismatch {
            expected: 5,
            got: 3,
        };
        assert_eq!(err.to_string(), "lease mismatch: expected epoch 5, got 3");
    }

    #[test]
    fn display_agent_mismatch() {
        let err = DomainError::AgentMismatch {
            expected: "agent-1".to_string(),
            got: "agent-2".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "agent mismatch: expected agent-1, got agent-2"
        );
    }

    #[test]
    fn display_lease_expired() {
        let err = DomainError::LeaseExpired;
        assert_eq!(err.to_string(), "lease expired");
    }

    #[test]
    fn display_retries_exhausted() {
        let err = DomainError::RetriesExhausted { attempt: 3, max: 3 };
        assert_eq!(err.to_string(), "retries exhausted: attempt 3 of 3");
    }

    #[test]
    fn display_invalid_pipeline() {
        let err = DomainError::InvalidPipeline("missing source".to_string());
        assert_eq!(err.to_string(), "invalid pipeline: missing source");
    }
}
