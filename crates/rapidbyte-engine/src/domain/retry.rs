//! Retry policy with exponential backoff.
//!
//! [`RetryPolicy`] encapsulates the decision of whether to retry an operation
//! and how long to wait, based on error classification and attempt count.

use std::time::Duration;

use rapidbyte_types::error::BackoffClass;

/// Decision returned by [`RetryPolicy::should_retry`].
#[derive(Debug)]
pub enum RetryDecision {
    /// Retry after the specified delay.
    Retry { delay: Duration },
    /// Give up with the specified reason.
    GiveUp { reason: String },
}

/// Retry policy with configurable max attempts and exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: u32,
}

impl RetryPolicy {
    /// Create a new retry policy with the given maximum number of attempts.
    #[must_use]
    pub fn new(max_attempts: u32) -> Self {
        Self { max_attempts }
    }

    /// Decide whether to retry based on the error classification and attempt number.
    ///
    /// `attempt` is 1-based (first attempt = 1).
    #[must_use]
    pub fn should_retry(
        &self,
        attempt: u32,
        retryable: bool,
        safe_to_retry: bool,
        backoff_class: BackoffClass,
        retry_after_hint: Option<Duration>,
    ) -> RetryDecision {
        if !retryable || !safe_to_retry {
            return RetryDecision::GiveUp {
                reason: "error is not retryable".into(),
            };
        }
        if attempt >= self.max_attempts {
            return RetryDecision::GiveUp {
                reason: format!("max attempts ({}) reached", self.max_attempts),
            };
        }
        let delay =
            retry_after_hint.unwrap_or_else(|| Self::compute_backoff(attempt, backoff_class));
        RetryDecision::Retry { delay }
    }

    /// Compute exponential backoff delay based on attempt number and backoff class.
    ///
    /// Base delays: Fast = 100ms, Normal = 1s, Slow = 5s.
    /// Doubles on each subsequent attempt, capped at 60 seconds.
    fn compute_backoff(attempt: u32, class: BackoffClass) -> Duration {
        let base_ms: u64 = match class {
            BackoffClass::Fast => 100,
            BackoffClass::Normal => 1_000,
            BackoffClass::Slow => 5_000,
        };
        let delay_ms = base_ms.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
        Duration::from_millis(delay_ms.min(60_000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gives_up_when_not_retryable() {
        let policy = RetryPolicy::new(3);
        let decision = policy.should_retry(1, false, true, BackoffClass::Normal, None);
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));
        if let RetryDecision::GiveUp { reason } = decision {
            assert!(reason.contains("not retryable"));
        }
    }

    #[test]
    fn gives_up_when_not_safe_to_retry() {
        let policy = RetryPolicy::new(3);
        let decision = policy.should_retry(1, true, false, BackoffClass::Normal, None);
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));
        if let RetryDecision::GiveUp { reason } = decision {
            assert!(reason.contains("not retryable"));
        }
    }

    #[test]
    fn retries_with_fast_backoff() {
        let policy = RetryPolicy::new(5);
        let decision = policy.should_retry(1, true, true, BackoffClass::Fast, None);
        match decision {
            RetryDecision::Retry { delay } => {
                assert_eq!(delay, Duration::from_millis(100));
            }
            RetryDecision::GiveUp { .. } => panic!("expected Retry"),
        }
    }

    #[test]
    fn exponential_backoff_doubles() {
        let policy = RetryPolicy::new(10);

        let d2 = policy.should_retry(2, true, true, BackoffClass::Fast, None);
        match d2 {
            RetryDecision::Retry { delay } => {
                assert_eq!(delay, Duration::from_millis(200));
            }
            RetryDecision::GiveUp { .. } => panic!("expected Retry"),
        }

        let d3 = policy.should_retry(3, true, true, BackoffClass::Fast, None);
        match d3 {
            RetryDecision::Retry { delay } => {
                assert_eq!(delay, Duration::from_millis(400));
            }
            RetryDecision::GiveUp { .. } => panic!("expected Retry"),
        }
    }

    #[test]
    fn backoff_capped_at_60_seconds() {
        let policy = RetryPolicy::new(100);
        let decision = policy.should_retry(30, true, true, BackoffClass::Normal, None);
        match decision {
            RetryDecision::Retry { delay } => {
                assert_eq!(delay, Duration::from_millis(60_000));
            }
            RetryDecision::GiveUp { .. } => panic!("expected Retry"),
        }
    }

    #[test]
    fn respects_retry_after_hint() {
        let policy = RetryPolicy::new(5);
        let hint = Some(Duration::from_millis(7500));
        let decision = policy.should_retry(1, true, true, BackoffClass::Fast, hint);
        match decision {
            RetryDecision::Retry { delay } => {
                assert_eq!(delay, Duration::from_millis(7500));
            }
            RetryDecision::GiveUp { .. } => panic!("expected Retry"),
        }
    }

    #[test]
    fn gives_up_after_max_attempts() {
        let policy = RetryPolicy::new(3);
        let decision = policy.should_retry(3, true, true, BackoffClass::Normal, None);
        assert!(matches!(decision, RetryDecision::GiveUp { .. }));
        if let RetryDecision::GiveUp { reason } = decision {
            assert!(reason.contains("max attempts"));
            assert!(reason.contains("3"));
        }
    }
}
