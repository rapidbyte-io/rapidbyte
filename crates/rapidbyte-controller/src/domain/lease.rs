use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    epoch: u64,
    expires_at: DateTime<Utc>,
}

impl Lease {
    #[must_use]
    pub fn new(epoch: u64, expires_at: DateTime<Utc>) -> Self {
        Self { epoch, expires_at }
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    #[must_use]
    pub fn extend(&self, duration: Duration, now: DateTime<Utc>) -> Self {
        Self {
            epoch: self.epoch,
            expires_at: now + duration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn fixed_time(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn new_stores_epoch_and_expires_at() {
        let expires = fixed_time(1000);
        let lease = Lease::new(42, expires);
        assert_eq!(lease.epoch(), 42);
        assert_eq!(lease.expires_at(), expires);
    }

    #[test]
    fn is_expired_returns_false_before_expiry() {
        let expires = fixed_time(1000);
        let lease = Lease::new(1, expires);
        assert!(!lease.is_expired(fixed_time(999)));
    }

    #[test]
    fn is_expired_returns_true_at_expiry() {
        let expires = fixed_time(1000);
        let lease = Lease::new(1, expires);
        assert!(lease.is_expired(fixed_time(1000)));
    }

    #[test]
    fn is_expired_returns_true_after_expiry() {
        let expires = fixed_time(1000);
        let lease = Lease::new(1, expires);
        assert!(lease.is_expired(fixed_time(1001)));
    }

    #[test]
    fn extend_keeps_epoch_updates_expiry() {
        let original = Lease::new(7, fixed_time(1000));
        let now = fixed_time(1500);
        let extended = original.extend(Duration::seconds(300), now);

        assert_eq!(extended.epoch(), 7);
        assert_eq!(extended.expires_at(), fixed_time(1800));
    }

    #[test]
    fn clone_and_eq() {
        let lease = Lease::new(1, fixed_time(500));
        let cloned = lease.clone();
        assert_eq!(lease, cloned);
    }
}
