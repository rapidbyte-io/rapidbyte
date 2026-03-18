use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct AgentCapabilities {
    pub plugins: Vec<String>,
    pub max_concurrent_tasks: u32,
}

#[derive(Clone)]
pub struct Agent {
    id: String,
    capabilities: AgentCapabilities,
    last_seen_at: DateTime<Utc>,
    registered_at: DateTime<Utc>,
}

impl Agent {
    #[must_use]
    pub fn new(id: String, capabilities: AgentCapabilities, now: DateTime<Utc>) -> Self {
        Self {
            id,
            capabilities,
            last_seen_at: now,
            registered_at: now,
        }
    }

    /// Rebuild an `Agent` from a database row. No invariant checks are performed.
    #[must_use]
    pub fn from_row(
        id: String,
        capabilities: AgentCapabilities,
        last_seen_at: DateTime<Utc>,
        registered_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            capabilities,
            last_seen_at,
            registered_at,
        }
    }

    /// Update the agent's heartbeat timestamp.
    pub fn touch(&mut self, now: DateTime<Utc>) {
        self.last_seen_at = now;
    }

    /// Check if the agent is still considered alive given the timeout duration.
    #[must_use]
    pub fn is_alive(&self, now: DateTime<Utc>, timeout: Duration) -> bool {
        now - self.last_seen_at < timeout
    }

    // --- Getters ---

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn capabilities(&self) -> &AgentCapabilities {
        &self.capabilities
    }

    #[must_use]
    pub fn last_seen_at(&self) -> DateTime<Utc> {
        self.last_seen_at
    }

    #[must_use]
    pub fn registered_at(&self) -> DateTime<Utc> {
        self.registered_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.timestamp_opt(1_000_000, 0).unwrap()
    }

    fn make_capabilities() -> AgentCapabilities {
        AgentCapabilities {
            plugins: vec!["source-postgres".into(), "dest-postgres".into()],
            max_concurrent_tasks: 4,
        }
    }

    #[test]
    fn new_agent_sets_both_timestamps() {
        let agent = Agent::new("agent-1".into(), make_capabilities(), now());
        assert_eq!(agent.id(), "agent-1");
        assert_eq!(agent.last_seen_at(), now());
        assert_eq!(agent.registered_at(), now());
        assert_eq!(agent.capabilities().max_concurrent_tasks, 4);
        assert_eq!(agent.capabilities().plugins.len(), 2);
    }

    #[test]
    fn touch_updates_last_seen_at() {
        let mut agent = Agent::new("agent-1".into(), make_capabilities(), now());
        let later = Utc.timestamp_opt(2_000_000, 0).unwrap();
        agent.touch(later);
        assert_eq!(agent.last_seen_at(), later);
        // registered_at should not change
        assert_eq!(agent.registered_at(), now());
    }

    #[test]
    fn is_alive_within_timeout() {
        let agent = Agent::new("agent-1".into(), make_capabilities(), now());
        let check_time = now() + Duration::seconds(29);
        assert!(agent.is_alive(check_time, Duration::seconds(30)));
    }

    #[test]
    fn is_alive_at_exact_timeout_is_false() {
        let agent = Agent::new("agent-1".into(), make_capabilities(), now());
        let check_time = now() + Duration::seconds(30);
        assert!(!agent.is_alive(check_time, Duration::seconds(30)));
    }

    #[test]
    fn is_alive_past_timeout() {
        let agent = Agent::new("agent-1".into(), make_capabilities(), now());
        let check_time = now() + Duration::seconds(60);
        assert!(!agent.is_alive(check_time, Duration::seconds(30)));
    }

    #[test]
    fn from_row_preserves_all_fields() {
        let registered = Utc.timestamp_opt(500_000, 0).unwrap();
        let last_seen = Utc.timestamp_opt(900_000, 0).unwrap();
        let agent = Agent::from_row("a-x".into(), make_capabilities(), last_seen, registered);
        assert_eq!(agent.id(), "a-x");
        assert_eq!(agent.last_seen_at(), last_seen);
        assert_eq!(agent.registered_at(), registered);
    }

    // --- Edge case tests ---

    #[test]
    fn is_alive_with_zero_timeout() {
        let agent = Agent::new("agent-1".into(), make_capabilities(), now());
        // Zero duration timeout means any elapsed time (even 0) fails: 0 < 0 is false
        assert!(!agent.is_alive(now(), Duration::zero()));
    }

    #[test]
    fn touch_then_is_alive() {
        let mut agent = Agent::new("agent-1".into(), make_capabilities(), now());
        // Advance time by 30 seconds
        let later = now() + Duration::seconds(30);
        agent.touch(later);
        // Check is_alive with 60s timeout at the same moment as touch
        assert!(agent.is_alive(later, Duration::seconds(60)));
    }
}
