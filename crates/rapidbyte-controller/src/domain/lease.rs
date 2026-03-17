//! Lease fencing value object.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub epoch: u64,
    pub owner_agent_id: String,
    pub expires_at: std::time::SystemTime,
}

impl Lease {
    #[must_use]
    pub fn new(
        epoch: u64,
        owner_agent_id: impl Into<String>,
        expires_at: std::time::SystemTime,
    ) -> Self {
        Self {
            epoch,
            owner_agent_id: owner_agent_id.into(),
            expires_at,
        }
    }

    #[must_use]
    pub fn matches(&self, agent_id: &str, epoch: u64) -> bool {
        self.owner_agent_id == agent_id && self.epoch == epoch
    }
}
