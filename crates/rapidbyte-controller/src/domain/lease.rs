//! Lease fencing value object.

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct Lease {
    pub(crate) epoch: u64,
    pub(crate) owner_agent_id: String,
    pub(crate) expires_at: std::time::SystemTime,
}

#[allow(dead_code)]
impl Lease {
    #[must_use]
    pub(crate) fn new(
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
    pub(crate) fn matches(&self, agent_id: &str, epoch: u64) -> bool {
        self.owner_agent_id == agent_id && self.epoch == epoch
    }
}
