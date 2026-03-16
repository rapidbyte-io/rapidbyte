//! Agent-facing gRPC service handlers.

use std::time::Duration;

pub(crate) mod complete;
mod dispatch;
pub(crate) mod heartbeat;
pub(crate) mod poll;
pub(crate) mod register;
pub(crate) mod secret;

pub(crate) use dispatch::resolve_and_build_response;

use crate::state::ControllerState;

/// Default lease TTL for assigned tasks.
pub(crate) const LEASE_TTL: Duration = Duration::from_secs(300);

pub(crate) const ERROR_CODE_SECRET_RESOLUTION: &str = "SECRET_RESOLUTION_FAILED";

pub struct AgentHandler {
    pub(crate) state: ControllerState,
    pub(crate) registry_url: String,
    pub(crate) registry_insecure: bool,
    pub(crate) trust_policy: String,
    pub(crate) trusted_key_pems: Vec<String>,
    #[cfg(test)]
    pub(crate) poll_barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
}

impl AgentHandler {
    #[must_use]
    pub fn new(state: ControllerState) -> Self {
        Self {
            state,
            registry_url: String::new(),
            registry_insecure: false,
            trust_policy: "skip".to_owned(),
            trusted_key_pems: Vec::new(),
            #[cfg(test)]
            poll_barrier: None,
        }
    }

    #[must_use]
    pub fn with_trust_config(
        state: ControllerState,
        registry_url: String,
        registry_insecure: bool,
        trust_policy: String,
        trusted_key_pems: Vec<String>,
    ) -> Self {
        Self {
            state,
            registry_url,
            registry_insecure,
            trust_policy,
            trusted_key_pems,
            #[cfg(test)]
            poll_barrier: None,
        }
    }
}
