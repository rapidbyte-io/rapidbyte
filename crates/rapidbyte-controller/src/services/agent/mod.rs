//! Agent-facing gRPC service handlers.

mod dispatch;
// These will be added as files are created in Tasks 7-9:
// pub(crate) mod register;
// pub(crate) mod heartbeat;
// pub(crate) mod poll;
// pub(crate) mod complete;
// pub(crate) mod secret;

pub(crate) use dispatch::resolve_and_build_response;

use crate::state::ControllerState;

pub struct AgentHandler {
    pub(crate) state: ControllerState,
    pub(crate) registry_url: String,
    pub(crate) registry_insecure: bool,
    pub(crate) trust_policy: String,
    pub(crate) trusted_key_pems: Vec<String>,
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
        }
    }
}
