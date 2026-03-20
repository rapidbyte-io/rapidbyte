//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub(crate) mod execute;
pub(crate) mod heartbeat;
#[cfg(test)]
pub(crate) mod testing;
pub mod worker;

pub use context::{AgentAppConfig, AgentContext};
pub use worker::run_agent;
