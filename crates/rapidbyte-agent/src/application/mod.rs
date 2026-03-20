//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub mod execute;
pub mod heartbeat;
#[cfg(test)]
pub mod testing;
pub mod worker;

pub use context::{AgentAppConfig, AgentContext};
pub use execute::execute_task;
pub use heartbeat::{heartbeat_loop, ActiveLeaseMap, LeaseEntry};
pub use worker::run_agent;
