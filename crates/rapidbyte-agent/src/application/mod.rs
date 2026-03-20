//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
pub mod execute;
#[cfg(test)]
pub mod testing;

pub use context::{AgentAppConfig, AgentContext};
pub use execute::execute_task;
