//! Application layer: DI context, use-case orchestration, test fakes.

pub mod context;
#[cfg(test)]
pub mod testing;

pub use context::{AgentAppConfig, AgentContext};
