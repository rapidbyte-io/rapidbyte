//! Concrete adapter implementations for agent port traits.

pub mod agent_factory;
pub mod channel_progress;
pub mod clock;
pub mod engine_executor;
pub mod grpc_controller;
pub mod metrics;
mod proto;

pub use agent_factory::{build_agent_context, AgentAdapters, AgentConfig};
pub use channel_progress::AtomicProgressCollector;
pub use clock::SystemClock;
pub use engine_executor::EngineExecutor;
pub use grpc_controller::{ClientTlsConfig, GrpcControllerGateway};
pub use metrics::OtelMetricsProvider;
