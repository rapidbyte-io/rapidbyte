//! Port traits for the agent's hexagonal architecture.
//!
//! | Module       | Trait                | Purpose                            |
//! |--------------|----------------------|------------------------------------|
//! | `controller` | `ControllerGateway`  | Controller communication           |
//! | `executor`   | `PipelineExecutor`   | Pipeline execution                 |
//! | `progress`   | `ProgressCollector`  | Progress snapshot for heartbeating |

pub mod controller;
pub mod executor;
pub mod progress;

pub use controller::{
    CompletionPayload, ControllerGateway, ControllerRegistryInfo, HeartbeatPayload,
    HeartbeatResponse, RegistrationConfig, RegistrationResponse, TaskAssignment, TaskDirective,
    TaskHeartbeat,
};
pub use executor::PipelineExecutor;
pub use progress::ProgressCollector;
