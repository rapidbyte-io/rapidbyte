//! Port traits for the agent's hexagonal architecture.
//!
//! | Module       | Trait                | Purpose                            |
//! |--------------|----------------------|------------------------------------|
//! | `clock`      | `Clock`              | Monotonic time abstraction         |
//! | `controller` | `ControllerGateway`  | Controller communication           |
//! | `executor`   | `PipelineExecutor`   | Pipeline execution                 |
//! | `metrics`    | `MetricsProvider`    | OTel metrics handles               |
//! | `progress`   | `ProgressCollector`  | Progress snapshot for heartbeating |

pub mod clock;
pub mod controller;
pub mod executor;
pub mod metrics;
pub mod progress;

pub use clock::Clock;
pub use controller::{
    CompletionPayload, ControllerGateway, ControllerRegistryInfo, HeartbeatPayload,
    HeartbeatResponse, RegistrationConfig, RegistrationResponse, TaskAssignment, TaskDirective,
    TaskHeartbeat,
};
pub use executor::PipelineExecutor;
pub use metrics::MetricsProvider;
pub use progress::ProgressCollector;
