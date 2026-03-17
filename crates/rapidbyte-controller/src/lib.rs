//! Rapidbyte controller — control-plane coordination server.
//!
//! Schedules pipeline tasks across agents, manages leases,
//! and streams progress to CLI watchers. Does NOT load WASM
//! or execute plugins.
//!
//! # Crate structure
//!
//! | Module             | Responsibility |
//! |--------------------|----------------|
//! | `proto`            | Generated protobuf types |
//! | `server`           | gRPC server startup and wiring |
//! | `scheduler`        | FIFO task queue, assignment, lease epochs |
//! | `registry`         | Agent registry, heartbeat monitoring |
//! | `run_state`        | Run state machine with attempt tracking |
//! | `lease`            | Lease epoch generation, validation, expiry |
//! | `preview`          | Signed ticket issuance, TTL |
//! | `watcher`          | Broadcast channels for `WatchRun` |
//! | `domain`           | V2 domain model and invariants |
//! | `app`              | V2 use-case orchestration |
//! | `ports`            | V2 interfaces for adapters |
//! | `adapters`         | V2 transport/storage/background adapters |
//! | `bootstrap`        | V2 composition and startup wiring |
//!
//! Legacy V1 controller service modules have been removed; this crate serves
//! the `rapidbyte.v2` control-plane and agent-session contracts only.

#![warn(clippy::pedantic)]

pub mod adapters;
pub mod app;
pub mod background;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod lease;
pub mod middleware;
pub mod ports;
pub mod preview;
pub mod proto;
pub mod registry;
pub mod run_state;
pub mod scheduler;
pub mod server;
pub mod state;
pub mod store;
pub mod terminal;
pub mod watcher;

pub use config::{
    AuthConfig, ControllerConfig, RegistryConfig, ServerTlsConfig, TimerConfig, TrustConfig,
};
pub use server::run;
