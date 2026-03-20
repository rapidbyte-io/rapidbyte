//! Agent worker for Rapidbyte — polls the controller, executes pipelines, reports results.
//!
//! # Crate structure
//!
//! | Module        | Responsibility                              |
//! |---------------|---------------------------------------------|
//! | `domain`      | Port traits, domain types, errors            |
//! | `application` | DI context, use-case orchestration, fakes    |
//! | `adapter`     | gRPC client, engine executor                 |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod domain;

// ---------------------------------------------------------------------------
// Public re-exports — canonical API surface
// ---------------------------------------------------------------------------

// Application layer
pub use application::{run_agent, AgentAppConfig, AgentContext};

// Adapter layer
pub use adapter::{build_agent_context, AgentAdapters, AgentConfig, ClientTlsConfig};

// Domain types
pub use domain::error::AgentError;
