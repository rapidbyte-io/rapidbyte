#![warn(clippy::pedantic)]

//! # rapidbyte-api
//!
//! Application-layer service traits and local-mode implementations for
//! the RapidByte data pipeline engine.
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `traits` | Six driving-port trait definitions |
//! | `types` | Request/response DTOs and SSE event model |
//! | `services` | Local-mode trait implementations |
//! | `context` | `ApiContext` DI container |
//! | `error` | `ApiError` type and engine error mapping |
//! | `run_manager` | In-memory run tracking, broadcast, cancel |

pub mod context;
pub mod error;
pub mod run_manager;
pub mod services;
pub mod traits;
pub mod types;

pub use context::{ApiContext, DeploymentMode};
pub use error::{ApiError, FieldError};
