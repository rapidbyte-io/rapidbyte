//! OpenTelemetry-based observability for Rapidbyte.
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | `cache`      | DashMap instrument cache for dynamic plugin metrics |
//! | `grpc_layer` | Tower layer for gRPC RED metrics |
//! | `instruments`| OnceLock instrument accessors (pipeline, host, plugin, ...) |
//! | `labels`     | Bounded label keys and parsing |
//! | `snapshot`   | InMemoryMetricReader and PipelineResult bridge |
//! | `views`      | Histogram bucket configuration |

#![warn(clippy::pedantic)]

pub mod cache;
pub mod grpc_layer;
pub mod instruments;
pub mod labels;
pub mod snapshot;
pub mod views;
