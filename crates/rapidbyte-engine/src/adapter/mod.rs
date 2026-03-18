//! Adapter implementations for engine port traits.
//!
//! Each module wraps a concrete infrastructure dependency behind the
//! corresponding port trait defined in [`crate::domain::ports`].
//!
//! | Module              | Adapter                    | Port trait       |
//! |---------------------|----------------------------|------------------|
//! | `metrics`           | `OtelMetricsSnapshot`      | `MetricsSnapshot`|
//! | `progress`          | `ChannelProgressReporter`  | `ProgressReporter`|
//! | `registry_resolver` | `RegistryPluginResolver`   | `PluginResolver` |

pub mod metrics;
pub mod progress;
pub mod registry_resolver;
