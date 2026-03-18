//! Adapter implementations for engine port traits.
//!
//! Each module wraps a concrete infrastructure dependency behind the
//! corresponding port trait defined in [`crate::domain::ports`].
//!
//! | Module              | Adapter                    | Port trait       |
//! |---------------------|----------------------------|------------------|
//! | `metrics`           | `OtelMetricsSnapshot`      | `MetricsSnapshot`|
//! | `postgres`          | `PgBackend`                | `Cursor/Run/Dlq/StateBackend` |
//! | `progress`          | `ChannelProgressReporter`  | `ProgressReporter`|
//! | `registry_resolver` | `RegistryPluginResolver`   | `PluginResolver` |
//! | `wasm_runner`       | `WasmPluginRunner`         | `PluginRunner`   |

pub mod metrics;
pub mod postgres;
pub mod progress;
pub mod registry_resolver;
pub mod wasm_runner;
