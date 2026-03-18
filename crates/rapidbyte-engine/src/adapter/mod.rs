//! Adapter implementations for engine port traits.
//!
//! Each module wraps a concrete infrastructure dependency behind the
//! corresponding port trait defined in [`crate::domain::ports`].
//!
//! | Module              | Adapter                    | Port trait       |
//! |---------------------|----------------------------|------------------|
//! | `engine_factory`    | `build_run_context` etc.   | (factory)        |
//! | `metrics`           | `OtelMetricsSnapshot`      | `MetricsSnapshot`|
//! | `postgres`          | `PgBackend`                | `Cursor/Run/Dlq/StateBackend` |
//! | `progress`          | `ChannelProgressReporter`  | `ProgressReporter`|
//! | `registry_resolver` | `RegistryPluginResolver`   | `PluginResolver` |
//! | `wasm_runner`       | `WasmPluginRunner`         | `PluginRunner`   |

pub mod engine_factory;
pub mod metrics;
pub mod postgres;
pub mod progress;
pub mod registry_resolver;
pub mod wasm_runner;
