//! Application layer: DI context, use-case orchestration, and testing fakes.
//!
//! | Module     | Responsibility |
//! |------------|----------------|
//! | `check`    | [`check_pipeline`] — resolve and validate all pipeline plugins |
//! | `context`  | [`EngineContext`] DI container and [`EngineConfig`] |
//! | `discover` | [`discover_plugin`] — resolve a source plugin and discover streams |
//! | `run`      | [`run_pipeline`] — core pipeline orchestration with retry loop |
//! | `testing`  | In-memory fakes and [`TestContext`] factory (test / `test-support` only) |

pub mod check;
pub mod context;
pub mod discover;
pub mod run;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;
