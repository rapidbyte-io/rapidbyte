//! Application layer: DI context, use-case orchestration, and testing fakes.
//!
//! | Module    | Responsibility |
//! |-----------|----------------|
//! | `context` | [`EngineContext`] DI container and [`EngineConfig`] |
//! | `testing` | In-memory fakes and [`TestContext`] factory (test / `test-support` only) |

pub mod context;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;
