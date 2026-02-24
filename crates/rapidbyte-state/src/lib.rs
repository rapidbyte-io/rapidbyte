//! Pipeline state persistence for the Rapidbyte engine.
//!
//! Provides the [`StateBackend`] trait and backend implementations
//! for cursor tracking, run history, and dead-letter queue storage.
//!
//! # Feature flags
//!
//! | Feature    | Default | Description |
//! |------------|---------|-------------|
//! | `sqlite`   | **yes** | SQLite backend via `rusqlite` |
//! | `postgres` | no      | PostgreSQL backend via `postgres` |

#![warn(clippy::pedantic)]

pub mod backend;
pub mod error;
#[cfg(feature = "sqlite")]
pub mod schema;
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Top-level re-exports for convenience.
pub use backend::StateBackend;
pub use error::StateError;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStateBackend;

/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_state::prelude::*;
/// ```
pub mod prelude {
    pub use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
    pub use crate::backend::StateBackend;
    pub use crate::error::{Result, StateError};
    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqliteStateBackend;
}

#[cfg(test)]
mod tests {
    #[test]
    fn prelude_re_exports_key_types() {
        use super::prelude::*;
        let _pid = PipelineId::new("test");
        let _sn = StreamName::new("s");
        let _status = RunStatus::Completed;
        let _stats = RunStats::default();
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn top_level_re_exports() {
        use super::{SqliteStateBackend, StateBackend, StateError};
        let backend = SqliteStateBackend::in_memory().unwrap();
        let _: &dyn StateBackend = &backend;
        let _err_type: Option<StateError> = None;
    }
}
