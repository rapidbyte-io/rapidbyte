//! Pipeline state persistence for the Rapidbyte engine.
//!
//! Provides the [`StateBackend`] trait and a [`SqliteStateBackend`]
//! implementation for cursor tracking, run history, and dead-letter
//! queue storage.
//!
//! # Quick start
//!
//! ```no_run
//! use rapidbyte_state::prelude::*;
//! use std::path::Path;
//!
//! let backend = SqliteStateBackend::open(Path::new("/tmp/state.db")).unwrap();
//! let cursor = backend.get_cursor(
//!     &PipelineId::new("my-pipeline"),
//!     &StreamName::new("public.users"),
//! ).unwrap();
//! ```

#![warn(clippy::pedantic)]

pub mod backend;
pub mod error;
pub mod schema;
pub mod sqlite;

// Top-level re-exports for convenience.
pub use backend::StateBackend;
pub use error::StateError;
pub use sqlite::SqliteStateBackend;

/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_state::prelude::*;
/// ```
pub mod prelude {
    pub use crate::backend::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
    pub use crate::backend::StateBackend;
    pub use crate::error::{Result, StateError};
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

    #[test]
    fn top_level_re_exports() {
        use super::{SqliteStateBackend, StateBackend, StateError};
        let backend = SqliteStateBackend::in_memory().unwrap();
        let _: &dyn StateBackend = &backend;
        let _err_type: Option<StateError> = None;
    }
}
