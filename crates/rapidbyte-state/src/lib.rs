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
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Top-level re-exports for convenience.
pub use backend::StateBackend;
pub use error::StateError;

/// Supported state backend implementations.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    #[default]
    Sqlite,
    Postgres,
}

#[cfg(feature = "postgres")]
pub use postgres::PostgresStateBackend;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStateBackend;

use std::sync::Arc;

use anyhow::Context;

/// Open a state backend by kind and connection string.
///
/// For `SQLite`, `connection` is a file path. For Postgres, it is a connection string.
///
/// # Errors
///
/// Returns an error if the database cannot be opened or connected to.
pub fn open_backend(kind: BackendKind, connection: &str) -> anyhow::Result<Arc<dyn StateBackend>> {
    match kind {
        BackendKind::Sqlite => {
            #[cfg(feature = "sqlite")]
            {
                let backend = SqliteStateBackend::open(std::path::Path::new(connection))
                    .context("Failed to open SQLite state backend")?;
                Ok(Arc::new(backend) as Arc<dyn StateBackend>)
            }
            #[cfg(not(feature = "sqlite"))]
            {
                let _ = connection;
                anyhow::bail!("SQLite backend requested but the `sqlite` feature is not enabled")
            }
        }
        BackendKind::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let backend = PostgresStateBackend::open(connection)
                    .map_err(|e| anyhow::anyhow!("Failed to open Postgres state backend: {e}"))?;
                Ok(Arc::new(backend) as Arc<dyn StateBackend>)
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = connection;
                anyhow::bail!(
                    "Postgres backend requested but the `postgres` feature is not enabled"
                )
            }
        }
    }
}

/// Default connection string for a given backend kind.
///
/// - **`SQLite`**: `~/.rapidbyte/state.db` (expands `$HOME`)
/// - **Postgres**: `host=localhost dbname=rapidbyte_state`
#[must_use]
pub fn default_connection(kind: BackendKind) -> String {
    match kind {
        BackendKind::Sqlite => {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            std::path::PathBuf::from(home)
                .join(".rapidbyte")
                .join("state.db")
                .to_string_lossy()
                .into_owned()
        }
        BackendKind::Postgres => "host=localhost dbname=rapidbyte_state".to_string(),
    }
}

/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_state::prelude::*;
/// ```
pub mod prelude {
    pub use crate::backend::StateBackend;
    pub use crate::error::{Result, StateError};
    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresStateBackend;
    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqliteStateBackend;
    pub use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prelude_re_exports_key_types() {
        use super::prelude::*;
        let _pid = PipelineId::new("test");
        let _sn = StreamName::new("s");
        let _ = RunStatus::Completed;
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

    #[cfg(feature = "sqlite")]
    #[test]
    fn test_open_backend_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let backend = open_backend(BackendKind::Sqlite, db_path.to_str().unwrap()).unwrap();
        let run_id = backend
            .start_run(
                &rapidbyte_types::state::PipelineId::new("test"),
                &rapidbyte_types::state::StreamName::new("all"),
            )
            .unwrap();
        assert!(run_id > 0);
    }

    #[test]
    fn test_default_connection_sqlite() {
        let conn = default_connection(BackendKind::Sqlite);
        assert!(conn.contains(".rapidbyte"), "expected .rapidbyte in path");
        assert!(conn.ends_with("state.db"), "expected state.db suffix");
    }

    #[test]
    fn test_default_connection_postgres() {
        let conn = default_connection(BackendKind::Postgres);
        assert_eq!(conn, "host=localhost dbname=rapidbyte_state");
    }
}
