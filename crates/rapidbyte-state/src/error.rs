//! State backend error types.

/// Errors produced by [`StateBackend`](crate::StateBackend) operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Underlying `SQLite` failure.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// File-system I/O failure (e.g. creating the database directory).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal mutex was poisoned by a panicked thread.
    #[error("state backend lock poisoned")]
    LockPoisoned,
}

/// Convenience alias used throughout this crate.
pub type Result<T> = std::result::Result<T, StateError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_error_displays_context() {
        let inner = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(1),
            Some("table not found".into()),
        );
        let err = StateError::Sqlite(inner);
        let msg = err.to_string();
        assert!(msg.contains("sqlite"), "got: {msg}");
    }

    #[test]
    fn lock_poisoned_displays() {
        let err = StateError::LockPoisoned;
        assert_eq!(err.to_string(), "state backend lock poisoned");
    }

    #[test]
    fn io_error_wraps() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err = StateError::Io(inner);
        assert!(err.to_string().contains("i/o"));
    }
}
