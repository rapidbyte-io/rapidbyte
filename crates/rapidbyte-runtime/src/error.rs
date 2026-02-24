//! Runtime error types.

/// Errors from the Wasmtime component runtime layer.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    /// Wasmtime engine or component error.
    #[error("wasmtime: {0}")]
    Wasmtime(#[from] anyhow::Error),

    /// I/O error (file reads, AOT cache writes).
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, RuntimeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let err = RuntimeError::from(io_err);
        assert!(err.to_string().contains("gone"));
    }

    #[test]
    fn runtime_error_from_anyhow() {
        let err = RuntimeError::from(anyhow::anyhow!("boom"));
        assert!(err.to_string().contains("boom"));
    }
}
