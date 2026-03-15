//! SHA-256 digest verification.

use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};

/// Compute the SHA-256 hex digest of the given data.
#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    hex::encode(hash)
}

/// Verify that the data matches the expected SHA-256 hex digest.
///
/// # Errors
///
/// Returns an error if the computed digest does not match.
pub fn verify_sha256(data: &[u8], expected: &str) -> Result<()> {
    let actual = sha256_hex(data);
    ensure!(
        actual == expected,
        "digest mismatch: expected {expected}, got {actual}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_of_empty() {
        let hash = sha256_hex(b"");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_of_hello_world() {
        let hash = sha256_hex(b"hello world");
        // Known value
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn verify_correct_digest() {
        let data = b"hello world";
        let digest = sha256_hex(data);
        verify_sha256(data, &digest).unwrap();
    }

    #[test]
    fn verify_wrong_digest() {
        let err = verify_sha256(b"hello", "0000000000000000").unwrap_err();
        assert!(err.to_string().contains("digest mismatch"));
    }
}
