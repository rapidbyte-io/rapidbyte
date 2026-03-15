//! Plugin signature trust policy.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Plugin signature verification policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrustPolicy {
    /// No signature checking (default).
    #[default]
    Skip,
    /// Log a warning on missing/invalid signatures but continue.
    Warn,
    /// Reject plugins without a valid signature from a trusted key.
    Verify,
}

impl TrustPolicy {
    /// Parse a trust policy from a CLI/config string.
    ///
    /// # Errors
    /// Returns an error if the string is not "skip", "warn", or "verify".
    pub fn from_str_name(s: &str) -> Result<Self> {
        match s {
            "skip" => Ok(Self::Skip),
            "warn" => Ok(Self::Warn),
            "verify" => Ok(Self::Verify),
            _ => anyhow::bail!("invalid trust policy: {s} (expected skip, warn, or verify)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_skip() {
        assert_eq!(
            TrustPolicy::from_str_name("skip").unwrap(),
            TrustPolicy::Skip
        );
    }

    #[test]
    fn parse_warn() {
        assert_eq!(
            TrustPolicy::from_str_name("warn").unwrap(),
            TrustPolicy::Warn
        );
    }

    #[test]
    fn parse_verify() {
        assert_eq!(
            TrustPolicy::from_str_name("verify").unwrap(),
            TrustPolicy::Verify
        );
    }

    #[test]
    fn parse_invalid() {
        assert!(TrustPolicy::from_str_name("invalid").is_err());
    }

    #[test]
    fn default_is_skip() {
        assert_eq!(TrustPolicy::default(), TrustPolicy::Skip);
    }

    #[test]
    fn serde_roundtrip() {
        let json = serde_json::to_string(&TrustPolicy::Verify).unwrap();
        assert_eq!(json, "\"verify\"");
        let parsed: TrustPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TrustPolicy::Verify);
    }
}
