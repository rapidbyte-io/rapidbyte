//! Plugin signature trust policy.

use anyhow::{Context, Result};
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

/// Verify a pulled plugin artifact against the configured trust policy.
///
/// Loads trusted keys from both `registry_config.trusted_key_paths` and
/// `registry_config.trusted_key_pems`, then enforces the policy:
///
/// - [`TrustPolicy::Skip`]: always passes without loading any keys.
/// - [`TrustPolicy::Warn`]: logs a warning on unsigned or invalid-signature
///   artifacts but does not fail.
/// - [`TrustPolicy::Verify`]: returns an error if the artifact is unsigned or
///   if the signature cannot be verified against any trusted key.
///
/// # Errors
///
/// Returns an error if a trusted key file cannot be read/parsed, or if
/// `trust_policy` is [`TrustPolicy::Verify`] and the artifact fails
/// signature verification.
pub fn verify_artifact_trust(
    config: &crate::artifact::PluginArtifactConfig,
    registry_config: &crate::client::RegistryConfig,
) -> Result<()> {
    match registry_config.trust_policy {
        TrustPolicy::Skip => Ok(()),
        TrustPolicy::Warn | TrustPolicy::Verify => {
            let mut keys = Vec::new();
            for path in &registry_config.trusted_key_paths {
                keys.push(crate::signing::load_verifying_key_file(path)?);
            }
            for pem in &registry_config.trusted_key_pems {
                keys.push(crate::signing::load_verifying_key_pem(pem)?);
            }

            match &config.signature {
                None => {
                    let msg = "plugin is unsigned";
                    if registry_config.trust_policy == TrustPolicy::Verify {
                        anyhow::bail!("{msg} and trust policy is 'verify'");
                    }
                    tracing::warn!("{msg}");
                    Ok(())
                }
                Some(sig) => {
                    match crate::signing::verify_against_any(&keys, &config.wasm_sha256, sig) {
                        Ok(()) => {
                            tracing::info!("plugin signature verified");
                            Ok(())
                        }
                        Err(err) => {
                            if registry_config.trust_policy == TrustPolicy::Verify {
                                Err(err).context("plugin signature verification failed")
                            } else {
                                tracing::warn!("plugin signature invalid: {err}");
                                Ok(())
                            }
                        }
                    }
                }
            }
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
