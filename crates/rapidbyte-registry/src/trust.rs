//! Plugin signature trust policy.

use anyhow::{Context, Result};
use ed25519_dalek::VerifyingKey;
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

/// Parse trusted keys from a registry config, respecting trust policy.
///
/// Returns an empty vec when `trust_policy` is [`TrustPolicy::Skip`] so
/// that bad/missing key files never cause failures for the no-verification
/// case.  For `Warn` and `Verify` it loads keys from both
/// `registry_config.trusted_key_paths` and `registry_config.trusted_key_pems`.
///
/// Call this once and pass the result to [`verify_artifact_trust`] to avoid
/// re-parsing keys on every verification.
///
/// # Errors
///
/// Returns an error if a key file cannot be read or any PEM is malformed
/// (only when policy is `Warn` or `Verify`).
pub fn parse_trusted_keys(
    registry_config: &crate::client::RegistryConfig,
) -> Result<Vec<VerifyingKey>> {
    if registry_config.trust_policy == TrustPolicy::Skip {
        return Ok(Vec::new());
    }

    let mut keys = Vec::with_capacity(
        registry_config.trusted_key_paths.len() + registry_config.trusted_key_pems.len(),
    );
    for path in &registry_config.trusted_key_paths {
        keys.push(crate::signing::load_verifying_key_file(path)?);
    }
    for pem in &registry_config.trusted_key_pems {
        keys.push(crate::signing::load_verifying_key_pem(pem)?);
    }
    Ok(keys)
}

/// Verify a pulled plugin artifact against the configured trust policy.
///
/// Accepts pre-parsed `trusted_keys` (from [`parse_trusted_keys`]) so
/// callers can parse keys once and verify multiple artifacts.
///
/// - [`TrustPolicy::Skip`]: always passes without checking keys.
/// - [`TrustPolicy::Warn`]: logs a warning on unsigned or invalid-signature
///   artifacts but does not fail.
/// - [`TrustPolicy::Verify`]: returns an error if the artifact is unsigned or
///   if the signature cannot be verified against any trusted key.
///
/// # Errors
///
/// Returns an error if `trust_policy` is [`TrustPolicy::Verify`] and the
/// artifact fails signature verification.
pub fn verify_artifact_trust(
    config: &crate::artifact::PluginArtifactConfig,
    trust_policy: TrustPolicy,
    trusted_keys: &[VerifyingKey],
) -> Result<()> {
    match trust_policy {
        TrustPolicy::Skip => Ok(()),
        TrustPolicy::Warn | TrustPolicy::Verify => match &config.signature {
            None => {
                let msg = "plugin is unsigned";
                if trust_policy == TrustPolicy::Verify {
                    anyhow::bail!("{msg} and trust policy is 'verify'");
                }
                tracing::warn!("{msg}");
                Ok(())
            }
            Some(sig) => {
                match crate::signing::verify_against_any(trusted_keys, &config.wasm_sha256, sig) {
                    Ok(()) => {
                        tracing::info!("plugin signature verified");
                        Ok(())
                    }
                    Err(err) => {
                        if trust_policy == TrustPolicy::Verify {
                            Err(err).context("plugin signature verification failed")
                        } else {
                            tracing::warn!("plugin signature invalid: {err}");
                            Ok(())
                        }
                    }
                }
            }
        },
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

    #[test]
    fn parse_trusted_keys_skips_for_skip_policy() {
        let config = crate::client::RegistryConfig {
            trust_policy: TrustPolicy::Skip,
            // Point at a nonexistent key file — should not error.
            trusted_key_paths: vec!["/nonexistent/bad-key.pem".into()],
            trusted_key_pems: vec!["not-valid-pem".to_owned()],
            ..Default::default()
        };
        let keys = parse_trusted_keys(&config).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn parse_trusted_keys_fails_for_bad_keys_when_warn() {
        let config = crate::client::RegistryConfig {
            trust_policy: TrustPolicy::Warn,
            trusted_key_paths: vec!["/nonexistent/bad-key.pem".into()],
            ..Default::default()
        };
        assert!(parse_trusted_keys(&config).is_err());
    }

    #[test]
    fn skip_policy_ignores_keys_entirely() {
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: "deadbeef".to_owned(),
            signature: Some("not-a-real-signature".to_owned()),
        };
        // Skip policy should succeed even with no keys and an invalid signature.
        verify_artifact_trust(&config, TrustPolicy::Skip, &[]).unwrap();
    }

    #[test]
    fn skip_policy_succeeds_for_unsigned_artifact() {
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: "deadbeef".to_owned(),
            signature: None,
        };
        verify_artifact_trust(&config, TrustPolicy::Skip, &[]).unwrap();
    }
}
