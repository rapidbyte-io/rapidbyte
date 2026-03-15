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
/// - [`TrustPolicy::Skip`]: returns `Ok(None)`.
/// - [`TrustPolicy::Warn`]: returns `Ok(Some(warning))` on unsigned or
///   invalid-signature artifacts so the caller can surface the warning
///   to the user (e.g. via `eprintln!`). The warning is **not** emitted
///   via `tracing` because the default CLI log filter suppresses it.
/// - [`TrustPolicy::Verify`]: returns `Err` if the artifact is unsigned
///   or if the signature cannot be verified against any trusted key.
///
/// On success with a valid signature, returns `Ok(None)`.
///
/// # Errors
///
/// Returns an error if `trust_policy` is [`TrustPolicy::Verify`] and the
/// artifact fails signature verification.
pub fn verify_artifact_trust(
    config: &crate::artifact::PluginArtifactConfig,
    trust_policy: TrustPolicy,
    trusted_keys: &[VerifyingKey],
) -> Result<Option<String>> {
    match trust_policy {
        TrustPolicy::Skip => Ok(None),
        TrustPolicy::Warn | TrustPolicy::Verify => match &config.signature {
            None => {
                if trust_policy == TrustPolicy::Verify {
                    anyhow::bail!("plugin is unsigned and trust policy is 'verify'");
                }
                Ok(Some("warning: plugin is unsigned".to_owned()))
            }
            Some(sig) => {
                match crate::signing::verify_against_any(trusted_keys, &config.wasm_sha256, sig) {
                    Ok(()) => Ok(None),
                    Err(err) => {
                        if trust_policy == TrustPolicy::Verify {
                            Err(err).context("plugin signature verification failed")
                        } else {
                            Ok(Some(format!("warning: plugin signature invalid: {err}")))
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
        let result = verify_artifact_trust(&config, TrustPolicy::Skip, &[]).unwrap();
        assert!(result.is_none(), "skip should return no warning");
    }

    #[test]
    fn skip_policy_succeeds_for_unsigned_artifact() {
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: "deadbeef".to_owned(),
            signature: None,
        };
        let result = verify_artifact_trust(&config, TrustPolicy::Skip, &[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn warn_policy_returns_warning_for_unsigned() {
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: "deadbeef".to_owned(),
            signature: None,
        };
        let warning = verify_artifact_trust(&config, TrustPolicy::Warn, &[])
            .unwrap()
            .expect("warn policy should return a warning for unsigned artifact");
        assert!(warning.contains("unsigned"), "got: {warning}");
    }

    #[test]
    fn warn_policy_returns_warning_for_invalid_signature() {
        let (_, pub_pem) = crate::signing::generate_keypair_pem();
        let vk = crate::signing::load_verifying_key_pem(&pub_pem).unwrap();
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: hex::encode(b"some-digest"),
            signature: Some("ab".repeat(64)), // wrong signature
        };
        let warning = verify_artifact_trust(&config, TrustPolicy::Warn, &[vk])
            .unwrap()
            .expect("warn policy should return a warning for invalid signature");
        assert!(warning.contains("invalid"), "got: {warning}");
    }

    #[test]
    fn warn_policy_returns_none_for_valid_signature() {
        let (priv_pem, pub_pem) = crate::signing::generate_keypair_pem();
        let sk = crate::signing::load_signing_key_pem(&priv_pem).unwrap();
        let vk = crate::signing::load_verifying_key_pem(&pub_pem).unwrap();
        let digest = hex::encode(b"payload");
        let sig = crate::signing::sign_digest(&digest, &sk);
        let config = crate::artifact::PluginArtifactConfig {
            wasm_sha256: digest,
            signature: Some(sig),
        };
        let result = verify_artifact_trust(&config, TrustPolicy::Warn, &[vk]).unwrap();
        assert!(
            result.is_none(),
            "valid signature should produce no warning"
        );
    }
}
