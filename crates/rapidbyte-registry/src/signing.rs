//! Ed25519 signing and verification for plugin artifacts.
//!
//! Uses PKCS8 PEM encoding for key serialization. Digests and signatures
//! are represented as hex-encoded strings.

use std::path::Path;

use anyhow::{bail, Context, Result};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;

/// Generate an Ed25519 keypair and return both keys as PKCS8 PEM strings.
///
/// Returns `(private_pem, public_pem)`.
///
/// # Panics
///
/// Panics if PEM encoding of the generated key fails (should never happen).
#[must_use]
pub fn generate_keypair_pem() -> (String, String) {
    let mut rng = OsRng;
    let signing_key = SigningKey::generate(&mut rng);

    let private_pem = signing_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("PKCS8 private key encoding should not fail");
    let public_pem = signing_key
        .verifying_key()
        .to_public_key_pem(LineEnding::LF)
        .expect("SPKI public key encoding should not fail");

    (private_pem.to_string(), public_pem)
}

/// Parse a PKCS8 PEM-encoded Ed25519 private key.
///
/// # Errors
///
/// Returns an error if the PEM string is malformed or not a valid Ed25519 private key.
pub fn load_signing_key_pem(pem: &str) -> Result<SigningKey> {
    SigningKey::from_pkcs8_pem(pem).context("failed to parse Ed25519 private key PEM")
}

/// Parse an SPKI PEM-encoded Ed25519 public key.
///
/// # Errors
///
/// Returns an error if the PEM string is malformed or not a valid Ed25519 public key.
pub fn load_verifying_key_pem(pem: &str) -> Result<VerifyingKey> {
    VerifyingKey::from_public_key_pem(pem).context("failed to parse Ed25519 public key PEM")
}

/// Read an Ed25519 public key from a PEM file on disk.
///
/// # Errors
///
/// Returns an error if the file cannot be read or does not contain a valid public key PEM.
pub fn load_verifying_key_file(path: &Path) -> Result<VerifyingKey> {
    let pem = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read public key file: {}", path.display()))?;
    load_verifying_key_pem(&pem)
}

/// Sign a hex-encoded digest with an Ed25519 private key.
///
/// Returns the signature as a hex-encoded string.
///
/// # Panics
///
/// Panics if `digest` is not valid hex.
#[must_use]
pub fn sign_digest(digest: &str, key: &SigningKey) -> String {
    let digest_bytes = hex::decode(digest).expect("digest must be valid hex");
    let signature = key.sign(&digest_bytes);
    hex::encode(signature.to_bytes())
}

/// Verify a hex-encoded signature against a hex-encoded digest.
///
/// # Errors
///
/// Returns an error if the digest or signature hex is malformed, the signature
/// length is wrong, or the signature does not match.
pub fn verify_signature(digest: &str, signature_hex: &str, key: &VerifyingKey) -> Result<()> {
    let digest_bytes = hex::decode(digest).context("digest is not valid hex")?;
    let sig_bytes = hex::decode(signature_hex).context("signature is not valid hex")?;
    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("signature must be exactly 64 bytes"))?;
    let signature = ed25519_dalek::Signature::from_bytes(&sig_array);
    key.verify(&digest_bytes, &signature)
        .context("signature verification failed")
}

/// Verify a signature against any of the provided trusted keys.
///
/// Succeeds if at least one key validates the signature; fails if none do.
///
/// # Errors
///
/// Returns an error if no keys are provided or none of the keys verify the signature.
pub fn verify_against_any(keys: &[VerifyingKey], digest: &str, sig: &str) -> Result<()> {
    if keys.is_empty() {
        bail!("no trusted keys provided");
    }
    for key in keys {
        if verify_signature(digest, sig, key).is_ok() {
            return Ok(());
        }
    }
    bail!("signature did not match any trusted key")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_roundtrip_keypair() {
        let (priv_pem, pub_pem) = generate_keypair_pem();

        let sk = load_signing_key_pem(&priv_pem).expect("private key should parse");
        let vk = load_verifying_key_pem(&pub_pem).expect("public key should parse");

        // The verifying key derived from the signing key should match the
        // independently-parsed public key.
        assert_eq!(sk.verifying_key(), vk);
    }

    #[test]
    fn sign_and_verify_digest() {
        let (priv_pem, pub_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&priv_pem).unwrap();
        let vk = load_verifying_key_pem(&pub_pem).unwrap();

        let digest = hex::encode(b"some artifact content hash");
        let sig = sign_digest(&digest, &sk);

        verify_signature(&digest, &sig, &vk).expect("valid signature should verify");
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let (priv_pem_a, _) = generate_keypair_pem();
        let (_, pub_pem_b) = generate_keypair_pem();

        let sk_a = load_signing_key_pem(&priv_pem_a).unwrap();
        let vk_b = load_verifying_key_pem(&pub_pem_b).unwrap();

        let digest = hex::encode(b"payload");
        let sig = sign_digest(&digest, &sk_a);

        assert!(
            verify_signature(&digest, &sig, &vk_b).is_err(),
            "wrong key should reject"
        );
    }

    #[test]
    fn verify_rejects_tampered_digest() {
        let (priv_pem, pub_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&priv_pem).unwrap();
        let vk = load_verifying_key_pem(&pub_pem).unwrap();

        let original = hex::encode(b"original");
        let tampered = hex::encode(b"tampered");
        let sig = sign_digest(&original, &sk);

        assert!(
            verify_signature(&tampered, &sig, &vk).is_err(),
            "tampered digest should reject"
        );
    }

    #[test]
    fn verify_rejects_invalid_signature_string() {
        let (_, pub_pem) = generate_keypair_pem();
        let vk = load_verifying_key_pem(&pub_pem).unwrap();
        let digest = hex::encode(b"data");

        // Not valid hex at all.
        assert!(verify_signature(&digest, "zzzz_not_hex!", &vk).is_err());

        // Valid hex but wrong length.
        assert!(verify_signature(&digest, "abcd", &vk).is_err());
    }

    #[test]
    fn load_signing_key_rejects_invalid_pem() {
        assert!(load_signing_key_pem("this is not a PEM").is_err());
    }

    #[test]
    fn load_verifying_key_rejects_invalid_pem() {
        assert!(load_verifying_key_pem("this is not a PEM").is_err());
    }

    #[test]
    fn verify_against_multiple_trusted_keys() {
        let (priv_a, pub_a) = generate_keypair_pem();
        let (_, pub_b) = generate_keypair_pem();
        let (_, pub_c) = generate_keypair_pem();

        let sk_a = load_signing_key_pem(&priv_a).unwrap();
        let vk_a = load_verifying_key_pem(&pub_a).unwrap();
        let vk_b = load_verifying_key_pem(&pub_b).unwrap();
        let vk_c = load_verifying_key_pem(&pub_c).unwrap();

        let digest = hex::encode(b"artifact-digest");
        let sig = sign_digest(&digest, &sk_a);

        // Should succeed — key A is in the list.
        let trusted = vec![vk_b.clone(), vk_a, vk_c.clone()];
        verify_against_any(&trusted, &digest, &sig)
            .expect("should verify when matching key is present");

        // Should fail — key A is absent.
        let untrusted = vec![vk_b, vk_c];
        assert!(
            verify_against_any(&untrusted, &digest, &sig).is_err(),
            "should fail when no matching key"
        );
    }
}
