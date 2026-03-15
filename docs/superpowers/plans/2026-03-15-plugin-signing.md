# Plugin Signing — Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Ed25519 plugin signing so publishers can cryptographically prove plugin provenance, with a configurable trust policy (skip/warn/verify) that defaults to skip.

**Architecture:** The `wasm_sha256` digest from the existing `PluginArtifactConfig` is signed with an Ed25519 private key during `plugin push --sign`. The signature is stored in the config blob alongside the digest. On pull/resolve, the trust policy determines whether signatures are checked: `skip` (default, no checking), `warn` (log warning on missing/invalid), `verify` (reject). Trust is configured via `RegistryConfig` which carries the policy and trusted public keys.

**Tech Stack:** Rust, `ed25519-dalek` (Ed25519 signing/verification), `pem` (key file parsing), existing `sha2` for digests

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/rapidbyte-registry/src/signing.rs` | Create | Key generation, signing, verification, trust policy types |
| `crates/rapidbyte-registry/src/artifact.rs` | Modify | Add `signature` field to `PluginArtifactConfig` |
| `crates/rapidbyte-registry/src/client.rs` | Modify | Add `TrustPolicy` and `trusted_keys` to `RegistryConfig` |
| `crates/rapidbyte-registry/src/lib.rs` | Modify | Add `pub mod signing`, re-exports |
| `crates/rapidbyte-cli/src/main.rs` | Modify | Add `--sign`, `--key`, `--trust-policy`, `--trust-key` flags |
| `crates/rapidbyte-cli/src/commands/plugin.rs` | Modify | Sign on push, verify on pull |
| `crates/rapidbyte-runtime/src/plugin.rs` | Modify | Verify signature on registry resolve |
| `crates/rapidbyte-registry/Cargo.toml` | Modify | Add `ed25519-dalek`, `pem`, `rand` deps |

## Dependency Graph

```
Task 1: Signing module (key gen, sign, verify) — independent
Task 2: Add signature to artifact config — depends on Task 1
Task 3: Trust policy in RegistryConfig — independent
Task 4: CLI key generation command — depends on Task 1
Task 5: Sign on push, verify on pull — depends on 1, 2, 3
Task 6: Verify on engine resolve — depends on 3, 5
```

Tasks 1 and 3 can be parallel. Task 2 depends on 1. Tasks 4-6 are sequential.

---

### Task 1: Ed25519 signing module

**Files:**
- Create: `crates/rapidbyte-registry/src/signing.rs`
- Modify: `crates/rapidbyte-registry/Cargo.toml`
- Modify: `crates/rapidbyte-registry/src/lib.rs`

Core cryptographic operations: key generation, signing digests, verifying signatures, reading keys from PEM files.

- [ ] **Step 1: Add dependencies**

In `crates/rapidbyte-registry/Cargo.toml`:
```toml
ed25519-dalek = { version = "2", features = ["pem", "rand_core"] }
rand = "0.8"
```

- [ ] **Step 2: Write tests first**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_roundtrip_keypair() {
        let (private_pem, public_pem) = generate_keypair_pem();
        assert!(private_pem.contains("PRIVATE KEY"));
        assert!(public_pem.contains("PUBLIC KEY"));
        // Both should be parseable
        let _sk = load_signing_key_pem(&private_pem).unwrap();
        let _vk = load_verifying_key_pem(&public_pem).unwrap();
    }

    #[test]
    fn sign_and_verify_digest() {
        let (private_pem, public_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&private_pem).unwrap();
        let vk = load_verifying_key_pem(&public_pem).unwrap();

        let digest = "abc123def456";
        let signature = sign_digest(digest, &sk);
        assert!(verify_signature(digest, &signature, &vk).is_ok());
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let (private_pem, _) = generate_keypair_pem();
        let (_, other_public_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&private_pem).unwrap();
        let other_vk = load_verifying_key_pem(&other_public_pem).unwrap();

        let digest = "abc123";
        let signature = sign_digest(digest, &sk);
        assert!(verify_signature(digest, &signature, &other_vk).is_err());
    }

    #[test]
    fn verify_rejects_tampered_digest() {
        let (private_pem, public_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&private_pem).unwrap();
        let vk = load_verifying_key_pem(&public_pem).unwrap();

        let signature = sign_digest("original", &sk);
        assert!(verify_signature("tampered", &signature, &vk).is_err());
    }

    #[test]
    fn verify_rejects_invalid_signature_string() {
        let (_, public_pem) = generate_keypair_pem();
        let vk = load_verifying_key_pem(&public_pem).unwrap();
        assert!(verify_signature("digest", "not-a-valid-signature", &vk).is_err());
    }

    #[test]
    fn load_signing_key_rejects_invalid_pem() {
        assert!(load_signing_key_pem("not a pem").is_err());
    }

    #[test]
    fn load_verifying_key_rejects_invalid_pem() {
        assert!(load_verifying_key_pem("not a pem").is_err());
    }

    #[test]
    fn verify_against_multiple_trusted_keys() {
        let (sk_pem, pk_pem) = generate_keypair_pem();
        let (_, other_pk_pem) = generate_keypair_pem();
        let sk = load_signing_key_pem(&sk_pem).unwrap();
        let vk1 = load_verifying_key_pem(&pk_pem).unwrap();
        let vk2 = load_verifying_key_pem(&other_pk_pem).unwrap();

        let signature = sign_digest("digest", &sk);
        // Should pass: vk1 is the matching key
        assert!(verify_against_any(&[vk2.clone(), vk1], "digest", &signature).is_ok());
        // Should fail: neither key matches
        assert!(verify_against_any(&[vk2], "digest", &signature).is_err());
    }
}
```

- [ ] **Step 3: Implement signing module**

```rust
//! Ed25519 plugin signing and verification.

use anyhow::{Context, Result};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};

/// Generate a new Ed25519 keypair and return (private_pem, public_pem).
pub fn generate_keypair_pem() -> (String, String) {
    let mut rng = rand::thread_rng();
    let signing_key = SigningKey::generate(&mut rng);
    let private_pem = signing_key
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .expect("private key PEM encoding should not fail")
        .to_string();
    let public_pem = signing_key
        .verifying_key()
        .to_public_key_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .expect("public key PEM encoding should not fail");
    (private_pem, public_pem)
}

/// Load an Ed25519 signing (private) key from PEM.
pub fn load_signing_key_pem(pem: &str) -> Result<SigningKey> {
    SigningKey::from_pkcs8_pem(pem).context("failed to parse Ed25519 private key PEM")
}

/// Load an Ed25519 verifying (public) key from PEM.
pub fn load_verifying_key_pem(pem: &str) -> Result<VerifyingKey> {
    VerifyingKey::from_public_key_pem(pem).context("failed to parse Ed25519 public key PEM")
}

/// Load a verifying key from a PEM file on disk.
pub fn load_verifying_key_file(path: &std::path::Path) -> Result<VerifyingKey> {
    let pem = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read key file: {}", path.display()))?;
    load_verifying_key_pem(&pem)
}

/// Sign a digest string with an Ed25519 private key.
/// Returns the signature as a hex-encoded string.
pub fn sign_digest(digest: &str, key: &SigningKey) -> String {
    let signature = key.sign(digest.as_bytes());
    hex::encode(signature.to_bytes())
}

/// Verify a hex-encoded signature against a digest and public key.
pub fn verify_signature(digest: &str, signature_hex: &str, key: &VerifyingKey) -> Result<()> {
    let sig_bytes = hex::decode(signature_hex).context("invalid hex signature")?;
    let signature = ed25519_dalek::Signature::from_slice(&sig_bytes)
        .context("invalid Ed25519 signature bytes")?;
    key.verify(digest.as_bytes(), &signature)
        .context("signature verification failed")
}

/// Verify a signature against any of the provided trusted keys.
/// Returns Ok if any key verifies, Err if none do.
pub fn verify_against_any(
    keys: &[VerifyingKey],
    digest: &str,
    signature_hex: &str,
) -> Result<()> {
    for key in keys {
        if verify_signature(digest, signature_hex, key).is_ok() {
            return Ok(());
        }
    }
    anyhow::bail!(
        "signature not verified by any of {} trusted key(s)",
        keys.len()
    )
}
```

Note: the exact `ed25519-dalek` 2.x API for PKCS8 PEM encoding/decoding may differ. Read the `ed25519-dalek` docs and adjust import paths. The key operations are: `SigningKey::generate`, `SigningKey::from_pkcs8_pem`, `VerifyingKey::from_public_key_pem`, `key.sign()`, `key.verify()`.

- [ ] **Step 4: Export from lib.rs**

Add `pub mod signing;` and update module table.

- [ ] **Step 5: Build and test**

```bash
cargo test -p rapidbyte-registry
```

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(registry): add Ed25519 signing and verification module"
```

---

### Task 2: Add signature to artifact config

**Files:**
- Modify: `crates/rapidbyte-registry/src/artifact.rs`

Add an optional `signature` field to `PluginArtifactConfig`. When present, it's the hex-encoded Ed25519 signature of the `wasm_sha256` digest.

- [ ] **Step 1: Add signature field to PluginArtifactConfig**

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginArtifactConfig {
    /// SHA-256 hex digest of the WASM binary (layer 1).
    pub wasm_sha256: String,
    /// Optional Ed25519 signature of `wasm_sha256` (hex-encoded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}
```

- [ ] **Step 2: Update pack_artifact to accept optional signing key**

Add `pack_artifact_signed` that takes an optional `&SigningKey`:

```rust
/// Pack a plugin artifact, optionally signing it.
pub fn pack_artifact_signed(
    manifest_json: &[u8],
    wasm_bytes: &[u8],
    signing_key: Option<&ed25519_dalek::SigningKey>,
) -> PackedArtifact {
    let wasm_sha256 = sha256_hex(wasm_bytes);
    let signature = signing_key.map(|sk| crate::signing::sign_digest(&wasm_sha256, sk));

    let artifact_config = PluginArtifactConfig {
        wasm_sha256,
        signature,
    };
    // ... rest same as pack_artifact
}
```

Update `pack_artifact` to delegate to `pack_artifact_signed(... None)` for backward compatibility.

- [ ] **Step 3: Update unpack_artifact return type**

`unpack_artifact` should return the full `PluginArtifactConfig` (including the optional signature) so callers can verify:

```rust
pub fn unpack_artifact(image: &ImageData) -> Result<UnpackedArtifact> { ... }

pub struct UnpackedArtifact {
    pub manifest_json: Vec<u8>,
    pub wasm_bytes: Vec<u8>,
    pub config: PluginArtifactConfig,
}
```

Update all callers of `unpack_artifact` — they currently destructure `(manifest_json, wasm_bytes)`.

- [ ] **Step 4: Update tests**

- [ ] **Step 5: Build and test**

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(registry): add optional Ed25519 signature to artifact config"
```

---

### Task 3: Trust policy in RegistryConfig

**Files:**
- Modify: `crates/rapidbyte-registry/src/client.rs`
- Modify: `crates/rapidbyte-registry/src/lib.rs`

- [ ] **Step 1: Add TrustPolicy enum and trust fields to RegistryConfig**

```rust
/// Plugin signature verification policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TrustPolicy {
    /// No signature checking (default).
    #[default]
    Skip,
    /// Log a warning on missing/invalid signatures but continue.
    Warn,
    /// Reject plugins without a valid signature from a trusted key.
    Verify,
}

#[derive(Debug, Clone, Default)]
pub struct RegistryConfig {
    pub insecure: bool,
    pub credentials: Option<(String, String)>,
    pub default_registry: Option<String>,
    /// Signature verification policy.
    pub trust_policy: TrustPolicy,
    /// Trusted Ed25519 public keys (PEM strings).
    pub trusted_keys: Vec<ed25519_dalek::VerifyingKey>,
}
```

- [ ] **Step 2: Add TrustPolicy parsing from string (for CLI flags)**

```rust
impl TrustPolicy {
    pub fn from_str_option(s: &str) -> Result<Self> {
        match s {
            "skip" => Ok(Self::Skip),
            "warn" => Ok(Self::Warn),
            "verify" => Ok(Self::Verify),
            _ => anyhow::bail!("invalid trust policy: {s} (expected skip, warn, or verify)"),
        }
    }
}
```

- [ ] **Step 3: Add verify_artifact helper**

A convenience function that checks an `UnpackedArtifact` against the trust policy:

```rust
pub fn check_trust(
    config: &PluginArtifactConfig,
    policy: TrustPolicy,
    trusted_keys: &[VerifyingKey],
) -> Result<()> {
    match policy {
        TrustPolicy::Skip => Ok(()),
        TrustPolicy::Warn | TrustPolicy::Verify => {
            match &config.signature {
                None => {
                    let msg = "plugin is unsigned";
                    if policy == TrustPolicy::Verify {
                        anyhow::bail!(msg);
                    }
                    tracing::warn!(msg);
                    Ok(())
                }
                Some(sig) => {
                    match signing::verify_against_any(trusted_keys, &config.wasm_sha256, sig) {
                        Ok(()) => {
                            tracing::debug!("plugin signature verified");
                            Ok(())
                        }
                        Err(err) => {
                            if policy == TrustPolicy::Verify {
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
```

- [ ] **Step 4: Tests for check_trust**

- [ ] **Step 5: Build and test**

- [ ] **Step 6: Commit**

```bash
git commit -am "feat(registry): add trust policy with skip/warn/verify modes"
```

---

### Task 4: CLI key generation command

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/plugin.rs`

Add `rapidbyte plugin keygen --output ~/.rapidbyte/keys/` command.

- [ ] **Step 1: Add Keygen variant to PluginCommands**

```rust
    /// Generate an Ed25519 signing keypair
    Keygen {
        /// Output directory for the key files
        #[arg(long, default_value = "~/.rapidbyte/keys")]
        output: PathBuf,
    },
```

- [ ] **Step 2: Implement keygen handler**

```rust
fn keygen(output_dir: &Path) -> Result<()> {
    let output_dir = if output_dir.starts_with("~") {
        dirs::home_dir()
            .context("cannot determine home directory")?
            .join(output_dir.strip_prefix("~").unwrap())
    } else {
        output_dir.to_path_buf()
    };
    std::fs::create_dir_all(&output_dir)?;

    let (private_pem, public_pem) = rapidbyte_registry::signing::generate_keypair_pem();

    let private_path = output_dir.join("signing-key.pem");
    let public_path = output_dir.join("signing-key.pub");

    std::fs::write(&private_path, &private_pem)?;
    std::fs::write(&public_path, &public_pem)?;

    // Restrict private key permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&private_path, std::fs::Permissions::from_mode(0o600))?;
    }

    eprintln!("Generated Ed25519 signing keypair:");
    eprintln!("  Private key: {}", private_path.display());
    eprintln!("  Public key:  {}", public_path.display());
    eprintln!("\nShare the public key with consumers. Keep the private key secret.");
    Ok(())
}
```

- [ ] **Step 3: Build and test**

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(cli): add plugin keygen command for Ed25519 keypair generation"
```

---

### Task 5: Sign on push, verify on pull

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs` — add `--sign`, `--key`, `--trust-policy`, `--trust-key` flags
- Modify: `crates/rapidbyte-cli/src/commands/plugin.rs` — use signing on push, verification on pull

- [ ] **Step 1: Add signing flags to Push command**

```rust
    Push {
        plugin_ref: String,
        wasm_path: PathBuf,
        #[arg(long)]
        insecure: bool,
        /// Sign the plugin with an Ed25519 private key
        #[arg(long)]
        sign: bool,
        /// Path to the Ed25519 private key PEM file (required with --sign)
        #[arg(long)]
        key: Option<PathBuf>,
    },
```

- [ ] **Step 2: Add trust flags as global CLI flags**

In `main.rs` `Cli` struct:
```rust
    /// Plugin signature trust policy (skip, warn, verify)
    #[arg(long, global = true, env = "RAPIDBYTE_TRUST_POLICY", default_value = "skip")]
    trust_policy: String,

    /// Trusted Ed25519 public key files for plugin verification (can be repeated)
    #[arg(long, global = true, env = "RAPIDBYTE_TRUST_KEY")]
    trust_key: Vec<PathBuf>,
```

- [ ] **Step 3: Build trust config and add to RegistryConfig**

In `main.rs`, after building `registry_config`:
```rust
    let trust_policy = rapidbyte_registry::TrustPolicy::from_str_option(&cli.trust_policy)?;
    let trusted_keys: Vec<_> = cli.trust_key.iter()
        .map(|p| rapidbyte_registry::signing::load_verifying_key_file(p))
        .collect::<Result<Vec<_>>>()?;
    registry_config.trust_policy = trust_policy;
    registry_config.trusted_keys = trusted_keys;
```

- [ ] **Step 4: Update push to sign when --sign is set**

In the push handler, use `pack_artifact_signed` with the signing key if `--sign` is set.

- [ ] **Step 5: Update pull to verify**

After unpacking the artifact, call `check_trust(&unpacked.config, trust_policy, &trusted_keys)`.

- [ ] **Step 6: Build and test**

- [ ] **Step 7: Commit**

```bash
git commit -am "feat(cli): sign plugins on push, verify on pull with trust policy"
```

---

### Task 6: Verify on engine resolve

**Files:**
- Modify: `crates/rapidbyte-runtime/src/plugin.rs`

When the engine resolves a plugin from the registry (pull-on-demand), it should also verify the signature based on the trust policy in `RegistryConfig`.

- [ ] **Step 1: Add verification after cache store in resolve_plugin_from_registry**

After `unpack_artifact` and before returning the cached path, call `check_trust`:

```rust
    let unpacked = rapidbyte_registry::unpack_artifact(&image)?;

    // Verify signature against trust policy
    rapidbyte_registry::check_trust(
        &unpacked.config,
        registry_config.trust_policy,
        &registry_config.trusted_keys,
    )?;

    let entry = cache.store(&plugin_ref, &unpacked.manifest_json, &unpacked.wasm_bytes)?;
```

- [ ] **Step 2: Also verify on cache hits when trust_policy is Verify**

When a plugin is found in cache, the signature was already verified on first pull. But for `Verify` mode, we might want to re-verify. For Phase 3, trust cache hits (the digest was verified on store). Add a comment documenting this decision.

- [ ] **Step 3: Build and test**

```bash
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude transform-validate
```

- [ ] **Step 4: Commit**

```bash
git commit -am "feat(engine): verify plugin signatures on registry resolve"
```

---

## Verification

After all tasks:

1. `cargo test --workspace` — all tests pass
2. `cargo clippy --workspace --all-targets -- -D warnings` — clean
3. Manual end-to-end flow:
```bash
docker compose up -d registry

# Generate keys
rapidbyte plugin keygen --output /tmp/test-keys

# Push a signed plugin
rapidbyte plugin push localhost:5050/source/postgres:1.0.0 target/plugins/sources/postgres.wasm \
  --insecure --sign --key /tmp/test-keys/signing-key.pem

# Pull with verify mode — should succeed
rapidbyte --trust-policy verify --trust-key /tmp/test-keys/signing-key.pub \
  plugin pull localhost:5050/source/postgres:1.0.0 --insecure

# Pull with verify mode and wrong key — should fail
rapidbyte plugin keygen --output /tmp/other-keys
rapidbyte --trust-policy verify --trust-key /tmp/other-keys/signing-key.pub \
  plugin pull localhost:5050/source/postgres:1.0.0 --insecure
# Expected: error "signature not verified by any of 1 trusted key(s)"

# Pull with skip mode (default) — should succeed regardless
rapidbyte plugin pull localhost:5050/source/postgres:1.0.0 --insecure

# Pull with warn mode — should succeed with warning
rapidbyte --trust-policy warn \
  plugin pull localhost:5050/source/postgres:1.0.0 --insecure

docker compose down
```
