//! OCI artifact layout for rapidbyte plugin packaging.
//!
//! A rapidbyte plugin artifact is stored as an OCI image with the following
//! layer structure:
//!
//! | Index | Media type                                           | Content           |
//! |-------|------------------------------------------------------|-------------------|
//! | —     | `application/vnd.rapidbyte.plugin.config.v1+json`    | Config blob (JSON)|
//! | 0     | `application/vnd.rapidbyte.plugin.manifest+json`     | PluginManifest JSON |
//! | 1     | `application/vnd.rapidbyte.plugin.wasm`              | WASM binary       |

use anyhow::{Context, Result};
use oci_client::client::{Config, ImageData, ImageLayer};
use serde::{Deserialize, Serialize};

use crate::verify::{sha256_hex, verify_sha256};

// ── Media type constants ──────────────────────────────────────────────────────

/// Media type for the config blob.
pub const MEDIA_TYPE_CONFIG: &str = "application/vnd.rapidbyte.plugin.config.v1+json";

/// Media type for the plugin manifest layer (layer 0).
pub const MEDIA_TYPE_MANIFEST_LAYER: &str = "application/vnd.rapidbyte.plugin.manifest+json";

/// Media type for the WASM binary layer (layer 1).
pub const MEDIA_TYPE_WASM_LAYER: &str = "application/vnd.rapidbyte.plugin.wasm";

/// Media type for the index artifact config.
pub const MEDIA_TYPE_INDEX_CONFIG: &str = "application/vnd.rapidbyte.index.config.v1+json";

/// Media type for the index data layer.
pub const MEDIA_TYPE_INDEX_LAYER: &str = "application/vnd.rapidbyte.index.v1+json";

// ── Config blob ───────────────────────────────────────────────────────────────

/// The config blob stored in an OCI artifact's config descriptor.
///
/// Contains a digest of the WASM binary so callers can verify integrity
/// independently of layer-level OCI digests, and an optional Ed25519
/// signature over that digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginArtifactConfig {
    /// SHA-256 hex digest of the WASM binary (layer 1).
    pub wasm_sha256: String,
    /// Optional Ed25519 signature (hex-encoded) over `wasm_sha256`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Result of unpacking an OCI plugin artifact.
///
/// Replaces the previous tuple return so callers can access the parsed
/// [`PluginArtifactConfig`] (including any signature) without re-parsing
/// the config blob.
#[derive(Debug, Clone)]
pub struct UnpackedArtifact {
    /// The plugin manifest JSON (layer 0).
    pub manifest_json: Vec<u8>,
    /// The WASM binary (layer 1).
    pub wasm_bytes: Vec<u8>,
    /// The parsed config blob, including the optional signature.
    pub config: PluginArtifactConfig,
}

// ── Packed artifact ───────────────────────────────────────────────────────────

/// The result of [`pack_artifact`], ready to be pushed to an OCI registry.
///
/// Pass `layers` and `config` directly to [`crate::client::RegistryClient::push`].
#[derive(Clone)]
pub struct PackedArtifact {
    /// OCI layers in the correct order: [manifest, wasm].
    pub layers: Vec<ImageLayer>,
    /// Config blob describing the artifact.
    pub config: Config,
}

// ── pack / unpack ─────────────────────────────────────────────────────────────

/// Pack a plugin manifest and WASM binary into an OCI [`PackedArtifact`],
/// optionally signing the WASM digest with an Ed25519 key.
///
/// When `signing_key` is `Some`, the hex-encoded WASM SHA-256 digest is
/// signed and the resulting signature is stored in the config blob.
/// [`unpack_artifact`] will surface this signature in the returned
/// [`UnpackedArtifact::config`] so callers can verify it against a
/// trusted public key.
///
/// # Panics
///
/// Panics if [`PluginArtifactConfig`] cannot be serialized to JSON.
#[must_use]
pub fn pack_artifact(
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
    let config_bytes = serde_json::to_vec(&artifact_config)
        .expect("PluginArtifactConfig serialization is infallible");

    let manifest_layer = ImageLayer::new(
        manifest_json.to_vec(),
        MEDIA_TYPE_MANIFEST_LAYER.to_owned(),
        None,
    );
    let wasm_layer = ImageLayer::new(wasm_bytes.to_vec(), MEDIA_TYPE_WASM_LAYER.to_owned(), None);

    let config = Config::new(config_bytes, MEDIA_TYPE_CONFIG.to_owned(), None);

    PackedArtifact {
        layers: vec![manifest_layer, wasm_layer],
        config,
    }
}

/// Unpack a pulled OCI [`ImageData`] into an [`UnpackedArtifact`].
///
/// Validation performed:
/// - Layer 0 must have media type [`MEDIA_TYPE_MANIFEST_LAYER`].
/// - Layer 1 must have media type [`MEDIA_TYPE_WASM_LAYER`].
/// - The WASM digest is verified against the value stored in the config blob.
///
/// The parsed [`PluginArtifactConfig`] — including any optional signature —
/// is returned in [`UnpackedArtifact::config`] so callers can perform
/// additional verification (e.g. checking the signature against a trusted
/// public key).
///
/// # Errors
///
/// Returns an error if any required layer is missing, has the wrong media type,
/// or if the WASM digest does not match the config blob.
pub fn unpack_artifact(image: &ImageData) -> Result<UnpackedArtifact> {
    // ── manifest layer (index 0) ──────────────────────────────────────────────
    let manifest_layer = image
        .layers
        .first()
        .filter(|l| l.media_type == MEDIA_TYPE_MANIFEST_LAYER)
        .with_context(|| {
            format!(
                "missing or wrong-typed manifest layer (expected media type \
                 {MEDIA_TYPE_MANIFEST_LAYER:?}); got layers: [{}]",
                image
                    .layers
                    .iter()
                    .map(|l| l.media_type.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    // ── wasm layer (index 1) ──────────────────────────────────────────────────
    let wasm_layer = image
        .layers
        .get(1)
        .filter(|l| l.media_type == MEDIA_TYPE_WASM_LAYER)
        .with_context(|| {
            if image.layers.len() < 2 {
                format!(
                    "missing WASM layer (expected media type {MEDIA_TYPE_WASM_LAYER:?}); \
                     artifact has only {} layer(s)",
                    image.layers.len()
                )
            } else {
                format!(
                    "wrong media type for WASM layer: expected {MEDIA_TYPE_WASM_LAYER:?}, \
                     got {:?}",
                    image.layers[1].media_type
                )
            }
        })?;

    // ── digest verification via config blob ───────────────────────────────────
    let artifact_config: PluginArtifactConfig = serde_json::from_slice(&image.config.data)
        .context("failed to deserialize plugin artifact config blob")?;

    verify_sha256(&wasm_layer.data, &artifact_config.wasm_sha256)
        .context("WASM layer digest mismatch")?;

    Ok(UnpackedArtifact {
        manifest_json: manifest_layer.data.to_vec(),
        wasm_bytes: wasm_layer.data.to_vec(),
        config: artifact_config,
    })
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use oci_client::client::ImageData;

    use super::*;

    fn dummy_manifest() -> Vec<u8> {
        br#"{"name":"source-postgres","version":"1.0.0","plugin_type":"source"}"#.to_vec()
    }

    fn dummy_wasm() -> Vec<u8> {
        b"\x00asm\x01\x00\x00\x00fake-wasm-payload".to_vec()
    }

    /// Build an [`ImageData`] from a [`PackedArtifact`] to simulate a pulled image.
    fn image_data_from_packed(packed: PackedArtifact) -> ImageData {
        ImageData {
            layers: packed.layers,
            config: packed.config,
            digest: None,
            manifest: None,
        }
    }

    // ── roundtrip ─────────────────────────────────────────────────────────────

    #[test]
    fn pack_then_unpack_roundtrip() {
        let manifest = dummy_manifest();
        let wasm = dummy_wasm();

        let packed = pack_artifact(&manifest, &wasm, None);
        let image = image_data_from_packed(packed);

        let unpacked = unpack_artifact(&image).expect("unpack must succeed");

        assert_eq!(unpacked.manifest_json, manifest);
        assert_eq!(unpacked.wasm_bytes, wasm);
        assert!(
            unpacked.config.signature.is_none(),
            "unsigned artifact should have no signature"
        );
    }

    #[test]
    fn pack_signed_then_unpack_roundtrip() {
        let (sk_pem, pk_pem) = crate::signing::generate_keypair_pem();
        let sk = crate::signing::load_signing_key_pem(&sk_pem).unwrap();
        let vk = crate::signing::load_verifying_key_pem(&pk_pem).unwrap();

        let manifest = dummy_manifest();
        let wasm = dummy_wasm();
        let packed = pack_artifact(&manifest, &wasm, Some(&sk));

        let image = image_data_from_packed(packed);
        let unpacked = unpack_artifact(&image).expect("unpack must succeed");

        assert_eq!(unpacked.manifest_json, manifest);
        assert_eq!(unpacked.wasm_bytes, wasm);

        let sig = unpacked
            .config
            .signature
            .as_deref()
            .expect("signed artifact must have a signature");

        crate::signing::verify_signature(&unpacked.config.wasm_sha256, sig, &vk)
            .expect("signature must verify against the public key");
    }

    #[test]
    fn packed_artifact_has_correct_layer_media_types() {
        let packed = pack_artifact(&dummy_manifest(), &dummy_wasm(), None);

        assert_eq!(packed.layers.len(), 2);
        assert_eq!(packed.layers[0].media_type, MEDIA_TYPE_MANIFEST_LAYER);
        assert_eq!(packed.layers[1].media_type, MEDIA_TYPE_WASM_LAYER);
        assert_eq!(packed.config.media_type, MEDIA_TYPE_CONFIG);
    }

    #[test]
    fn packed_config_contains_correct_wasm_digest() {
        let wasm = dummy_wasm();
        let packed = pack_artifact(&dummy_manifest(), &wasm, None);

        let config: PluginArtifactConfig = serde_json::from_slice(&packed.config.data).unwrap();

        assert_eq!(config.wasm_sha256, sha256_hex(&wasm));
    }

    // ── error: missing manifest layer ────────────────────────────────────────

    #[test]
    fn unpack_rejects_missing_manifest_layer() {
        // Only the WASM layer, no manifest layer.
        let wasm = dummy_wasm();
        let wasm_layer = ImageLayer::new(wasm.clone(), MEDIA_TYPE_WASM_LAYER.to_owned(), None);
        let config_blob = serde_json::to_vec(&PluginArtifactConfig {
            wasm_sha256: sha256_hex(&wasm),
            signature: None,
        })
        .unwrap();
        let config = Config::new(config_blob, MEDIA_TYPE_CONFIG.to_owned(), None);

        let image = ImageData {
            layers: vec![wasm_layer],
            config,
            digest: None,
            manifest: None,
        };

        let err = unpack_artifact(&image).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing or wrong-typed manifest layer"),
            "unexpected error: {err}"
        );
    }

    // ── error: missing WASM layer ─────────────────────────────────────────────

    #[test]
    fn unpack_rejects_missing_wasm_layer() {
        // Only the manifest layer, no WASM layer.
        let manifest = dummy_manifest();
        let manifest_layer =
            ImageLayer::new(manifest.clone(), MEDIA_TYPE_MANIFEST_LAYER.to_owned(), None);

        // Config blob with a placeholder digest (won't be reached).
        let config_blob = serde_json::to_vec(&PluginArtifactConfig {
            wasm_sha256: "0".repeat(64),
            signature: None,
        })
        .unwrap();
        let config = Config::new(config_blob, MEDIA_TYPE_CONFIG.to_owned(), None);

        let image = ImageData {
            layers: vec![manifest_layer],
            config,
            digest: None,
            manifest: None,
        };

        let err = unpack_artifact(&image).unwrap_err();
        assert!(
            err.to_string().contains("missing WASM layer"),
            "unexpected error: {err}"
        );
    }

    // ── error: wasm digest mismatch ───────────────────────────────────────────

    #[test]
    fn unpack_verifies_wasm_digest() {
        let manifest = dummy_manifest();
        let wasm = dummy_wasm();

        // Pack normally…
        let packed = pack_artifact(&manifest, &wasm, None);

        // …then tamper with the WASM layer data, keeping layers structure intact.
        let tampered_wasm = b"tampered-wasm-content".to_vec();
        let tampered_wasm_layer =
            ImageLayer::new(tampered_wasm, MEDIA_TYPE_WASM_LAYER.to_owned(), None);

        let image = ImageData {
            layers: vec![packed.layers[0].clone(), tampered_wasm_layer],
            config: packed.config,
            digest: None,
            manifest: None,
        };

        let err = unpack_artifact(&image).unwrap_err();
        assert!(
            err.to_string().contains("digest mismatch") || err.to_string().contains("WASM layer"),
            "unexpected error: {err}"
        );
    }

    // ── error: wrong manifest layer media type ────────────────────────────────

    #[test]
    fn unpack_rejects_wrong_manifest_media_type() {
        let manifest = dummy_manifest();
        let wasm = dummy_wasm();

        // Layer 0 has the wrong media type.
        let wrong_layer = ImageLayer::new(manifest, "application/octet-stream".to_owned(), None);
        let wasm_layer = ImageLayer::new(wasm.clone(), MEDIA_TYPE_WASM_LAYER.to_owned(), None);
        let config_blob = serde_json::to_vec(&PluginArtifactConfig {
            wasm_sha256: sha256_hex(&wasm),
            signature: None,
        })
        .unwrap();
        let config = Config::new(config_blob, MEDIA_TYPE_CONFIG.to_owned(), None);

        let image = ImageData {
            layers: vec![wrong_layer, wasm_layer],
            config,
            digest: None,
            manifest: None,
        };

        let err = unpack_artifact(&image).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing or wrong-typed manifest layer"),
            "unexpected error: {err}"
        );
    }

    // ── error: invalid config blob ────────────────────────────────────────────

    #[test]
    fn unpack_rejects_invalid_config_blob() {
        let manifest = dummy_manifest();
        let wasm = dummy_wasm();
        let manifest_layer = ImageLayer::new(manifest, MEDIA_TYPE_MANIFEST_LAYER.to_owned(), None);
        let wasm_layer = ImageLayer::new(wasm, MEDIA_TYPE_WASM_LAYER.to_owned(), None);

        // Garbage config blob.
        let config = Config::new(
            b"not-valid-json".to_vec(),
            MEDIA_TYPE_CONFIG.to_owned(),
            None,
        );

        let image = ImageData {
            layers: vec![manifest_layer, wasm_layer],
            config,
            digest: None,
            manifest: None,
        };

        let err = unpack_artifact(&image).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to deserialize plugin artifact config blob"),
            "unexpected error: {err}"
        );
    }
}
