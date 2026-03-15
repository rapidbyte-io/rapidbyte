//! OCI registry client wrapper.
//!
//! Provides typed operations for rapidbyte plugin OCI artifacts,
//! wrapping [`oci_client::Client`] with [`PluginRef`]-based addressing.

use anyhow::{Context, Result};
use oci_client::client::{
    ClientConfig as OciClientConfig, ClientProtocol, Config, ImageData, ImageLayer, PushResponse,
};
use oci_client::manifest::OciImageManifest;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client as OciClient, Reference};
use tracing::debug;

use crate::trust::TrustPolicy;
use crate::PluginRef;

/// Configuration for connecting to an OCI registry.
#[derive(Debug, Clone, Default)]
pub struct RegistryConfig {
    /// Use plain HTTP instead of HTTPS.
    pub insecure: bool,
    /// Optional basic-auth credentials `(username, password)`.
    pub credentials: Option<(String, String)>,
    /// Default registry to prepend when plugin refs don't include a registry host.
    /// E.g. `"registry.example.com"` makes `source/postgres:1.0.0` resolve to
    /// `registry.example.com/source/postgres:1.0.0`.
    pub default_registry: Option<String>,
    /// Signature verification policy.
    pub trust_policy: TrustPolicy,
    /// Paths to trusted Ed25519 public key PEM files.
    pub trusted_key_paths: Vec<std::path::PathBuf>,
}

/// High-level OCI registry client for rapidbyte plugin artifacts.
///
/// All operations accept a [`PluginRef`] so callers never deal with
/// raw OCI references.
pub struct RegistryClient {
    inner: OciClient,
    auth: RegistryAuth,
}

impl RegistryClient {
    /// Create a new registry client from the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(config: &RegistryConfig) -> Result<Self> {
        let protocol = if config.insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };

        let oci_config = OciClientConfig {
            protocol,
            ..Default::default()
        };

        let inner =
            OciClient::try_from(oci_config).context("failed to create OCI registry client")?;

        let auth = match &config.credentials {
            Some((username, password)) => RegistryAuth::Basic(username.clone(), password.clone()),
            None => RegistryAuth::Anonymous,
        };

        Ok(Self { inner, auth })
    }

    /// List all tags for the repository identified by `plugin_ref`.
    ///
    /// The tag component of `plugin_ref` is ignored; only the registry
    /// and repository are used.
    ///
    /// # Errors
    ///
    /// Returns an error on network or authentication failures.
    pub async fn list_tags(&self, plugin_ref: &PluginRef) -> Result<Vec<String>> {
        let reference = to_reference(plugin_ref)?;
        debug!(%plugin_ref, "listing tags");

        let response = self
            .inner
            .list_tags(&reference, &self.auth, None, None)
            .await
            .context("failed to list tags")?;

        Ok(response.tags)
    }

    /// Pull the full artifact (manifest, config, and all layers).
    ///
    /// # Errors
    ///
    /// Returns an error on network, authentication, or manifest-parsing failures.
    pub async fn pull(&self, plugin_ref: &PluginRef) -> Result<ImageData> {
        let reference = to_reference(plugin_ref)?;
        debug!(%plugin_ref, "pulling artifact");

        let accepted_media_types = vec![
            oci_client::manifest::OCI_IMAGE_MEDIA_TYPE,
            oci_client::manifest::IMAGE_MANIFEST_MEDIA_TYPE,
            oci_client::manifest::IMAGE_LAYER_MEDIA_TYPE,
            oci_client::manifest::IMAGE_LAYER_GZIP_MEDIA_TYPE,
            oci_client::manifest::WASM_LAYER_MEDIA_TYPE,
            crate::artifact::MEDIA_TYPE_CONFIG,
            crate::artifact::MEDIA_TYPE_MANIFEST_LAYER,
            crate::artifact::MEDIA_TYPE_WASM_LAYER,
            crate::artifact::MEDIA_TYPE_INDEX_CONFIG,
            crate::artifact::MEDIA_TYPE_INDEX_LAYER,
        ];

        self.inner
            .pull(&reference, &self.auth, accepted_media_types)
            .await
            .context("failed to pull artifact")
    }

    /// Pull only the manifest and its digest (useful for inspection).
    ///
    /// Returns `(manifest, digest)`.
    ///
    /// # Errors
    ///
    /// Returns an error on network, authentication, or manifest-parsing failures.
    pub async fn pull_manifest_only(
        &self,
        plugin_ref: &PluginRef,
    ) -> Result<(OciImageManifest, String)> {
        let reference = to_reference(plugin_ref)?;
        debug!(%plugin_ref, "pulling manifest");

        let (manifest, digest) = self
            .inner
            .pull_manifest(&reference, &self.auth)
            .await
            .context("failed to pull manifest")?;

        // `pull_manifest` returns an `OciManifest` enum; we need the image variant.
        match manifest {
            oci_client::manifest::OciManifest::Image(image_manifest) => {
                Ok((image_manifest, digest))
            }
            oci_client::manifest::OciManifest::ImageIndex(_) => {
                anyhow::bail!(
                    "expected an image manifest for {plugin_ref}, got an image index manifest"
                );
            }
        }
    }

    /// Push an artifact to the registry.
    ///
    /// Returns the manifest URL on success.
    ///
    /// # Errors
    ///
    /// Returns an error on network, authentication, or push failures.
    pub async fn push(
        &self,
        plugin_ref: &PluginRef,
        layers: Vec<ImageLayer>,
        config: Config,
    ) -> Result<String> {
        let reference = to_reference(plugin_ref)?;
        debug!(%plugin_ref, "pushing artifact");

        let response: PushResponse = self
            .inner
            .push(&reference, &layers, config, &self.auth, None)
            .await
            .context("failed to push artifact")?;

        Ok(response.manifest_url)
    }

    /// Pull the plugin index from `<registry>/rapidbyte-index:latest`.
    ///
    /// Returns `None` if the index does not exist yet (any pull error is
    /// treated as "index not available").
    ///
    /// # Errors
    ///
    /// Returns an error only if the JSON in the index layer is malformed.
    pub async fn pull_index(&self, registry: &str) -> Result<Option<crate::index::PluginIndex>> {
        let index_ref = PluginRef {
            registry: registry.to_owned(),
            repository: crate::index::INDEX_REPOSITORY.to_owned(),
            tag: crate::index::INDEX_TAG.to_owned(),
        };

        debug!(%index_ref, "pulling plugin index");

        let image = match self.pull(&index_ref).await {
            Ok(data) => data,
            Err(err) => {
                if is_not_found_error(&err) {
                    debug!("index not found in registry, treating as empty");
                    return Ok(None);
                }
                return Err(err).context("failed to pull plugin index");
            }
        };

        let layer = image
            .layers
            .into_iter()
            .find(|l| l.media_type == crate::artifact::MEDIA_TYPE_INDEX_LAYER)
            .context("index artifact is missing the index data layer")?;

        let index: crate::index::PluginIndex =
            serde_json::from_slice(&layer.data).context("failed to deserialize plugin index")?;

        Ok(Some(index))
    }

    /// Push an updated plugin index to `<registry>/rapidbyte-index:latest`.
    ///
    /// # Errors
    ///
    /// Returns an error on serialization, network, authentication, or push failures.
    pub async fn push_index(
        &self,
        registry: &str,
        index: &crate::index::PluginIndex,
    ) -> Result<()> {
        let index_ref = PluginRef {
            registry: registry.to_owned(),
            repository: crate::index::INDEX_REPOSITORY.to_owned(),
            tag: crate::index::INDEX_TAG.to_owned(),
        };

        debug!(%index_ref, "pushing plugin index");

        let index_bytes = serde_json::to_vec(index).context("failed to serialize plugin index")?;

        let layer = ImageLayer::new(
            index_bytes,
            crate::artifact::MEDIA_TYPE_INDEX_LAYER.to_owned(),
            None,
        );

        let config = Config::new(
            b"{}".to_vec(),
            crate::artifact::MEDIA_TYPE_INDEX_CONFIG.to_owned(),
            None,
        );

        self.push(&index_ref, vec![layer], config).await?;

        Ok(())
    }
}

/// Convert a [`PluginRef`] to an OCI [`Reference`].
fn to_reference(plugin_ref: &PluginRef) -> Result<Reference> {
    let raw = plugin_ref.to_string();
    raw.parse::<Reference>()
        .context(format!("invalid OCI reference: {raw}"))
}

/// Check if an error indicates the resource was not found (vs auth/network failures).
///
/// Used by [`RegistryClient::pull_index`] to distinguish "index doesn't exist yet"
/// from real errors that should be propagated.
fn is_not_found_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("not found")
        || msg.contains("MANIFEST_UNKNOWN")
        || msg.contains("manifest unknown")
        || msg.contains("404")
        || msg.contains("NAME_UNKNOWN")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_https_anonymous() {
        let config = RegistryConfig::default();
        assert!(!config.insecure);
        assert!(config.credentials.is_none());
    }

    #[test]
    fn creates_client_with_default_config() {
        let config = RegistryConfig::default();
        let client = RegistryClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn creates_client_with_insecure_config() {
        let config = RegistryConfig {
            insecure: true,
            ..Default::default()
        };
        let client = RegistryClient::new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn creates_client_with_basic_auth() {
        let config = RegistryConfig {
            credentials: Some(("user".to_owned(), "pass".to_owned())),
            ..Default::default()
        };
        let client = RegistryClient::new(&config).unwrap();
        assert_eq!(
            client.auth,
            RegistryAuth::Basic("user".to_owned(), "pass".to_owned())
        );
    }

    #[test]
    fn to_reference_roundtrips() {
        let plugin_ref = PluginRef::parse("registry.example.com/source/postgres:1.2.0").unwrap();
        let reference = to_reference(&plugin_ref).unwrap();

        assert_eq!(reference.registry(), "registry.example.com");
        assert_eq!(reference.repository(), "source/postgres");
        assert_eq!(reference.tag().unwrap(), "1.2.0");
    }

    #[test]
    fn to_reference_with_port() {
        let plugin_ref = PluginRef::parse("localhost:5050/test/plugin:latest").unwrap();
        let reference = to_reference(&plugin_ref).unwrap();

        assert_eq!(reference.registry(), "localhost:5050");
        assert_eq!(reference.repository(), "test/plugin");
        assert_eq!(reference.tag().unwrap(), "latest");
    }

    #[test]
    fn not_found_errors_are_classified_correctly() {
        let cases = [
            "manifest not found",
            "MANIFEST_UNKNOWN: manifest unknown",
            "HTTP 404: not found",
            "NAME_UNKNOWN: repository does not exist",
        ];
        for msg in cases {
            let err = anyhow::anyhow!("{msg}");
            assert!(is_not_found_error(&err), "expected not-found for: {msg}");
        }
    }

    #[test]
    fn auth_and_network_errors_are_not_classified_as_not_found() {
        let cases = [
            "unauthorized: authentication required",
            "connection refused",
            "timeout: request timed out",
            "TLS handshake failed",
        ];
        for msg in cases {
            let err = anyhow::anyhow!("{msg}");
            assert!(
                !is_not_found_error(&err),
                "expected propagated error for: {msg}"
            );
        }
    }
}
