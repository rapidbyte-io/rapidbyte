//! Plugin reference parsing.
//!
//! A plugin reference follows the OCI convention: `registry/repository:tag`.
//! For example: `registry.example.com/source/postgres:1.2.0`

use std::fmt;

/// A parsed plugin OCI reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PluginRef {
    /// Registry host (e.g. `registry.example.com` or `localhost:5050`).
    pub registry: String,
    /// Repository path (e.g. `source/postgres`).
    pub repository: String,
    /// Tag (e.g. `1.2.0`). Defaults to `latest` if omitted.
    pub tag: String,
}

impl PluginRef {
    /// Parse a plugin reference string.
    ///
    /// # Errors
    ///
    /// Returns an error if the reference is empty, missing a registry host,
    /// or missing a repository path.
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!input.is_empty(), "plugin reference cannot be empty");

        // Split tag: everything after the last ':' that doesn't contain '/'
        let (base, tag) = match input.rfind(':') {
            Some(pos) if !input[pos + 1..].contains('/') => (&input[..pos], &input[pos + 1..]),
            _ => (input, "latest"),
        };

        // Split registry from repository at the first '/'
        let slash = base.find('/').ok_or_else(|| {
            anyhow::anyhow!("plugin reference must include registry and repository path: {input}")
        })?;

        let registry = &base[..slash];
        let repository = &base[slash + 1..];

        // Registry must look like a host (contains '.', ':', or is 'localhost').
        // Reject single-char or relative-path segments like '.' or '..'.
        anyhow::ensure!(
            (registry.contains('.') || registry.contains(':') || registry == "localhost")
                && registry.len() >= 2
                && registry != "..",
            "plugin reference must start with a registry host \
             (e.g. registry.example.com/source/postgres:1.0.0), got: {input}"
        );
        anyhow::ensure!(
            !repository.is_empty(),
            "repository path cannot be empty: {input}"
        );
        anyhow::ensure!(!tag.is_empty(), "tag cannot be empty: {input}");

        // Reject path traversal and unsafe characters in all components.
        for (label, value) in [
            ("registry", registry),
            ("repository", repository),
            ("tag", tag),
        ] {
            anyhow::ensure!(
                !value.contains(".."),
                "{label} must not contain '..': {input}"
            );
            anyhow::ensure!(
                !value.starts_with('/'),
                "{label} must not start with '/': {input}"
            );
            anyhow::ensure!(
                !value.contains('\\'),
                "{label} must not contain backslash: {input}"
            );
        }

        Ok(Self {
            registry: registry.to_owned(),
            repository: repository.to_owned(),
            tag: tag.to_owned(),
        })
    }

    /// Return the reference without the tag (for operations like `list_tags`).
    #[must_use]
    pub fn without_tag(&self) -> String {
        format!("{}/{}", self.registry, self.repository)
    }
}

/// Normalize a registry URL for use as `default_registry`.
///
/// Strips `https://` / `http://` schemes and trailing slashes so that
/// `https://registry.example.com/plugins/` becomes `registry.example.com/plugins`.
#[must_use]
pub fn normalize_registry_url(url: &str) -> String {
    let stripped = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    stripped.trim_end_matches('/').to_owned()
}

/// Normalize an optional registry URL, filtering out blank/empty values.
///
/// Combines the common `filter(not_blank) + map(normalize)` pattern used
/// when building [`crate::client::RegistryConfig`] from CLI flags or
/// controller responses.
#[must_use]
pub fn normalize_registry_url_option(url: Option<&str>) -> Option<String> {
    url.filter(|s| !s.trim().is_empty())
        .map(normalize_registry_url)
}

impl fmt::Display for PluginRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}:{}", self.registry, self.repository, self.tag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_reference() {
        let r = PluginRef::parse("registry.example.com/source/postgres:1.2.0").unwrap();
        assert_eq!(r.registry, "registry.example.com");
        assert_eq!(r.repository, "source/postgres");
        assert_eq!(r.tag, "1.2.0");
    }

    #[test]
    fn parse_reference_with_port() {
        let r = PluginRef::parse("localhost:5050/source/postgres:latest").unwrap();
        assert_eq!(r.registry, "localhost:5050");
        assert_eq!(r.repository, "source/postgres");
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn parse_without_tag_defaults_to_latest() {
        let r = PluginRef::parse("registry.example.com/source/postgres").unwrap();
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn parse_nested_repository() {
        let r = PluginRef::parse("ghcr.io/rapidbyte-io/plugins/source/postgres:1.0.0").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "rapidbyte-io/plugins/source/postgres");
        assert_eq!(r.tag, "1.0.0");
    }

    #[test]
    fn parse_with_port_no_tag() {
        let r = PluginRef::parse("localhost:5050/test/plugin").unwrap();
        assert_eq!(r.registry, "localhost:5050");
        assert_eq!(r.repository, "test/plugin");
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn reject_empty_string() {
        assert!(PluginRef::parse("").is_err());
    }

    #[test]
    fn reject_bare_name_without_registry() {
        let err = PluginRef::parse("postgres:1.0.0").unwrap_err();
        assert!(err.to_string().contains("registry and repository"));
    }

    #[test]
    fn reject_bare_name_no_slash() {
        let err = PluginRef::parse("postgres").unwrap_err();
        assert!(err.to_string().contains("registry and repository"));
    }

    #[test]
    fn reject_registry_only_no_repo() {
        let err = PluginRef::parse("registry.example.com/").unwrap_err();
        assert!(err.to_string().contains("repository path cannot be empty"));
    }

    #[test]
    fn reject_empty_tag() {
        let err = PluginRef::parse("registry.example.com/source/postgres:").unwrap_err();
        assert!(err.to_string().contains("tag cannot be empty"));
    }

    #[test]
    fn accept_localhost_without_port() {
        let r = PluginRef::parse("localhost/source/postgres:1.0.0").unwrap();
        assert_eq!(r.registry, "localhost");
        assert_eq!(r.repository, "source/postgres");
        assert_eq!(r.tag, "1.0.0");
    }

    #[test]
    fn reject_path_traversal_in_repository() {
        let err = PluginRef::parse("registry.example.com/../etc/passwd:1.0").unwrap_err();
        assert!(err.to_string().contains(".."));
    }

    #[test]
    fn reject_path_traversal_in_tag() {
        let err = PluginRef::parse("registry.example.com/source/pg:../../etc").unwrap_err();
        assert!(err.to_string().contains(".."));
    }

    #[test]
    fn normalize_strips_https_scheme() {
        assert_eq!(
            normalize_registry_url("https://registry.example.com/plugins"),
            "registry.example.com/plugins"
        );
    }

    #[test]
    fn normalize_strips_http_scheme() {
        assert_eq!(
            normalize_registry_url("http://localhost:5050"),
            "localhost:5050"
        );
    }

    #[test]
    fn normalize_strips_trailing_slashes() {
        assert_eq!(
            normalize_registry_url("registry.example.com/plugins/"),
            "registry.example.com/plugins"
        );
    }

    #[test]
    fn normalize_strips_scheme_and_trailing_slash() {
        assert_eq!(
            normalize_registry_url("https://registry.example.com/"),
            "registry.example.com"
        );
    }

    #[test]
    fn normalize_leaves_bare_host_unchanged() {
        assert_eq!(
            normalize_registry_url("registry.example.com"),
            "registry.example.com"
        );
    }

    #[test]
    fn reject_backslash_in_repository() {
        let err = PluginRef::parse("registry.example.com/source\\pg:1.0").unwrap_err();
        assert!(err.to_string().contains("backslash"));
    }

    #[test]
    fn display_roundtrips() {
        let input = "registry.example.com/source/postgres:1.2.0";
        let r = PluginRef::parse(input).unwrap();
        assert_eq!(r.to_string(), input);
    }

    #[test]
    fn display_with_default_tag() {
        let r = PluginRef::parse("registry.example.com/source/postgres").unwrap();
        assert_eq!(r.to_string(), "registry.example.com/source/postgres:latest");
    }

    #[test]
    fn without_tag_strips_tag() {
        let r = PluginRef::parse("registry.example.com/source/postgres:1.2.0").unwrap();
        assert_eq!(r.without_tag(), "registry.example.com/source/postgres");
    }

    #[test]
    fn serde_roundtrip() {
        let r = PluginRef::parse("registry.example.com/source/postgres:1.2.0").unwrap();
        let json = serde_json::to_string(&r).unwrap();
        let deserialized: PluginRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, deserialized);
    }
}
