//! Network ACL derivation and host allow/deny matching for connector sockets.

use std::collections::HashSet;
use std::net::IpAddr;

use rapidbyte_types::manifest::Permissions;

#[derive(Debug, Clone)]
struct IntersectedAcl {
    left: NetworkAcl,
    right: NetworkAcl,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NetworkAcl {
    allow_all: bool,
    exact_hosts: HashSet<String>,
    suffix_wildcards: Vec<String>,
    inner: Option<Box<IntersectedAcl>>,
}

impl NetworkAcl {
    fn allow_all() -> Self {
        Self {
            allow_all: true,
            ..Self::default()
        }
    }

    fn add_host(&mut self, raw: &str) {
        let host = normalize_host(raw);
        if host.is_empty() {
            return;
        }
        if host == "*" {
            self.allow_all = true;
            return;
        }
        if let Some(suffix) = host.strip_prefix("*.") {
            self.suffix_wildcards.push(format!(".{}", suffix));
            return;
        }
        self.exact_hosts.insert(host);
    }

    fn intersect(self, other: NetworkAcl) -> NetworkAcl {
        if self.allow_all && other.allow_all {
            return NetworkAcl::allow_all();
        }
        if self.allow_all {
            return other;
        }
        if other.allow_all {
            return self;
        }
        NetworkAcl {
            allow_all: false,
            exact_hosts: HashSet::new(),
            suffix_wildcards: Vec::new(),
            inner: Some(Box::new(IntersectedAcl {
                left: self,
                right: other,
            })),
        }
    }

    pub(crate) fn allows(&self, host: &str) -> bool {
        if self.allow_all {
            return true;
        }

        if let Some(ref inner) = self.inner {
            return inner.left.allows(host) && inner.right.allows(host);
        }

        let normalized = normalize_host(host);
        if normalized.is_empty() {
            return false;
        }

        if self.exact_hosts.contains(&normalized) {
            return true;
        }

        self.suffix_wildcards
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
    }
}

fn normalize_host(host: &str) -> String {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Bracketed IPv6 literals: [::1] or [::1]:5432
    if let Some(stripped) = trimmed.strip_prefix('[') {
        if let Some((inside, _)) = stripped.split_once(']') {
            return inside.trim().to_ascii_lowercase();
        }
    }

    // Preserve raw IPv4/IPv6 literals as-is (normalized case), including IPv6.
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return ip.to_string().to_ascii_lowercase();
    }

    // Only treat `host:port` as having a port separator when there is exactly one colon.
    // Multi-colon forms are likely IPv6 literals and must not be truncated.
    if trimmed.matches(':').count() == 1 {
        if let Some((left, right)) = trimmed.rsplit_once(':') {
            if right.chars().all(|c| c.is_ascii_digit()) {
                return left.trim().to_ascii_lowercase();
            }
        }
    }

    trimmed.to_ascii_lowercase()
}

fn extract_host_from_url(raw: &str) -> Option<String> {
    let (_, rest) = raw.split_once("://")?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let authority = authority.rsplit('@').next().unwrap_or(authority);

    if authority.starts_with('[') {
        let end = authority.find(']')?;
        return Some(normalize_host(&authority[1..end]));
    }

    Some(normalize_host(
        authority.split(':').next().unwrap_or(authority),
    ))
}

fn collect_runtime_hosts(value: &serde_json::Value, hosts: &mut HashSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map {
                let key_lower = key.to_ascii_lowercase();
                if let serde_json::Value::String(s) = v {
                    if matches!(
                        key_lower.as_str(),
                        "host" | "hostname" | "server" | "address"
                    ) {
                        hosts.insert(normalize_host(s));
                    } else if key_lower.contains("url") {
                        if let Some(host) = extract_host_from_url(s) {
                            hosts.insert(host);
                        }
                    }
                }
                collect_runtime_hosts(v, hosts);
            }
        }
        serde_json::Value::Array(list) => {
            for item in list {
                collect_runtime_hosts(item, hosts);
            }
        }
        _ => {}
    }
}

pub(crate) fn derive_network_acl(
    permissions: Option<&Permissions>,
    config: &serde_json::Value,
    pipeline_allowed_hosts: Option<&[String]>,
) -> NetworkAcl {
    let Some(perms) = permissions else {
        // Backwards compatibility for connectors without manifests.
        return NetworkAcl::allow_all();
    };

    let mut manifest_acl = NetworkAcl::default();

    if let Some(domains) = &perms.network.allowed_domains {
        for domain in domains {
            manifest_acl.add_host(domain);
        }
    }

    if perms.network.allow_runtime_config_domains {
        let mut dynamic_hosts = HashSet::new();
        collect_runtime_hosts(config, &mut dynamic_hosts);
        for host in dynamic_hosts {
            manifest_acl.add_host(&host);
        }
    }

    match pipeline_allowed_hosts {
        Some(hosts) => {
            let mut pipeline_acl = NetworkAcl::default();
            for host in hosts {
                pipeline_acl.add_host(host);
            }
            manifest_acl.intersect(pipeline_acl)
        }
        None => manifest_acl,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::manifest::{NetworkPermissions, Permissions};
    use std::collections::HashSet;

    #[test]
    fn normalize_host_preserves_ipv6_literal() {
        assert_eq!(normalize_host("2001:db8::1"), "2001:db8::1");
        assert_eq!(normalize_host("::1"), "::1");
    }

    #[test]
    fn normalize_host_handles_bracketed_ipv6_with_port() {
        assert_eq!(normalize_host("[2001:db8::1]:5432"), "2001:db8::1");
        assert_eq!(normalize_host("[::1]"), "::1");
    }

    #[test]
    fn normalize_host_strips_port_for_single_colon_hosts() {
        assert_eq!(normalize_host("example.com:5432"), "example.com");
        assert_eq!(normalize_host("127.0.0.1:5432"), "127.0.0.1");
    }

    // ── ACL allows() tests ──────────────────────────────────────────

    #[test]
    fn acl_exact_host_match() {
        let mut acl = NetworkAcl::default();
        acl.add_host("db.example.com");
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("other.example.com"));
        assert!(!acl.allows("example.com"));
    }

    #[test]
    fn acl_wildcard_suffix_match() {
        let mut acl = NetworkAcl::default();
        acl.add_host("*.example.com");
        assert!(acl.allows("db.example.com"));
        assert!(acl.allows("foo.bar.example.com"));
        assert!(!acl.allows("example.com"));
        assert!(!acl.allows("evil.com"));
    }

    #[test]
    fn acl_star_allows_all() {
        let mut acl = NetworkAcl::default();
        acl.add_host("*");
        assert!(acl.allows("anything.example.com"));
        assert!(acl.allows("127.0.0.1"));
    }

    #[test]
    fn acl_allow_all_constructor() {
        let acl = NetworkAcl::allow_all();
        assert!(acl.allows("anything"));
    }

    #[test]
    fn acl_empty_denied() {
        let acl = NetworkAcl::default();
        assert!(!acl.allows(""));
        assert!(!acl.allows("  "));
    }

    #[test]
    fn acl_case_insensitive() {
        let mut acl = NetworkAcl::default();
        acl.add_host("DB.Example.COM");
        assert!(acl.allows("db.example.com"));
        assert!(acl.allows("DB.EXAMPLE.COM"));
    }

    #[test]
    fn acl_host_with_port_stripped() {
        let mut acl = NetworkAcl::default();
        acl.add_host("db.example.com:5432");
        assert!(acl.allows("db.example.com"));
    }

    // ── extract_host_from_url() tests ───────────────────────────────

    #[test]
    fn extract_host_basic() {
        assert_eq!(
            extract_host_from_url("postgres://db.example.com:5432/mydb"),
            Some("db.example.com".to_string())
        );
    }

    #[test]
    fn extract_host_with_auth() {
        assert_eq!(
            extract_host_from_url("https://user:pass@api.example.com/path"),
            Some("api.example.com".to_string())
        );
    }

    #[test]
    fn extract_host_ipv6() {
        assert_eq!(
            extract_host_from_url("postgres://[::1]:5432/mydb"),
            Some("::1".to_string())
        );
    }

    #[test]
    fn extract_host_no_scheme_returns_none() {
        assert_eq!(extract_host_from_url("not-a-url"), None);
    }

    // ── collect_runtime_hosts() tests ───────────────────────────────

    #[test]
    fn collect_hosts_from_host_key() {
        let config = serde_json::json!({"host": "db.example.com", "port": 5432});
        let mut hosts = HashSet::new();
        collect_runtime_hosts(&config, &mut hosts);
        assert!(hosts.contains("db.example.com"));
    }

    #[test]
    fn collect_hosts_from_nested_hostname() {
        let config = serde_json::json!({"nested": {"hostname": "nested.example.com"}});
        let mut hosts = HashSet::new();
        collect_runtime_hosts(&config, &mut hosts);
        assert!(hosts.contains("nested.example.com"));
    }

    #[test]
    fn collect_hosts_from_url_key() {
        let config = serde_json::json!({"connection_url": "postgres://db.example.com:5432/mydb"});
        let mut hosts = HashSet::new();
        collect_runtime_hosts(&config, &mut hosts);
        assert!(hosts.contains("db.example.com"));
    }

    // ── derive_network_acl() tests ──────────────────────────────────

    #[test]
    fn derive_acl_no_permissions_allows_all() {
        let config = serde_json::json!({});
        let acl = derive_network_acl(None, &config, None);
        assert!(acl.allows("anything.com"));
    }

    #[test]
    fn derive_acl_with_allowed_domains() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec!["db.example.com".to_string()]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, None);
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("other.com"));
    }

    #[test]
    fn derive_acl_with_runtime_config_domains() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: None,
                allow_runtime_config_domains: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let config = serde_json::json!({"host": "dynamic.example.com", "port": 5432});
        let acl = derive_network_acl(Some(&perms), &config, None);
        assert!(acl.allows("dynamic.example.com"));
        assert!(!acl.allows("other.com"));
    }

    // ── Pipeline ACL intersection tests ────────────────────────────

    #[test]
    fn derive_acl_pipeline_hosts_intersects_manifest() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec![
                    "db.example.com".to_string(),
                    "api.example.com".to_string(),
                ]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let pipeline_hosts = vec!["db.example.com".to_string()];
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, Some(&pipeline_hosts));
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("api.example.com"));
        assert!(!acl.allows("evil.com"));
    }

    #[test]
    fn derive_acl_pipeline_cannot_widen_manifest() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec!["db.example.com".to_string()]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let pipeline_hosts = vec!["db.example.com".to_string(), "evil.com".to_string()];
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, Some(&pipeline_hosts));
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("evil.com"));
    }

    #[test]
    fn derive_acl_pipeline_none_preserves_manifest() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec!["db.example.com".to_string()]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, None);
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("other.com"));
    }

    #[test]
    fn derive_acl_pipeline_wildcard_intersects_manifest_wildcard() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec!["*.example.com".to_string()]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let pipeline_hosts = vec!["db.example.com".to_string()];
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, Some(&pipeline_hosts));
        assert!(acl.allows("db.example.com"));
        assert!(!acl.allows("api.example.com"));
    }

    #[test]
    fn derive_acl_no_manifest_ignores_pipeline() {
        let config = serde_json::json!({});
        let pipeline_hosts = vec!["db.example.com".to_string()];
        let acl = derive_network_acl(None, &config, Some(&pipeline_hosts));
        assert!(acl.allows("anything.com"));
    }

    #[test]
    fn derive_acl_pipeline_empty_hosts_blocks_all() {
        let perms = Permissions {
            network: NetworkPermissions {
                allowed_domains: Some(vec!["db.example.com".to_string()]),
                allow_runtime_config_domains: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let pipeline_hosts: Vec<String> = vec![];
        let config = serde_json::json!({});
        let acl = derive_network_acl(Some(&perms), &config, Some(&pipeline_hosts));
        assert!(!acl.allows("db.example.com"));
    }
}
