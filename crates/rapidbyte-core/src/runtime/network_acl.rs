use std::collections::HashSet;
use std::net::IpAddr;

use rapidbyte_sdk::manifest::Permissions;

#[derive(Debug, Clone, Default)]
pub(crate) struct NetworkAcl {
    allow_all: bool,
    exact_hosts: HashSet<String>,
    suffix_wildcards: Vec<String>,
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

    pub(crate) fn allows(&self, host: &str) -> bool {
        if self.allow_all {
            return true;
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
) -> NetworkAcl {
    let Some(perms) = permissions else {
        // Backwards compatibility for connectors without manifests.
        return NetworkAcl::allow_all();
    };

    let mut acl = NetworkAcl::default();

    if let Some(domains) = &perms.network.allowed_domains {
        for domain in domains {
            acl.add_host(domain);
        }
    }

    if perms.network.allow_runtime_config_domains {
        let mut dynamic_hosts = HashSet::new();
        collect_runtime_hosts(config, &mut dynamic_hosts);
        for host in dynamic_hosts {
            acl.add_host(&host);
        }
    }

    acl
}

#[cfg(test)]
mod tests {
    use super::normalize_host;

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
}
