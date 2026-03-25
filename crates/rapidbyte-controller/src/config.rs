use std::net::SocketAddr;
use std::time::Duration;

/// Authentication settings for the controller gRPC server.
#[derive(Clone)]
pub struct AuthConfig {
    pub signing_key: Vec<u8>,
    pub tokens: Vec<String>,
    pub allow_unauthenticated: bool,
    pub allow_insecure_default_signing_key: bool,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field(
                "signing_key",
                &format_args!("<{} bytes>", self.signing_key.len()),
            )
            .field("tokens", &format_args!("<{} tokens>", self.tokens.len()))
            .field("allow_unauthenticated", &self.allow_unauthenticated)
            .field(
                "allow_insecure_default_signing_key",
                &self.allow_insecure_default_signing_key,
            )
            .finish()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            signing_key: b"default-insecure-key".to_vec(),
            tokens: vec![],
            allow_unauthenticated: false,
            allow_insecure_default_signing_key: false,
        }
    }
}

/// Timer / interval settings for background tasks.
#[derive(Debug, Clone)]
pub struct TimerConfig {
    pub reconciliation_timeout: Duration,
    pub default_lease_duration: Duration,
    pub lease_check_interval: Duration,
    pub agent_reap_timeout: Duration,
    pub agent_reap_interval: Duration,
    pub default_max_retries: u32,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            reconciliation_timeout: Duration::from_secs(300),
            default_lease_duration: Duration::from_secs(300),
            lease_check_interval: Duration::from_secs(30),
            agent_reap_timeout: Duration::from_secs(60),
            agent_reap_interval: Duration::from_secs(30),
            default_max_retries: 0,
        }
    }
}

/// TLS configuration for the gRPC server.
#[derive(Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for ServerTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTlsConfig")
            .field("cert_pem", &format_args!("<{} bytes>", self.cert_pem.len()))
            .field("key_pem", &"<redacted>")
            .finish()
    }
}

/// Plugin registry configuration relayed to agents on registration.
#[derive(Debug, Clone, Default)]
pub struct RegistryConfig {
    pub url: Option<String>,
    pub insecure: bool,
}

/// Top-level configuration for the controller server.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub listen_addr: SocketAddr,
    pub metadata_database_url: Option<String>,
    pub auth: AuthConfig,
    pub timers: TimerConfig,
    pub tls: Option<ServerTlsConfig>,
    pub metrics_listen: Option<String>,
    pub registry: RegistryConfig,
    /// REST API listen address. If None, REST server is not started.
    pub rest_listen_addr: Option<SocketAddr>,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".parse().expect("valid default address"),
            metadata_database_url: None,
            auth: AuthConfig::default(),
            timers: TimerConfig::default(),
            tls: None,
            metrics_listen: None,
            registry: RegistryConfig::default(),
            rest_listen_addr: None,
        }
    }
}
