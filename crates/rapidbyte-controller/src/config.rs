use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Authentication settings for the controller gRPC server.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub signing_key: Vec<u8>,
    pub tokens: Vec<String>,
    pub allow_unauthenticated: bool,
    pub allow_insecure_default_signing_key: bool,
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
#[derive(Debug, Clone)]
pub struct ServerTlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Plugin registry configuration relayed to agents on registration.
#[derive(Debug, Clone, Default)]
pub struct RegistryConfig {
    pub url: Option<String>,
    pub insecure: bool,
}

/// Signature trust policy configuration.
#[derive(Debug, Clone)]
pub struct TrustConfig {
    pub policy: String,
    pub trusted_key_paths: Vec<PathBuf>,
}

impl Default for TrustConfig {
    fn default() -> Self {
        Self {
            policy: "skip".to_string(),
            trusted_key_paths: vec![],
        }
    }
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
    pub trust: TrustConfig,
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
            trust: TrustConfig::default(),
        }
    }
}
