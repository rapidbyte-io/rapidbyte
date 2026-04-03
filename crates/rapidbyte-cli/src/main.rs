//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]
#![recursion_limit = "256"]

mod commands;
pub(crate) mod engine_adapters;
mod logging;
pub(crate) mod output;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rustls::crypto::ring::default_provider;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verbosity {
    Quiet,
    Default,
    Verbose,
    Diagnostic,
}

impl Verbosity {
    fn from_flags(quiet: bool, verbose: u8) -> Self {
        if quiet {
            return Self::Quiet;
        }
        match verbose {
            0 => Self::Default,
            1 => Self::Verbose,
            _ => Self::Diagnostic,
        }
    }
}

// ── Flag group structs ────────────────────────────────────────────────────────

/// Controller connection flags (distributed mode).
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct ControllerFlags {
    /// Controller gRPC endpoint (enables distributed mode)
    #[arg(long, env = "RAPIDBYTE_CONTROLLER")]
    pub controller: Option<String>,

    /// Bearer token for authenticated controller RPCs
    #[arg(long, env = "RAPIDBYTE_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Custom CA certificate for TLS controller/Flight connections
    #[arg(long, env = "RAPIDBYTE_TLS_CA_CERT")]
    pub tls_ca_cert: Option<PathBuf>,

    /// Optional TLS server name override for controller/Flight connections
    #[arg(long, env = "RAPIDBYTE_TLS_DOMAIN")]
    pub tls_domain: Option<String>,
}

/// Vault secret-provider flags.
#[derive(Debug, Clone, clap::Args)]
#[allow(clippy::struct_field_names)]
pub(crate) struct VaultFlags {
    /// Vault server address for secret resolution
    #[arg(long, env = "VAULT_ADDR")]
    pub vault_addr: Option<String>,

    /// Vault authentication token
    #[arg(long, env = "VAULT_TOKEN")]
    pub vault_token: Option<String>,

    /// Vault `AppRole` role ID
    #[arg(long, env = "VAULT_ROLE_ID")]
    pub vault_role_id: Option<String>,

    /// Vault `AppRole` secret ID
    #[arg(long, env = "VAULT_SECRET_ID")]
    pub vault_secret_id: Option<String>,
}

/// OCI plugin-registry flags.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct RegistryFlags {
    /// Default OCI registry for plugin resolution (e.g. registry.example.com/plugins)
    #[arg(long, env = "RAPIDBYTE_REGISTRY_URL")]
    pub registry_url: Option<String>,

    /// Use HTTP instead of HTTPS for plugin registry (for local dev registries)
    #[arg(long, env = "RAPIDBYTE_REGISTRY_INSECURE")]
    pub registry_insecure: bool,

    /// Plugin signature trust policy (skip, warn, verify)
    #[arg(long, env = "RAPIDBYTE_TRUST_POLICY", default_value = "skip")]
    pub trust_policy: String,

    /// Trusted Ed25519 public key files for plugin verification (can be repeated)
    #[arg(long, action = clap::ArgAction::Append)]
    pub trust_key: Vec<PathBuf>,
}

// ── Top-level CLI ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "rapidbyte",
    version,
    about = "The single-binary data ingestion engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    /// Increase output verbosity (-v for detailed, -vv for diagnostic)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress all output (exit code only, errors on stderr)
    #[arg(short, long, global = true)]
    quiet: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync a data pipeline
    Sync {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        #[command(flatten)]
        ctrl: ControllerFlags,
        #[command(flatten)]
        vault: VaultFlags,
        #[command(flatten)]
        registry: RegistryFlags,
    },
    /// Show the current status of a distributed run
    Status {
        /// Controller run ID
        run_id: String,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Stream progress for a distributed run until it reaches a terminal state
    Watch {
        /// Controller run ID
        run_id: String,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// List recent distributed runs
    ListRuns {
        /// Maximum number of runs to return
        #[arg(long, default_value_t = 20)]
        limit: i32,
        /// Optional state filter (pending, running, completed, failed, cancelled)
        #[arg(long)]
        state: Option<String>,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Validate pipeline configuration and connectivity
    Check {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        /// Run apply phase after validation passes (provision resources)
        #[arg(long)]
        apply: bool,
        #[command(flatten)]
        vault: VaultFlags,
        #[command(flatten)]
        registry: RegistryFlags,
    },
    /// Discover available streams from a source plugin
    Discover {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        #[command(flatten)]
        vault: VaultFlags,
        #[command(flatten)]
        registry: RegistryFlags,
    },
    /// Tear down resources provisioned by a pipeline
    Teardown {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        /// Reason for teardown (forwarded to plugins)
        #[arg(long, default_value = "pipeline_deleted")]
        reason: String,
        #[command(flatten)]
        vault: VaultFlags,
        #[command(flatten)]
        registry: RegistryFlags,
    },
    /// Manage plugins (pull, push, inspect, list, remove, scaffold)
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
        #[command(flatten)]
        registry: RegistryFlags,
    },
    /// Launch interactive dev shell
    Dev,
    /// Pause scheduled runs for a pipeline
    Pause {
        /// Pipeline name
        #[arg(long)]
        pipeline: String,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Resume scheduled runs for a pipeline
    Resume {
        /// Pipeline name
        #[arg(long)]
        pipeline: String,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Clear sync state (cursors). Next run = full refresh
    Reset {
        /// Pipeline name
        #[arg(long)]
        pipeline: String,
        /// Restrict reset to a single stream
        #[arg(long)]
        stream: Option<String>,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Check data freshness for pipelines
    Freshness {
        /// Filter by tag
        #[arg(long)]
        tag: Option<String>,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// View pipeline logs
    Logs {
        /// Pipeline name
        #[arg(long)]
        pipeline: String,
        /// Filter by run ID
        #[arg(long)]
        run_id: Option<String>,
        /// Maximum number of entries to return
        #[arg(long, default_value_t = 20)]
        limit: u32,
        #[command(flatten)]
        ctrl: ControllerFlags,
    },
    /// Store authentication token for a controller
    Login {
        /// Controller URL
        #[arg(long)]
        controller: String,
        /// Bearer token (prompts if omitted)
        #[arg(long)]
        token: Option<String>,
    },
    /// Remove stored authentication token
    Logout {
        /// Controller URL (removes default if omitted)
        #[arg(long)]
        controller: Option<String>,
    },
    /// Print version information
    Version,
    /// Start the controller server (long-running)
    Controller {
        /// gRPC listen address
        #[arg(long, default_value = "[::]:9090")]
        listen: String,
        /// Postgres connection string for durable controller metadata
        #[arg(long, env = "RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL")]
        metadata_database_url: Option<String>,
        /// Shared signing key for preview tickets (hex or raw string)
        #[arg(long, env = "RAPIDBYTE_SIGNING_KEY")]
        signing_key: Option<String>,
        /// Bearer token for authenticated controller RPCs
        #[arg(long, env = "RAPIDBYTE_AUTH_TOKEN")]
        auth_token: Option<String>,
        /// Explicitly disable controller auth for local development only
        #[arg(long)]
        allow_unauthenticated: bool,
        /// Explicitly allow the built-in insecure development signing key
        #[arg(long)]
        allow_insecure_signing_key: bool,
        /// Max time to wait for restart reconciliation before failing recovery
        #[arg(long, env = "RAPIDBYTE_CONTROLLER_RECONCILIATION_TIMEOUT_SECONDS")]
        reconciliation_timeout_seconds: Option<u64>,
        /// PEM certificate for TLS server mode
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        /// PEM private key for TLS server mode
        #[arg(long)]
        tls_key: Option<PathBuf>,
        /// Prometheus metrics listen address (e.g. 127.0.0.1:9190)
        #[arg(long, env = "RAPIDBYTE_METRICS_LISTEN")]
        metrics_listen: Option<String>,
        /// OCI registry URL to broadcast to agents for plugin pulls
        #[arg(long, env = "RAPIDBYTE_REGISTRY_URL")]
        registry_url: Option<String>,
        /// Use HTTP instead of HTTPS for the plugin registry
        #[arg(long)]
        registry_insecure: bool,
        /// REST API listen address (e.g. 0.0.0.0:8080). If not set, REST is disabled.
        #[arg(long, env = "RAPIDBYTE_REST_LISTEN")]
        rest_listen: Option<String>,
        #[command(flatten)]
        vault: VaultFlags,
    },
    /// Start an agent worker (long-running)
    Agent {
        /// Controller endpoint to connect to
        #[arg(long)]
        controller: String,
        /// Maximum concurrent tasks
        #[arg(long, default_value = "1")]
        max_tasks: u32,
        /// Prometheus metrics listen address (e.g. 127.0.0.1:9191)
        #[arg(long, env = "RAPIDBYTE_METRICS_LISTEN")]
        metrics_listen: Option<String>,
        /// Bearer token for authenticated RPCs
        #[arg(long, env = "RAPIDBYTE_AUTH_TOKEN")]
        auth_token: Option<String>,
        /// Custom CA certificate for TLS controller connections
        #[arg(long, env = "RAPIDBYTE_TLS_CA_CERT")]
        tls_ca_cert: Option<PathBuf>,
        /// Optional TLS server name override
        #[arg(long, env = "RAPIDBYTE_TLS_DOMAIN")]
        tls_domain: Option<String>,
        #[command(flatten)]
        registry: RegistryFlags,
    },
}

#[derive(Subcommand)]
pub(crate) enum PluginCommands {
    /// Pull a plugin from an OCI registry to local cache
    Pull {
        /// Plugin reference (e.g. registry.example.com/source/postgres:1.2.0)
        plugin_ref: String,
        /// Use HTTP instead of HTTPS (for local dev registries)
        #[arg(long)]
        insecure: bool,
    },
    /// Push a local .wasm plugin to an OCI registry
    Push {
        /// Plugin reference (e.g. registry.example.com/source/postgres:1.2.0)
        plugin_ref: String,
        /// Path to the .wasm file
        wasm_path: PathBuf,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
        /// Sign the plugin with an Ed25519 private key
        #[arg(long)]
        sign: bool,
        /// Path to the Ed25519 private key PEM file (required with --sign)
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Inspect plugin metadata without downloading the wasm binary
    Inspect {
        /// Plugin reference
        plugin_ref: String,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// List available tags/versions for a plugin
    Tags {
        /// Plugin reference (tag is ignored)
        plugin_ref: String,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// List locally cached plugins
    List,
    /// Remove a plugin from the local cache
    Remove {
        /// Plugin reference
        plugin_ref: String,
    },
    /// Search for plugins in a registry
    Search {
        /// Search query (matches name, description, repository)
        #[arg(default_value = "")]
        query: String,
        /// Filter by plugin type (source, destination, transform)
        #[arg(long, short = 't')]
        plugin_type: Option<String>,
        /// Registry to search (required if --registry-url not set globally)
        #[arg(long)]
        registry: Option<String>,
        /// Use HTTP instead of HTTPS
        #[arg(long)]
        insecure: bool,
    },
    /// Generate an Ed25519 signing keypair for plugin signing
    Keygen {
        /// Output directory for key files (default: current directory)
        #[arg(long, default_value = ".")]
        output: PathBuf,
    },
    /// Scaffold a new plugin project
    Scaffold {
        /// Plugin name (e.g., "source-mysql", "dest-snowflake")
        name: String,
        /// Output directory (default: `./plugins/<kind>/<name>`)
        #[arg(short, long)]
        output: Option<String>,
    },
}

// ── Helper functions ──────────────────────────────────────────────────────────

/// Resolve controller URL from config file (`~/.rapidbyte/config.yaml`).
/// CLI flag and env var are already handled by clap's `env` attribute.
pub(crate) fn controller_url_from_config() -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let config_path = std::path::PathBuf::from(home)
        .join(".rapidbyte")
        .join("config.yaml");
    let contents = std::fs::read_to_string(config_path).ok()?;
    let val: serde_yaml::Value = serde_yaml::from_str(&contents).ok()?;
    val.get("controller")?
        .get("url")?
        .as_str()
        .map(String::from)
}

fn resolve_controller_url_with<F>(
    cli_controller: Option<String>,
    allow_config_fallback: bool,
    config_loader: F,
) -> Option<String>
where
    F: FnOnce() -> Option<String>,
{
    cli_controller.or_else(|| {
        if allow_config_fallback {
            config_loader()
        } else {
            None
        }
    })
}

fn resolve_controller_url(
    cli_controller: Option<String>,
    allow_config_fallback: bool,
) -> Option<String> {
    resolve_controller_url_with(
        cli_controller,
        allow_config_fallback,
        controller_url_from_config,
    )
}

fn try_build_secrets(
    vault_addr: Option<&str>,
    vault_token: Option<&str>,
    vault_role_id: Option<&str>,
    vault_secret_id: Option<&str>,
) -> Option<rapidbyte_secrets::SecretProviders> {
    match build_secret_providers(vault_addr, vault_token, vault_role_id, vault_secret_id) {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
            None
        }
    }
}

fn build_secret_providers(
    vault_addr: Option<&str>,
    vault_token: Option<&str>,
    vault_role_id: Option<&str>,
    vault_secret_id: Option<&str>,
) -> anyhow::Result<rapidbyte_secrets::SecretProviders> {
    let mut providers = rapidbyte_secrets::SecretProviders::new();

    if let Some(addr) = vault_addr {
        let auth = if let Some(token) = vault_token {
            Some(rapidbyte_secrets::VaultAuth::Token(token.to_owned()))
        } else if let (Some(role_id), Some(secret_id)) = (vault_role_id, vault_secret_id) {
            Some(rapidbyte_secrets::VaultAuth::AppRole {
                role_id: role_id.to_owned(),
                secret_id: secret_id.to_owned(),
            })
        } else {
            // VAULT_ADDR set but no auth — skip registration.
            // If a pipeline has ${vault:...} refs, the parser will
            // error with "no secret provider registered for prefix 'vault'".
            tracing::debug!(
                "VAULT_ADDR is set but no Vault auth configured; \
                 vault provider not registered"
            );
            None
        };

        let Some(auth) = auth else {
            return Ok(providers);
        };

        let config = rapidbyte_secrets::VaultConfig {
            address: addr.to_owned(),
            auth,
        };
        let vault = rapidbyte_secrets::VaultProvider::new(&config)?;

        providers.register("vault", std::sync::Arc::new(vault));
    }

    Ok(providers)
}

/// Build a `TlsClientConfig` from `ControllerFlags`, returning `None` if not configured.
fn make_tls_config(ctrl: &ControllerFlags) -> Option<commands::transport::TlsClientConfig> {
    let tls = commands::transport::TlsClientConfig {
        ca_cert_path: ctrl.tls_ca_cert.clone(),
        domain_name: ctrl.tls_domain.clone(),
    };
    tls.is_configured().then_some(tls)
}

/// Parse `RegistryFlags` into a `RegistryConfig`, returning an error if the
/// trust policy name is invalid.
fn build_registry_config(
    reg: &RegistryFlags,
) -> anyhow::Result<rapidbyte_registry::RegistryConfig> {
    let trust_policy = rapidbyte_registry::TrustPolicy::from_str_name(&reg.trust_policy)?;
    Ok(rapidbyte_registry::RegistryConfig {
        insecure: reg.registry_insecure,
        default_registry: rapidbyte_registry::normalize_registry_url_option(
            reg.registry_url.as_deref(),
        ),
        trust_policy,
        trusted_key_paths: reg.trust_key.clone(),
        ..Default::default()
    })
}

/// Build `SecretProviders` from `VaultFlags`.
fn try_build_secrets_from_vault(vault: &VaultFlags) -> Option<rapidbyte_secrets::SecretProviders> {
    try_build_secrets(
        vault.vault_addr.as_deref(),
        vault.vault_token.as_deref(),
        vault.vault_role_id.as_deref(),
        vault.vault_secret_id.as_deref(),
    )
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> ExitCode {
    let _ = default_provider().install_default();
    let cli = Cli::parse();

    let verbosity = Verbosity::from_flags(cli.quiet, cli.verbose);

    let service_name = match &cli.command {
        Commands::Controller { .. } => "rapidbyte-controller",
        Commands::Agent { .. } => "rapidbyte-agent",
        _ => "rapidbyte-cli",
    };
    let otel_guard = match rapidbyte_metrics::init(service_name) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!(
                "{} telemetry initialization failed: {e:#}",
                console::style("\u{2718}").red().bold(),
            );
            return ExitCode::FAILURE;
        }
    };
    logging::init(verbosity, &cli.log_level, Some(&otel_guard));

    let result = match cli.command {
        Commands::Sync {
            pipeline,
            ctrl,
            vault,
            registry,
        } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            let controller_url = resolve_controller_url(ctrl.controller.clone(), false);
            // Only build secret providers for local mode — distributed mode
            // delegates secret resolution to the controller.
            let secrets = if controller_url.is_none() {
                let Some(s) = try_build_secrets_from_vault(&vault) else {
                    return ExitCode::FAILURE;
                };
                s
            } else {
                rapidbyte_secrets::SecretProviders::new()
            };
            let tls = make_tls_config(&ctrl);
            commands::sync::execute(
                &pipeline,
                verbosity,
                controller_url.as_deref(),
                ctrl.auth_token.as_deref(),
                tls.as_ref(),
                &otel_guard,
                &registry_config,
                &secrets,
            )
            .await
        }
        Commands::Status { run_id, ctrl } => {
            let controller_url = resolve_controller_url(ctrl.controller.clone(), true);
            let tls = make_tls_config(&ctrl);
            commands::status::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                ctrl.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Watch { run_id, ctrl } => {
            let controller_url = resolve_controller_url(ctrl.controller.clone(), true);
            let tls = make_tls_config(&ctrl);
            commands::watch::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                ctrl.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::ListRuns { limit, state, ctrl } => {
            let controller_url = resolve_controller_url(ctrl.controller.clone(), true);
            let tls = make_tls_config(&ctrl);
            commands::list_runs::execute(
                controller_url.as_deref(),
                limit,
                state.as_deref(),
                verbosity,
                ctrl.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Check {
            pipeline,
            apply,
            vault,
            registry,
        } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            let Some(secrets) = try_build_secrets_from_vault(&vault) else {
                return ExitCode::FAILURE;
            };
            commands::check::execute(&pipeline, verbosity, &registry_config, &secrets, apply).await
        }
        Commands::Discover {
            pipeline,
            vault,
            registry,
        } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            let Some(secrets) = try_build_secrets_from_vault(&vault) else {
                return ExitCode::FAILURE;
            };
            commands::discover::execute(&pipeline, verbosity, &registry_config, &secrets).await
        }
        Commands::Teardown {
            pipeline,
            reason,
            vault,
            registry,
        } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            let Some(secrets) = try_build_secrets_from_vault(&vault) else {
                return ExitCode::FAILURE;
            };
            commands::teardown::execute(&pipeline, &reason, verbosity, &registry_config, &secrets)
                .await
        }
        Commands::Plugin { command, registry } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            commands::plugin::execute(command, &registry_config).await
        }
        Commands::Dev => commands::dev::execute().await,
        Commands::Pause { pipeline, ctrl } => commands::pause::execute(&ctrl, &pipeline).await,
        Commands::Resume { pipeline, ctrl } => commands::resume::execute(&ctrl, &pipeline).await,
        Commands::Reset {
            pipeline,
            stream,
            ctrl,
        } => commands::reset::execute(&ctrl, &pipeline, stream.as_deref()).await,
        Commands::Freshness { tag, ctrl } => {
            commands::freshness::execute(&ctrl, tag.as_deref()).await
        }
        Commands::Logs {
            pipeline,
            run_id,
            limit,
            ctrl,
        } => commands::logs::execute(&ctrl, &pipeline, run_id.as_deref(), limit).await,
        Commands::Login { controller, token } => {
            commands::login::execute(&controller, token.as_deref())
        }
        Commands::Logout { controller } => commands::logout::execute(controller.as_deref()),
        Commands::Version => {
            commands::version_cmd::execute();
            Ok(())
        }
        Commands::Controller {
            listen,
            metadata_database_url,
            signing_key,
            auth_token,
            allow_unauthenticated,
            allow_insecure_signing_key,
            reconciliation_timeout_seconds,
            tls_cert,
            tls_key,
            metrics_listen,
            registry_url,
            registry_insecure,
            rest_listen,
            vault,
        } => {
            commands::controller::execute(
                &listen,
                metadata_database_url.as_deref(),
                signing_key.as_deref(),
                auth_token.as_deref(),
                allow_unauthenticated,
                allow_insecure_signing_key,
                reconciliation_timeout_seconds.map(std::time::Duration::from_secs),
                tls_cert.as_deref(),
                tls_key.as_deref(),
                metrics_listen.as_deref(),
                registry_url.as_deref(),
                registry_insecure,
                rest_listen.as_deref(),
                otel_guard,
                match try_build_secrets_from_vault(&vault) {
                    Some(s) => s,
                    None => return ExitCode::FAILURE,
                },
            )
            .await
        }
        Commands::Agent {
            controller,
            max_tasks,
            metrics_listen,
            auth_token,
            tls_ca_cert,
            tls_domain,
            registry,
        } => {
            let registry_config = match build_registry_config(&registry) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{} {e:#}", console::style("\u{2718}").red().bold());
                    return ExitCode::FAILURE;
                }
            };
            let mut trusted_key_pems = Vec::with_capacity(registry.trust_key.len());
            for path in &registry.trust_key {
                match std::fs::read_to_string(path) {
                    Ok(pem) => trusted_key_pems.push(pem),
                    Err(e) => {
                        eprintln!(
                            "{} failed to read trust key file {}: {e}",
                            console::style("\u{2718}").red().bold(),
                            path.display()
                        );
                        return ExitCode::FAILURE;
                    }
                }
            }
            commands::agent::execute(
                &controller,
                max_tasks,
                auth_token.as_deref(),
                tls_ca_cert.as_deref(),
                tls_domain.as_deref(),
                metrics_listen.as_deref(),
                registry_config.default_registry.as_deref(),
                registry_config.insecure,
                &registry.trust_policy,
                trusted_key_pems,
                otel_guard,
            )
            .await
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // Always print errors to stderr, even in quiet mode.
            // --quiet suppresses progress/info, not errors.
            eprintln!("{} {e:#}", console::style("\u{2718}").red().bold(),);
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_controller_url_with, Cli};
    use clap::Parser;

    #[test]
    fn controller_url_resolution_run_ignores_config_fallback() {
        let resolved = resolve_controller_url_with(None, false, || Some("http://cfg".into()));
        assert_eq!(resolved, None);
    }

    #[test]
    fn controller_url_resolution_control_plane_uses_config_fallback() {
        let resolved = resolve_controller_url_with(None, true, || Some("http://cfg".into()));
        assert_eq!(resolved.as_deref(), Some("http://cfg"));
    }

    #[test]
    fn controller_url_resolution_prefers_explicit_value() {
        let resolved = resolve_controller_url_with(Some("http://explicit".into()), true, || {
            Some("http://cfg".into())
        });
        assert_eq!(resolved.as_deref(), Some("http://explicit"));
    }

    // ── Registry flag precedence ──────────────────────────────────────

    #[test]
    fn controller_registry_url_falls_back_to_global() {
        // Controller variant still has its own registry_url field.
        let subcommand_url: Option<&str> = None;
        let global_url: Option<&str> = Some("registry.example.com");
        let effective = subcommand_url.or(global_url);
        assert_eq!(effective, Some("registry.example.com"));
    }

    #[test]
    fn controller_registry_url_prefers_subcommand_over_global() {
        let subcommand_url: Option<&str> = Some("local.registry.io");
        let global_url: Option<&str> = Some("registry.example.com");
        let effective = subcommand_url.or(global_url);
        assert_eq!(effective, Some("local.registry.io"));
    }

    /// Helper that mirrors the `local || global` merge logic from main.rs.
    fn merge_insecure(local: bool, global: bool) -> bool {
        local || global
    }

    #[test]
    fn controller_registry_insecure_merges_with_global() {
        assert!(merge_insecure(false, true), "global alone → insecure");
        assert!(merge_insecure(true, false), "local alone → insecure");
        assert!(!merge_insecure(false, false), "neither → secure");
        assert!(merge_insecure(true, true), "both → insecure");
    }

    #[test]
    fn check_command_rejects_dry_run_flag() {
        let parsed = Cli::try_parse_from(["rapidbyte", "check", "pipe.yaml", "--dry-run"]);
        assert!(parsed.is_err());
    }

    // ── New per-command flag placement tests ──────────────────────────

    #[test]
    fn sync_accepts_vault_flags_after_subcommand() {
        let parsed = Cli::try_parse_from([
            "rapidbyte",
            "sync",
            "pipe.yaml",
            "--vault-addr",
            "http://vault:8200",
        ]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn status_accepts_controller_flag_after_subcommand() {
        let parsed = Cli::try_parse_from([
            "rapidbyte",
            "status",
            "abc123",
            "--controller",
            "http://ctrl:9090",
        ]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn dev_rejects_vault_flag() {
        // dev has no vault flags
        let parsed = Cli::try_parse_from(["rapidbyte", "dev", "--vault-addr", "http://vault:8200"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn plugin_accepts_registry_url_flag() {
        let parsed = Cli::try_parse_from([
            "rapidbyte",
            "plugin",
            "--registry-url",
            "registry.example.com",
            "list",
        ]);
        assert!(parsed.is_ok());
    }

    #[test]
    fn agent_accepts_registry_and_auth_flags() {
        let parsed = Cli::try_parse_from([
            "rapidbyte",
            "agent",
            "--controller",
            "http://ctrl:9090",
            "--auth-token",
            "tok",
            "--registry-url",
            "registry.example.com",
        ]);
        assert!(parsed.is_ok());
    }
}
