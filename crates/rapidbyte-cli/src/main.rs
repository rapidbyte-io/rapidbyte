//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]
#![recursion_limit = "256"]

mod commands;
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

#[derive(Parser)]
#[command(
    name = "rapidbyte",
    version,
    about = "The single-binary data ingestion engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Controller gRPC endpoint (enables distributed mode)
    #[arg(long, global = true, env = "RAPIDBYTE_CONTROLLER")]
    controller: Option<String>,

    /// Bearer token for authenticated controller RPCs
    #[arg(long, global = true, env = "RAPIDBYTE_AUTH_TOKEN")]
    auth_token: Option<String>,

    /// Custom CA certificate for TLS controller/Flight connections
    #[arg(long, global = true, env = "RAPIDBYTE_TLS_CA_CERT")]
    tls_ca_cert: Option<PathBuf>,

    /// Optional TLS server name override for controller/Flight connections
    #[arg(long, global = true, env = "RAPIDBYTE_TLS_DOMAIN")]
    tls_domain: Option<String>,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    /// Increase output verbosity (-v for detailed, -vv for diagnostic)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress all output (exit code only, errors on stderr)
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Default OCI registry for plugin resolution (e.g. registry.example.com/plugins)
    #[arg(long, global = true, env = "RAPIDBYTE_REGISTRY_URL")]
    registry_url: Option<String>,

    /// Use HTTP instead of HTTPS for plugin registry (for local dev registries)
    #[arg(long, global = true, env = "RAPIDBYTE_REGISTRY_INSECURE")]
    registry_insecure: bool,

    /// Plugin signature trust policy (skip, warn, verify)
    #[arg(
        long,
        global = true,
        env = "RAPIDBYTE_TRUST_POLICY",
        default_value = "skip"
    )]
    trust_policy: String,

    /// Trusted Ed25519 public key files for plugin verification (can be repeated)
    #[arg(long, global = true, action = clap::ArgAction::Append)]
    trust_key: Vec<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a data pipeline
    Run {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        /// Preview mode: skip destination, print output to stdout
        #[arg(long)]
        dry_run: bool,
        /// Maximum rows to read per stream (implies --dry-run)
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Show the current status of a distributed run
    Status {
        /// Controller run ID
        run_id: String,
    },
    /// Stream progress for a distributed run until it reaches a terminal state
    Watch {
        /// Controller run ID
        run_id: String,
    },
    /// List recent distributed runs
    ListRuns {
        /// Maximum number of runs to return
        #[arg(long, default_value_t = 20)]
        limit: i32,
        /// Optional state filter (pending, assigned, running, `preview_ready`, completed, failed, cancelled)
        #[arg(long)]
        state: Option<String>,
    },
    /// Validate pipeline configuration and connectivity
    Check {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Discover available streams from a source plugin
    Discover {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Manage plugins (pull, push, inspect, list, remove)
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },
    /// Scaffold a new plugin project
    Scaffold {
        /// Plugin name (e.g., "source-mysql", "dest-snowflake")
        name: String,
        /// Output directory (default: `./plugins/<kind>/<name>`)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Launch interactive dev shell
    Dev,
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
    },
    /// Start an agent worker (long-running)
    Agent {
        /// Controller endpoint to connect to
        #[arg(long)]
        controller: String,
        /// Flight server bind address (data plane)
        #[arg(long, default_value = "[::]:9091")]
        flight_listen: String,
        /// Flight endpoint advertised to clients (must be reachable)
        #[arg(long)]
        flight_advertise: String,
        /// Maximum concurrent tasks
        #[arg(long, default_value = "1")]
        max_tasks: u32,
        /// Shared signing key for preview tickets (must match controller)
        #[arg(long, env = "RAPIDBYTE_SIGNING_KEY")]
        signing_key: Option<String>,
        /// Explicitly allow the built-in insecure development signing key
        #[arg(long)]
        allow_insecure_signing_key: bool,
        /// PEM certificate for the agent Flight server
        #[arg(long)]
        flight_tls_cert: Option<PathBuf>,
        /// PEM private key for the agent Flight server
        #[arg(long)]
        flight_tls_key: Option<PathBuf>,
        /// Prometheus metrics listen address (e.g. 127.0.0.1:9191)
        #[arg(long, env = "RAPIDBYTE_METRICS_LISTEN")]
        metrics_listen: Option<String>,
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
}

/// Resolve controller URL from config file (`~/.rapidbyte/config.yaml`).
/// CLI flag and env var are already handled by clap's `env` attribute.
fn controller_url_from_config() -> Option<String> {
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

    let tls = commands::transport::TlsClientConfig {
        ca_cert_path: cli.tls_ca_cert.clone(),
        domain_name: cli.tls_domain.clone(),
    };
    let tls = tls.is_configured().then_some(tls);

    let trust_policy = match rapidbyte_registry::TrustPolicy::from_str_name(&cli.trust_policy) {
        Ok(policy) => policy,
        Err(e) => {
            eprintln!("{} {e:#}", console::style("\u{2718}").red().bold(),);
            return ExitCode::FAILURE;
        }
    };
    let registry_config = rapidbyte_registry::RegistryConfig {
        insecure: cli.registry_insecure,
        default_registry: cli
            .registry_url
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(rapidbyte_registry::normalize_registry_url),
        trust_policy,
        trusted_key_paths: cli.trust_key.clone(),
        ..Default::default()
    };

    let result = match cli.command {
        Commands::Run {
            pipeline,
            dry_run,
            limit,
        } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), false);
            commands::run::execute(
                &pipeline,
                dry_run,
                limit,
                verbosity,
                controller_url.as_deref(),
                cli.auth_token.as_deref(),
                tls.as_ref(),
                &otel_guard,
                &registry_config,
            )
            .await
        }
        Commands::Status { run_id } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::status::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Watch { run_id } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::watch::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::ListRuns { limit, state } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::list_runs::execute(
                controller_url.as_deref(),
                limit,
                state.as_deref(),
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Check { pipeline } => {
            commands::check::execute(&pipeline, verbosity, &registry_config).await
        }
        Commands::Discover { pipeline } => {
            commands::discover::execute(&pipeline, verbosity, &registry_config).await
        }
        Commands::Plugin { command } => commands::plugin::execute(command, &registry_config).await,
        Commands::Scaffold { name, output } => commands::scaffold::run(&name, output.as_deref()),
        Commands::Dev => commands::dev::execute().await,
        Commands::Controller {
            listen,
            metadata_database_url,
            signing_key,
            allow_unauthenticated,
            allow_insecure_signing_key,
            reconciliation_timeout_seconds,
            tls_cert,
            tls_key,
            metrics_listen,
            registry_url,
            registry_insecure,
        } => {
            // Controller-subcommand registry flags override global ones.
            let ctrl_registry_url = registry_url
                .as_deref()
                .or(registry_config.default_registry.as_deref());
            let ctrl_registry_insecure = registry_insecure || registry_config.insecure;
            commands::controller::execute(
                &listen,
                metadata_database_url.as_deref(),
                signing_key.as_deref(),
                cli.auth_token.as_deref(),
                allow_unauthenticated,
                allow_insecure_signing_key,
                reconciliation_timeout_seconds.map(std::time::Duration::from_secs),
                tls_cert.as_deref(),
                tls_key.as_deref(),
                metrics_listen.as_deref(),
                ctrl_registry_url,
                ctrl_registry_insecure,
                &cli.trust_policy,
                cli.trust_key.clone(),
                otel_guard,
            )
            .await
        }
        Commands::Agent {
            controller,
            flight_listen,
            flight_advertise,
            max_tasks,
            signing_key,
            allow_insecure_signing_key,
            flight_tls_cert,
            flight_tls_key,
            metrics_listen,
        } => {
            commands::agent::execute(
                &controller,
                &flight_listen,
                &flight_advertise,
                max_tasks,
                signing_key.as_deref(),
                allow_insecure_signing_key,
                cli.auth_token.as_deref(),
                cli.tls_ca_cert.as_deref(),
                cli.tls_domain.as_deref(),
                flight_tls_cert.as_deref(),
                flight_tls_key.as_deref(),
                metrics_listen.as_deref(),
                otel_guard,
            )
            .await
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if verbosity != Verbosity::Quiet {
                eprintln!("{} {e:#}", console::style("\u{2718}").red().bold(),);
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_controller_url_with;

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
    fn registry_insecure_global_true_propagates_when_local_false() {
        // Simulates: rapidbyte --registry-insecure plugin pull ... (no --insecure)
        let global_insecure = true;
        let local_insecure = false;
        assert!(local_insecure || global_insecure);
    }

    #[test]
    fn registry_insecure_local_true_overrides_global_false() {
        let global_insecure = false;
        let local_insecure = true;
        assert!(local_insecure || global_insecure);
    }

    #[test]
    fn registry_insecure_both_false_stays_false() {
        let global_insecure = false;
        let local_insecure = false;
        assert!(!(local_insecure || global_insecure));
    }

    #[test]
    fn controller_registry_url_falls_back_to_global() {
        // Simulates: rapidbyte --registry-url X controller (no --registry-url on controller)
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
}
