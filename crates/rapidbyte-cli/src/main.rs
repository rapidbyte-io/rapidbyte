mod commands;
mod logging;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "rapidbyte", version, about = "The single-binary data ingestion engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a data pipeline
    Run {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Validate pipeline configuration and connectivity
    Check {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    logging::init(&cli.log_level);

    match cli.command {
        Commands::Run { pipeline } => {
            commands::run::execute(&pipeline).await
        }
        Commands::Check { pipeline } => {
            commands::check::execute(&pipeline).await
        }
    }
}
