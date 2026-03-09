use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(author, version, about = "Connector-agnostic benchmark runner")]
pub struct BenchCli {
    #[command(subcommand)]
    pub command: BenchCommand,
}

#[derive(Debug, Subcommand)]
pub enum BenchCommand {
    Run(RunArgs),
    Compare(CompareArgs),
}

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(long, default_value = "benchmarks/scenarios")]
    pub scenario_dir: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct CompareArgs {
    pub baseline: PathBuf,
    pub candidate: PathBuf,
}
