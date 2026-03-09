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

    #[arg(long)]
    pub suite: Option<String>,

    #[arg(long)]
    pub env_profile: Option<String>,

    #[arg(long = "scenario")]
    pub scenarios: Vec<String>,

    #[arg(long, default_value = "target/benchmarks/results.jsonl")]
    pub output: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct CompareArgs {
    pub baseline: PathBuf,
    pub candidate: PathBuf,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{BenchCli, BenchCommand};

    #[test]
    fn run_args_accept_env_profile_flag() {
        let cli = BenchCli::parse_from([
            "rapidbyte-benchmarks",
            "run",
            "--suite",
            "lab",
            "--scenario",
            "pg_dest_insert",
            "--env-profile",
            "local-dev-postgres",
        ]);

        match cli.command {
            BenchCommand::Run(args) => {
                assert_eq!(args.suite.as_deref(), Some("lab"));
                assert_eq!(args.env_profile.as_deref(), Some("local-dev-postgres"));
                assert_eq!(args.scenarios, vec!["pg_dest_insert"]);
            }
            BenchCommand::Compare(_) => panic!("expected run command"),
        }
    }
}
