//! Interactive dev shell for exploring data pipelines.
//!
//! Provides a REPL that connects to source connectors, streams data
//! into an in-memory Arrow workspace, and queries with SQL via DataFusion.

#![warn(clippy::pedantic)]

/// Entry point for the dev shell.
///
/// # Errors
///
/// Returns an error if the REPL setup fails.
pub async fn run() -> anyhow::Result<()> {
    eprintln!("rapidbyte dev shell — not yet implemented");
    Ok(())
}
