use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use once_cell::sync::OnceCell;

static CONNECTOR_DIR: OnceCell<PathBuf> = OnceCell::new();

pub fn prepare_connector_dir() -> Result<PathBuf> {
    CONNECTOR_DIR
        .get_or_try_init(|| {
            let repo_root = repo_root();
            let output_dir = repo_root.join("target/connectors");
            std::fs::create_dir_all(&output_dir).context("failed to create target/connectors")?;

            ensure_connector(
                &repo_root,
                "source-postgres",
                "source_postgres",
                &output_dir,
            )?;
            ensure_connector(&repo_root, "dest-postgres", "dest_postgres", &output_dir)?;
            ensure_connector(&repo_root, "transform-sql", "transform_sql", &output_dir)?;
            ensure_connector(
                &repo_root,
                "transform-validate",
                "transform_validate",
                &output_dir,
            )?;

            Ok(output_dir)
        })
        .cloned()
}

fn ensure_connector(
    repo_root: &Path,
    connector_dir_name: &str,
    wasm_file_stem: &str,
    output_dir: &Path,
) -> Result<()> {
    let target_file = output_dir.join(format!("{wasm_file_stem}.wasm"));
    if target_file.exists() {
        return Ok(());
    }

    let connector_dir = repo_root.join("connectors").join(connector_dir_name);
    let status = Command::new("cargo")
        .arg("build")
        .current_dir(&connector_dir)
        .status()
        .with_context(|| format!("failed to run cargo build in {}", connector_dir.display()))?;

    if !status.success() {
        bail!("cargo build failed in {}", connector_dir.display());
    }

    let built_wasm = connector_dir
        .join("target/wasm32-wasip2/debug")
        .join(format!("{wasm_file_stem}.wasm"));
    std::fs::copy(&built_wasm, &target_file).with_context(|| {
        format!(
            "failed to copy wasm {} -> {}",
            built_wasm.display(),
            target_file.display()
        )
    })?;

    Ok(())
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tests/e2e must be nested under repository root")
        .to_path_buf()
}
