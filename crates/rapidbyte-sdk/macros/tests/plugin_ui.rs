use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("repo root")
        .to_path_buf()
}

fn fixture_manifest(name: &str) -> PathBuf {
    repo_root()
        .join("tests")
        .join("plugin-ui")
        .join(name)
        .join("Cargo.toml")
}

fn cargo_check_fixture(name: &str) -> std::process::Output {
    let _guard = test_lock().lock().expect("plugin ui test lock");
    let target_dir = std::env::temp_dir().join(format!("rapidbyte-plugin-ui-{name}"));
    let _ = std::fs::remove_dir_all(&target_dir);
    std::fs::create_dir_all(&target_dir).expect("create target dir");

    Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
        .arg("check")
        .arg("--manifest-path")
        .arg(fixture_manifest(name))
        .arg("--offline")
        .arg("--quiet")
        .env("CARGO_TARGET_DIR", target_dir)
        .current_dir(repo_root())
        .output()
        .expect("cargo check fixture")
}

#[test]
fn plugin_accepts_v2_source_export_with_feature_traits() {
    let output = cargo_check_fixture("source-v2");
    assert!(
        output.status.success(),
        "expected v2 source fixture to compile, stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn plugin_accepts_v2_destination_export() {
    let output = cargo_check_fixture("destination-v2");
    assert!(
        output.status.success(),
        "expected v2 destination fixture to compile, stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn plugin_accepts_v2_transform_export() {
    let output = cargo_check_fixture("transform-v2");
    assert!(
        output.status.success(),
        "expected v2 transform fixture to compile, stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn plugin_rejects_old_context_shaped_source_signatures() {
    for fixture in [
        "source-old-context",
        "destination-old-context",
        "transform-old-context",
    ] {
        let output = cargo_check_fixture(fixture);
        assert!(
            !output.status.success(),
            "expected old-context fixture `{fixture}` to fail compilation"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("incompatible type for trait")
                || stderr.contains("declaration in trait")
                || stderr.contains("expected signature"),
            "expected trait-signature mismatch in stderr for `{fixture}`, got:\n{stderr}"
        );
    }
}
