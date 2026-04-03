# CLI Alignment Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Run /simplify after each major task completes.

**Goal:** Align the CLI with docs/CLI.md — rename `run` → `sync`, add 8 new commands, update docs.

**Architecture:** Rename `run` command to `sync` and consolidate local+distributed paths into one module. Add operational commands (pause/resume/reset/freshness/logs) via a shared REST client. Add auth commands (login/logout) via local config file. Update docs/CLI.md to match reality.

**Tech Stack:** Rust, clap, reqwest (new), tokio, serde_json/serde_yaml

**Spec:** `docs/superpowers/specs/2026-04-03-cli-alignment-design.md`

**IMPORTANT:** Work in a git worktree: `.worktrees/cli-alignment` on branch `feature/cli-alignment`

---

## File Map

### New files in `crates/rapidbyte-cli/src/`

| File | Responsibility |
|------|---------------|
| `commands/sync.rs` | Consolidated sync command (replaces run.rs + distributed_run.rs) |
| `commands/rest_client.rs` | Shared HTTP client for REST API calls |
| `commands/config.rs` | `~/.rapidbyte/config.yaml` read/write helpers |
| `commands/pause.rs` | Pause pipeline command |
| `commands/resume.rs` | Resume pipeline command |
| `commands/reset.rs` | Reset pipeline state command |
| `commands/freshness.rs` | Freshness check command |
| `commands/logs.rs` | Pipeline logs query command |
| `commands/login.rs` | Store controller token locally |
| `commands/logout.rs` | Remove stored token |
| `commands/version_cmd.rs` | Print version info |

### Deleted files

| File | Reason |
|------|--------|
| `commands/run.rs` | Renamed/consolidated into `sync.rs` |
| `commands/distributed_run.rs` | Consolidated into `sync.rs` |

### Modified files

| File | Changes |
|------|---------|
| `main.rs` | `Commands::Run` → `Commands::Sync`, 8 new command variants, dispatch |
| `commands/mod.rs` | Module declarations updated |
| `Cargo.toml` | Add `reqwest` dependency |
| `docs/CLI.md` | Full alignment pass |

---

## Task 1: Create worktree + add reqwest dependency

**Files:**
- Modify: `crates/rapidbyte-cli/Cargo.toml`

- [ ] **Step 1: Create worktree**

```bash
cd /home/netf/rapidbyte
git worktree add .worktrees/cli-alignment -b feature/cli-alignment
```

- [ ] **Step 2: Add reqwest to Cargo.toml**

Add under `[dependencies]`:
```toml
reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
```

- [ ] **Step 3: Verify compilation**

```bash
cargo check -p rapidbyte-cli
```

- [ ] **Step 4: Commit**

```bash
git commit -m "deps: add reqwest for REST client"
```

---

## Task 2: Rename `run` → `sync` (command + consolidation)

**Files:**
- Delete: `crates/rapidbyte-cli/src/commands/run.rs`
- Delete: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Create: `crates/rapidbyte-cli/src/commands/sync.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

- [ ] **Step 1: Create sync.rs by merging run.rs + distributed_run.rs**

Copy `run.rs` content into `sync.rs`. Move `distributed_run.rs` content into the same file as `execute_remote()`. The structure:

```rust
// commands/sync.rs

// Main entry point — routes local vs remote
pub async fn execute(
    pipeline: &Path,
    verbosity: Verbosity,
    controller: Option<&str>,
    auth_token: Option<&str>,
    tls: Option<&TlsClientConfig>,
    otel_guard: &rapidbyte_metrics::OtelGuard,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    if let Some(url) = controller {
        return execute_remote(pipeline, url, auth_token, tls, verbosity).await;
    }
    execute_local(pipeline, verbosity, otel_guard, registry_config, secrets).await
}

// Local execution (was run.rs body)
async fn execute_local(...) -> Result<()> { ... }

// Remote execution via gRPC (was distributed_run.rs)
async fn execute_remote(...) -> Result<()> { ... }

// All helper functions from both files
```

- [ ] **Step 2: Update commands/mod.rs**

Replace:
```rust
pub mod distributed_run;
pub mod run;
```
With:
```rust
pub mod sync;
```

- [ ] **Step 3: Update main.rs**

Change `Commands::Run` → `Commands::Sync`:
```rust
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
```

Update dispatch:
```rust
Commands::Sync { pipeline, ctrl, vault, registry } => {
    // ... same logic, calling commands::sync::execute
}
```

- [ ] **Step 4: Delete old files**

```bash
git rm crates/rapidbyte-cli/src/commands/run.rs
git rm crates/rapidbyte-cli/src/commands/distributed_run.rs
```

- [ ] **Step 5: Verify**

```bash
cargo test -p rapidbyte-cli
cargo clippy --workspace --all-targets -- -D warnings
```

- [ ] **Step 6: Commit**

```bash
git commit -m "refactor: rename run → sync, consolidate local+distributed into sync.rs"
```

---

## Task 3: REST client helper

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/rest_client.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Create rest_client.rs**

```rust
use anyhow::{Context, Result, bail};

pub struct RestClient {
    base_url: String,
    client: reqwest::Client,
    auth_token: Option<String>,
}

impl RestClient {
    /// Build a REST client from controller URL and optional auth token.
    pub fn new(base_url: &str, auth_token: Option<&str>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .context("failed to create HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            auth_token: auth_token.map(String::from),
        })
    }

    /// Build from ControllerFlags, resolving URL from flag/env/config.
    pub fn from_controller_url(url: &str, auth_token: Option<&str>) -> Result<Self> {
        Self::new(url, auth_token)
    }

    pub async fn get(&self, path: &str) -> Result<serde_json::Value> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self.client.get(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let resp = req.send().await.context("REST request failed")?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await
            .context("failed to parse response JSON")?;
        if !status.is_success() {
            let msg = body.get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            bail!("{msg} (HTTP {status})");
        }
        Ok(body)
    }

    pub async fn post(&self, path: &str, body: Option<&serde_json::Value>) -> Result<serde_json::Value> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self.client.post(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await.context("REST request failed")?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await
            .context("failed to parse response JSON")?;
        if !status.is_success() {
            let msg = body.get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            bail!("{msg} (HTTP {status})");
        }
        Ok(body)
    }
}
```

- [ ] **Step 2: Add to mod.rs**

```rust
pub mod rest_client;
```

- [ ] **Step 3: Verify and commit**

```bash
cargo check -p rapidbyte-cli
git commit -m "feat: add shared REST client for operational commands"
```

---

## Task 4: Operational commands (pause/resume/reset)

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/pause.rs`
- Create: `crates/rapidbyte-cli/src/commands/resume.rs`
- Create: `crates/rapidbyte-cli/src/commands/reset.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`

- [ ] **Step 1: Create pause.rs**

```rust
use anyhow::{Result, bail};
use super::rest_client::RestClient;

pub async fn execute(controller_url: &str, auth_token: Option<&str>, pipeline: &str) -> Result<()> {
    let client = RestClient::new(controller_url, auth_token)?;
    let resp = client.post(&format!("/api/v1/pipelines/{pipeline}/pause"), None).await?;
    let state = resp.get("state").and_then(|v| v.as_str()).unwrap_or("paused");
    eprintln!("Pipeline '{pipeline}' {state}");
    Ok(())
}
```

- [ ] **Step 2: Create resume.rs** (same pattern)

- [ ] **Step 3: Create reset.rs**

```rust
pub async fn execute(
    controller_url: &str,
    auth_token: Option<&str>,
    pipeline: &str,
    stream: Option<&str>,
) -> Result<()> {
    let client = RestClient::new(controller_url, auth_token)?;
    let body = stream.map(|s| serde_json::json!({ "stream": s }));
    let resp = client.post(
        &format!("/api/v1/pipelines/{pipeline}/reset"),
        body.as_ref(),
    ).await?;
    let cleared = resp.get("cursors_cleared").and_then(|v| v.as_u64()).unwrap_or(0);
    eprintln!("Cleared {cleared} cursor(s) for '{pipeline}'. Next sync: full refresh.");
    Ok(())
}
```

- [ ] **Step 4: Add command variants to main.rs**

```rust
/// Pause scheduled runs for a pipeline
Pause {
    #[arg(long)]
    pipeline: String,
    #[command(flatten)]
    ctrl: ControllerFlags,
},
/// Resume scheduled runs for a pipeline
Resume {
    #[arg(long)]
    pipeline: String,
    #[command(flatten)]
    ctrl: ControllerFlags,
},
/// Clear sync state (cursors). Next run = full refresh
Reset {
    #[arg(long)]
    pipeline: String,
    #[arg(long)]
    stream: Option<String>,
    #[command(flatten)]
    ctrl: ControllerFlags,
},
```

- [ ] **Step 5: Add dispatch in main()**

For each command, resolve controller URL (with config fallback), then call the command:
```rust
Commands::Pause { pipeline, ctrl } => {
    let url = resolve_controller_url(ctrl.controller.clone(), true)
        .ok_or_else(|| anyhow::anyhow!("--controller is required"))?;
    commands::pause::execute(&url, ctrl.auth_token.as_deref(), &pipeline).await
}
```

- [ ] **Step 6: Add to mod.rs, verify, commit**

```bash
cargo test -p rapidbyte-cli
git commit -m "feat: add pause/resume/reset CLI commands via REST"
```

---

## Task 5: Freshness + Logs commands

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/freshness.rs`
- Create: `crates/rapidbyte-cli/src/commands/logs.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Create freshness.rs**

```rust
pub async fn execute(controller_url: &str, auth_token: Option<&str>, tag: Option<&str>) -> Result<()> {
    let client = RestClient::new(controller_url, auth_token)?;
    let path = match tag {
        Some(t) => format!("/api/v1/freshness?tag={t}"),
        None => "/api/v1/freshness".to_string(),
    };
    let resp = client.get(&path).await?;
    let items = resp.as_array().unwrap_or(&vec![]);
    if items.is_empty() {
        eprintln!("No freshness data available.");
        return Ok(());
    }
    for item in items {
        let pipeline = item.get("pipeline").and_then(|v| v.as_str()).unwrap_or("?");
        let age = item.get("last_sync_age").and_then(|v| v.as_str()).unwrap_or("?");
        let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let symbol = if status == "fresh" { "✓" } else { "✗" };
        eprintln!("  {pipeline:<30} {age:<12} {symbol} {status}");
    }
    Ok(())
}
```

- [ ] **Step 2: Create logs.rs**

```rust
pub async fn execute(
    controller_url: &str,
    auth_token: Option<&str>,
    pipeline: &str,
    run_id: Option<&str>,
    limit: u32,
) -> Result<()> {
    let client = RestClient::new(controller_url, auth_token)?;
    let mut path = format!("/api/v1/logs?pipeline={pipeline}&limit={limit}");
    if let Some(id) = run_id {
        path.push_str(&format!("&run_id={id}"));
    }
    let resp = client.get(&path).await?;
    let items = resp.get("items").and_then(|v| v.as_array()).unwrap_or(&vec![]);
    for entry in items {
        let ts = entry.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let level = entry.get("level").and_then(|v| v.as_str()).unwrap_or("info");
        let msg = entry.get("message").and_then(|v| v.as_str()).unwrap_or("");
        eprintln!("{ts}  [{level}]  {msg}");
    }
    Ok(())
}
```

- [ ] **Step 3: Add command variants + dispatch to main.rs**

```rust
/// Check data freshness for pipelines
Freshness {
    #[arg(long)]
    tag: Option<String>,
    #[command(flatten)]
    ctrl: ControllerFlags,
},
/// View pipeline logs
Logs {
    #[arg(long)]
    pipeline: String,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long, default_value_t = 20)]
    limit: u32,
    #[command(flatten)]
    ctrl: ControllerFlags,
},
```

- [ ] **Step 4: Add to mod.rs, verify, commit**

```bash
cargo test -p rapidbyte-cli
git commit -m "feat: add freshness and logs CLI commands via REST"
```

---

## Task 6: Config helpers + login/logout

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/config.rs`
- Create: `crates/rapidbyte-cli/src/commands/login.rs`
- Create: `crates/rapidbyte-cli/src/commands/logout.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Create config.rs**

```rust
use std::path::PathBuf;
use anyhow::{Context, Result};

pub fn config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".rapidbyte").join("config.yaml")
}

pub fn read_config() -> Result<serde_yaml::Value> {
    let path = config_path();
    if !path.exists() {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&contents).context("failed to parse config.yaml")
}

pub fn write_config(value: &serde_yaml::Value) -> Result<()> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let yaml = serde_yaml::to_string(value).context("failed to serialize config")?;
    std::fs::write(&path, yaml)
        .with_context(|| format!("failed to write {}", path.display()))
}
```

- [ ] **Step 2: Create login.rs**

```rust
use anyhow::{Result, bail};
use super::config;

pub fn execute(controller_url: &str, token: Option<&str>) -> Result<()> {
    let token = match token {
        Some(t) => t.to_string(),
        None => {
            eprint!("Token: ");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            let t = input.trim().to_string();
            if t.is_empty() {
                bail!("token cannot be empty");
            }
            t
        }
    };

    let mut cfg = config::read_config()?;
    let map = cfg.as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("config is not a mapping"))?;

    let controller = serde_yaml::Value::Mapping({
        let mut m = serde_yaml::Mapping::new();
        m.insert(
            serde_yaml::Value::String("url".into()),
            serde_yaml::Value::String(controller_url.into()),
        );
        m.insert(
            serde_yaml::Value::String("token".into()),
            serde_yaml::Value::String(token),
        );
        m
    });
    map.insert(
        serde_yaml::Value::String("controller".into()),
        controller,
    );

    config::write_config(&cfg)?;
    eprintln!("Logged in to {controller_url}");
    Ok(())
}
```

- [ ] **Step 3: Create logout.rs**

```rust
use anyhow::Result;
use super::config;

pub fn execute(controller_url: Option<&str>) -> Result<()> {
    let mut cfg = config::read_config()?;
    if let Some(map) = cfg.as_mapping_mut() {
        if controller_url.is_some() {
            // Remove token but keep URL if it matches
            if let Some(ctrl) = map.get_mut(&serde_yaml::Value::String("controller".into())) {
                if let Some(ctrl_map) = ctrl.as_mapping_mut() {
                    ctrl_map.remove(&serde_yaml::Value::String("token".into()));
                }
            }
        } else {
            // Remove entire controller config
            map.remove(&serde_yaml::Value::String("controller".into()));
        }
        config::write_config(&cfg)?;
    }
    eprintln!("Logged out");
    Ok(())
}
```

- [ ] **Step 4: Add command variants to main.rs**

```rust
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
```

- [ ] **Step 5: Add dispatch, mod.rs, verify, commit**

```bash
cargo test -p rapidbyte-cli
git commit -m "feat: add login/logout commands for token management"
```

---

## Task 7: Version command

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/version_cmd.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`

- [ ] **Step 1: Create version_cmd.rs**

```rust
pub fn execute() {
    println!("rapidbyte {} (rustc {})",
        env!("CARGO_PKG_VERSION"),
        rustc_version(),
    );
}

fn rustc_version() -> &'static str {
    env!("RUSTC_VERSION", "unknown")  // Set via build.rs if needed
    // Or just use a compile-time constant
}
```

Actually simpler — just use `env!("CARGO_PKG_VERSION")`:

```rust
pub fn execute() {
    eprintln!("rapidbyte {}", env!("CARGO_PKG_VERSION"));
}
```

- [ ] **Step 2: Add command variant + dispatch**

```rust
/// Print version information
Version,
```

Dispatch:
```rust
Commands::Version => {
    commands::version_cmd::execute();
    Ok(())
}
```

- [ ] **Step 3: Verify and commit**

```bash
cargo test -p rapidbyte-cli
git commit -m "feat: add version command"
```

---

## Task 8: Update docs/CLI.md

**Files:**
- Modify: `docs/CLI.md`

- [ ] **Step 1: Rename `serve` → `controller` throughout**

Find all `rapidbyte serve` references and replace with `rapidbyte controller`.

- [ ] **Step 2: Mark deferred commands**

For `compile`, `diff`, `assert`, `init`, `discover`: change status to `(deferred)` with one-line reason.

- [ ] **Step 3: Update `scaffold` path**

Change `rapidbyte scaffold <name>` to `rapidbyte plugin scaffold <name>`.

- [ ] **Step 4: Update operational commands from "planned" to documented**

Add proper documentation for `pause`, `resume`, `reset`, `freshness`, `logs`.

- [ ] **Step 5: Update login/logout/version from "planned" to documented**

- [ ] **Step 6: Verify consistency and commit**

```bash
git commit -m "docs: align CLI.md with implementation (serve→controller, deferred features, new commands)"
```

---

## Task 9: Final verification

- [ ] **Step 1: Full test suite**

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

- [ ] **Step 2: Manual help verification**

```bash
./target/debug/rapidbyte sync --help        # shows controller+vault+registry
./target/debug/rapidbyte pause --help       # shows --pipeline + controller
./target/debug/rapidbyte freshness --help   # shows --tag + controller
./target/debug/rapidbyte login --help       # shows --controller + --token
./target/debug/rapidbyte version            # prints version
./target/debug/rapidbyte dev --help         # shows only output flags
```

- [ ] **Step 3: Commit any cleanup**

```bash
git commit -m "chore: final cleanup for CLI alignment"
```
