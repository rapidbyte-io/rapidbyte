# Vault Secret Management Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable pipeline YAML to reference secrets from HashiCorp Vault via `${vault:mount/path#key}` syntax, with controller-side resolution for distributed agents.

**Architecture:** New `rapidbyte-vault` crate wraps the `vaultrs` client library. The engine's config parser gains an async `parse_pipeline` function that resolves both env vars and Vault references. In distributed mode, the controller resolves secrets at dispatch time (not at enqueue), so unresolved YAML is what persists in the database.

**Tech Stack:** `vaultrs` (Vault client), `reqwest` (transitive via vaultrs), `regex` (existing), `tokio` (existing)

**Spec:** `docs/superpowers/specs/2026-03-15-vault-secret-management-design.md`

---

## Chunk 1: `rapidbyte-vault` Crate

### Task 1: Create crate skeleton and VaultClient with token auth

**Files:**
- Create: `crates/rapidbyte-vault/Cargo.toml`
- Create: `crates/rapidbyte-vault/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Create `Cargo.toml`**

```toml
[package]
name = "rapidbyte-vault"
version.workspace = true
edition.workspace = true

[dependencies]
anyhow = { workspace = true }
vaultrs = "0.7"
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
wiremock = "0.6"
```

- [ ] **Step 2: Add to workspace members in root `Cargo.toml`**

Find the `[workspace]` members list and add `"crates/rapidbyte-vault"`.

- [ ] **Step 3: Write `lib.rs` with types and token auth**

```rust
//! Vault secret resolution for pipeline configs.
//!
//! Wraps the `vaultrs` crate to provide a simplified API for reading
//! KV v2 secrets referenced in pipeline YAML via `${vault:mount/path#key}`.

use std::collections::HashMap;

use anyhow::{Context, Result};

/// Vault connection and authentication configuration.
///
/// `Debug` is deliberately not derived — contains secrets.
pub struct VaultConfig {
    /// Vault server address (e.g. `http://127.0.0.1:8200`).
    pub address: String,
    /// Authentication method.
    pub auth: VaultAuth,
}

/// Vault authentication method.
pub enum VaultAuth {
    /// Pre-existing token (e.g. from `VAULT_TOKEN` env var).
    Token(String),
    /// AppRole machine-to-machine authentication.
    AppRole {
        role_id: String,
        secret_id: String,
    },
}

/// A configured Vault client for reading KV v2 secrets.
pub struct VaultClient {
    client: vaultrs::client::VaultClient,
}

impl VaultClient {
    /// Create a new Vault client and authenticate.
    ///
    /// For `VaultAuth::Token`, the token is set directly.
    /// For `VaultAuth::AppRole`, the client exchanges role_id/secret_id
    /// for a token immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if Vault is unreachable or authentication fails.
    pub async fn new(config: VaultConfig) -> Result<Self> {
        let mut settings = vaultrs::client::VaultClientSettingsBuilder::default();
        settings.address(&config.address);

        match &config.auth {
            VaultAuth::Token(token) => {
                settings.token(token);
            }
            VaultAuth::AppRole { .. } => {
                // Token set after login below
            }
        }

        let client = vaultrs::client::VaultClient::new(
            settings.build().context("invalid Vault client settings")?,
        )
        .context("failed to create Vault client")?;

        let client = Self { client };

        if let VaultAuth::AppRole {
            role_id,
            secret_id,
        } = &config.auth
        {
            client.approle_login(role_id, secret_id).await?;
        }

        Ok(client)
    }

    async fn approle_login(&self, role_id: &str, secret_id: &str) -> Result<()> {
        vaultrs::auth::approle::login(&self.client, "approle", role_id, secret_id)
            .await
            .context("Vault AppRole authentication failed")?;
        Ok(())
    }

    /// Read a single field from a KV v2 secret.
    ///
    /// # Arguments
    ///
    /// * `mount` — KV v2 mount point (e.g. `"secret"`)
    /// * `path` — Secret path within the mount (e.g. `"postgres"`)
    /// * `key` — Field name within the secret data (e.g. `"password"`)
    ///
    /// # Errors
    ///
    /// Returns an error if the path does not exist, the key is not found
    /// in the secret data, or Vault is unreachable.
    pub async fn read_secret(&self, mount: &str, path: &str, key: &str) -> Result<String> {
        let data: HashMap<String, String> =
            vaultrs::kv2::read(&self.client, mount, path)
                .await
                .with_context(|| format!("failed to read Vault secret at {mount}/{path}"))?;

        data.get(key)
            .cloned()
            .with_context(|| {
                format!("key '{key}' not found in Vault secret {mount}/{path}")
            })
    }
}
```

- [ ] **Step 4: Verify crate compiles**

Run: `cargo check -p rapidbyte-vault`
Expected: clean compilation

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-vault/ Cargo.toml
git commit -m "feat: add rapidbyte-vault crate with token + AppRole auth"
```

### Task 2: Add unit tests for VaultClient

**Files:**
- Modify: `crates/rapidbyte-vault/src/lib.rs` (add tests module)

- [ ] **Step 1: Add tests with wiremock**

Add a `#[cfg(test)] mod tests` section to `lib.rs` with tests for:
- `token_auth_reads_secret` — mock Vault KV v2 response, verify `read_secret` returns the correct value
- `read_secret_missing_path_returns_error` — mock 404 response, verify error
- `read_secret_missing_key_returns_error` — mock response with data that lacks the requested key
- `approle_login_and_read` — mock AppRole login endpoint + KV read

The wiremock server replaces the real Vault. Set the `VaultConfig.address` to the wiremock server URL.

Note: `vaultrs` expects specific Vault API response shapes. The mock must return:
```json
{
  "data": {
    "data": { "password": "s3cret" },
    "metadata": { "version": 1 }
  }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-vault`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-vault/
git commit -m "test: add VaultClient unit tests with wiremock"
```

---

## Chunk 2: Config Parser — Variable Substitution

### Task 3: Add Vault reference detection to sync parser

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/parser.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml` (add `rapidbyte-vault` dependency)

- [ ] **Step 1: Add a regex for Vault references and a detection function**

In `parser.rs`, add a second static regex:

```rust
static VAULT_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{vault:([^#\}]+)#([^}]+)\}")
        .expect("valid vault ref regex")
});

fn contains_vault_refs(input: &str) -> bool {
    VAULT_REF_RE.is_match(input)
}
```

- [ ] **Step 2: Update `parse_pipeline_str` to reject Vault references**

```rust
pub fn parse_pipeline_str(yaml_str: &str) -> Result<PipelineConfig> {
    if contains_vault_refs(yaml_str) {
        anyhow::bail!(
            "pipeline contains ${{vault:...}} references but no Vault client is configured"
        );
    }
    let substituted = substitute_env_vars(yaml_str)?;
    let config: PipelineConfig =
        serde_yaml::from_str(&substituted).context("Failed to parse pipeline YAML")?;
    Ok(config)
}
```

- [ ] **Step 3: Add test for Vault reference rejection**

```rust
#[test]
fn test_sync_parser_rejects_vault_refs() {
    let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config:
    password: ${vault:secret/postgres#password}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
    let result = parse_pipeline_str(yaml);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Vault client"));
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-engine -- parser`
Expected: all parser tests pass including the new one

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-engine/
git commit -m "feat: sync parser detects and rejects vault references"
```

### Task 4: Add async `parse_pipeline_with_vault` function

**Files:**
- Modify: `crates/rapidbyte-engine/src/config/parser.rs`
- Modify: `crates/rapidbyte-engine/Cargo.toml`

- [ ] **Step 1: Add `rapidbyte-vault` dependency to engine**

In `crates/rapidbyte-engine/Cargo.toml`, add:
```toml
rapidbyte-vault = { path = "../rapidbyte-vault", optional = true }
```

Add a feature:
```toml
[features]
vault = ["dep:rapidbyte-vault"]
```

Note: Making it optional keeps the vault dependency out of the agent binary (which uses the engine but doesn't need vault).

- [ ] **Step 2: Implement `substitute_variables` (async)**

```rust
/// Substitute both `${ENV_VAR}` and `${vault:mount/path#key}` references.
///
/// Env vars are resolved from the process environment. Vault references
/// are resolved via the provided `VaultClient`. Each unique Vault path
/// is fetched once even if referenced multiple times.
///
/// # Errors
///
/// Returns an error if any env var is missing or any Vault read fails.
/// Resolution is atomic — all references must resolve or the entire
/// substitution fails.
#[cfg(feature = "vault")]
pub async fn substitute_variables(
    input: &str,
    vault: Option<&rapidbyte_vault::VaultClient>,
) -> Result<String> {
    // First collect all vault refs and resolve them
    let mut vault_values: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut vault_errors = Vec::new();

    if let Some(client) = vault {
        // Deduplicate vault paths
        let mut seen = std::collections::HashSet::new();
        for cap in VAULT_REF_RE.captures_iter(input) {
            let full_match = cap[0].to_string();
            if seen.insert(full_match.clone()) {
                let mount_path = &cap[1];
                let key = &cap[2];
                // Split mount from path: "secret/postgres" -> mount="secret", path="postgres"
                let (mount, path) = mount_path.split_once('/').unwrap_or((mount_path, ""));
                match client.read_secret(mount, path, key).await {
                    Ok(val) => { vault_values.insert(full_match, val); }
                    Err(e) => { vault_errors.push(format!("vault:{mount_path}#{key}: {e}")); }
                }
            }
        }
    } else if contains_vault_refs(input) {
        anyhow::bail!(
            "pipeline contains ${{vault:...}} references but no Vault client is configured"
        );
    }

    if !vault_errors.is_empty() {
        anyhow::bail!("Failed to resolve Vault secret(s):\n  {}", vault_errors.join("\n  "));
    }

    // Now do env var + vault substitution in one pass
    let mut result = input.to_string();
    let mut env_errors = Vec::new();

    // Replace vault refs first (they contain special chars that won't match ENV_VAR_RE)
    for (pattern, value) in &vault_values {
        result = result.replace(pattern, value);
    }

    // Then env vars
    for cap in ENV_VAR_RE.captures_iter(&result.clone()) {
        let var_name = &cap[1];
        match std::env::var(var_name) {
            Ok(val) => { result = result.replace(&cap[0], &val); }
            Err(_) => { env_errors.push(var_name.to_string()); }
        }
    }

    if !env_errors.is_empty() {
        anyhow::bail!("Missing environment variable(s): {}", env_errors.join(", "));
    }

    Ok(result)
}
```

- [ ] **Step 3: Implement `parse_pipeline_with_vault` (async)**

```rust
/// Parse a pipeline YAML string, resolving both env vars and Vault secrets.
///
/// After secret resolution, YAML parse errors are redacted to prevent
/// secret leakage in error messages.
///
/// # Errors
///
/// Returns an error if variable substitution fails or YAML is invalid.
#[cfg(feature = "vault")]
pub async fn parse_pipeline_with_vault(
    yaml_str: &str,
    vault: Option<&rapidbyte_vault::VaultClient>,
) -> Result<PipelineConfig> {
    let substituted = substitute_variables(yaml_str, vault).await?;
    let has_secrets = vault.is_some() && contains_vault_refs(yaml_str);
    serde_yaml::from_str(&substituted).map_err(|e| {
        if has_secrets {
            // Redact source to prevent secret leakage
            anyhow::anyhow!(
                "pipeline YAML parsing failed after secret resolution \
                 (source redacted): line {}, column {}",
                e.location().map_or(0, |l| l.line()),
                e.location().map_or(0, |l| l.column()),
            )
        } else {
            anyhow::anyhow!(e).context("Failed to parse pipeline YAML")
        }
    })
}
```

- [ ] **Step 4: Run check**

Run: `cargo check -p rapidbyte-engine --features vault`
Expected: clean compilation

- [ ] **Step 5: Add tests for async parser**

Test: `substitute_variables` with vault=None falls back to env-only.
Test: `substitute_variables` with vault refs but no client errors.
Test: `parse_pipeline_with_vault` redacts YAML errors after vault substitution.
Test: mixed env + vault refs both resolve correctly (using a mock VaultClient — this needs a trait or test helper).

- [ ] **Step 6: Run tests**

Run: `cargo test -p rapidbyte-engine --features vault -- parser`
Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-engine/
git commit -m "feat: async parser with vault secret substitution and error redaction"
```

---

## Chunk 3: CLI Integration

### Task 5: Add Vault CLI flags and client construction

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/Cargo.toml`

- [ ] **Step 1: Add `rapidbyte-vault` dependency to CLI**

In `crates/rapidbyte-cli/Cargo.toml`:
```toml
rapidbyte-vault = { path = "../rapidbyte-vault" }
```

Enable the vault feature on engine:
```toml
rapidbyte-engine = { path = "../rapidbyte-engine", features = ["vault"] }
```

- [ ] **Step 2: Add global Vault flags to CLI struct**

In `main.rs`, add to the `Cli` struct:

```rust
/// Vault server address
#[arg(long, global = true, env = "VAULT_ADDR")]
vault_addr: Option<String>,

/// Vault authentication token
#[arg(long, global = true, env = "VAULT_TOKEN")]
vault_token: Option<String>,

/// Vault AppRole role ID
#[arg(long, global = true, env = "VAULT_ROLE_ID")]
vault_role_id: Option<String>,

/// Vault AppRole secret ID
#[arg(long, global = true, env = "VAULT_SECRET_ID")]
vault_secret_id: Option<String>,
```

- [ ] **Step 3: Add VaultClient construction helper**

```rust
async fn build_vault_client(cli: &Cli) -> Result<Option<rapidbyte_vault::VaultClient>> {
    let Some(addr) = &cli.vault_addr else {
        return Ok(None);
    };

    let auth = if let Some(token) = &cli.vault_token {
        rapidbyte_vault::VaultAuth::Token(token.clone())
    } else if let (Some(role_id), Some(secret_id)) = (&cli.vault_role_id, &cli.vault_secret_id) {
        rapidbyte_vault::VaultAuth::AppRole {
            role_id: role_id.clone(),
            secret_id: secret_id.clone(),
        }
    } else {
        anyhow::bail!(
            "--vault-addr is set but no authentication provided; \
             set --vault-token or --vault-role-id + --vault-secret-id"
        );
    };

    let client = rapidbyte_vault::VaultClient::new(rapidbyte_vault::VaultConfig {
        address: addr.clone(),
        auth,
    })
    .await?;

    Ok(Some(client))
}
```

- [ ] **Step 4: Wire VaultClient into `run` and `check` commands**

Update the `Commands::Run` and `Commands::Check` match arms to construct the Vault client and pass it to the engine's async parser. The run command's `load_pipeline` call should use `parse_pipeline_with_vault` when vault is available, falling back to `parse_pipeline_str` when not.

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p rapidbyte-cli`
Expected: clean

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-cli/
git commit -m "feat: CLI vault flags and client construction"
```

---

## Chunk 4: Controller Dispatch-Time Resolution

### Task 6: Resolve secrets at dispatch time in controller

**Files:**
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/Cargo.toml`

- [ ] **Step 1: Add vault feature to controller's engine dependency**

In `crates/rapidbyte-controller/Cargo.toml`, enable the vault feature on engine:
```toml
rapidbyte-engine = { path = "../rapidbyte-engine", features = ["vault"] }
```

Add vault dependency:
```toml
rapidbyte-vault = { path = "../rapidbyte-vault" }
```

- [ ] **Step 2: Add VaultClient to controller state**

In `server.rs`, add `vault_client: Option<Arc<rapidbyte_vault::VaultClient>>` to `ControllerConfig` or the shared state struct. Construct it at startup from `VAULT_ADDR`/`VAULT_TOKEN`/`VAULT_ROLE_ID`/`VAULT_SECRET_ID` config.

- [ ] **Step 3: Resolve secrets in `make_task_response`**

In `agent_service.rs`, the `make_task_response` function (line ~1038) currently passes `assignment.pipeline_yaml` directly into the gRPC response. Change this to:

1. Accept an `Option<&VaultClient>` parameter
2. If vault is configured, parse the YAML bytes as a string, call `substitute_variables`, and use the resolved bytes in the response
3. If vault is not configured, pass through unchanged (existing behavior)

This is the dispatch point — secrets are resolved here, in memory, and sent to the agent. The task record in the database still has the unresolved YAML.

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p rapidbyte-controller`
Expected: clean

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-controller/
git commit -m "feat: controller resolves vault secrets at dispatch time"
```

---

## Chunk 5: Docker Compose and Dev Environment

### Task 7: Add Vault to Docker Compose and seed script

**Files:**
- Modify: `docker-compose.yml`
- Create: `scripts/vault-seed.sh`
- Modify: `justfile`

- [ ] **Step 1: Add Vault service to `docker-compose.yml`**

```yaml
  vault:
    image: hashicorp/vault:1.18
    ports:
      - "8200:8200"
    environment:
      VAULT_DEV_ROOT_TOKEN_ID: rapidbyte-dev-vault-token
      VAULT_DEV_LISTEN_ADDRESS: "0.0.0.0:8200"
    cap_add:
      - IPC_LOCK
    healthcheck:
      test: ["CMD", "vault", "status"]
      interval: 2s
      timeout: 5s
      retries: 5
```

- [ ] **Step 2: Create `scripts/vault-seed.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-rapidbyte-dev-vault-token}"

export VAULT_ADDR VAULT_TOKEN

echo "Seeding Vault test secrets..."

# Write test postgres credentials
vault kv put secret/postgres \
  host=localhost \
  port=5433 \
  user=postgres \
  password=postgres \
  database=rapidbyte_test

echo "Vault secrets seeded."
```

Make executable: `chmod +x scripts/vault-seed.sh`

- [ ] **Step 3: Update `just dev-up` to seed Vault**

Add `scripts/vault-seed.sh` call after `docker compose up -d --wait`.

- [ ] **Step 4: Test**

Run: `just dev-up`
Expected: Postgres, registry, and Vault start. Vault secrets are seeded.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml scripts/vault-seed.sh justfile
git commit -m "feat: add Vault to dev environment with seed script"
```

### Task 8: Add integration test with real Vault

**Files:**
- Create: `crates/rapidbyte-vault/tests/integration.rs`

- [ ] **Step 1: Write integration test**

```rust
//! Integration tests requiring a running Vault at localhost:8200.
//! Start with: `just dev-up`
//! Run with: `cargo test -p rapidbyte-vault --test integration -- --ignored`

use rapidbyte_vault::{VaultAuth, VaultClient, VaultConfig};

#[tokio::test]
#[ignore]
async fn read_seeded_postgres_secret() {
    let client = VaultClient::new(VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .await
    .expect("should connect to dev Vault");

    let password = client
        .read_secret("secret", "postgres", "password")
        .await
        .expect("should read seeded secret");

    assert_eq!(password, "postgres");
}

#[tokio::test]
#[ignore]
async fn read_missing_path_returns_error() {
    let client = VaultClient::new(VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .await
    .unwrap();

    let result = client.read_secret("secret", "nonexistent", "key").await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn read_missing_key_returns_error() {
    let client = VaultClient::new(VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .await
    .unwrap();

    let result = client
        .read_secret("secret", "postgres", "nonexistent_key")
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}
```

- [ ] **Step 2: Run integration test**

Run: `cargo test -p rapidbyte-vault --test integration -- --ignored`
Expected: all 3 tests pass (requires `just dev-up`)

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-vault/tests/
git commit -m "test: add Vault integration tests with real dev server"
```

### Task 9: Add pipeline YAML integration test with Vault

**Files:**
- Create: `tests/fixtures/pipelines/vault_pg_to_pg.yaml`

- [ ] **Step 1: Create a test pipeline YAML with Vault refs**

```yaml
version: "1.0"
pipeline: vault_test
source:
  use: source-postgres
  config:
    host: ${vault:secret/postgres#host}
    port: 5433
    user: ${vault:secret/postgres#user}
    password: ${vault:secret/postgres#password}
    database: ${vault:secret/postgres#database}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: ${vault:secret/postgres#password}
    database: rapidbyte_test
  write_mode: append
```

- [ ] **Step 2: Commit**

```bash
git add tests/fixtures/pipelines/vault_pg_to_pg.yaml
git commit -m "test: add example pipeline YAML with vault references"
```

---

## Chunk 6: Final Verification

### Task 10: Full workspace verification

- [ ] **Step 1: Run full workspace check**

Run: `cargo check --workspace`
Expected: clean

- [ ] **Step 2: Run full workspace tests**

Run: `cargo test --workspace --all-targets`
Expected: all tests pass

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean

- [ ] **Step 4: Verify no vault dependency in agent binary**

Run: `cargo tree -p rapidbyte-agent | grep vault`
Expected: no output (agent has no vault dependency)

- [ ] **Step 5: Verify vault dependency in CLI and controller**

Run: `cargo tree -p rapidbyte-cli | grep vault`
Expected: `rapidbyte-vault` appears

Run: `cargo tree -p rapidbyte-controller | grep vault`
Expected: `rapidbyte-vault` appears (transitively through engine)
