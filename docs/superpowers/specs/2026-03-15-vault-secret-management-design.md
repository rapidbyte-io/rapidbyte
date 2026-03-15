# Vault Secret Management for Plugin Configs

**Date:** 2026-03-15
**Scope:** Plugin config secrets (database passwords, API keys in pipeline YAML)
**Out of scope:** Infrastructure secrets (signing keys, auth tokens, TLS certs) — use ESO/K8s secrets

## Problem

Plugin configs contain credentials (database passwords, API keys) that are either hardcoded in pipeline YAML or injected via environment variables. In distributed deployments, agents need credentials but may not have direct access to a secret store. There is no centralized, auditable secret management.

## Design

### Reference Syntax

Extend the existing `${...}` substitution to support Vault KV v2 paths:

```yaml
source:
  config:
    host: ${PG_HOST}                                  # env var (unchanged)
    password: ${vault:secret/data/postgres#password}   # Vault KV v2
```

Format: `${vault:PATH#KEY}` where `PATH` is the Vault secret path and `KEY` selects a field from the secret's data map. `#KEY` is required.

Unresolved references (missing env var, Vault path not found, key absent) are hard errors — same as today.

### Resolution Modes

**Local mode** (`rapidbyte run pipeline.yaml`): The CLI process authenticates to Vault and resolves `${vault:...}` references before parsing the YAML.

**Distributed mode** (`rapidbyte run --controller`): The controller authenticates to Vault and resolves `${vault:...}` references before dispatching the resolved YAML to agents over the existing mTLS gRPC channel. Agents never contact Vault. The controller never persists resolved secrets — they exist in memory only for the duration of the resolve-and-dispatch call.

Security properties:
- Transit encryption via existing TLS on controller-agent gRPC
- No secret persistence in controller metadata store
- No secrets in tracing/logs (opaque config blobs)
- Vault audit log records which paths the controller accessed

### New Crate: `rapidbyte-vault`

Minimal Vault KV v2 client. Read-only, no caching, no write operations.

```
crates/rapidbyte-vault/
  src/
    lib.rs        — VaultClient, VaultConfig
    auth.rs       — token + AppRole authentication
    kv.rs         — KV v2 read_secret(path) -> HashMap<String, String>
```

**Types:**

```rust
pub struct VaultConfig {
    pub address: String,
    pub auth: VaultAuth,
}

pub enum VaultAuth {
    Token(String),
    AppRole { role_id: String, secret_id: String },
}

impl VaultClient {
    pub async fn new(config: VaultConfig) -> Result<Self>;
    pub async fn read_secret(&self, path: &str) -> Result<HashMap<String, String>>;
}
```

- `new()` authenticates immediately (AppRole exchanges credentials for a token)
- `read_secret()` calls `GET /v1/{path}`, returns `data.data` map
- AppRole token renewal: re-authenticate on expiry (simple retry)
- Uses `reqwest` (already a workspace dependency)
- No `Debug` derive on config types — contains secrets

### Integration Points

**Config parser (`parser.rs`):**

`substitute_env_vars()` becomes:

```rust
pub async fn substitute_variables(
    yaml: &str,
    vault: Option<&VaultClient>,
) -> Result<String>
```

Single regex pass matches both `${WORD}` (env var) and `${vault:PATH#KEY}` (Vault). Unique Vault paths are batched — each path fetched once even if referenced multiple times. No Vault dependency when no `${vault:...}` references exist.

`parse_pipeline_str()` becomes `parse_pipeline()` (async).

**CLI (`main.rs`):**

New global flags:
- `--vault-addr` / `VAULT_ADDR`
- `--vault-token` / `VAULT_TOKEN`
- `--vault-role-id` / `VAULT_ROLE_ID`
- `--vault-secret-id` / `VAULT_SECRET_ID`

VaultClient constructed only when Vault is configured. Pipelines without `${vault:...}` references never touch Vault.

**Controller task dispatch:**

Controller constructs a `VaultClient` at startup (if configured) and reuses it. Before dispatching a task to an agent, the controller resolves `${vault:...}` in the pipeline YAML. The resolved YAML is what agents receive — no Vault awareness needed in the agent crate.

**What doesn't change:**
- Plugin code — receives opaque JSON config
- `#[schema(secret)]` metadata — orthogonal to resolution
- `${ENV_VAR}` substitution — works exactly as before
- Agent crate — no Vault dependency

### Local Development

**Docker Compose** — add Vault alongside existing Postgres and OCI registry:

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
```

**`just dev-up`** gains a `vault-seed` step after `docker compose up -d --wait` that writes test secrets (e.g. `secret/data/postgres` with `password=postgres`).

### Testing

- **Unit tests** (`rapidbyte-vault`): mock HTTP responses, test token auth, AppRole exchange, KV read, error cases (path not found, key missing, auth failure)
- **Unit tests** (`parser.rs`): mock `VaultClient`, test mixed `${ENV_VAR}` + `${vault:...}` substitution, missing refs fail hard, duplicate paths fetched once
- **Integration test** (`--ignored`): real Vault from dev-up, seed secret, resolve pipeline YAML, verify values

### Vault Auth Methods

| Method | Config | Use case |
|--------|--------|----------|
| Token | `VAULT_TOKEN` or `--vault-token` | Dev, simple deployments |
| AppRole | `VAULT_ROLE_ID` + `VAULT_SECRET_ID` | Production machine-to-machine |

Kubernetes auth can be added later. Operators on K8s can bridge with ESO injecting a `VAULT_TOKEN` env var in the meantime.

### Dependency Graph

```
rapidbyte-vault (new, leaf — no internal deps)
  ├── engine → vault (for parser substitution)
  └── cli → vault (for VaultClient construction)
```

Agent and controller crates do not depend on `rapidbyte-vault`. The controller uses it indirectly through engine's parser.
