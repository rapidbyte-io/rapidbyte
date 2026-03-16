# Vault Secret Management for Plugin Configs

**Date:** 2026-03-15
**Scope:** Plugin config secrets (database passwords, API keys in pipeline YAML)
**Out of scope:** Infrastructure secrets (signing keys, auth tokens, TLS certs) — use ESO/K8s secrets

## Problem

Plugin configs contain credentials (database passwords, API keys) that are either hardcoded in pipeline YAML or injected via environment variables. In distributed deployments, agents need credentials but may not have direct access to a secret store. There is no centralized, auditable secret management.

## Design

### Reference Syntax

Extend the existing `${...}` substitution to support Vault KV v2 logical paths:

```yaml
source:
  config:
    host: ${PG_HOST}                                # env var (unchanged)
    password: ${vault:secret/postgres#password}      # Vault KV v2
```

Format: `${vault:MOUNT/PATH#KEY}` where `MOUNT/PATH` is the **logical** Vault path (matching `vault kv get` convention) and `KEY` selects a field from the secret's data map. `#KEY` is required.

The client automatically injects the `/data/` segment for the KV v2 HTTP API — users write `secret/postgres`, not `secret/data/postgres`. This matches the `vault kv get` CLI behavior and avoids a common misconfiguration.

Unresolved references (missing env var, Vault path not found, key absent) are hard errors — same as today.

### Resolution Modes

**Local mode** (`rapidbyte run pipeline.yaml`): The CLI process authenticates to Vault and resolves `${vault:...}` references before parsing the YAML.

**Distributed mode** (`rapidbyte run --controller`): The controller stores **unresolved** pipeline YAML in its task queue (metadata store). When an agent polls for a task, the controller resolves `${vault:...}` references at dispatch time and sends the resolved YAML over the existing mTLS gRPC channel. The unresolved YAML is what persists in the database — resolved secrets never touch disk.

Security properties:
- Resolved secrets exist in controller memory only during the dispatch call
- No secret persistence in controller metadata store (task records contain unresolved YAML)
- Transit encryption via existing TLS on controller↔agent gRPC
- No secrets in tracing/logs — after resolution, YAML parse errors are redacted (see Error Redaction)
- Vault audit log records which paths the controller accessed and when

### Error Redaction

After Vault substitution, the resolved YAML string may contain plaintext secrets. If YAML parsing then fails (syntax error, schema mismatch), the `serde_yaml` error message could include a snippet containing secrets.

To prevent this, YAML parse errors after secret resolution must not include the raw YAML source. The parser wraps the error with a generic message: `"pipeline YAML parsing failed after secret resolution (source redacted)"`. The original error's structural information (line/column) is preserved, but the source snippet is stripped.

### New Crate: `rapidbyte-vault`

Thin wrapper around the `vaultrs` crate, providing a simplified API for pipeline secret resolution.

```
crates/rapidbyte-vault/
  src/
    lib.rs        — VaultClient, VaultConfig, public API
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
    pub async fn read_secret(&self, mount: &str, path: &str, key: &str) -> Result<String>;
}
```

- Uses the `vaultrs` crate which handles KV v2 API paths, token management, and HTTP transport
- `new()` creates the `vaultrs::client::VaultClient` with settings; for AppRole, authenticates immediately
- `read_secret(mount, path, key)` calls `vaultrs::kv2::read()` and extracts the requested key
- No `Debug` derive on config types — contains secrets
- The `vaultrs` client uses `reqwest` internally (already in the dependency tree via `oci-client`)

**Timeouts and failure behavior:**
- Connect/read timeouts configured via `vaultrs` client settings
- Resolution is atomic: all `${vault:...}` paths in a YAML must resolve successfully or the entire substitution fails. No partial resolution.
- Vault down at startup: fail fast with a clear error message

### Integration Points

**Config parser (`parser.rs`):**

Two parser functions coexist:

```rust
// Sync — errors if ${vault:...} references are present
pub fn parse_pipeline_str(yaml: &str) -> Result<PipelineConfig>

// Async — resolves both ${ENV_VAR} and ${vault:...}
pub async fn parse_pipeline(yaml: &str, vault: Option<&VaultClient>) -> Result<PipelineConfig>
```

`parse_pipeline_str()` is unchanged for callers that don't need Vault (tests, benchmarks, autotune). If it encounters `${vault:...}` references, it returns a clear error: `"pipeline contains Vault references but no Vault client is configured"`.

`parse_pipeline()` is the new async entrypoint. Single regex pass matches both `${WORD}` (env var) and `${vault:MOUNT/PATH#KEY}` (Vault). Unique Vault paths are batched — each path fetched once even if referenced multiple times.

**CLI (`main.rs`):**

New global flags:
- `--vault-addr` / `VAULT_ADDR`
- `--vault-token` / `VAULT_TOKEN`
- `--vault-role-id` / `VAULT_ROLE_ID`
- `--vault-secret-id` / `VAULT_SECRET_ID`

VaultClient constructed only when Vault flags/env vars are set. Pipelines without `${vault:...}` references never touch Vault.

`rapidbyte check` with Vault references but no Vault configured errors with: `"pipeline contains ${vault:...} references; provide --vault-addr to validate"`.

**Controller task dispatch:**

Controller constructs a `VaultClient` at startup (if configured) and reuses it. Resolution happens at dispatch time (when agent polls), not at enqueue time. The task record in the metadata store always contains unresolved YAML.

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

**`just dev-up`** gains a `vault-seed` step after `docker compose up -d --wait` that writes test secrets (e.g. `secret/postgres` with `password=postgres`).

### Testing

- **Unit tests** (`rapidbyte-vault`): mock HTTP responses, test token auth, AppRole exchange, KV read, error cases (path not found, key missing, auth failure, timeout)
- **Unit tests** (`parser.rs`): test `parse_pipeline_str` rejects `${vault:...}` references; test `parse_pipeline` with mock `VaultClient` for mixed `${ENV_VAR}` + `${vault:...}` substitution, missing refs fail hard, duplicate paths fetched once, error redaction after resolution
- **Integration test** (`--ignored`): real Vault from dev-up, seed secret, resolve pipeline YAML, verify values

### Vault Auth Methods

| Method | Config | Use case |
|--------|--------|----------|
| Token | `VAULT_TOKEN` or `--vault-token` | Dev, simple deployments |
| AppRole | `VAULT_ROLE_ID` + `VAULT_SECRET_ID` | Production machine-to-machine |

Kubernetes auth can be added later. Operators on K8s can bridge with ESO injecting a `VAULT_TOKEN` env var in the meantime.

### Dependency Graph

```
rapidbyte-vault (new, leaf — wraps vaultrs, no internal deps)
  ├── engine → vault (for parse_pipeline)
  └── cli → vault (for VaultClient construction + passing to engine)
```

The controller depends on `engine`, so it transitively gains the `vault` dependency. This is intentional — the controller uses `engine::parse_pipeline()` to resolve secrets at dispatch time. The agent crate has no Vault dependency (direct or transitive).

### Future Extensions (Not In Scope)

- **KV v2 version pinning**: pin `${vault:secret/postgres@3#password}` to a specific secret version
- **Kubernetes auth method**: exchange K8s service account JWT for Vault token
- **Secret rotation notifications**: Vault watches that trigger pipeline re-resolution
