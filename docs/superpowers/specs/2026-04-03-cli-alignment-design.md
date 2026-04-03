# CLI Alignment Refactor — Design Spec

**Date:** 2026-04-03
**Status:** Approved
**Scope:** Rename `run` → `sync`, add 8 new commands (pause/resume/reset/freshness/logs/login/logout/version), update docs/CLI.md

---

## 1. Summary

The CLI diverged from docs/CLI.md. This spec aligns the implementation:

1. **Rename `run` → `sync`** — consolidate `run.rs` + `distributed_run.rs` into single `sync.rs`
2. **Add operational commands** — `pause`, `resume`, `reset`, `freshness`, `logs` via REST API
3. **Add auth commands** — `login`, `logout` for `~/.rapidbyte/config.yaml` token management
4. **Add `version` command** — prints build info
5. **Update docs/CLI.md** — align with implementation, mark deferred features

### Key Decisions

| Decision | Choice |
|---|---|
| `run` → `sync` | Rename to match spec |
| `controller` stays (not `serve`) | Update docs instead |
| Operational commands use REST | New `reqwest`-based client, not gRPC |
| `compile`, `diff`, `assert`, `init`, `discover` | Deferred — mark in docs |
| `logs --follow` | Deferred (needs SSE client) |

---

## 2. `run` → `sync` Rename

### CLI Change

- `Commands::Run` → `Commands::Sync`
- `rapidbyte run pipeline.yml` → `rapidbyte sync pipeline.yml`

### File Changes

- `commands/run.rs` → `commands/sync.rs`
- `commands/distributed_run.rs` → consolidated into `sync.rs`
- `commands/mod.rs` — update module declarations

### Consolidation

`sync.rs` becomes a single module with internal routing:

```
sync.rs
├── execute()           — entry point, routes based on --controller
├── execute_local()     — builds engine context, runs pipeline
└── execute_remote()    — gRPC submit + watch (was distributed_run.rs)
```

### Rename Scope

- Module names, function names, variable names where they refer to the command
- NOT renamed: `Run` domain entity, `RunService`, `run_id`, `RunHandle` — these are execution records, not CLI commands

---

## 3. New Operational Commands

All operational commands talk to the controller's REST API. They need `ControllerFlags` for endpoint resolution and auth.

### Shared REST Client

New file: `commands/rest_client.rs`

```rust
pub struct RestClient {
    base_url: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

impl RestClient {
    pub fn new(base_url: &str, auth_token: Option<&str>) -> Result<Self>;
    pub async fn get(&self, path: &str) -> Result<serde_json::Value>;
    pub async fn post(&self, path: &str, body: Option<&serde_json::Value>) -> Result<serde_json::Value>;
}
```

The client:
- Adds `Authorization: Bearer <token>` header when token is present
- Parses JSON responses
- Maps HTTP errors to user-friendly messages
- Supports TLS via `ControllerFlags` (ca_cert, domain override)

### Commands

**`pause`**
```
rapidbyte pause --pipeline <name>
```
- Calls `POST /api/v1/pipelines/{name}/pause`
- Prints: `Pipeline '{name}' paused`
- Flags: `--pipeline` (required) + `ControllerFlags`

**`resume`**
```
rapidbyte resume --pipeline <name>
```
- Calls `POST /api/v1/pipelines/{name}/resume`
- Prints: `Pipeline '{name}' resumed`
- Flags: `--pipeline` (required) + `ControllerFlags`

**`reset`**
```
rapidbyte reset --pipeline <name> [--stream <stream>]
```
- Calls `POST /api/v1/pipelines/{name}/reset` with optional `{"stream": "..."}` body
- Prints: `Cleared N cursors for '{name}'. Next sync: full refresh.`
- Flags: `--pipeline` (required), `--stream` (optional) + `ControllerFlags`

**`freshness`**
```
rapidbyte freshness [--tag <tag>]
```
- Calls `GET /api/v1/freshness[?tag=<tag>]`
- Prints table: pipeline, last sync age, SLA, status (fresh/stale)
- Flags: `--tag` (optional) + `ControllerFlags`

**`logs`**
```
rapidbyte logs --pipeline <name> [--run-id <id>] [--limit <n>]
```
- Calls `GET /api/v1/logs?pipeline=<name>[&run_id=<id>][&limit=<n>]`
- Prints formatted log entries: timestamp, level, message
- Flags: `--pipeline` (required), `--run-id` (optional), `--limit` (optional, default 20) + `ControllerFlags`

---

## 4. Auth Commands

### Config File

Path: `~/.rapidbyte/config.yaml`

```yaml
controller:
  url: http://localhost:8080
  token: rb_tok_abc123
```

New shared module: `commands/config.rs`

```rust
pub fn config_path() -> PathBuf;
pub fn read_config() -> Result<serde_yaml::Value>;
pub fn write_config(value: &serde_yaml::Value) -> Result<()>;
```

### `login`

```
rapidbyte login --controller <url> [--token <token>]
```

- If `--token` omitted, prompts interactively via stdin
- Writes `controller.url` and `controller.token` to config file
- Creates `~/.rapidbyte/` directory if missing
- Prints: `Logged in to <url>`
- No flag groups needed — has its own `--controller` (required) and `--token` (optional)

### `logout`

```
rapidbyte logout [--controller <url>]
```

- Removes `controller.token` from config
- If `--controller` specified, only removes token for that URL
- If no `--controller`, removes the default controller config
- Prints: `Logged out`

---

## 5. Version Command

```
rapidbyte version
```

- Prints: `rapidbyte <version> (rustc <rustc_version>)`
- Uses `env!("CARGO_PKG_VERSION")` for version
- No flags needed beyond global output flags

---

## 6. Docs Updates

### `docs/CLI.md` changes:

1. **`serve` → `controller`** — all references updated
2. **Deferred commands** — `compile`, `diff`, `assert`, `init`, `discover` marked `(deferred)`:
   - `compile` — deferred (YAGNI, security concern with exposing resolved secrets)
   - `diff` — deferred (needs engine schema comparison support)
   - `assert` — deferred (needs engine assertion execution)
   - `init` — deferred (low priority, one-time use)
   - `discover` — deferred (design divergence to resolve)
3. **`scaffold`** — updated from `rapidbyte scaffold` to `rapidbyte plugin scaffold`
4. **Operational commands** — `pause`, `resume`, `reset`, `freshness`, `logs` updated from "planned" to documented
5. **`login`/`logout`/`version`** — updated from "planned" to documented

---

## 7. New Dependencies

```toml
# crates/rapidbyte-cli/Cargo.toml
reqwest = { version = "0.12", features = ["json", "rustls-tls"] }
```

`reqwest` for the REST client used by operational commands. TLS support via rustls (consistent with the rest of the project).

---

## 8. File Map

### New files

| File | Responsibility |
|------|---------------|
| `commands/sync.rs` | Consolidated sync command (local + remote) |
| `commands/rest_client.rs` | Shared REST HTTP client |
| `commands/config.rs` | `~/.rapidbyte/config.yaml` read/write |
| `commands/pause.rs` | Pause command |
| `commands/resume.rs` | Resume command |
| `commands/reset.rs` | Reset command |
| `commands/freshness.rs` | Freshness command |
| `commands/logs.rs` | Logs command |
| `commands/login.rs` | Login command |
| `commands/logout.rs` | Logout command |
| `commands/version.rs` | Version command |

### Deleted files

| File | Reason |
|------|--------|
| `commands/run.rs` | Renamed to `sync.rs` |
| `commands/distributed_run.rs` | Consolidated into `sync.rs` |

### Modified files

| File | Changes |
|------|---------|
| `main.rs` | `Commands::Run` → `Commands::Sync`, new command variants, dispatch |
| `commands/mod.rs` | Module declarations updated |
| `docs/CLI.md` | Full alignment pass |

---

## 9. Build Order

1. **`sync` rename** — prerequisite, do first
2. **REST client + operational commands** — `rest_client.rs`, then pause/resume/reset/freshness/logs
3. **Auth + info commands** — `config.rs`, then login/logout/version
4. **Docs update** — after all commands land
