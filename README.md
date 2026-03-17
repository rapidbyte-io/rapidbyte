# Rapidbyte

[![CI](https://github.com/rapidbyte-io/rapidbyte/actions/workflows/ci.yml/badge.svg)](https://github.com/rapidbyte-io/rapidbyte/actions/workflows/ci.yml) [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE) [![Rust 1.75+](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](rust-toolchain.toml) [![Docs: Contributing](https://img.shields.io/badge/docs-contributing-0A7EA4.svg)](CONTRIBUTING.md)

Single-executable data pipeline runtime with standalone and distributed
execution modes.

Rapidbyte replaces managed ETL platforms like Fivetran and Airbyte with a single
native binary. Plugins run as WASI components inside a Wasmtime sandbox with
host-proxied networking. The same `rapidbyte` executable can run pipelines
locally or as a distributed controller/agent runtime. Data flows between stages
as Arrow IPC batches -- no JVM, no Docker, no sidecar processes in the data
path.

## Features

- **Sync modes:** full refresh, incremental, and CDC (via pgoutput logical replication)
- **Write modes:** append, replace, and upsert
- **Bulk loading:** INSERT and COPY protocols for Postgres destinations
- **Partitioned reads** with range/mod sharding for parallel full refreshes
- **SQL transforms** via DataFusion (in-flight, inside the Wasm sandbox)
- **Data validation** with rule-based contracts (not_null, regex, range, unique) and dead letter queue (DLQ)
- **Schema evolution** across 4 independent axes (new column, removed column, type change, nullability change)
- **LZ4 / Zstd compression** for Arrow IPC batches between stages
- **Projection pushdown** -- select only the columns you need
- **Host-proxied networking** with per-plugin ACLs (no raw sockets in guest)
- **Per-plugin permissions** -- network hosts, environment variables, and filesystem preopens
- **Per-plugin resource limits** -- memory caps and timeouts per stage
- **State backends:** SQLite or Postgres for cursor, checkpoint, and run metadata
- **Configurable checkpointing** by bytes, rows, or elapsed time
- **Autotune** for parallelism, partition strategy, and COPY flush thresholds
- **Environment variable substitution** in pipeline YAML configs (`${VAR_NAME}`)
- **Dry-run mode** with `--limit` for instant feedback without writing data
- **Interactive dev shell** (REPL) with DataFusion, Arrow workspace, and stream discovery
- **Distributed controller/agent runtime** for queueing, remote execution, and run tracking
- **Distributed preview replay** over Arrow Flight with signed preview access
- **Pipeline parallelism** -- stages run concurrently, connected by bounded channels

## Quick Start

**Prerequisites:** Rust 1.75+, [`just`](https://github.com/casey/just), Docker

Start the dev environment (Docker Postgres, build host + plugins, seed data):

```bash
just dev-up
```

Run a pipeline:

```bash
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml
```

Run with diagnostic-level output:

```bash
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv
```

Preview the first 100 rows without writing to the destination:

```bash
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml --dry-run --limit 100
```

Launch the interactive dev shell:

```bash
just dev
```

Tear down the dev environment:

```bash
just dev-down
```

Install the lightweight local Git hooks once after clone:

```bash
just install-hooks
```

For distributed controller/agent execution, see
[Distributed Runtime](#distributed-runtime).

## CLI

| Command | Description |
|---------|-------------|
| `rapidbyte run <pipeline.yaml>` | Execute a data pipeline |
| `rapidbyte check <pipeline.yaml>` | Validate config, manifests, and connectivity |
| `rapidbyte discover <pipeline.yaml>` | Discover available streams from a source |
| `rapidbyte dev` | Launch interactive dev shell (REPL with DataFusion and Arrow workspace) |
| `rapidbyte plugins` | List available plugins |
| `rapidbyte scaffold <name>` | Scaffold a new plugin project (name must start with `source-`, `dest-`, or `transform-`) |
| `rapidbyte controller` | Start the distributed controller server |
| `rapidbyte agent` | Start a distributed worker agent |
| `rapidbyte status <run_id>` | Fetch the current state of a distributed run |
| `rapidbyte watch <run_id>` | Stream progress and terminal status for a distributed run |
| `rapidbyte list-runs` | List recent distributed runs (`--limit N`, `--state <filter>`) |

### Verbosity Flags

| Flag | Level | Behavior |
|------|-------|----------|
| *(none)* | Default | Standard progress output |
| `-v` | Verbose | Detailed command output and `info`-level server logs |
| `-vv` | Diagnostic | Diagnostic command output and `debug`-level tracing |
| `--quiet` | Quiet | Suppress all output; exit code only, errors on stderr |

**Other flags:** `--dry-run`, `--limit N`, `--log-level <level>`

### Global Flags

| Flag | Env | Description |
|------|-----|-------------|
| `--controller <url>` | `RAPIDBYTE_CONTROLLER` | Controller gRPC endpoint (enables distributed mode) |
| `--auth-token <token>` | `RAPIDBYTE_AUTH_TOKEN` | Bearer token for authenticated controller RPCs |
| `--tls-ca-cert <path>` | `RAPIDBYTE_TLS_CA_CERT` | Custom CA certificate for TLS connections |
| `--tls-domain <domain>` | `RAPIDBYTE_TLS_DOMAIN` | TLS server name override |

`run` works in standalone mode by default. Distributed execution is enabled
when you pass `--controller <url>` or set `RAPIDBYTE_CONTROLLER`. The
controller URL can also be set in `~/.rapidbyte/config.yaml`:

```yaml
controller:
  url: http://127.0.0.1:9090
```

## Pipeline Configuration

```yaml
version: "1.0"
pipeline: example

source:
  use: postgres
  config:
    host: localhost
    port: 5432
    user: app
    password: ${PG_PASSWORD}
    database: mydb
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      columns: [id, email, updated_at]
    - name: orders
      sync_mode: full_refresh
      partition_key: region

transforms:
  - use: sql
    config:
      query: "SELECT id, lower(email) AS email, updated_at FROM users"

destination:
  use: postgres
  config:
    host: warehouse
    port: 5432
    user: loader
    password: ${DEST_PG_PASSWORD}
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]
  on_data_error: dlq
  schema_evolution:
    new_column: add
    type_change: coerce

state:
  backend: sqlite

resources:
  compression: lz4
  checkpoint_interval_bytes: 128mb
```

### Source Streams

Each stream declares a sync strategy and optional projection:

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Stream identifier |
| `sync_mode` | yes | `full_refresh`, `incremental`, or `cdc` |
| `cursor_field` | incremental/cdc | Column used for tracking position |
| `tie_breaker_field` | no | Secondary ordering column for incremental (must differ from cursor) |
| `partition_key` | no | Column for parallel sharding (`full_refresh` only) |
| `columns` | no | Projection -- select only these columns |

### Destination Options

| Field | Default | Description |
|-------|---------|-------------|
| `write_mode` | *(required)* | `append`, `replace`, or `upsert` |
| `primary_key` | `[]` | Required for `upsert` mode |
| `on_data_error` | `fail` | `fail`, `skip`, or `dlq` |
| `schema_evolution` | *(none)* | Per-axis schema change policies (see below) |

### Schema Evolution

Four independent policy axes, each configured separately:

| Axis | Options | Default |
|------|---------|---------|
| `new_column` | `add`, `ignore`, `fail` | `add` |
| `removed_column` | `add`, `ignore`, `fail` | `ignore` |
| `type_change` | `coerce`, `fail`, `null` | `fail` |
| `nullability_change` | `allow`, `fail` | `allow` |

### Resources

Fine-grained control over pipeline execution, batching, and resource limits:

| Field | Default | Description |
|-------|---------|-------------|
| `max_memory` | `256mb` | Process memory limit |
| `max_batch_bytes` | `64mb` | Per-batch size limit |
| `parallelism` | `auto` | `auto` or a fixed integer |
| `checkpoint_interval_bytes` | `64mb` | Commit interval by bytes (`0` disables) |
| `checkpoint_interval_rows` | `0` | Commit interval by rows (`0` disables) |
| `checkpoint_interval_seconds` | `0` | Commit interval by elapsed time (`0` disables) |
| `max_retries` | `3` | Max retry attempts for transient errors |
| `compression` | *(none)* | `lz4` or `zstd` for inter-stage IPC |
| `max_inflight_batches` | `16` | Channel backpressure between stages (min: 1) |

Byte sizes accept human-readable units: `64mb`, `1gb`, `512kb`.

#### Autotune

```yaml
resources:
  autotune:
    enabled: true                    # default: true
    parallelism: 8                   # override auto-parallelism
    partition_mode: range            # override partition strategy (range or mod)
    flush_bytes: 8388608             # override flush chunk size (1mb-32mb)
```

### Per-Plugin Permissions and Limits

Each stage (source, destination, transform) can declare its own sandbox
overrides:

```yaml
source:
  use: postgres
  config: { ... }
  permissions:
    network:
      allowed_hosts: [db.internal.com, "*.aws.internal"]
    env:
      allowed_vars: [PG_PASSWORD]
    fs:
      allowed_preopens: [/data/exports]
  limits:
    max_memory: 512mb
    timeout_seconds: 300
  streams:
    - name: users
      sync_mode: full_refresh
```

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full plugin protocol specification.

## Distributed Runtime

Rapidbyte supports two execution modes:

- **Standalone:** `rapidbyte run pipeline.yaml`
- **Distributed:** `rapidbyte --controller http://127.0.0.1:9090 run pipeline.yaml`

In distributed mode, the controller owns run/task/agent metadata and scheduling,
while agents execute pipelines and serve dry-run previews. The user-facing
distributed CLI surface is:

- `run`
- `status`
- `watch`
- `list-runs`
- `controller`
- `agent`

### Controller

```bash
rapidbyte controller \
  --listen [::]:9090 \
  --signing-key <hex-key> \
  --auth-token <token>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | `[::]:9090` | gRPC listen address |
| `--metadata-database-url` | *(required)* | Postgres metadata backend for durable controller state (`RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL`) |
| `--signing-key` | *(required)* | Shared key for preview ticket signing (env: `RAPIDBYTE_SIGNING_KEY`) |
| `--tls-cert` / `--tls-key` | | PEM cert/key pair for TLS |

Controller V2 migration note: insecure controller escape hatches were removed.
`--allow-unauthenticated` and `--allow-insecure-signing-key` are no longer
supported; pass `--auth-token` and `--signing-key` explicitly.

### Agent

```bash
rapidbyte agent \
  --controller http://127.0.0.1:9090 \
  --flight-advertise 127.0.0.1:9091 \
  --signing-key <hex-key>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--controller` | *(required)* | Controller endpoint |
| `--flight-advertise` | *(required)* | Flight endpoint advertised to clients |
| `--flight-listen` | `[::]:9091` | Flight server bind address |
| `--max-tasks` | `1` | Max concurrent tasks |
| `--signing-key` | *(required)* | Must match controller key (env: `RAPIDBYTE_SIGNING_KEY`) |
| `--allow-insecure-signing-key` | | Allow built-in dev signing key |
| `--flight-tls-cert` / `--flight-tls-key` | | PEM cert/key pair for Flight TLS |

High-level operational rules:

- Distributed mode requires a shared state backend such as Postgres.
- SQLite is for standalone and local development workflows.
- Controller auth and signing key are required for startup.
- Insecure controller auth/signing fallback flags were removed in V2.
- TLS is supported for both control-plane gRPC and Flight preview access.

See [`docs/ARCHITECTUREv2.md`](docs/ARCHITECTUREv2.md) for the detailed
controller/agent design and security model.

## Plugins

| Plugin | `use` value | Type | Description |
|--------|-------------|------|-------------|
| `source-postgres` | `postgres` | Source | Read from PostgreSQL (full refresh, incremental, CDC via pgoutput) |
| `dest-postgres` | `postgres` | Destination | Write to PostgreSQL (append, replace, upsert; INSERT and COPY) |
| `transform-sql` | `sql` | Transform | SQL transforms via DataFusion |
| `transform-validate` | `validate` | Transform | Rule-based data validation (not_null, regex, range, unique) with DLQ support |

The `use` value is what you put in the pipeline YAML -- the plugin kind is
inferred from the section (`source`, `destination`, or `transforms`).

Plugins are compiled to `wasm32-wasip2` and loaded by the engine at runtime.
To build your own, see the [Plugin Developer Guide](docs/PLUGIN_DEV.md).

### Source Postgres Config

| Field | Default | Description |
|-------|---------|-------------|
| `host` | | Database hostname |
| `port` | `5432` | Database port |
| `user` | | Database user |
| `password` | | Database password |
| `database` | | Target database |
| `replication_slot` | *(auto)* | Custom replication slot name (CDC) |
| `publication` | *(auto)* | Custom publication name (CDC) |

### Destination Postgres Config

| Field | Default | Description |
|-------|---------|-------------|
| `host` | | Database hostname |
| `port` | `5432` | Database port |
| `user` | | Database user |
| `password` | | Database password |
| `database` | | Target database |
| `schema` | `public` | Destination schema |
| `load_method` | `copy` | `copy` (high-performance) or `insert` |

## Architecture

The engine orchestrates a pipeline as a sequence of stages -- source, zero or
more transforms, and a destination -- running concurrently and connected by
bounded `mpsc::channel`s. Each plugin is a `wasm32-wasip2` component executed
inside a Wasmtime sandbox. Arrow IPC batches flow between stages via host-managed
frames (`frame-new` / `frame-write` / `frame-seal` / `emit-batch`), with optional
LZ4/Zstd compression handled transparently by the host. Plugins cannot open
raw sockets; all network I/O is proxied through `connect-tcp` with host-enforced
ACLs.

```
                   mpsc::channel         mpsc::channel
  Source ────────────> Transform(s) ────────────> Destination
  (wasm32-wasip2)      (wasm32-wasip2)            (wasm32-wasip2)
                  Arrow IPC batches          Arrow IPC batches
```

The distributed runtime layers a controller control plane and agent preview
data plane on top of this same engine core. The controller schedules work and
tracks runs; agents execute full pipelines and expose replay-based dry-run
previews over Arrow Flight.

### Crate Dependency Graph

```
types          (leaf -- no internal deps)
  +-- state    (types)
  +-- runtime  (types, state)
  +-- engine   (types, runtime, state)
  +-- controller (types, state, engine)
  +-- agent    (types, runtime, engine, state)
  +-- dev      (engine, runtime, types, state)
  +-- cli      (engine, runtime, types, controller, agent, dev)

sdk            (types -- plugins depend only on this)
```

| Crate | Purpose |
|-------|---------|
| `rapidbyte-types` | Shared protocol types (leaf crate, no internal deps) |
| `rapidbyte-state` | State backend (SQLite, Postgres) |
| `rapidbyte-runtime` | Wasmtime component runtime, host imports, sandbox |
| `rapidbyte-engine` | Pipeline orchestrator, config parsing, Arrow utilities |
| `rapidbyte-controller` | Distributed control plane (scheduling, run/task/agent state) |
| `rapidbyte-agent` | Distributed worker runtime and Flight preview server |
| `rapidbyte-dev` | Interactive dev shell (REPL with DataFusion, Arrow workspace) |
| `rapidbyte-cli` | CLI binary (`run`, `check`, `discover`, `dev`, `plugins`, `scaffold`, `controller`, `agent`, `status`, `watch`, `list-runs`) |
| `rapidbyte-sdk` | Plugin SDK (protocol types, component host bindings) |

## Development

| Command | Description |
|---------|-------------|
| `just dev-up` | Start dev environment (Docker, build, seed) |
| `just dev-down` | Stop dev environment and clean state |
| `just dev` | Launch interactive dev shell (builds host + plugins first) |
| `just run <pipeline>` | Build and run a pipeline (supports `-v`, `-vv`, `--dry-run`) |
| `just test` | Run workspace tests (`cargo test`) |
| `just e2e` | End-to-end tests (requires Docker) |
| `just lint` | Clippy with `-D warnings` |
| `just fmt` | Format all crates |
| `just fix` | Run the fast local auto-fix path (`cargo fmt --all`) |
| `just install-hooks` | Configure repo-managed Git hooks in `.githooks/` |
| `just ci` | Run the external-readiness baseline (`fmt`, `clippy`, workspace tests, e2e compile) |
| `just scaffold <name>` | Scaffold a new plugin project |
| `just bench-lab <scenario>` | Run one lab scenario against its manifest-declared benchmark environment |
| `just bench-pr` | Run PR smoke suite and compare against checked-in baseline |
| `just bench-compare <base> <candidate>` | Compare two benchmark artifact sets |
| `just bench-summary <artifact>` | Print readable summary for a single JSONL artifact |

By default, `just` builds in release mode. Set `MODE=debug` for debug builds:

```bash
MODE=debug just dev-up
```

Before opening a pull request, run:

```bash
just ci
```

The local hooks are intentionally light:
- `pre-commit` auto-formats Rust changes, re-stages touched staged paths, then
  stops so you can review the diff and re-run `git commit`
- `pre-push` auto-formats the checked-out repo state, stages newly formatted
  files, then stops so you can review, create a follow-up commit, and push again
- once formatting is clean, `pre-push` runs `just lint` and blocks the push on
  clippy failures

They do not replace `just ci`.

For the native Postgres destination benchmarks:

```bash
just bench-lab pg_dest_insert
just bench-lab pg_dest_copy_regression
just bench-lab pg_dest_copy_release
just bench-lab pg_dest_copy_release_distributed
```

`pg_dest_copy_regression` is the cheap regression-tracking COPY benchmark.
`pg_dest_copy_release` is the production-like local release benchmark, and
`pg_dest_copy_release_distributed` is its single-node distributed analogue.
Benchmark throughput and bandwidth are reported as end-to-end wall-clock
pipeline rates, and the rendered bandwidth unit is `MiB/sec`. `just bench-lab`
uses the scenario manifest's declared environment by default; pass `env=...`
only when you intentionally want to override it.

For local performance-regression checks against the checked-in PR baseline:

```bash
just bench-pr
```

The GitHub benchmark workflow is manual-only for now and is intended for
maintainer-triggered runs, not routine pull-request CI.

Benchmark details live in [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md).

## Further Reading

- [`docs/ARCHITECTUREv2.md`](docs/ARCHITECTUREv2.md) -- distributed controller/agent architecture
- [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) -- local and distributed benchmark usage
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) -- protocol and wire-format details
- [`docs/PLUGIN_DEV.md`](docs/PLUGIN_DEV.md) -- plugin development guide
- [`docs/PLUGIN_ARCHITECTURE.md`](docs/PLUGIN_ARCHITECTURE.md) -- plugin architecture details
- [`docs/BUILD.md`](docs/BUILD.md) -- build configuration and sccache
- [`docs/CODING_STYLE.md`](docs/CODING_STYLE.md) -- code standards and conventions
- [`CONTRIBUTING.md`](CONTRIBUTING.md) -- contributor workflow

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for guidelines on submitting issues,
pull requests, and setting up your development environment.

## License

[Apache-2.0](LICENSE)
