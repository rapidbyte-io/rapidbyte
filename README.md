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

- **Sync modes:** full refresh, incremental, and CDC
- **Write modes:** append, replace, and upsert
- **Bulk loading:** INSERT and COPY protocols for Postgres destinations
- **SQL transforms** via DataFusion (in-flight, inside the Wasm sandbox)
- **Data validation** with dead letter queue (DLQ) for bad rows
- **Schema evolution** across 4 dimensions (strict, additive, permissive, auto)
- **LZ4 / Zstd compression** for Arrow IPC batches between stages
- **Projection pushdown** -- select only the columns you need
- **Host-proxied networking** with per-plugin ACLs (no raw sockets in guest)
- **State backends:** SQLite or Postgres for cursor, checkpoint, and run metadata
- **Dry-run mode** with `--limit` for instant feedback without writing data
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
| `rapidbyte status <run_id>` | Fetch the current state of a distributed run |
| `rapidbyte watch <run_id>` | Stream progress and terminal status for a distributed run |
| `rapidbyte list-runs` | List recent distributed runs |
| `rapidbyte check <pipeline.yaml>` | Validate config, manifests, and connectivity |
| `rapidbyte discover <pipeline.yaml>` | Discover available streams from a source |
| `rapidbyte controller` | Start the distributed controller server |
| `rapidbyte agent` | Start a distributed worker agent |
| `rapidbyte plugins` | List available plugins |
| `rapidbyte scaffold <name>` | Scaffold a new plugin project |

### Verbosity Flags

| Flag | Level | Behavior |
|------|-------|----------|
| *(none)* | Default | Standard progress output |
| `-v` | Verbose | Detailed per-stream stats and timing |
| `-vv` | Diagnostic | Full internal tracing (frame lifecycle, host calls) |
| `--quiet` | Quiet | Suppress all output; exit code only, errors on stderr |

**Other flags:** `--dry-run`, `--limit N`

`run` works in standalone mode by default. Distributed execution is enabled
when you pass `--controller <url>` or set `RAPIDBYTE_CONTROLLER`.

## Pipeline Configuration

```yaml
version: "1.0"
pipeline: example

source:
  use: source-postgres
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

transforms:
  - use: transform-sql
    config:
      query: "SELECT id, lower(email) AS email FROM input"

destination:
  use: dest-postgres
  config:
    host: warehouse
    port: 5432
    user: loader
    password: ${DEST_PG_PASSWORD}
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]

state:
  backend: sqlite
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

High-level operational rules:

- Distributed mode requires a shared state backend such as Postgres.
- SQLite is for standalone and local development workflows.
- Controller auth is required by default.
- Preview signing keys should be explicitly configured outside local/dev.
- TLS is supported for both control-plane gRPC and Flight preview access.

See [`docs/ARCHITECTUREv2.md`](docs/ARCHITECTUREv2.md) for the detailed
controller/agent design and security model.

## Plugins

| Plugin | Type | Description |
|--------|------|-------------|
| `source-postgres` | Source | Read from PostgreSQL (full refresh, incremental) |
| `dest-postgres` | Destination | Write to PostgreSQL (append, replace, upsert; INSERT and COPY) |
| `transform-sql` | Transform | SQL transforms via DataFusion |
| `transform-validate` | Transform | Row-level data validation with DLQ support |

Plugins are compiled to `wasm32-wasip2` and loaded by the engine at runtime.
To build your own, see the [Plugin Developer Guide](docs/PLUGIN_DEV.md).

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
  +-- cli      (engine, runtime, types, controller, agent)

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
| `rapidbyte-cli` | CLI binary (`run`, `status`, `watch`, `list-runs`, `check`, `discover`, `controller`, `agent`) |
| `rapidbyte-sdk` | Plugin SDK (protocol types, component host bindings) |

## Development

| Command | Description |
|---------|-------------|
| `just dev-up` | Start dev environment (Docker, build, seed) |
| `just dev-down` | Stop dev environment and clean state |
| `just run <pipeline>` | Build and run a pipeline (supports `-v`, `-vv`, `--dry-run`) |
| `just test` | Run workspace tests (`cargo test`) |
| `just e2e` | End-to-end tests (requires Docker) |
| `just lint` | Clippy with `-D warnings` |
| `just fmt` | Format all crates |
| `just fix` | Run the fast local auto-fix path (`cargo fmt --all`) |
| `just install-hooks` | Configure repo-managed Git hooks in `.githooks/` |
| `just ci` | Run the external-readiness baseline (`fmt`, `clippy`, workspace tests, e2e compile) |
| `just bench` | Run benchmark scenarios (`--suite pr` or `--suite lab --scenario <id> --env-profile <profile>`) |
| `just bench-lab <scenario>` | Run one lab scenario against its manifest-declared benchmark environment |

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

- [`docs/ARCHITECTUREv2.md`](docs/ARCHITECTUREv2.md) — distributed controller/agent architecture
- [`docs/BENCHMARKING.md`](docs/BENCHMARKING.md) — local and distributed benchmark usage
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) — protocol and wire-format details
- [`docs/PLUGIN_DEV.md`](docs/PLUGIN_DEV.md) — plugin development guide
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — contributor workflow

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for guidelines on submitting issues,
pull requests, and setting up your development environment.

## License

[Apache-2.0](LICENSE)
