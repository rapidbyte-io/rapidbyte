# Documentation & Contributor Onboarding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Rapidbyte ready for external contributors with updated README, CONTRIBUTING.md, connector dev guide, PR template, and LICENSE file.

**Architecture:** Pure documentation changes across 5 files. No code changes. Each task produces one committed file. Tasks are independent except Task 1 (README) should be done first since other docs link to it.

**Tech Stack:** Markdown, GitHub templates

---

### Task 1: Rewrite README.md

**Files:**
- Modify: `README.md`

**References to read before writing:**
- Current `README.md` (being replaced)
- `Justfile` (for accurate dev commands)
- `docs/DRAFT.md` (architecture diagram to adapt)
- `crates/rapidbyte-cli/src/main.rs` (CLI commands and flags)

**Step 1: Write the new README.md**

Replace the entire file. Structure:

```markdown
# Rapidbyte

Single-binary data pipeline engine with Wasm-sandboxed connectors.

Rapidbyte replaces managed ETL platforms (Fivetran, Airbyte) with a single native
binary. Connectors run as WASI components inside a Wasmtime sandbox with
host-proxied networking. Data flows between stages as Arrow IPC batches -- no JVM,
no Docker, no sidecar processes.

## Features

- Full refresh, incremental, and CDC sync modes
- Append, replace, and upsert write modes
- INSERT and COPY bulk load paths for PostgreSQL
- In-flight SQL transforms via DataFusion
- Data validation with dead letter queue for bad rows
- Schema evolution policies (4 dimensions)
- LZ4/Zstd batch compression
- Projection pushdown (column selection)
- Host-proxied networking with ACLs (no raw sockets in guest)
- SQLite or Postgres state backend for checkpoints
- Dry-run mode with `--limit` for instant feedback
- Pipeline parallelism with configurable concurrency

## Quick Start

**Prerequisites:** Rust 1.75+, [just](https://github.com/casey/just), Docker

```bash
# Start dev environment (Docker Postgres + build + seed 1M rows)
just dev-up

# Run a pipeline
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml

# Run with verbose output
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml -vv

# Preview without writing to destination
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml --dry-run --limit 100

# Stop dev environment
just dev-down
```

## CLI

| Command | Description |
|---------|-------------|
| `rapidbyte run <pipeline.yaml>` | Execute a data pipeline |
| `rapidbyte check <pipeline.yaml>` | Validate config, manifests, and connectivity |
| `rapidbyte discover <pipeline.yaml>` | Discover available streams from a source |
| `rapidbyte connectors` | List available connector plugins |
| `rapidbyte scaffold <name>` | Scaffold a new connector project |

**Verbosity flags:**

| Flag | Effect |
|------|--------|
| (default) | Compact summary: records, bytes, throughput, streams |
| `-v` | Per-stream breakdown + stage timing |
| `-vv` | Diagnostics: WASM overhead, compression, shard skew, CPU/RSS |
| `--quiet` | No output except errors |

## Pipeline Configuration

```yaml
version: "1.0"
pipeline: my_pipeline

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
      sync_mode: full_refresh
    - name: orders
      sync_mode: incremental
      cursor_field: updated_at

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

See [docs/PROTOCOL.md](docs/PROTOCOL.md) for the full connector protocol specification.

## Connectors

| Connector | Type | Description |
|-----------|------|-------------|
| `source-postgres` | Source | Full refresh, incremental cursor, CDC via pgoutput |
| `dest-postgres` | Destination | INSERT and COPY modes, schema evolution, upsert |
| `transform-sql` | Transform | SQL transforms via DataFusion on Arrow batches |
| `transform-validate` | Transform | Data contracts: not-null, regex, range, unique |

Want to build a connector? See the [Connector Developer Guide](docs/CONNECTOR_DEV.md).

## Architecture

```
Source --> Transform(s) --> Destination
       mpsc::channel      mpsc::channel
```

- **Engine** orchestrates stages connected with bounded `mpsc::channel`
- **Runtime** embeds Wasmtime component model with typed WIT imports/exports
- **Connectors** are `wasm32-wasip2` components using the `rapidbyte-sdk`
- **Arrow IPC** batches flow via host-managed frames with optional LZ4/Zstd compression
- **Network ACL** enforces connector-declared permissions; no direct WASI socket access

**Crate structure:**

```
types (leaf -- no internal deps)
  +-- state    -> types
  +-- runtime  -> types, state
  +-- sdk      -> types
  +-- engine   -> types, runtime, state
      +-- cli  -> engine, runtime, types
```

## Development

```bash
just dev-up          # Docker + build + seed (1M rows)
just dev-down        # Stop and clean Docker state
just run <yaml> -vv  # Build + run a pipeline
just test            # Workspace tests
just e2e             # End-to-end tests (requires Docker)
just lint            # Clippy
just fmt             # Format
just bench           # E2E benchmarks
```

Build mode defaults to release. Use `MODE=debug just dev-up` for debug builds.

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

## License

[Apache-2.0](LICENSE)
```

Key differences from current README:
- Quick Start uses `just dev-up` / `just run` instead of manual build steps
- Adds verbosity flags table
- Trims pipeline config to essentials
- Adds crate structure diagram
- Links to CONTRIBUTING.md and connector dev guide
- Removes `--log-level` global flag mention (replaced by verbosity flags)

**Step 2: Verify links will work**

Check that referenced files exist or will exist after all tasks complete:
- `CONTRIBUTING.md` -- Task 2
- `docs/CONNECTOR_DEV.md` -- Task 3
- `docs/PROTOCOL.md` -- already exists
- `LICENSE` -- Task 5

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README for external contributors

Updated quick start (just dev-up), added verbosity flags, crate
structure diagram, links to CONTRIBUTING.md and connector dev guide."
```

---

### Task 2: Create CONTRIBUTING.md

**Files:**
- Create: `CONTRIBUTING.md`

**References to read before writing:**
- `docs/CODING_STYLE.md` (key rules to highlight)
- `Justfile` (exact commands)
- `CLAUDE.md` (crate dependency graph)

**Step 1: Write CONTRIBUTING.md**

```markdown
# Contributing to Rapidbyte

Contributions are welcome -- bug fixes, new connectors, documentation improvements, and test coverage.

## Getting Started

**Prerequisites:**

- Rust 1.75+ with `wasm32-wasip2` target (`rustup target add wasm32-wasip2`)
- [just](https://github.com/casey/just) (command runner)
- Docker (for PostgreSQL in dev/test)

**Setup:**

```bash
git clone https://github.com/<org>/rapidbyte.git
cd rapidbyte
just dev-up
```

This starts a local PostgreSQL, builds the host binary and all connectors (release mode), and seeds 1M test rows. Verify everything works:

```bash
just run tests/fixtures/pipelines/simple_pg_to_pg.yaml -v
```

## How to Contribute

### Bug Fixes & Features

1. Fork the repo and create a branch: `git checkout -b fix/describe-the-fix` or `feat/describe-the-feature`
2. Make your changes
3. Run checks: `just fmt && just lint && just test`
4. Run E2E tests if you touched engine or connector code: `just e2e`
5. Open a pull request

### New Connectors

Building a new connector (source, destination, or transform) is the best way to contribute. Connectors are independent Wasm components that don't require changes to the engine.

See the **[Connector Developer Guide](docs/CONNECTOR_DEV.md)** for a complete walkthrough from scaffold to publishing.

### Documentation & Tests

Always welcome. Same PR process as above.

## Code Standards

We follow strict coding standards documented in [docs/CODING_STYLE.md](docs/CODING_STYLE.md). Key points:

- **Clippy pedantic:** All host crates use `#![warn(clippy::pedantic)]`
- **Import ordering:** std, external crates, workspace crates, crate-local (separated by blank lines)
- **Error handling:** Use `ConnectorError` factory methods at connector boundaries, `thiserror` internally
- **Visibility:** Default to `pub(crate)` for internal types
- **Tests:** Colocated in `#[cfg(test)] mod tests` at the bottom of each file

## Testing Expectations

Before submitting a PR, run:

| Command | When |
|---------|------|
| `just fmt` | Always |
| `just lint` | Always |
| `just test` | Always |
| `just e2e` | If you touched engine, runtime, or connectors |
| `cargo bench` | If you touched hot-path code (Arrow codec, state backend) |

## Pull Request Process

- Fill out the PR template (checklist auto-populated)
- Keep PRs focused: one logical change per PR
- Include test coverage for new behavior
- Update docs if you changed config or protocol contracts

## Architecture Quick Reference

```
crates/
  rapidbyte-types/    # Shared protocol types (leaf crate)
  rapidbyte-state/    # State backend (SQLite, Postgres)
  rapidbyte-runtime/  # Wasmtime component runtime
  rapidbyte-sdk/      # Connector SDK (traits, macros, host FFI)
  rapidbyte-engine/   # Pipeline orchestrator
  rapidbyte-cli/      # CLI binary

connectors/
  source-postgres/    # Source connector (wasm32-wasip2)
  dest-postgres/      # Destination connector (wasm32-wasip2)
  transform-sql/      # SQL transform (wasm32-wasip2)
  transform-validate/ # Validation transform (wasm32-wasip2)
```

Dependency graph: `types` <- `state` <- `runtime` <- `engine` <- `cli`. Connectors depend only on `sdk`.

For protocol details see [docs/PROTOCOL.md](docs/PROTOCOL.md). For coding standards see [docs/CODING_STYLE.md](docs/CODING_STYLE.md).
```

**Step 2: Commit**

```bash
git add CONTRIBUTING.md
git commit -m "docs: add CONTRIBUTING.md for external contributors

Three contribution paths: bug fixes, new connectors, docs.
Prerequisites, setup, code standards, PR process, architecture reference."
```

---

### Task 3: Create Connector Developer Guide

**Files:**
- Create: `docs/CONNECTOR_DEV.md`

**References to read before writing:**
- `connectors/source-postgres/src/main.rs` (Source trait impl)
- `connectors/source-postgres/build.rs` (ManifestBuilder source example)
- `connectors/dest-postgres/src/main.rs` (Destination trait impl)
- `connectors/dest-postgres/build.rs` (ManifestBuilder dest example)
- `connectors/transform-sql/src/main.rs` (Transform trait impl)
- `connectors/transform-sql/build.rs` (ManifestBuilder transform example)
- `connectors/source-postgres/src/config.rs` (Config + ConfigSchema example)
- `connectors/source-postgres/src/client.rs` (HostTcpStream usage)
- `docs/CODING_STYLE.md` sections 9.1-9.5 (connector conventions)
- `docs/PROTOCOL.md` (connector lifecycle, host imports)

**Step 1: Write docs/CONNECTOR_DEV.md**

This is the longest and most important document. It must be self-contained -- a connector author should not need to read engine code. Every section needs a concrete code example.

Structure (write the complete file):

1. **Overview** (what a connector is, three roles, Wasm sandbox model)
2. **Scaffold a new connector** (`rapidbyte scaffold my-source`, explain generated files: `Cargo.toml`, `build.rs`, `src/main.rs`, `src/config.rs`, `.cargo/config.toml`)
3. **Project structure** (standard module layout table from CODING_STYLE S9.3)
4. **Manifest (build.rs)** -- ManifestBuilder API:
   - Source example (from `source-postgres/build.rs`)
   - Destination example (from `dest-postgres/build.rs`)
   - Transform example (from `transform-sql/build.rs`)
   - Explain: `allow_runtime_network()`, `env_vars()`, sync_modes, write_modes, features
5. **Config type** -- show a complete config struct with:
   - `#[derive(Deserialize, ConfigSchema)]`
   - `#[schema(default = ...)]`, `#[schema(secret)]`, `#[schema(values(...))]`
   - `#[serde(default)]` patterns
   - `validate()` method returning `Result<(), ConnectorError>`
6. **Implementing Source** -- walk through each method:
   - `init(config)` -- validate, return ConnectorInfo
   - `validate(config, ctx)` -- connectivity test
   - `discover(ctx)` -- return Catalog
   - `read(ctx, stream)` -- main data path, emit Arrow batches via `FrameWriter` + `emit_batch`
   - `close(ctx)` -- cleanup
   - Use simplified but real code from source-postgres
7. **Implementing Destination** -- walk through each method:
   - `init(config)` -- return ConnectorInfo with features
   - `validate(config, ctx)` -- connectivity test
   - `write(ctx, stream)` -- receive batches via `next_batch`, write to target, checkpoint
   - `close(ctx)` -- cleanup
   - Use simplified but real code from dest-postgres
8. **Implementing Transform** -- briefer, walk through:
   - `init(config)` -- return ConnectorInfo
   - `transform(ctx, stream)` -- receive batch, process, emit batch
   - `close(ctx)` -- cleanup
9. **Networking** -- `HostTcpStream::connect`, why no raw sockets, example with `tokio-postgres` `connect_raw`
10. **Data flow: Arrow IPC batches**:
    - Sources: `FrameWriter` wraps Arrow IPC encoding -> `emit_batch` sends to host
    - Destinations: `next_batch` receives -> decode Arrow IPC -> process
    - Explain frame lifecycle: `frame-new` / `frame-write` / `frame-seal` / `emit-batch`
11. **Error handling**:
    - `ConnectorError` factory methods table: `config()`, `auth()`, `transient_network()`, `transient_db()`, `data()`, `schema()`, `internal()`
    - When to use each category
    - Builder chain: `.with_details()`, `.with_commit_state()`
    - Retry semantics: retryable flag, backoff classes
12. **Testing your connector**:
    - Build: `cd connectors/my-source && cargo build`
    - Copy wasm to `target/connectors/`
    - Create a test pipeline YAML
    - Run: `just run my-pipeline.yaml --dry-run --limit 10`
    - Run with diagnostics: `just run my-pipeline.yaml -vv`
13. **Publishing** -- release build, wasm strip, connector resolution from `RAPIDBYTE_CONNECTOR_DIR`

**Step 2: Review for completeness**

Verify every code example compiles conceptually (uses real SDK types, correct method signatures). Cross-check against actual connector source code.

**Step 3: Commit**

```bash
git add docs/CONNECTOR_DEV.md
git commit -m "docs: add Connector Developer Guide

Complete walkthrough for building source, destination, and transform
connectors. Covers scaffold, manifest, config, SDK traits, networking,
Arrow data flow, error handling, testing, and publishing."
```

---

### Task 4: Create PR Template

**Files:**
- Create: `.github/pull_request_template.md`

**References to read:**
- `docs/CODING_STYLE.md` section 18 (PR template snippet)

**Step 1: Create directory and write template**

```bash
mkdir -p .github
```

Write `.github/pull_request_template.md`:

```markdown
## Summary

<!-- What does this PR do and why? -->

## Changes

-

## Coding Style Compliance

- [ ] Correctness semantics unchanged or documented
- [ ] Error categories preserved at boundaries
- [ ] No unbounded buffering introduced
- [ ] Hot-path allocation impact reviewed
- [ ] Metrics/logging impact reviewed
- [ ] Serde roundtrip tests for new/changed types

## Verification

- [ ] `just fmt`
- [ ] `just lint`
- [ ] `just test`
- [ ] E2E tests (if engine/connector changed): `just e2e`
- [ ] Benchmark evidence (if hot path touched)
```

**Step 2: Commit**

```bash
git add .github/pull_request_template.md
git commit -m "chore: add GitHub PR template with coding style checklist"
```

---

### Task 5: Add LICENSE File

**Files:**
- Create: `LICENSE`

**Step 1: Write the Apache-2.0 license text**

Use the standard Apache License, Version 2.0 full text. Set copyright to the current year and project name.

**Step 2: Commit**

```bash
git add LICENSE
git commit -m "chore: add Apache-2.0 LICENSE file"
```
