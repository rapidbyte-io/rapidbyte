# Rapidbyte

Data pipeline engine using WASI component connectors (Wasmtime runtime).

## Project Structure

```
crates/
  rapidbyte-cli/     # CLI binary (`rapidbyte run`, `rapidbyte check`)
  rapidbyte-core/    # Pipeline orchestrator, Wasmtime component runtime, state backend
  rapidbyte-sdk/     # Connector SDK (protocol types, component host bindings)
connectors/
  source-postgres/   # Source connector (wasm32-wasip2 target)
  dest-postgres/     # Destination connector (wasm32-wasip2 target)
tests/
  e2e.sh             # End-to-end test (Docker PG)
  bench.sh           # Benchmark script (variable row counts)
  fixtures/          # SQL seeds, pipeline YAMLs
```

- Workspace has 3 crates. Connectors are excluded from workspace and build separately.
- Connectors target `wasm32-wasip2` via their `.cargo/config.toml`.

## Commands

```bash
just build              # Build host binary
just build-connectors   # Build both connectors (wasm32-wasip2)
just test               # cargo test --workspace (host tests)
just e2e                # Full E2E: build, Docker PG, pipeline, verify
just bench-connector-postgres       # Benchmark: INSERT vs COPY comparison
just fmt                # cargo fmt
just lint               # cargo clippy
```

## Building

```bash
# Host (native)
cargo build

# Connectors (wasm32-wasip2)
cd connectors/source-postgres && cargo build
cd connectors/dest-postgres && cargo build
```

## Key Architecture

- Orchestrator (`crates/rapidbyte-core/src/engine/orchestrator.rs`) resolves component binaries and manifests, then runs source/transform/destination in blocking stages connected with `mpsc::sync_channel`.
- Runtime (`crates/rapidbyte-core/src/runtime/component_runtime.rs`) embeds Wasmtime component model and typed WIT imports/exports.
- Host imports enforce connector-side ACLs for `connect-tcp` and disable direct WASI socket networking.
- State backend is SQLite (`rusqlite` bundled), used for run metadata and cursor/checkpoint state.
- Arrow IPC batches flow between stages; optional lz4/zstd channel compression is handled in host imports.

## Notes

- Connectors must use `rapidbyte-sdk` component macros (`source_connector_main!`, `dest_connector_main!`, `transform_connector_main!`).
- Network I/O for connectors should go through `HostTcpStream` (`rapidbyte_sdk::host_tcp`) to preserve host permission checks.
- Protocol/documentation source of truth is `docs/PROTOCOL.md`.
