# Rapidbyte

Data pipeline engine using WASI connectors (WasmEdge runtime).

## Project Structure

```
crates/
  rapidbyte-cli/     # CLI binary (`rapidbyte run`, `rapidbyte check`)
  rapidbyte-core/    # Pipeline orchestrator, WASM runtime, state backend
  rapidbyte-sdk/     # Connector SDK (protocol types, host FFI bindings)
connectors/
  source-postgres/   # Source connector (wasm32-wasip1 target)
  dest-postgres/     # Destination connector (wasm32-wasip1 target)
tests/
  e2e.sh             # End-to-end test (Docker PG, 6 records)
  bench.sh           # Benchmark script (variable row counts)
  fixtures/          # SQL seeds, pipeline YAMLs
```

- Workspace has 3 crates. Connectors are **excluded** from workspace and build separately.
- Connectors target `wasm32-wasip1` via their `.cargo/config.toml`.
- Connectors have `[workspace]` in Cargo.toml to prevent parent workspace discovery (required for git worktrees).

## Commands

```bash
just build              # Build host binary
just build-connectors   # Build both connectors (wasm32-wasip1)
just test               # cargo test --workspace (host tests only)
just e2e                # Full E2E: build, Docker PG, pipeline, verify
just bench-connector-postgres       # Benchmark: INSERT vs COPY comparison (10K rows)
just bench-connector-postgres 100000  # Same with custom row count
just fmt                # cargo fmt
just lint               # cargo clippy
```

## Building

Host and connector builds use **different targets** — never mix them:

```bash
# Host (native)
cargo build

# Connectors (wasm32-wasip1) — must cd into connector dir
cd connectors/source-postgres && cargo build
cd connectors/dest-postgres && cargo build
```

WasmEdge env must be sourced: `source ~/.wasmedge/env`

## Key Architecture

- **Orchestrator** (`crates/rapidbyte-core/src/engine/orchestrator.rs`): Loads WASM modules, spawns source/dest on blocking threads, connects via `mpsc::sync_channel`.
- **Host functions** registered under `"rapidbyte"` import namespace + WASI module.
- **State backend**: SQLite (`rusqlite` bundled). Tracks sync runs.
- **Arrow IPC**: Source emits RecordBatches as IPC bytes through channel to destination.
- **IPC compression**: Optional lz4/zstd compression between stages, configured via `resources.compression: lz4|zstd` in pipeline YAML. Compression/decompression happens in host functions (`host_emit_batch`/`host_next_batch`), transparent to WASI connectors. Uses `lz4_flex` and `zstd` crates.
- **Channel backpressure**: `mpsc::sync_channel` capacity is configurable via `resources.max_inflight_batches` (default: 16).

## Gotchas

- **WASI module registration is required**: Must create `WasiModule` and add to instances HashMap alongside "rapidbyte" imports, or get `unknown import: wasi_snapshot_preview1::sock_getsockopt`.
- **Debug builds**: `wasi_mio` panics on unsupported socket ops. Connectors need `--cfg skip_wasi_unsupported` in rustflags. Release builds are fine.
- **Connector WASM can't be tested natively** — `rb_allocate` etc. use i32 ptrs, SIGSEGV on 64-bit.
- **PG TIMESTAMP columns**: Use nullable `TIMESTAMP DEFAULT NOW()`, not `TIMESTAMP NOT NULL`. The source connector's Arrow schema mapping produces nulls for non-nullable timestamps.
- **`records_written` counter**: Currently stays at 0 (known stats tracking issue in dest connector).
- **SyncMode enum**: Has 3 variants: `FullRefresh`, `Incremental`, `Cdc`. CDC is protocol-ready but no connector implements logical replication yet.
- **Docker PG readiness**: Use `psql -c "SELECT 1"`, not `pg_isready` (passes before init scripts complete).
- **Tokio pin**: Connectors must pin `tokio = "=1.36"` (exact) for WASI fork patch to apply.

## WASI Connector Dependencies

Connectors use forks from `second-state` GitHub org:
- `wasi_tokio` (v1.36.x), `socket2` (v0.5.x), `rust-postgres` (main branch)

## E2E / Benchmark

- Single PostgreSQL instance (port 5433 via docker-compose.yml)
- Source reads from `public` schema, dest writes to `raw` schema
- Benchmark emits `@@BENCH_JSON@@{...}` for machine-readable results
- `PipelineResult` includes per-phase timing: source/dest duration, module load times, VM setup, recv loop, source connect/query/fetch, dest connect/flush/commit
- `bench.sh` runs both INSERT and COPY modes automatically with comparison report
- Dest connector supports `load_method: insert|copy` in pipeline YAML (default: insert)
- AOT compilation via `wasmedge compile` reduces module load from ~500ms to ~4ms
