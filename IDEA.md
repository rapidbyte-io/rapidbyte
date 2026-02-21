# Rapidbyte Architecture Notes

This document captures the current connector runtime architecture after migration to
Wasmtime components and WIT-based host imports.

## Runtime

- Runtime: `wasmtime` component model.
- Connector target: `wasm32-wasip2`.
- Protocol version: `2`.
- Interface contract: `wit/rapidbyte-connector.wit`.

Connectors run as components and communicate with the host through typed WIT imports
(`emit-batch`, `next-batch`, `checkpoint`, `metric`, state APIs, and host-proxied TCP).

## Connector Model

- Source connectors export `source-connector`.
- Destination connectors export `dest-connector`.
- Transform connectors export `transform-connector`.

Lifecycle is component-native:

- `open(config-json)`
- `discover()` / `validate()` where applicable
- `run-read(ctx-json)` / `run-write(ctx-json)` / `run-transform(ctx-json)`
- `close()`

## Networking

Connectors do not use direct WASI networking. Outbound TCP is mediated by host imports:

- `connect-tcp`
- `socket-read`
- `socket-write`
- `socket-close`

The host enforces connector network permissions from manifests plus optional runtime host
allow-list derivation from validated config values.

## Data Flow

Pipeline stages exchange Arrow IPC batches via bounded channels. The host tracks timing for
batch emission/receipt and optional compression/decompression overhead.

## State and Checkpointing

State and checkpoint operations are exposed as host imports and persisted by the host engine.
Protocol envelopes use version `2`.

## PostgreSQL Connectors

Source and destination PostgreSQL connectors use `tokio-postgres` with `connect_raw` over a
host-proxied TCP stream adapter in `rapidbyte-sdk`.

## Migration Status

Legacy pointer-based ABI and legacy runtime integration paths have been removed from active
runtime and connector codepaths.
