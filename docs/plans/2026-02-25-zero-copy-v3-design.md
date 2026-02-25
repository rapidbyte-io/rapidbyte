# Zero-Copy V3 Transport Design

**Date:** 2026-02-25  
**Status:** Approved for planning  
**Owner:** Runtime/SDK/Connector stack

## 1. Goal

Replace the current V2 byte transport (`list<u8>` payload passing) with a V3 frame-handle transport that minimizes transient allocations and copy amplification across runtime, SDK, and connectors.

This is a breaking change. Backward compatibility is intentionally out of scope.

## 2. Scope

- Replace WIT package `rapidbyte:connector@2.0.0` with `rapidbyte:connector@3.0.0`.
- Remove V2 compatibility codepaths from runtime and SDK bindings.
- Introduce frame resources for batch transport and socket I/O.
- Move internal runtime payload routing to `bytes::Bytes` / `BytesMut`.
- Migrate in-tree connectors (`source-postgres`, `dest-postgres`) to V3.
- Update tests and benches to validate V3-only behavior.

## 3. Non-Goals

- Supporting both V2 and V3 simultaneously.
- Preserving old ABI/WIT symbols.
- Optimizing every clone in non-byte control-plane logic in this pass.

## 4. Architecture

### 4.1 WIT V3 transport model

Introduce host-managed `resource frame` with explicit lifecycle:

- `frame-new(capacity: u64) -> result<frame, connector-error>`
- `frame-write(handle: frame, chunk: list<u8>) -> result<u64, connector-error>`
- `frame-seal(handle: frame) -> result<_, connector-error>`
- `frame-len(handle: frame) -> result<u64, connector-error>`
- `frame-read(handle: frame, offset: u64, len: u64) -> result<list<u8>, connector-error>`
- `frame-drop(handle: frame)`
- `emit-batch(handle: frame) -> result<_, connector-error>`
- `next-batch() -> result<option<frame>, connector-error>`

Socket APIs move to frame handles as well:

- `socket-read-frame(handle: u64, len: u64) -> result<socket-read-result-v3, connector-error>`
- `socket-write-frame(handle: u64, frame: frame) -> result<socket-write-result, connector-error>`

### 4.2 Runtime internals

- Channel payload enum becomes `Frame::Data(Bytes)` instead of `Vec<u8>`.
- Host state adds frame table:
  - writable state backed by `BytesMut`
  - sealed state backed by `Bytes`
- Ownership transfer:
  - `emit-batch` consumes sealed frame handle into channel payload
  - `next-batch` returns a new frame handle referencing existing `Bytes`

### 4.3 SDK/connector interaction

- SDK host FFI hides frame resource mechanics behind safe helpers.
- Existing high-level connector APIs (`ctx.emit_batch`, `ctx.next_batch`) remain stable at SDK surface but use V3 transport internally.
- Connector implementations are updated to V3-generated bindings only.

## 5. End-to-End Data Flow

1. Source builds Arrow batch.
2. SDK encodes IPC bytes, allocates host frame, writes payload once, seals, emits handle.
3. Runtime routes `Bytes` payload through stage channels.
4. Destination/transform calls `next-batch`, receives frame handle to same payload bytes.
5. SDK reads frame bytes and decodes Arrow IPC.
6. Frame handles are explicitly dropped (or auto-cleaned at teardown).

## 6. Zero-Copy / Allocation Reduction Strategy

- Prefer borrowing from DB rows where possible (`&str`, `&[u8]`) before Arrow builder append.
- Replace repeated `Vec<u8>` reallocation paths with pooled `BytesMut` buffers.
- Keep unavoidable copies limited to:
  - wasm canonical ABI boundary for `list<u8>` reads/writes
  - Arrow IPC encode/decode operations

## 7. Error Model

Add V3 transport-specific error codes:

- `INVALID_FRAME_HANDLE`
- `FRAME_NOT_SEALED`
- `FRAME_ALREADY_SEALED`
- `FRAME_BOUNDS`
- `FRAME_IO_FAILED`

Lifecycle violations fail fast and return `connector-error` with existing retry metadata semantics.

## 8. Testing Plan

### Unit tests

- Frame lifecycle transitions (new/write/seal/read/drop/consume).
- Bounds validation and invalid-handle behavior.
- Compression/decompression over `Bytes`.
- SDK frame helper correctness for emit/receive flows.

### Integration tests

- Source -> destination V3 transport roundtrip.
- Source -> transform -> destination multi-hop frame routing.
- Failure injection: dropped handles, connector traps, channel close.

### Performance checks

- Update arrow codec and pipeline benches to V3 paths.
- Record allocation and throughput snapshots pre/post migration.
- Treat numbers as regression guardrails (code-first acceptance, metrics still captured).

## 9. Migration Sequence

1. Add V3 WIT definitions and regenerate bindings.
2. Implement runtime frame table and frame host methods.
3. Switch runtime channel payloads to `Bytes`.
4. Update SDK host FFI to frame transport helpers.
5. Migrate source-postgres and dest-postgres to V3 bindings.
6. Remove V2 codepaths and stale tests.
7. Run full test + benchmark verification.

## 10. Risks and Mitigations

- **Risk:** Resource leaks in frame table.  
  **Mitigation:** explicit drop APIs + teardown cleanup + lifecycle tests.

- **Risk:** Semantic regressions during one-shot ABI break.  
  **Mitigation:** broad integration coverage and staged task execution with checkpoints.

- **Risk:** Limited gains due to unavoidable wasm boundary copies.  
  **Mitigation:** focus optimizations on high-volume runtime/channel and connector extraction paths where ownership can be preserved.
