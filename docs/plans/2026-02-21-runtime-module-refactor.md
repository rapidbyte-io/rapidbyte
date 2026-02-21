# Runtime Module Refactor — Break Up the God Object

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the 1697-line `component_runtime.rs` god object into focused single-responsibility modules while preserving every public API symbol and all 88 passing tests.

**Architecture:** Extract 6 logical domains from `ComponentHostState` impl and the surrounding free functions into their own files. The existing `component_runtime.rs` becomes a thin re-export facade so that no consumer import paths change. Each new module owns one concern: network ACL, host socket I/O, WIT bindings/error converters, connector resolution/manifest, WASI context builder, and the AOT-cached Wasmtime loader.

**Tech Stack:** Rust, Wasmtime component model, `libc::poll` (unix), `serde_json`, `sha2`

---

## Current State Analysis

`component_runtime.rs` (1697 lines) contains **7 unrelated responsibilities** jammed into one file:

| Domain | Lines | Concern |
|--------|-------|---------|
| Network ACL | 74-225 | `NetworkAcl`, `normalize_host`, `extract_host_from_url`, `collect_runtime_hosts`, `derive_network_acl` |
| WASI context | 227-260 | `build_wasi_ctx` |
| Socket I/O | 262-868 | `SocketEntry`, `SocketInterest`, `wait_socket_ready`, `socket_poll_timeout_ms`, socket read/write/close/connect impls, `resolve_socket_addrs`, result enums |
| Host state + impls | 340-843 | `ComponentHostState` struct, constructor, batch/state/checkpoint/metric/log impls, `WasiView` impl |
| WIT bindings & converters | 33-52, 881-1146 | bindgen macros, error converter macros, host trait macros, validation converters |
| Wasmtime runtime | 1205-1403 | `WasmRuntime`, `LoadedComponent`, AOT cache, linkers, store |
| Connector resolution | 1405-1612 | `parse_connector_ref`, `resolve_connector_path`, manifest loading, checksum verify |

Additionally, `compression.rs` (97 lines) is in `runtime/` but conceptually is channel-level compression, not connector runtime. It's fine where it is since `ComponentHostState` uses it through `super::compression`, but the plan keeps it in place.

## Target File Structure

```
crates/rapidbyte-core/src/runtime/
├── mod.rs                    # Module declarations
├── compression.rs            # (unchanged)
├── component_runtime.rs      # Thin facade: re-exports only, ~50 lines
├── network_acl.rs            # NetworkAcl, host normalization, ACL derivation
├── host_socket.rs            # Socket I/O: SocketEntry, poll, read/write/close/connect
├── host_state.rs             # ComponentHostState struct, constructor, batch/state/checkpoint impls
├── wit_bindings.rs           # bindgen!, error converters, host trait impls, validation converters
├── wasm_runtime.rs           # WasmRuntime, LoadedComponent, AOT cache, linkers
└── connector_resolve.rs      # parse_connector_ref, resolve_connector_path, manifest, checksum
```

## Invariants (Must Hold After Every Task)

1. `cargo test --workspace` — all 88 tests pass (+ any new ones)
2. `cargo build` — compiles clean, no warnings
3. Every public symbol currently importable from `component_runtime::` remains importable from `component_runtime::`
4. No behavioral change — pure structural refactor

---

### Task 1: Create `network_acl.rs` — Extract ACL logic

**Files:**
- Create: `crates/rapidbyte-core/src/runtime/network_acl.rs`
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs` (delete moved code, add `use`)
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs` (add module declaration)

**Step 1: Create the new file with moved code**

Move these items from `component_runtime.rs` into `network_acl.rs`:
- `NetworkAcl` struct and impl (lines 74-122)
- `normalize_host` fn (lines 124-153)
- `extract_host_from_url` fn (lines 155-168)
- `collect_runtime_hosts` fn (lines 170-197)
- `derive_network_acl` pub fn (lines 199-225)
- The 3 `normalize_host` tests from `mod tests` (lines 1618-1634)

The new file needs these imports:
```rust
use std::collections::HashSet;
use std::net::IpAddr;

use rapidbyte_sdk::manifest::Permissions;
```

Make all items that were `pub` stay `pub`. Items that were private (`normalize_host`, `extract_host_from_url`, `collect_runtime_hosts`) become `pub(crate)` so `component_runtime.rs` can still re-export `derive_network_acl` and tests can reach `normalize_host`.

Move the test module into `network_acl.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::normalize_host;

    #[test]
    fn normalize_host_preserves_ipv6_literal() { /* ... */ }

    #[test]
    fn normalize_host_handles_bracketed_ipv6_with_port() { /* ... */ }

    #[test]
    fn normalize_host_strips_port_for_single_colon_hosts() { /* ... */ }
}
```

**Step 2: Update `mod.rs`**

Add `pub mod network_acl;` to `crates/rapidbyte-core/src/runtime/mod.rs`.

**Step 3: Update `component_runtime.rs`**

- Delete the moved code (lines 74-225 and the 3 test functions)
- Add at the top: `use super::network_acl::{derive_network_acl, NetworkAcl};`
- Keep the existing `pub` re-export of `derive_network_acl` via the facade (or just remove it if no external consumer uses it directly — check first)

**Step 4: Run tests to verify**

Run: `cargo test --workspace`
Expected: All 88 tests pass (the 3 normalize_host tests now run from `network_acl::tests`)

**Step 5: Run build to verify no warnings**

Run: `cargo build 2>&1 | grep -i warning`
Expected: No warnings

**Step 6: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: extract network ACL logic into runtime/network_acl.rs"
```

---

### Task 2: Create `host_socket.rs` — Extract socket I/O

**Files:**
- Create: `crates/rapidbyte-core/src/runtime/host_socket.rs`
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs`

**Step 1: Create the new file with moved code**

Move these items:
- `SOCKET_READY_POLL_MS` const (line 264)
- `SOCKET_POLL_ACTIVATION_THRESHOLD` const (line 269)
- `SocketInterest` enum (lines 272-276)
- `wait_socket_ready` fn (lines 290-319)
- `socket_poll_timeout_ms` fn (lines 322-330)
- `SocketEntry` struct (lines 332-337)
- `resolve_socket_addrs` fn (lines 854-868)
- `SocketReadResultInternal` enum (lines 870-874)
- `SocketWriteResultInternal` enum (lines 876-879)
- The `socket_poll_tests` module from tests (lines 1636-1695)

The socket read/write/close/connect `_impl` methods stay on `ComponentHostState` for now — they reference `self.sockets`, `self.network_acl`, etc. The extracted items are the standalone types and free functions that those methods depend on.

Required imports in `host_socket.rs`:
```rust
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
```

Visibility: Make all extracted items `pub(crate)` so `component_runtime.rs` (now `host_state.rs` in future tasks) can use them.

**Step 2: Update `mod.rs`**

Add `pub mod host_socket;`

**Step 3: Update `component_runtime.rs`**

- Delete the moved code
- Add: `use super::host_socket::{...};` importing all needed symbols
- The `_impl` methods on `ComponentHostState` that use `SocketEntry`, `wait_socket_ready`, etc. now reference them via the import

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All 88 tests pass (4 socket_poll_tests now in `host_socket::tests`)

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: extract socket I/O types and helpers into runtime/host_socket.rs"
```

---

### Task 3: Create `connector_resolve.rs` — Extract connector resolution

**Files:**
- Create: `crates/rapidbyte-core/src/runtime/connector_resolve.rs`
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs`

**Step 1: Create the new file**

Move these items:
- `parse_connector_ref` pub fn (lines 1508-1520)
- `resolve_connector_path` pub fn (lines 1531-1566)
- `manifest_path_from_wasm` pub fn (lines 1570-1573)
- `verify_wasm_checksum` pub fn (lines 1576-1585)
- `load_connector_manifest` pub fn (lines 1589-1612)
- `sha256_hex` fn (lines 1496-1500) — shared with `WasmRuntime`, so make it `pub(crate)`

Required imports:
```rust
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rapidbyte_sdk::manifest::ConnectorManifest;
use sha2::{Digest, Sha256};
```

**Step 2: Update `mod.rs`**

Add `pub mod connector_resolve;`

**Step 3: Update `component_runtime.rs`**

- Delete moved code
- Add re-exports so existing consumers don't break:
```rust
// Re-export connector resolution functions for backwards compatibility
pub use super::connector_resolve::{
    load_connector_manifest, manifest_path_from_wasm, parse_connector_ref,
    resolve_connector_path, verify_wasm_checksum,
};
```
- `WasmRuntime::load_module_aot` uses `sha256_hex` — import it: `use super::connector_resolve::sha256_hex;`

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass including `test_connector_path_resolution_*` integration tests

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: extract connector resolution into runtime/connector_resolve.rs"
```

---

### Task 4: Create `wasm_runtime.rs` — Extract Wasmtime loader

**Files:**
- Create: `crates/rapidbyte-core/src/runtime/wasm_runtime.rs`
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs`

**Step 1: Create the new file**

Move these items:
- `RAPIDBYTE_WASMTIME_AOT_ENV` const (line 30)
- `RAPIDBYTE_WASMTIME_AOT_DIR_ENV` const (line 31)
- `LoadedComponent` struct (lines 1206-1209)
- `WasmRuntime` struct and full impl (lines 1212-1403)
- `env_flag_enabled` fn (lines 1405-1421)
- `resolve_aot_cache_dir` fn (lines 1423-1439)
- `sanitize_cache_component_name` fn (lines 1441-1458)
- `write_file_atomic` fn (lines 1460-1494)

This module needs to import `ComponentHostState` to parameterize `Linker<ComponentHostState>` and `Store<ComponentHostState>`. It also needs the bindgen modules for linker setup:

```rust
use super::component_runtime::{
    source_bindings, dest_bindings, transform_bindings, ComponentHostState,
};
use super::connector_resolve::sha256_hex;
```

Note: The bindgen modules (`source_bindings`, `dest_bindings`, `transform_bindings`) are generated in `component_runtime.rs` via `wasmtime::component::bindgen!`. They stay there because the macro uses relative paths (`../../wit`). The new `wasm_runtime.rs` imports them.

**Step 2: Update `mod.rs`**

Add `pub mod wasm_runtime;`

**Step 3: Update `component_runtime.rs`**

- Delete moved code
- Add re-exports:
```rust
pub use super::wasm_runtime::{LoadedComponent, WasmRuntime};
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: extract WasmRuntime and AOT cache into runtime/wasm_runtime.rs"
```

---

### Task 5: Create `wit_bindings.rs` — Extract WIT error converters and host trait impls

**Files:**
- Create: `crates/rapidbyte-core/src/runtime/wit_bindings.rs`
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs`

**Step 1: Create the new file**

Move these items:
- `define_error_converters!` macro and its 3 invocations (lines 881-1007)
- `impl_host_trait_for_world!` macro and its 3 invocations (lines 1009-1098)
- The 3 empty `Host for ComponentHostState` impls (lines 1100-1102)
- `source_validation_to_sdk` pub fn (lines 1148-1165)
- `dest_validation_to_sdk` pub fn (lines 1167-1184)
- `transform_validation_to_sdk` pub fn (lines 1186-1203)

This module imports types from the parent and SDK:
```rust
use rapidbyte_sdk::errors::{
    BackoffClass, CommitState, ConnectorError, ErrorCategory, ErrorScope,
    ValidationResult, ValidationStatus,
};

use super::component_runtime::{
    source_bindings, dest_bindings, transform_bindings,
    ComponentHostState, SocketReadResultInternal, SocketWriteResultInternal,
};
```

Note: `SocketReadResultInternal` and `SocketWriteResultInternal` need to be `pub(crate)` in `host_socket.rs` (already done in Task 2). The `impl_host_trait_for_world!` macro references them.

**Step 2: Update `mod.rs`**

Add `pub mod wit_bindings;`

**Step 3: Update `component_runtime.rs`**

- Delete moved code
- Add re-exports:
```rust
pub use super::wit_bindings::{
    dest_error_to_sdk, source_error_to_sdk, transform_error_to_sdk,
    source_validation_to_sdk, dest_validation_to_sdk, transform_validation_to_sdk,
};
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: extract WIT bindings and error converters into runtime/wit_bindings.rs"
```

---

### Task 6: Consolidate `component_runtime.rs` as facade + verify

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs`

After tasks 1-5, `component_runtime.rs` should contain only:

1. The `bindgen!` macro invocations (must stay due to `path: "../../wit"` relative paths)
2. `Frame` enum
3. `HostTimings` struct
4. `build_wasi_ctx` fn
5. `ComponentHostState` struct and its impl block (with the `_impl` methods)
6. `WasiView` impl
7. Re-exports from the extracted modules

**Step 1: Verify file size**

The file should be roughly ~600-700 lines (down from 1697). Count lines:
```bash
wc -l crates/rapidbyte-core/src/runtime/component_runtime.rs
```

**Step 2: Verify all imports still work**

Run: `cargo build 2>&1`
Expected: Clean build, no errors or warnings

**Step 3: Run full test suite**

Run: `cargo test --workspace`
Expected: All 88 tests pass

**Step 4: Verify no public API breakage**

Check that `runner.rs` and `orchestrator.rs` compile without changes:
```bash
cargo build 2>&1 | grep -E "(error|warning)"
```

If any consumer needed changes (they shouldn't due to re-exports), fix them.

**Step 5: Run clippy**

Run: `cargo clippy --workspace 2>&1`
Expected: No new warnings

**Step 6: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: finalize runtime module split — component_runtime.rs as facade"
```

---

### Task 7: Clean up `ComponentHostState` field visibility

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs` (now host_state code)

**Step 1: Audit public fields on `ComponentHostState`**

Currently all fields except `network_acl`, `sockets`, `next_socket_handle`, `ctx`, `table` are `pub`. Check if any external code directly accesses these fields (not through the constructor).

Run:
```bash
# Search for direct field access outside the runtime module
```

Look for patterns like `host_state.pipeline_name`, `host_state.batch_sender`, etc. in `runner.rs` and `orchestrator.rs`.

**Step 2: Change appropriate fields to `pub(crate)`**

Fields that are only accessed within `rapidbyte-core` should be `pub(crate)`, not `pub`. This prevents accidental coupling from external crates.

**Step 3: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 4: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/
git commit -m "refactor: tighten ComponentHostState field visibility to pub(crate)"
```

---

## Summary

| Task | New File | Lines Moved | Key Symbols |
|------|----------|-------------|-------------|
| 1 | `network_acl.rs` | ~155 | `NetworkAcl`, `derive_network_acl`, `normalize_host` |
| 2 | `host_socket.rs` | ~170 | `SocketEntry`, `wait_socket_ready`, `SocketInterest`, result enums |
| 3 | `connector_resolve.rs` | ~120 | `parse_connector_ref`, `resolve_connector_path`, manifest fns |
| 4 | `wasm_runtime.rs` | ~200 | `WasmRuntime`, `LoadedComponent`, AOT cache |
| 5 | `wit_bindings.rs` | ~270 | Error converter macros, host trait macros, validation converters |
| 6 | Facade cleanup | — | Verify re-exports, verify ~600 line facade |
| 7 | Visibility | — | `pub` → `pub(crate)` where appropriate |

After all tasks: `component_runtime.rs` shrinks from **1697 → ~600 lines**, each new file is **100-270 lines** with a single clear responsibility, and the public API is unchanged.
