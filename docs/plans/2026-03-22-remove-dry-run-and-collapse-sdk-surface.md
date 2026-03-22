# Remove Dry Run And Collapse SDK Surface Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove `dry_run` and preview/replay from the product and plugin contract, and collapse redundant public/plugin-facing `_with_` APIs into one canonical SDK surface.

**Architecture:** This is a clean-cut protocol and product simplification. First remove `dry_run` from WIT/types/runtime and finish the macro session contract. Then collapse the SDK’s public/plugin-facing API to canonical names and constructors. Finally migrate engine/CLI/plugins/docs/tests to the new surface and delete dry-run/preview branches.

**Tech Stack:** Rust, WIT, Wasmtime component bindings, Clap CLI, Rapidbyte SDK/macros/runtime/engine/plugin crates

---

## Task 1: Lock The New Contract With Failing Protocol Tests

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-types/src/run.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-types/src/lifecycle.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-types/src/wire.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-runtime/src/bindings.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/wit/rapidbyte-plugin.wit`

**Step 1: Write failing tests for request shapes without `dry_run`**

- Remove old `dry_run` field assertions from `RunRequest` and `ApplyRequest` tests.
- Add/adjust binding tests so `run-request` and `apply-request` only carry the remaining fields.
- Add a failing test that the protocol version bump is required after this incompatible change.

**Step 2: Run the focused tests and confirm they fail**

Run:
```bash
cargo test -p rapidbyte-types run lifecycle wire
cargo test -p rapidbyte-runtime bindings
```

Expected:
- tests fail because `dry_run` is still present in types/WIT/bindings

**Step 3: Remove `dry_run` from types and WIT, and bump protocol/version metadata**

- Delete `dry-run: bool` from `run-request` and `apply-request` in WIT.
- Delete `dry_run` from `RunRequest` and `ApplyRequest`.
- Bump protocol version in `crates/rapidbyte-types/src/wire.rs`.
- Regenerate/adapt bindings as required by the current build flow.

**Step 4: Re-run focused tests**

Run:
```bash
cargo test -p rapidbyte-types run lifecycle wire
cargo test -p rapidbyte-runtime bindings
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add wit/rapidbyte-plugin.wit crates/rapidbyte-types/src/run.rs crates/rapidbyte-types/src/lifecycle.rs crates/rapidbyte-types/src/wire.rs crates/rapidbyte-runtime/src/bindings.rs
git commit -m "refactor(protocol): remove dry-run from plugin contract"
```

## Task 2: Fix Macro Session Semantics And Canonical Constructor Generation

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/macros/src/plugin.rs`
- Test: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/macros/src/plugin.rs`

**Step 1: Write failing macro tests**

- Add a failing test that generated `open()` does not return hard-coded `1`.
- Add a failing test that generated lifecycle entrypoints reject mismatched `input.session`.
- Add failing assertions for canonical constructor generation (`host(...)` / `new(...)`) and the absence of `with_dry_run`.

**Step 2: Run macro tests and confirm failure**

Run:
```bash
cargo test -p rapidbyte-sdk-macros plugin
```

Expected:
- FAIL on hard-coded session handling and outdated generated API names

**Step 3: Implement session storage/validation and generator cleanup**

- Store a real session id alongside the instance state.
- Reject stale/mismatched session ids in every generated lifecycle entrypoint.
- Remove generated `with_dry_run` call sites.
- Update generated constructors to the new canonical names.

**Step 4: Re-run macro tests**

Run:
```bash
cargo test -p rapidbyte-sdk-macros plugin
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/macros/src/plugin.rs
git commit -m "fix(macros): validate sessions and generate canonical v2 inputs"
```

## Task 3: Collapse Public SDK Input Constructors

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/input.rs`
- Test: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/input.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/plugin.rs`

**Step 1: Write failing SDK tests around canonical constructors**

- Replace tests that rely on `with_dry_run`, `with_capabilities`, or `with_stream_name`.
- Add tests for:
  - `ValidateInput::new(upstream, stream_name)`
  - `ReadInput::host(stream)` and `ReadInput::new(stream, emit, cancel, ...)`
  - `WriteInput::host(stream)` and explicit `new(...)`
  - `TransformInput::host(stream, plugin_id)` and explicit `new(...)`

**Step 2: Run SDK input tests and confirm failure**

Run:
```bash
cargo test -p rapidbyte-sdk input
```

Expected:
- FAIL because old constructor names still exist

**Step 3: Implement canonical constructors and remove redundant public builders**

- Remove all `dry_run` fields from typed inputs.
- Replace public `with_capabilities` / `with_dry_run` constructors with `new(...)` and `host(...)`.
- Change `ValidateInput::new` to take `upstream` and `stream_name`.
- Update `rapidbyte-sdk/src/plugin.rs` helper tests/defaults to match.

**Step 4: Re-run SDK input tests**

Run:
```bash
cargo test -p rapidbyte-sdk input
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/input.rs crates/rapidbyte-sdk/src/plugin.rs
git commit -m "refactor(sdk): collapse typed input constructors"
```

## Task 4: Collapse Public Batch And Metrics APIs

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/capabilities.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/testing.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-sdk/src/context.rs`

**Step 1: Write failing tests for the canonical APIs**

- Add tests that call:
  - `emit.batch(batch, metadata)`
  - `reader.next_batch(max_bytes)` returning the richer decoded result
  - `metrics.counter(name, value, labels)`
  - `metrics.gauge(name, value, labels)`
  - `metrics.histogram(name, value, labels)`
- Remove test usage of `*_with_*` public APIs being deleted.

**Step 2: Run focused SDK tests and confirm failure**

Run:
```bash
cargo test -p rapidbyte-sdk capabilities testing context
```

Expected:
- FAIL because the old surface is still exposed

**Step 3: Implement the canonical names**

- Rename `Emit::batch_for_stream` to `batch`, taking `&BatchMetadata`.
- Merge `Reader::next_batch_with_decode_timing` into `next_batch`.
- Merge labeled metric methods into `counter`, `gauge`, and `histogram`.
- Remove or adapt mirrored context/testing wrappers so the public/plugin-facing surface stays consistent.

**Step 4: Re-run focused SDK tests**

Run:
```bash
cargo test -p rapidbyte-sdk capabilities testing context
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/capabilities.rs crates/rapidbyte-sdk/src/host_ffi.rs crates/rapidbyte-sdk/src/testing.rs crates/rapidbyte-sdk/src/context.rs
git commit -m "refactor(sdk): collapse batch and metric api surface"
```

## Task 5: Remove Dry-Run And Preview Branches From Engine And CLI

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-engine/src/domain/ports/runner.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-engine/src/application/run.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-cli/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-cli/src/commands/check.rs`
- Modify any preview-specific controller/agent/CLI flow that now becomes dead code

**Step 1: Write failing engine/CLI tests for the new no-dry-run behavior**

- Remove/update tests that still expect dry-run requests or preview behavior.
- Add a failing CLI parsing test that `check --dry-run` is no longer accepted.
- Add/update engine tests so run/apply requests compile and execute without any dry-run field.

**Step 2: Run focused tests and confirm failure**

Run:
```bash
cargo test -p rapidbyte-engine wasm_runner run
cargo test -p rapidbyte-cli check
```

Expected:
- FAIL because engine and CLI still thread `dry_run`

**Step 3: Remove the product flow**

- Delete CLI `dry_run` flag from `check`.
- Remove dry-run logging/output distinctions.
- Remove request plumbing and runtime branching that only exists for dry-run/preview.
- Delete preview-only code paths if no longer referenced.

**Step 4: Re-run focused tests**

Run:
```bash
cargo test -p rapidbyte-engine wasm_runner run
cargo test -p rapidbyte-cli check
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-engine/src/adapter/wasm_runner.rs crates/rapidbyte-engine/src/domain/ports/runner.rs crates/rapidbyte-engine/src/application/run.rs crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/check.rs
git commit -m "refactor(engine): remove dry-run and preview flow"
```

## Task 6: Migrate Plugins And Scaffold Output To The Canonical Surface

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/sources/postgres/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/destinations/postgres/src/apply.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/transforms/sql/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/transforms/sql/src/transform.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/transforms/validate/src/transform.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/tests/test-source/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/tests/test-destination/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/plugins/tests/test-transform/src/main.rs`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write failing compile/use-site tests**

- Update scaffold/plugin tests to the new input constructors and batch/metric APIs.
- Add a failing test that scaffold output does not include removed dry-run or `_with_` helpers.

**Step 2: Run focused tests and confirm failure**

Run:
```bash
cargo test --manifest-path plugins/transforms/sql/Cargo.toml
cargo test --manifest-path plugins/transforms/validate/Cargo.toml
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p rapidbyte-cli scaffold
```

Expected:
- FAIL because plugins and scaffold still use old names/fields

**Step 3: Migrate call sites**

- Remove dry-run branches from plugins.
- Update plugin code to use canonical `input` constructors and capability methods.
- Update scaffold templates/examples to only emit the new surface.

**Step 4: Re-run focused tests**

Run the same commands as Step 2.

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/sources/postgres/src/main.rs plugins/destinations/postgres/src/apply.rs plugins/transforms/sql/src/main.rs plugins/transforms/sql/src/transform.rs plugins/transforms/validate/src/transform.rs plugins/tests/test-source/src/main.rs plugins/tests/test-destination/src/main.rs plugins/tests/test-transform/src/main.rs crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "refactor(plugins): adopt canonical v2 sdk surface"
```

## Task 7: Update Public Docs And Examples

**Files:**
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/README.md`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/docs/PLUGIN_DEV.md`
- Modify: `/home/netf/rapidbyte/.worktrees/plugin-sdk-v2/docs/TESTING.md`
- Modify any current docs that still describe dry-run/preview as active behavior

**Step 1: Write failing doc assertions or grep checks**

- Identify stale `--dry-run`, preview, `with_dry_run`, `next_batch_with_decode_timing`, `counter_with_labels`, and `emit_batch_with_metadata` references in active docs.

**Step 2: Run the grep checks and confirm matches exist**

Run:
```bash
rg -n "dry_run|dry-run|preview|with_dry_run|next_batch_with_decode_timing|counter_with_labels|gauge_with_labels|histogram_with_labels|emit_batch_with_metadata" README.md docs crates/rapidbyte-cli/src/commands/scaffold.rs
```

Expected:
- matches remain in active docs/examples

**Step 3: Update docs/examples**

- Remove dry-run/preview product references that are no longer true.
- Update examples to the canonical API names only.

**Step 4: Re-run the grep checks**

Run the same command as Step 2.

Expected:
- no stale matches in active docs/examples

**Step 5: Commit**

```bash
git add README.md docs/PLUGIN_DEV.md docs/TESTING.md
git commit -m "docs: remove dry-run and canonicalize sdk examples"
```

## Task 8: Full Verification And Cleanup

**Files:**
- Verify the whole worktree

**Step 1: Run workspace tests**

Run:
```bash
cargo test --workspace
```

Expected:
- PASS

**Step 2: Run workspace clippy**

Run:
```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- PASS

**Step 3: Run a final stale-surface grep**

Run:
```bash
rg -n "dry_run|with_dry_run|with_capabilities|next_batch_with_decode_timing|counter_with_labels|gauge_with_labels|histogram_with_labels|emit_batch_with_metadata|batch_for_stream" /home/netf/rapidbyte/.worktrees/plugin-sdk-v2
```

Expected:
- no matches in active source code outside historical docs/specs/plans that intentionally preserve past context

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove dry-run and collapse sdk surface"
```
