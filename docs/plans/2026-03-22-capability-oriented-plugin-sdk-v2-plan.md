# Capability-Oriented Plugin SDK V2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current `Context`-centric plugin contract with a clean-cut capability-oriented v2 WIT and Rust SDK that optimize for plugin-author ergonomics without embedding connector behavior in the SDK.

**Architecture:** Redesign the protocol around typed lifecycle inputs and generic capability handles, then rebuild the Rust SDK around those inputs while keeping connector implementation details in connector code. Migrate one representative source and one representative destination to validate the API, then remove the legacy `Context`-driven v1 surface.

**Tech Stack:** Rust, WIT component model, Wasmtime host/runtime bindings, proc macros, Tokio async tests, plugin integration tests.

---

## Task 1: Define failing protocol-shape tests for typed inputs and capability separation

**Files:**
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`
- Modify: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`
- Test: `crates/rapidbyte-sdk/src/plugin.rs`
- Test: `crates/rapidbyte-sdk/src/features.rs`

**Step 1: Write the failing tests**

Add compile-shape tests and trait tests that assert the new Rust authoring
surface:

- `Source::init(config, init)`
- `Source::discover(&self, input: DiscoverInput<'_>)`
- `Source::read(&self, input: ReadInput<'_>)`
- `Destination::write(&self, input: WriteInput<'_>)`
- feature traits use typed inputs instead of `Context`

Example test shape:

```rust
fn assert_source_v2<T: Source<Config = TestConfig>>() {
    let _ = T::init;
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk plugin
```

Expected:
- FAIL because the current traits are still `Context`-based

**Step 3: Write minimal implementation**

Introduce the new trait signatures and update the SDK trait tests to the v2
shape without implementing the full runtime yet.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk plugin
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/plugin.rs crates/rapidbyte-sdk/src/features.rs crates/rapidbyte-sdk/macros/src/plugin.rs
git commit -m "refactor(sdk): define capability-oriented v2 plugin traits"
```

## Task 2: Add failing SDK tests for capability wrappers and typed lifecycle inputs

**Files:**
- Create: `crates/rapidbyte-sdk/src/input.rs`
- Create: `crates/rapidbyte-sdk/src/capabilities.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Test: `crates/rapidbyte-sdk/src/input.rs`
- Test: `crates/rapidbyte-sdk/src/capabilities.rs`

**Step 1: Write the failing tests**

Add unit tests that assert:

- `ReadInput` exposes `stream`, `emit`, `cancel`, `state`, `checkpoints`,
  `metrics`, and `log`
- `WriteInput` exposes `stream`, `reader`, `cancel`, `state`, and
  `checkpoints`
- `InitInput` exposes only plugin-scope capabilities such as `log`, `metrics`,
  and `network`
- no single input exposes unrelated capabilities

Example test shape:

```rust
let input = test_read_input();
let _ = input.stream();
let _ = input.emit();
let _ = input.cancel();
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk input
cargo test -p rapidbyte-sdk capabilities
```

Expected:
- FAIL because the new input and capability modules do not exist

**Step 3: Write minimal implementation**

Create typed input structs and small capability wrappers that delegate to the
existing host FFI underneath without adding hidden behavior.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk input
cargo test -p rapidbyte-sdk capabilities
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/input.rs crates/rapidbyte-sdk/src/capabilities.rs crates/rapidbyte-sdk/src/lib.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "feat(sdk): add typed lifecycle inputs and capability wrappers"
```

## Task 3: Add failing WIT and runtime binding tests for the v2 capability contract

**Files:**
- Modify: `wit/rapidbyte-plugin.wit`
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`
- Test: runtime binding generation tests
- Test: `crates/rapidbyte-engine/tests/wasm_runner_integration.rs`

**Step 1: Write the failing tests**

Add tests that assert the runtime and engine can bind the v2 protocol:

- lifecycle calls decode typed input records
- host capability handles are available at the right lifecycle points
- batch IO and checkpoint operations still function through the new binding

If dedicated binding tests are sparse, add focused integration assertions in
the WASM runner tests.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-runtime bindings
cargo test -p rapidbyte-engine wasm_runner
```

Expected:
- FAIL because the current WIT and runtime bindings are still v1/v7 shaped

**Step 3: Write minimal implementation**

Refactor the WIT contract and regenerate/update the runtime bindings so the new
typed lifecycle inputs and capability handles are represented explicitly.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-runtime bindings
cargo test -p rapidbyte-engine wasm_runner
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add wit/rapidbyte-plugin.wit crates/rapidbyte-runtime/src/bindings.rs crates/rapidbyte-engine/tests/wasm_runner_integration.rs
git commit -m "refactor(wit): introduce capability-oriented plugin v2 contract"
```

## Task 4: Add failing tests for proc-macro export of the v2 plugin surface

**Files:**
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`
- Test: proc-macro tests for source/destination/transform export

**Step 1: Write the failing tests**

Add macro tests that assert:

- a v2 source plugin with `InitInput` and `ReadInput` exports correctly
- feature traits like `CdcSource` and `PartitionPlanner` wire through the macro
- old `Context`-shaped signatures are rejected

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk-macros plugin
```

Expected:
- FAIL because macro validation still expects the old signatures

**Step 3: Write minimal implementation**

Update the proc macro to validate and export the new trait method shapes and
feature traits.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk-macros plugin
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/macros/src/plugin.rs
git commit -m "refactor(macros): export capability-oriented v2 plugins"
```

## Task 5: Add failing SDK tests for author-facing test fakes and harnesses

**Files:**
- Create: `crates/rapidbyte-sdk/src/testing.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Test: `crates/rapidbyte-sdk/src/testing.rs`

**Step 1: Write the failing tests**

Add tests that prove authors can build a `ReadInput` or `WriteInput` with test
fakes for:

- emitted batches
- input batches
- checkpoints
- state store
- cancellation
- logs/metrics

Example test shape:

```rust
let harness = TestHarness::new();
let input = harness.read_input(test_stream());
assert!(!input.cancel().is_cancelled());
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk testing
```

Expected:
- FAIL because the testing harness does not exist

**Step 3: Write minimal implementation**

Add a small testing module that constructs fake capability wrappers and typed
inputs without requiring the full host runtime.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk testing
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/testing.rs crates/rapidbyte-sdk/src/lib.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "feat(sdk): add v2 plugin testing harnesses"
```

## Task 6: Migrate one representative source plugin to v2 and validate author DX

**Files:**
- Modify: `plugins/sources/postgres/src/main.rs`
- Modify: `plugins/sources/postgres/src/reader.rs`
- Modify: `plugins/sources/postgres/src/prerequisites.rs`
- Modify: `plugins/sources/postgres/src/discovery.rs`
- Test: `plugins/sources/postgres/src/main.rs`
- Test: `plugins/sources/postgres/src/prerequisites.rs`

**Step 1: Write the failing tests**

Add or update tests that assert the source plugin:

- initializes shared resources in `init`
- uses typed `ReadInput` and `DiscoverInput`
- compiles and behaves correctly without `Context`

If compile-fail tests are hard to add here, use direct unit tests plus full
crate compilation as the failure gate.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
```

Expected:
- FAIL because the plugin still implements the old SDK surface

**Step 3: Write minimal implementation**

Migrate the source plugin to v2 inputs and capability wrappers while keeping
connector-specific logic in the connector crate.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/sources/postgres/src/main.rs plugins/sources/postgres/src/reader.rs plugins/sources/postgres/src/prerequisites.rs plugins/sources/postgres/src/discovery.rs
git commit -m "refactor(source-postgres): migrate to plugin sdk v2"
```

## Task 7: Migrate one representative destination plugin to v2 and validate symmetry

**Files:**
- Modify: `plugins/destinations/postgres/src/main.rs`
- Modify: `plugins/destinations/postgres/src/writer.rs`
- Modify: `plugins/destinations/postgres/src/prerequisites.rs`
- Modify: `plugins/destinations/postgres/src/apply.rs`
- Test: `plugins/destinations/postgres/src/main.rs`
- Test: `plugins/destinations/postgres/src/writer.rs`

**Step 1: Write the failing tests**

Add or update tests that assert the destination plugin:

- uses typed `WriteInput`, `ApplyInput`, and `PrerequisitesInput`
- compiles and behaves correctly without `Context`
- keeps connector-specific setup and write behavior inside the connector crate

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Expected:
- FAIL because the plugin still implements the old SDK surface

**Step 3: Write minimal implementation**

Migrate the destination plugin to v2 inputs and capability wrappers while
keeping DDL, drift, write, and checkpoint policy in the connector.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add plugins/destinations/postgres/src/main.rs plugins/destinations/postgres/src/writer.rs plugins/destinations/postgres/src/prerequisites.rs plugins/destinations/postgres/src/apply.rs
git commit -m "refactor(dest-postgres): migrate to plugin sdk v2"
```

## Task 8: Remove the legacy `Context` API and old trait surface

**Files:**
- Modify: `crates/rapidbyte-sdk/src/context.rs`
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`
- Modify: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Modify: scaffold/dev docs that describe plugin authoring
- Test: SDK tests
- Test: plugin compile/test suites

**Step 1: Write the failing tests**

Add tests that assert:

- no core trait methods accept `&Context`
- old example plugins or scaffold output no longer compile against the removed
  API
- prelude exports the new input/capability types

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-sdk
```

Expected:
- FAIL because the old `Context` surface still exists

**Step 3: Write minimal implementation**

Delete or sharply reduce the legacy `Context` API, update exports and docs, and
remove remaining references to the old trait shape.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/context.rs crates/rapidbyte-sdk/src/plugin.rs crates/rapidbyte-sdk/src/features.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "refactor(sdk): remove legacy context-driven plugin api"
```

## Task 9: Run full branch verification and update author docs

**Files:**
- Modify: plugin author documentation files that describe SDK usage
- Modify: any scaffold templates that still emit old signatures
- Test: workspace verification targets

**Step 1: Write the failing docs/tests**

Update docs expectations to the v2 API and add any missing scaffold/doc tests
that assert the new authoring shape.

**Step 2: Run verification to identify failures**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- FAIL until remaining docs, scaffold, and integration references are updated

**Step 3: Write minimal implementation**

Update docs, scaffold output, and any remaining integration points to reflect
the capability-oriented v2 API.

**Step 4: Run verification to confirm it passes**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add docs crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "docs(sdk): document capability-oriented plugin v2"
```
