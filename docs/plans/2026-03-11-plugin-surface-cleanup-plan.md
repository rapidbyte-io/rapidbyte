# Plugin Surface Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove destination bulk-load as a separate public plugin trait, make bulk capability declarative instead of a host dispatch switch, update in-tree destination plugins to the cleaned-up model, and add first-class transform scaffolding.

**Architecture:** Collapse `write_bulk()` into a default method on `Destination`, keep only source feature traits where the contract actually changes, simplify destination macro glue to a single execution path, and move strategy choice fully inside plugins. Update `dest-postgres` to match the new contract and extend CLI scaffolding so transform plugins are first-class.

**Tech Stack:** Rust, proc macros, WIT component bindings, `cargo test`, `cargo clippy`, Rapidbyte SDK/CLI/plugins.

---

### Task 1: Add failing SDK tests for default destination bulk behavior

**Files:**
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`
- Modify: `crates/rapidbyte-sdk/src/features.rs`

**Step 1: Write the failing test**

Add a destination test in `crates/rapidbyte-sdk/src/plugin.rs` proving that `Destination::write_bulk()` defaults to `write()`. Use a small test destination that increments a counter in `write()` and call `write_bulk()` through the trait.

Example shape:

```rust
#[tokio::test]
async fn destination_write_bulk_defaults_to_write() {
    let mut dest = TestDest::new();
    let ctx = Context::new("dest-test", "users");
    let stream = StreamContext::test_default("users");

    let summary = Destination::write_bulk(&mut dest, &ctx, stream)
        .await
        .expect("write_bulk should succeed");

    assert_eq!(dest.write_calls, 1);
    assert_eq!(summary.records_written, 42);
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-sdk destination_write_bulk_defaults_to_write
```

Expected: FAIL because `Destination` does not define `write_bulk()` yet.

**Step 3: Write minimal implementation**

In `crates/rapidbyte-sdk/src/plugin.rs`, add:

```rust
async fn write_bulk(
    &mut self,
    ctx: &Context,
    stream: StreamContext,
) -> Result<WriteSummary, PluginError> {
    self.write(ctx, stream).await
}
```

Do not remove `BulkLoadDestination` yet in this task.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-sdk destination_write_bulk_defaults_to_write
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/plugin.rs crates/rapidbyte-sdk/src/features.rs
git commit -m "test(sdk): cover destination default bulk write"
```

### Task 2: Remove `BulkLoadDestination` from the public SDK surface

**Files:**
- Modify: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`

**Step 1: Write the failing compile-shape test**

Replace the existing `assert_bulk_load_dest` compile-shape test in
`crates/rapidbyte-sdk/src/features.rs` with source-only assertions and add a
macro-facing unit test or compile-shape assertion showing that destination
plugins no longer need any bulk feature trait.

Minimum expectation:

- `PartitionedSource` and `CdcSource` compile assertions remain
- there is no remaining bulk destination assertion path

**Step 2: Run tests to verify current behavior still depends on the old trait**

Run:

```bash
cargo test -p rapidbyte-sdk
```

Expected: FAIL after test edits because macro/features code still references `BulkLoadDestination`.

**Step 3: Write minimal implementation**

- Delete `BulkLoadDestination` from `crates/rapidbyte-sdk/src/features.rs`
- Stop exporting it from `crates/rapidbyte-sdk/src/prelude.rs`
- Remove bulk-load feature assertions from `crates/rapidbyte-sdk/macros/src/plugin.rs`

Do not change destination run dispatch yet; keep this task focused on public SDK shape.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-sdk
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/features.rs crates/rapidbyte-sdk/src/prelude.rs crates/rapidbyte-sdk/macros/src/plugin.rs
git commit -m "refactor(sdk): remove bulk destination feature trait"
```

### Task 3: Simplify destination macro dispatch to one execution path

**Files:**
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`
- Test: `crates/rapidbyte-sdk/macros/src/plugin.rs`

**Step 1: Write the failing test**

Add a focused macro-unit test, if practical in-tree, or a narrow helper test
covering `gen_dest_methods()` so destination `run` always emits a call to
`Destination::write_bulk(...)` regardless of manifest feature flags.

If direct token testing is easier, assert the generated token stream contains:

```rust
<#struct_name as #trait_path>::write_bulk(conn, &ctx, stream)
```

and no longer contains:

```rust
::rapidbyte_sdk::features::BulkLoadDestination
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-sdk-macros gen_dest_methods
```

Expected: FAIL because destination run code still branches on `has_bulk_load`.

**Step 3: Write minimal implementation**

In `crates/rapidbyte-sdk/macros/src/plugin.rs`:

- delete destination bulk dispatch branching in `gen_dest_methods()`
- always route destination run through:

```rust
rt.block_on(<#struct_name as #trait_path>::write_bulk(conn, &ctx, stream))
```

- keep source CDC/partitioned dispatch unchanged
- remove now-unused `has_bulk_load` plumbing if it becomes dead

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-sdk-macros
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/macros/src/plugin.rs
git commit -m "refactor(macros): unify destination run dispatch"
```

### Task 4: Make manifest bulk capability declarative and clean up `dest-postgres`

**Files:**
- Modify: `plugins/destinations/postgres/src/main.rs`
- Modify: `plugins/destinations/postgres/build.rs`
- Test: `plugins/destinations/postgres/src/main.rs`
- Check: `crates/rapidbyte-sdk/src/build.rs`

**Step 1: Write the failing test**

Add or update a destination plugin test proving:

- `PluginInfo.features` is not conditionally derived from config
- `DestPostgres` implements only `Destination`
- `write_bulk()` default or explicit override routes through the normal write path

Prefer a unit test around `init()` that asserts `Feature::BulkLoad` is not
mutated based on `load_method`.

**Step 2: Run test to verify current behavior fails**

Run:

```bash
cargo test -p dest-postgres
```

Expected: FAIL after test edits because `init()` still conditionally pushes `Feature::BulkLoad`
and there is still a separate `BulkLoadDestination` impl.

**Step 3: Write minimal implementation**

- In `plugins/destinations/postgres/src/main.rs`:
  - remove dynamic `Feature::BulkLoad` push from `init()`
  - remove `impl BulkLoadDestination for DestPostgres`
  - rely on `Destination::write_bulk()` default unless an override is still useful
- Keep `plugins/destinations/postgres/build.rs` as the single declarative source
  of bulk capability if `COPY` support is always compiled in

Do not widen this into writer-module cleanup.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p dest-postgres
```

Expected: PASS

**Step 5: Commit**

```bash
git add plugins/destinations/postgres/src/main.rs plugins/destinations/postgres/build.rs
git commit -m "refactor(dest-postgres): make bulk capability declarative"
```

### Task 5: Add failing scaffold tests for transform plugins

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing test**

Add a new scaffold test:

```rust
#[test]
fn scaffold_transform_generates_explicit_unimplemented_stubs() {
    // run("transform-example", ...)
    // assert generated files: main.rs, config.rs, transform.rs, README.md
    // assert README contains NOT PRODUCTION-READY / UNIMPLEMENTED
    // assert transform.rs contains PluginError::internal("UNIMPLEMENTED", ...)
}
```

Also add a role-detection assertion that `transform-*` names are accepted.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-cli scaffold_transform_generates_explicit_unimplemented_stubs
```

Expected: FAIL because scaffold only supports `source-` and `dest-`.

**Step 3: Write minimal implementation**

In `crates/rapidbyte-cli/src/commands/scaffold.rs`:

- add `Role::Transform`
- accept `transform-` prefixes
- map default output to `plugins/transforms/<name>`
- generate transform `build.rs`
- generate `src/main.rs`, `src/config.rs`, `src/transform.rs`
- update summary/next-step output for transform role

Reuse the current thin-`main.rs` scaffold style.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-cli scaffold_transform_generates_explicit_unimplemented_stubs
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "feat(cli): scaffold transform plugins"
```

### Task 6: Align scaffold templates with the cleaned role model

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing test**

Extend scaffold output assertions so generated destination templates do not
reference `BulkLoadDestination`, and transform templates use the normal
`Transform` role trait with thin `main.rs` wiring.

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli scaffold_
```

Expected: FAIL until the generated templates are fully aligned.

**Step 3: Write minimal implementation**

Update the scaffold template generators so:

- destination scaffold uses only `Destination`
- transform scaffold follows current plugin conventions
- README text mentions transform plugins where relevant

Keep templates intentionally minimal and unimplemented.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-cli
```

Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "refactor(cli): align scaffold templates with plugin roles"
```

### Task 7: Run full verification for the refactor slice

**Files:**
- No new files

**Step 1: Run targeted verification**

Run:

```bash
cargo test -p rapidbyte-sdk
cargo test -p rapidbyte-sdk-macros
cargo test -p dest-postgres
cargo test -p rapidbyte-cli
```

Expected: PASS

**Step 2: Run workspace lint gate**

Run:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS

**Step 3: Check docs/plan state**

Run:

```bash
git status --short
```

Expected: only intended implementation files and this plan file are present

**Step 4: Final commit**

```bash
git add crates/rapidbyte-sdk/src/plugin.rs crates/rapidbyte-sdk/src/features.rs crates/rapidbyte-sdk/src/prelude.rs crates/rapidbyte-sdk/macros/src/plugin.rs plugins/destinations/postgres/src/main.rs plugins/destinations/postgres/build.rs crates/rapidbyte-cli/src/commands/scaffold.rs docs/plans/2026-03-11-plugin-surface-cleanup-plan.md
git commit -m "refactor(plugins): simplify destination bulk and add transform scaffold"
```
