# Plugin Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the plugin layer toward a documented standard by splitting oversized modules, aligning scaffolding and SDK helpers with that standard, and keeping behavior unchanged.

**Architecture:** The work is staged to keep risk low. First land the standards/documentation and the smallest SDK/scaffold support changes, then perform behavior-preserving structural moves in the highest-value plugins, and only then clean up secondary layout issues that still improve clarity.

**Tech Stack:** Rust, `rapidbyte-sdk`, proc macros, plugin WASM components, Cargo test/clippy

---

### Task 1: Land the Plugin Architecture Standard

**Files:**
- Modify: `docs/PLUGIN_ARCHITECTURE.md`
- Reference: `docs/plans/2026-03-11-plugin-refactor-design.md`

**Step 1: Write the failing documentation expectation**

Document the required and recommended plugin conventions:
- thin `main.rs`
- config normalization and validation
- error-category guidance
- metric module guidance
- default source/destination/transform layouts
- when to add feature traits
- when to extract shared kits

**Step 2: Verify the doc is missing or incomplete**

Run: `rg -n "Plugin Architecture|thin main.rs|shared kits" docs`

Expected: either no architecture doc exists or current docs do not cover this standard.

**Step 3: Write the architecture doc**

Create or update `docs/PLUGIN_ARCHITECTURE.md` using the approved design language. Keep required vs recommended rules explicit.

**Step 4: Review the doc for overlap**

Run: `sed -n '1,260p' docs/PLUGIN_ARCHITECTURE.md`

Expected: concise, no contradiction with `docs/PLUGIN_DEV.md` or scaffold behavior after later tasks.

**Step 5: Commit**

```bash
git add docs/PLUGIN_ARCHITECTURE.md
git commit -m "docs: define plugin architecture standard"
```

### Task 2: Add an SDK Logging Helper for Spawned Tasks

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Test: `crates/rapidbyte-sdk/src/host_ffi.rs`

**Step 1: Write the failing test**

Add a small unit test or compile-shape test for a helper like `log_error(message)` that routes through existing host logging facilities without requiring `Context`.

**Step 2: Run the focused test**

Run: `cargo test -p rapidbyte-sdk host_ffi`

Expected: fail if the helper does not exist yet or the new assertion is unmet.

**Step 3: Implement the helper**

Add a lightweight helper in `host_ffi.rs` that wraps the current raw logging path and re-export it from the prelude only if that improves plugin ergonomics.

**Step 4: Run focused verification**

Run: `cargo test -p rapidbyte-sdk host_ffi`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_ffi.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "feat(sdk): add spawned-task logging helper"
```

### Task 3: Update Postgres Plugins to Use the SDK Logging Helper

**Files:**
- Modify: `plugins/sources/postgres/src/client.rs`
- Modify: `plugins/destinations/postgres/src/client.rs`
- Test: `plugins/sources/postgres/src/client.rs`
- Test: `plugins/destinations/postgres/src/client.rs`

**Step 1: Write the smallest useful regression test or compile assertion**

If a direct unit test is awkward, add focused compile-safe assertions around the logging call site pattern or rely on targeted plugin tests after the edit.

**Step 2: Replace direct FFI logging**

Swap raw `rapidbyte_sdk::host_ffi::log(...)` calls for the new SDK helper.

**Step 3: Run targeted plugin verification**

Run:
- `cargo test --manifest-path plugins/sources/postgres/Cargo.toml`
- `cargo test --manifest-path plugins/destinations/postgres/Cargo.toml`

Expected: pass with no behavior changes.

**Step 4: Commit**

```bash
git add plugins/sources/postgres/src/client.rs plugins/destinations/postgres/src/client.rs
git commit -m "refactor(plugins): use sdk logging helper"
```

### Task 4: Extract `transform-sql` Reference Analysis

**Files:**
- Create: `plugins/transforms/sql/src/references.rs`
- Modify: `plugins/transforms/sql/src/main.rs`
- Test: `plugins/transforms/sql/src/main.rs`
- Test: `plugins/transforms/sql/src/references.rs`

**Step 1: Write the failing test**

Move or add focused tests that prove:
- quoted current stream names validate
- cross-stream subqueries fail
- CTE shadowing is rejected

Target the new reference-analysis module where possible.

**Step 2: Run focused transform tests**

Run: `cargo test --manifest-path plugins/transforms/sql/Cargo.toml`

Expected: still passing before the extraction, giving a baseline.

**Step 3: Extract the analysis code**

Move `query_external_table_references()`, `validate_query_for_stream_name()`, and the AST walker helpers out of `main.rs` into `references.rs`. Keep `main.rs` focused on plugin wiring and delegation.

**Step 4: Re-run focused tests**

Run: `cargo test --manifest-path plugins/transforms/sql/Cargo.toml`

Expected: pass with no behavior changes.

**Step 5: Commit**

```bash
git add plugins/transforms/sql/src/main.rs plugins/transforms/sql/src/references.rs
git commit -m "refactor(transform-sql): extract query reference analysis"
```

### Task 5: Split `dest-postgres` Writer by Responsibility

**Files:**
- Create: `plugins/destinations/postgres/src/contract.rs`
- Create: `plugins/destinations/postgres/src/session.rs`
- Create: `plugins/destinations/postgres/src/metrics.rs` (only if warranted by resulting code shape)
- Modify: `plugins/destinations/postgres/src/writer.rs`
- Modify: `plugins/destinations/postgres/src/main.rs`
- Test: `plugins/destinations/postgres/src/writer.rs`
- Test: `plugins/destinations/postgres/src/session.rs`
- Test: `plugins/destinations/postgres/src/contract.rs`

**Step 1: Capture baseline verification**

Run: `cargo test --manifest-path plugins/destinations/postgres/Cargo.toml`

Expected: pass before the structural move.

**Step 2: Extract contract setup**

Move `WriteContract`, `prepare_stream_once`, and related preparation helpers into `contract.rs`.

**Step 3: Extract session lifecycle**

Move `WriteSession` and its methods into `session.rs`.

**Step 4: Decide metrics extraction pragmatically**

If the resulting writer/session split still leaves repeated metric helpers, extract them to `metrics.rs`; otherwise keep them with the most local module.

**Step 5: Leave `writer.rs` thin**

Keep `writer.rs` as the orchestration entrypoint around `write_stream()`.

**Step 6: Run focused verification**

Run: `cargo test --manifest-path plugins/destinations/postgres/Cargo.toml`

Expected: pass with no behavior changes.

**Step 7: Commit**

```bash
git add plugins/destinations/postgres/src/main.rs plugins/destinations/postgres/src/writer.rs plugins/destinations/postgres/src/contract.rs plugins/destinations/postgres/src/session.rs plugins/destinations/postgres/src/metrics.rs
git commit -m "refactor(dest-postgres): split writer responsibilities"
```

### Task 6: Align Destination Type Module Boundaries

**Files:**
- Modify: `plugins/destinations/postgres/src/decode.rs`
- Modify: `plugins/destinations/postgres/src/type_map.rs`
- Optionally create: `plugins/destinations/postgres/src/types.rs`
- Modify: `plugins/destinations/postgres/src/main.rs`
- Test: `plugins/destinations/postgres/src/decode.rs`
- Test: `plugins/destinations/postgres/src/type_map.rs`

**Step 1: Re-evaluate after Task 5**

Inspect whether `type_map.rs` plus `decode.rs` still feels fragmented after the writer split.

**Step 2: Only if clarity improves, consolidate**

Either:
- rename/co-locate mapping code into a single `types.rs`, or
- keep current file names and only move the most obviously related type helpers.

Do not force this if it creates churn without clarity.

**Step 3: Run focused verification**

Run: `cargo test --manifest-path plugins/destinations/postgres/Cargo.toml`

Expected: pass.

**Step 4: Commit**

```bash
git add plugins/destinations/postgres/src/decode.rs plugins/destinations/postgres/src/type_map.rs plugins/destinations/postgres/src/types.rs plugins/destinations/postgres/src/main.rs
git commit -m "refactor(dest-postgres): clarify type mapping modules"
```

### Task 7: Refactor `transform-validate` to Match the Transform Standard

**Files:**
- Create as needed: `plugins/transforms/validate/src/validate.rs`
- Create as needed: `plugins/transforms/validate/src/rules.rs` or `engine.rs`
- Modify: `plugins/transforms/validate/src/main.rs`
- Modify: `plugins/transforms/validate/src/transform.rs`
- Modify: `plugins/transforms/validate/src/config.rs`
- Test: `plugins/transforms/validate/src/config.rs`
- Test: `plugins/transforms/validate/src/transform.rs`

**Step 1: Capture baseline verification**

Run: `cargo test --manifest-path plugins/transforms/validate/Cargo.toml`

Expected: pass before the refactor.

**Step 2: Split by concern**

Move substantial validation/planning/rule logic out of `transform.rs` into role-appropriate modules. Keep `main.rs` thin and keep runtime behavior unchanged.

**Step 3: Run focused verification**

Run: `cargo test --manifest-path plugins/transforms/validate/Cargo.toml`

Expected: pass.

**Step 4: Commit**

```bash
git add plugins/transforms/validate/src/main.rs plugins/transforms/validate/src/config.rs plugins/transforms/validate/src/transform.rs plugins/transforms/validate/src/validate.rs plugins/transforms/validate/src/rules.rs plugins/transforms/validate/src/engine.rs
git commit -m "refactor(transform-validate): split large transform module"
```

### Task 8: Align Scaffold Templates to the Standard

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`
- Test: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Write the failing test**

Add or adjust scaffold snapshot/assertion tests so generated plugins better match the documented standard:
- source scaffold uses clearer role-oriented module naming
- destination scaffold has a thin write entrypoint
- transform scaffold separates validation/runtime concerns when appropriate

**Step 2: Run focused scaffold tests**

Run: `cargo test -p rapidbyte-cli scaffold`

Expected: fail if current templates don’t meet the new assertions.

**Step 3: Update templates**

Edit scaffold generation to better reflect the standard while keeping templates small and practical.

**Step 4: Re-run focused verification**

Run: `cargo test -p rapidbyte-cli scaffold`

Expected: pass.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "refactor(scaffold): align plugin templates with architecture"
```

### Task 9: Clean Up Module Visibility and Small Layout Inconsistencies

**Files:**
- Modify: `plugins/sources/postgres/src/main.rs`
- Modify: any affected modules under `plugins/sources/postgres/src/`
- Test: `plugins/sources/postgres/src/main.rs`

**Step 1: Minimize public exposure**

Remove unnecessary `pub mod` declarations and prefer private modules plus `pub(crate)` items where access is internal.

**Step 2: Run focused verification**

Run: `cargo test --manifest-path plugins/sources/postgres/Cargo.toml`

Expected: pass.

**Step 3: Commit**

```bash
git add plugins/sources/postgres/src/main.rs plugins/sources/postgres/src/types.rs plugins/sources/postgres/src/config.rs
git commit -m "refactor(source-postgres): tighten module visibility"
```

### Task 10: Final Verification and Branch Wrap-Up

**Files:**
- Review: all touched plugin, SDK, CLI, and docs files

**Step 1: Run targeted verification**

Run:
- `cargo test --manifest-path plugins/transforms/sql/Cargo.toml`
- `cargo test --manifest-path plugins/transforms/validate/Cargo.toml`
- `cargo test --manifest-path plugins/sources/postgres/Cargo.toml`
- `cargo test --manifest-path plugins/destinations/postgres/Cargo.toml`
- `cargo test -p rapidbyte-sdk -p rapidbyte-sdk-macros -p rapidbyte-cli`

Expected: all pass.

**Step 2: Run workspace lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: pass.

**Step 3: Review branch diff**

Run:
- `git diff --stat origin/<branch>...HEAD`
- `git status --short`

Expected: only intended files changed.

**Step 4: Commit any remaining doc or cleanup adjustments**

Use a narrow commit message matching the final cleanup.

