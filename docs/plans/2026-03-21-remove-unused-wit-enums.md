# Remove Unused WIT Enums Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the unused `plugin-kind` and `run-phase` enums from the v7 WIT contract and its design/spec documentation.

**Architecture:** This is a contract-surface cleanup only. Update the source-of-truth WIT file and the v7 design spec so they stay aligned, then verify the removed enums are no longer referenced from the active contract/docs path.

**Tech Stack:** WIT component model, Markdown docs, Rust bindgen consumers

---

### Task 1: Remove Dead Enums From WIT

**Files:**
- Modify: `wit/rapidbyte-plugin.wit`

**Step 1: Verify current references exist**

Run: `rg -n "plugin-kind|run-phase" wit/rapidbyte-plugin.wit`

Expected: matches for both enum declarations.

**Step 2: Remove the enum declarations**

Delete the `plugin-kind` and `run-phase` enum blocks from the `types` interface.

**Step 3: Verify references are gone**

Run: `rg -n "plugin-kind|run-phase" wit/rapidbyte-plugin.wit`

Expected: no matches.

### Task 2: Align The V7 Design Spec

**Files:**
- Modify: `docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

**Step 1: Verify current references exist**

Run: `rg -n "plugin-kind|run-phase" docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

Expected: matches in the embedded WIT definition.

**Step 2: Remove the enum declarations**

Delete the matching `plugin-kind` and `run-phase` entries from the spec's WIT definition block.

**Step 3: Verify references are gone**

Run: `rg -n "plugin-kind|run-phase" docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

Expected: no matches.

### Task 3: Validate The Active Contract Surface

**Files:**
- Verify: `wit/rapidbyte-plugin.wit`
- Verify: `crates/rapidbyte-runtime/src/bindings.rs`
- Verify: `crates/rapidbyte-sdk/src/host_ffi.rs`

**Step 1: Run targeted contract search**

Run: `rg -n "plugin-kind|run-phase" wit crates/rapidbyte-runtime crates/rapidbyte-sdk`

Expected: no matches in the active contract/runtime/SDK path.

**Step 2: Run a WIT/bindgen-facing check**

Run one available validation command:
- `wasm-tools component wit wit/rapidbyte-plugin.wit`
- or `cargo test -p rapidbyte-runtime`

Expected: successful validation/build for the updated WIT consumers.
