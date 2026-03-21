# WIT Protocol v7 — Phase 4: Plugin Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild all 3 plugins (postgres source, postgres destination, sql transform) against the v7 SDK.

**Architecture:** Update each plugin's trait implementations to match v7 signatures, remove PluginInfo from init, update discover/validate return types, change &mut self to &self, and add v7 lifecycle methods where beneficial. Clean up all v6 references.

**Tech Stack:** Rust, rapidbyte-sdk, wasm32-wasip2

**Spec:** `docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

**Depends on:** Phases 1-3 complete

---

### Task 1: Migrate postgres source plugin to v7

**Files:**
- Modify: `plugins/sources/postgres/src/main.rs`
- Modify: `plugins/sources/postgres/src/discovery.rs`
- Modify: `plugins/sources/postgres/src/reader.rs` (PartitionStrategy::Mod → ModHash)

Key changes:
1. `init()` → return `Result<Self, PluginError>` (remove PluginInfo tuple)
2. `discover()` → return `Vec<DiscoveredStream>` (not Catalog). Update discovery.rs to build DiscoveredStream with StreamSchema/SchemaField instead of Stream with ColumnSchema.
3. `validate()` → change to `&self` with `Option<&StreamSchema>`, return ValidationReport
4. `read()` → change `&mut self` to `&self`
5. `close()` → change `&mut self` to `&self`
6. `PartitionedSource::read_partition()` → change `&mut self` to `&self`
7. `CdcSource::read_changes()` → change `&mut self` to `&self`
8. `PartitionStrategy::Mod` → `PartitionStrategy::ModHash` in reader.rs
9. Add `prerequisites()` with CDC checks (wal_level, replication role)
10. Add `teardown()` to drop replication slots

Verify: `cargo check` in `plugins/sources/postgres/` (or `cargo check --target wasm32-wasip2` if configured)

Commit: `feat(plugin-source-postgres): migrate to v7 protocol`

---

### Task 2: Migrate postgres destination plugin to v7

**Files:**
- Modify: `plugins/destinations/postgres/src/main.rs`

Key changes:
1. `init()` → return `Result<Self, PluginError>` (remove PluginInfo tuple)
2. `validate()` → change to `&self` with `Option<&StreamSchema>`, return ValidationReport
3. `write()` → change `&mut self` to `&self`
4. `close()` → change `&mut self` to `&self`
5. Remove Feature enum usage from init, PluginInfo construction

Verify: `cargo check` in `plugins/destinations/postgres/`

Commit: `feat(plugin-dest-postgres): migrate to v7 protocol`

---

### Task 3: Migrate SQL transform plugin to v7

**Files:**
- Modify: `plugins/transforms/sql/src/main.rs`

Key changes:
1. `init()` → return `Result<Self, PluginError>` (remove PluginInfo tuple)
2. `validate()` → change to `&self` with `Option<&StreamSchema>`, return ValidationReport
3. `transform()` → change `&mut self` to `&self`
4. `close()` → change `&mut self` to `&self`
5. Update tests referencing old types (ValidationResult → ValidationReport, etc.)

Verify: `cargo check` in `plugins/transforms/sql/`

Commit: `feat(plugin-transform-sql): migrate to v7 protocol`

---

### Task 4: Migrate test plugins to v7

**Files:**
- Modify: `plugins/tests/test-source/src/main.rs`
- Modify: `plugins/tests/test-destination/src/main.rs`
- Modify: `plugins/tests/test-transform/src/main.rs`

Same pattern as above — update trait signatures for each test plugin.

Verify: `cargo check` for each test plugin

Commit: `feat(test-plugins): migrate test plugins to v7 protocol`

---

### Task 5: Build all plugins and verify

- [ ] Build all plugins: `just build-plugins`
- [ ] Run host tests: `cargo test --workspace`
- [ ] Verify no v6 references remain: grep for PluginInfo, ValidationResult, Catalog (as struct), SchemaHint across all plugin directories

Commit (if fixes needed): `fix(plugins): resolve remaining v7 migration issues`

Phase 4 complete.
