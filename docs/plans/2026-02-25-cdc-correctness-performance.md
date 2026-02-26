# CDC Correctness and Postgres Connector Performance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate CDC WAL data-loss risk and remove identified high-impact performance bottlenecks in source/destination Postgres paths and runtime socket polling.

**Architecture:** Switch CDC from destructive read semantics to two-phase processing by peeking WAL changes and advancing the replication slot only from previously correlated state. Replace repeated runtime type-guessing and hot-path allocations with schema-driven extraction and reusable buffers. Keep behavior backward-compatible while improving throughput and crash safety.

**Tech Stack:** Rust, tokio-postgres, Arrow arrays/builders, bytes::BytesMut, PostgreSQL logical replication (`test_decoding`), rapidbyte runtime host adapter.

---

### Task 1: CDC Non-Destructive Read + Explicit Slot Advance

**Files:**
- Modify: `connectors/source-postgres/src/cdc/mod.rs`
- Modify: `connectors/source-postgres/src/cdc/parser.rs`
- Test: `connectors/source-postgres/src/cdc/mod.rs` (unit tests)

**Step 1: Write failing tests**
- Add tests for helper logic that:
  - chooses `pg_logical_slot_peek_changes` path
  - safely advances slot only when persisted `cursor_info.last_value` is ahead of current confirmed LSN
  - skips advance when no correlated cursor is present.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p source-postgres cdc::`
- Expected: FAIL on missing helper/behavior.

**Step 3: Write minimal implementation**
- Replace `pg_logical_slot_get_changes` with `pg_logical_slot_peek_changes`.
- Add helper(s): fetch current confirmed LSN and conditionally call `pg_replication_slot_advance` using `stream.cursor_info.last_value` (LSN).
- Keep source checkpoint emission with max observed LSN for downstream correlation.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p source-postgres cdc::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add connectors/source-postgres/src/cdc/mod.rs connectors/source-postgres/src/cdc/parser.rs
git commit -m "fix(source-postgres): make cdc reads non-destructive with slot advance"
```

### Task 2: Remove Hot-Loop Type Guessing (Cursor + Arrow Timestamp)

**Files:**
- Modify: `connectors/source-postgres/src/cursor.rs`
- Modify: `connectors/source-postgres/src/reader.rs`
- Modify: `connectors/source-postgres/src/encode.rs`
- Test: `connectors/source-postgres/src/cursor.rs`

**Step 1: Write failing tests**
- Add tests for cursor extraction strategy selection from `Column` metadata (`int2/int4/int8`, `timestamp` vs `timestamptz`, `date`, `json`).

**Step 2: Run test to verify it fails**
- Run: `cargo test -p source-postgres cursor::`
- Expected: FAIL until extraction strategy exists.

**Step 3: Write minimal implementation**
- Add explicit extraction strategy enum in `CursorTracker` chosen once in `new()`.
- Replace fallback `try_get(...).or_else(...)` chains in `reader.rs` with single direct extraction per strategy.
- In `encode.rs`, use `Column.pg_type` to choose timestamp extraction type and avoid fallback attempts.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p source-postgres cursor::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add connectors/source-postgres/src/cursor.rs connectors/source-postgres/src/reader.rs connectors/source-postgres/src/encode.rs
git commit -m "perf(source-postgres): use schema-driven cursor and timestamp decoding"
```

### Task 3: Reduce CDC Parser Allocation Churn

**Files:**
- Modify: `connectors/source-postgres/src/cdc/parser.rs`
- Modify: `connectors/source-postgres/src/cdc/mod.rs`
- Test: `connectors/source-postgres/src/cdc/parser.rs`

**Step 1: Write failing tests**
- Add parser tests asserting borrowed parsing behavior and escaped-quote handling via `Cow<'_, str>`.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p source-postgres cdc::parser::`
- Expected: FAIL until borrowed parser output is implemented.

**Step 3: Write minimal implementation**
- Change parser output to `CdcChange<'a>` with borrowed fields (`&'a str`) and `Cow<'a, str>` for values.
- Refactor CDC batching path to append parsed rows directly to Arrow builders without owning per-field `String` allocations.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p source-postgres cdc::parser::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add connectors/source-postgres/src/cdc/parser.rs connectors/source-postgres/src/cdc/mod.rs
git commit -m "perf(source-postgres): make cdc parser zero-copy with borrowed fields"
```

### Task 4: COPY Buffer Reuse and INSERT SQL Placeholder Optimization

**Files:**
- Modify: `connectors/dest-postgres/src/copy.rs`
- Modify: `connectors/dest-postgres/src/insert.rs`
- Test: `connectors/dest-postgres/src/insert.rs` (unit tests)

**Step 1: Write failing test**
- Add test for SQL placeholder generation shape to avoid per-cell `write!` formatting.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p dest-postgres insert::`
- Expected: FAIL until helper exists.

**Step 3: Write minimal implementation**
- Replace `Vec<u8>` + `std::mem::take` pattern with `bytes::BytesMut` + `split().freeze()` in COPY path.
- Prebuild per-chunk placeholder/value tuples using integer-to-string appends instead of `write!` in tight loop.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p dest-postgres insert::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add connectors/dest-postgres/src/copy.rs connectors/dest-postgres/src/insert.rs
git commit -m "perf(dest-postgres): reuse copy buffers and optimize insert sql building"
```

### Task 5: Lower Socket Poll Activation Threshold

**Files:**
- Modify: `crates/rapidbyte-runtime/src/socket.rs`
- Test: existing runtime socket poll tests

**Step 1: Write failing test (if needed)**
- Assert threshold constant is low enough for immediate OS-backed waiting.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p rapidbyte-runtime socket::`
- Expected: FAIL if threshold remains high.

**Step 3: Write minimal implementation**
- Reduce `SOCKET_POLL_ACTIVATION_THRESHOLD` from `1024` to a small value (e.g., `5`) and update comments accordingly.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p rapidbyte-runtime socket::`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/rapidbyte-runtime/src/socket.rs
git commit -m "perf(runtime): reduce socket wouldblock spin threshold"
```

### Task 6: End-to-End Verification

**Files:**
- N/A (verification only)

**Step 1: Run focused test suites**
- Run: `cargo test -p source-postgres`
- Run: `cargo test -p dest-postgres`
- Run: `cargo test -p rapidbyte-runtime`

**Step 2: Run lint/check for touched crates**
- Run: `cargo check -p source-postgres -p dest-postgres -p rapidbyte-runtime`

**Step 3: Validate diff scope**
- Run: `git diff -- connectors/source-postgres connectors/dest-postgres crates/rapidbyte-runtime docs/plans/2026-02-25-cdc-correctness-performance.md`

**Step 4: Final status summary**
- Report exact test/check command outcomes and any residual risk.
