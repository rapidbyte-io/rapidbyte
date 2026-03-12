# Architecture v1.1 Alignment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Align the distributed controller/agent/CLI implementation with `docs/ARCHITECTUREv2.md` v1.1 and harden the resulting behavior.

**Architecture:** Keep the current controller/agent model, but finish the missing v1.1 surfaces and tighten correctness at the boundaries. The work is split into preview identity/storage, run metadata and controller responses, distributed CLI commands, Flight diagnostics, and TLS-capable transport wiring.

**Tech Stack:** Rust, Tokio, tonic gRPC, Arrow Flight, Clap, in-memory controller state, temp-file backed preview storage.

---

### Task 1: Fence Preview Replay By Full Identity

**Files:**
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Test: `crates/rapidbyte-agent/src/flight.rs`
- Test: `crates/rapidbyte-controller/src/scheduler.rs`

**Step 1: Write the failing tests**

Add tests for:

- task IDs are UUID-like rather than sequential `task-N`
- Flight lookup refuses to serve a preview when the signed `run_id` / `lease_epoch`
  does not match the stored preview identity

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller scheduler::tests::enqueue_uses_unique_task_ids -- --exact
cargo test -p rapidbyte-agent flight::tests::lookup_stream_rejects_mismatched_preview_identity -- --exact
```

Expected: failures showing sequential task IDs and task-id-only preview lookup.

**Step 3: Write minimal implementation**

- change task ID generation to UUIDs
- change the preview spool key from raw `task_id` to a preview identity struct
- store preview entries with `run_id`, `task_id`, and `lease_epoch`
- change Flight lookup to require full ticket identity match

**Step 4: Run tests to verify they pass**

Run the same commands and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/scheduler.rs crates/rapidbyte-controller/src/agent_service.rs crates/rapidbyte-agent/src/spool.rs crates/rapidbyte-agent/src/flight.rs
git commit -m "fix: fence preview replay by signed identity"
```

### Task 2: Populate Real Run Metadata And Stable Recent Listings

**Files:**
- Modify: `crates/rapidbyte-controller/src/run_state.rs`
- Modify: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Test: `crates/rapidbyte-controller/src/pipeline_service.rs`
- Test: `crates/rapidbyte-controller/src/run_state.rs`

**Step 1: Write the failing tests**

Add tests for:

- `GetRun` returns non-`None` submitted/current-task metadata once a run is queued or assigned
- `ListRuns` returns most-recent-first order deterministically

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-controller pipeline_service::tests::get_run_returns_real_metadata -- --exact
cargo test -p rapidbyte-controller pipeline_service::tests::list_runs_returns_most_recent_first -- --exact
```

Expected: failures showing placeholder metadata and unsorted truncation.

**Step 3: Write minimal implementation**

- move run timestamps to wall-clock serializable values
- record current task information on enqueue / assignment / retry / completion
- populate `GetRun` timestamps and `TaskRef`
- sort `ListRuns` by recency before truncating

**Step 4: Run tests to verify they pass**

Run the same commands and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/run_state.rs crates/rapidbyte-controller/src/pipeline_service.rs crates/rapidbyte-controller/src/agent_service.rs
git commit -m "feat(controller): expose real run metadata"
```

### Task 3: Add The Missing Distributed CLI Commands

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Create or Modify: `crates/rapidbyte-cli/src/commands/status.rs`
- Create or Modify: `crates/rapidbyte-cli/src/commands/watch.rs`
- Create or Modify: `crates/rapidbyte-cli/src/commands/list_runs.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Test: `crates/rapidbyte-cli/src/commands/status.rs`
- Test: `crates/rapidbyte-cli/src/commands/watch.rs`
- Test: `crates/rapidbyte-cli/src/commands/list_runs.rs`

**Step 1: Write the failing tests**

Add tests for:

- `status` requires a controller and calls `GetRun`
- `watch` requires a controller and treats terminal states correctly
- `list-runs` requires a controller and formats returned runs

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli commands::status::tests::status_requires_controller -- --exact
cargo test -p rapidbyte-cli commands::watch::tests::watch_requires_controller -- --exact
cargo test -p rapidbyte-cli commands::list_runs::tests::list_runs_requires_controller -- --exact
```

Expected: failures because the commands do not exist yet.

**Step 3: Write minimal implementation**

- add `Status`, `Watch`, and `ListRuns` CLI subcommands
- route them through the controller with the existing bearer helper
- make them fail clearly when no controller is configured

**Step 4: Run tests to verify they pass**

Run the same commands and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/mod.rs crates/rapidbyte-cli/src/commands/status.rs crates/rapidbyte-cli/src/commands/watch.rs crates/rapidbyte-cli/src/commands/list_runs.rs
git commit -m "feat(cli): add distributed status watch and list-runs"
```

### Task 4: Implement Preview Spill-To-Disk

**Files:**
- Modify: `crates/rapidbyte-agent/src/spool.rs`
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Test: `crates/rapidbyte-agent/src/spool.rs`
- Test: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Write the failing tests**

Add tests for:

- large preview entries spill to temp files rather than storing all batches in memory
- file-backed preview entries still serve correctly through Flight replay

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-agent spool::tests::large_preview_spills_to_disk -- --exact
cargo test -p rapidbyte-agent flight::tests::do_get_serves_file_backed_preview -- --exact
```

Expected: failure because the spool is memory-only.

**Step 3: Write minimal implementation**

- add a threshold-based spool representation
- serialize large preview streams to temp Arrow IPC files
- read them back for `DoGet` / `GetFlightInfo`
- ensure TTL cleanup removes temp files

**Step 4: Run tests to verify they pass**

Run the same commands and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/spool.rs crates/rapidbyte-agent/src/flight.rs crates/rapidbyte-agent/src/worker.rs
git commit -m "feat(agent): spill large previews to disk"
```

### Task 5: Implement Diagnostic Flight Listing

**Files:**
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Modify: `crates/rapidbyte-agent/src/spool.rs`
- Test: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Write the failing test**

Add a test proving `ListFlights` returns active preview streams with usable
`FlightInfo` entries.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-agent flight::tests::list_flights_returns_active_previews -- --exact
```

Expected: failure because `list_flights` is currently unimplemented.

**Step 3: Write minimal implementation**

- iterate active preview entries
- emit one `FlightInfo` per preview stream with ticket, schema when available,
  and row/byte counts

**Step 4: Run test to verify it passes**

Run the same command and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-agent/src/flight.rs crates/rapidbyte-agent/src/spool.rs
git commit -m "feat(agent): expose diagnostic flight listings"
```

### Task 6: Add TLS-Capable Transport Wiring

**Files:**
- Modify: `crates/rapidbyte-controller/src/server.rs`
- Modify: `crates/rapidbyte-controller/src/lib.rs`
- Modify: `crates/rapidbyte-agent/src/worker.rs`
- Modify: `crates/rapidbyte-cli/src/commands/controller.rs`
- Modify: `crates/rapidbyte-cli/src/commands/agent.rs`
- Modify: `crates/rapidbyte-cli/src/commands/distributed_run.rs`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Test: `crates/rapidbyte-cli/src/commands/controller.rs`
- Test: `crates/rapidbyte-cli/src/commands/agent.rs`
- Test: `crates/rapidbyte-cli/src/commands/distributed_run.rs`

**Step 1: Write the failing tests**

Add tests for:

- controller config builder wires TLS assets when provided
- agent config builder wires TLS settings for controller client and Flight server
- distributed CLI builds a TLS channel configuration when CA/domain settings are supplied

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-cli commands::controller::tests::controller_execute_wires_tls -- --exact
cargo test -p rapidbyte-cli commands::agent::tests::agent_execute_wires_tls -- --exact
cargo test -p rapidbyte-cli commands::distributed_run::tests::distributed_run_builds_tls_channel_when_configured -- --exact
```

Expected: failures because TLS options do not exist.

**Step 3: Write minimal implementation**

- add optional TLS config structs to controller/agent runtime config
- add CLI flags/env wiring for cert/key/CA/domain inputs
- use tonic server/client TLS config when TLS settings are present
- keep plaintext behavior as the default when TLS is not configured

**Step 4: Run tests to verify they pass**

Run the same commands and expect PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-controller/src/server.rs crates/rapidbyte-controller/src/lib.rs crates/rapidbyte-agent/src/worker.rs crates/rapidbyte-cli/src/main.rs crates/rapidbyte-cli/src/commands/controller.rs crates/rapidbyte-cli/src/commands/agent.rs crates/rapidbyte-cli/src/commands/distributed_run.rs crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat: add tls-capable distributed transport"
```

### Task 7: Cleanup And Full Verification

**Files:**
- Modify: any stale docs/comments in touched source files

**Step 1: Clean up stale docs/comments**

Update any comments that still claim behavior we no longer have, especially
around preview spool behavior and engine cancellation assumptions.

**Step 2: Run full package verification**

Run:

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-agent
cargo test -p rapidbyte-cli
```

If preview spill implementation touches shared Arrow utilities in a way that
requires it, also run:

```bash
cargo test -p rapidbyte-engine
```

**Step 3: Inspect working tree**

Run:

```bash
git status --short
git diff --stat
```

**Step 4: Final commit**

```bash
git add <touched files>
git commit -m "align distributed architecture with v1.1 spec"
```
