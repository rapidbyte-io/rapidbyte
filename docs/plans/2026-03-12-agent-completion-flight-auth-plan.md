# Agent Completion And Flight Authorization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bind task completion to the assigned agent and disable public Flight ticket minting.

**Architecture:** Reuse existing scheduler ownership state for controller-side completion fencing, and remove the unauthenticated `ListFlights` ticket-issuance path while preserving controller-issued preview tickets for `GetFlightInfo` and `DoGet`.

**Tech Stack:** Rust, tonic gRPC, Arrow Flight, in-memory scheduler/controller state.

---

### Task 1: Fence `CompleteTask` By Assigned Agent

**Files:**
- Modify: `crates/rapidbyte-controller/src/scheduler.rs`
- Modify: `crates/rapidbyte-controller/src/agent_service.rs`
- Test: `crates/rapidbyte-controller/src/agent_service.rs`

**Step 1: Write the failing test**

Add a regression proving one registered agent cannot complete a task assigned to a
different agent.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-controller agent_service::tests::test_complete_task_rejects_wrong_agent -- --exact
```

Expected: FAIL because the controller currently only validates `task_id` and
`lease_epoch`.

**Step 3: Write minimal implementation**

- extend scheduler completion validation to take `agent_id`
- require `assigned_agent_id == agent_id`
- return `Ok(None)` on mismatch so the RPC responds with `acknowledged: false`

**Step 4: Run test to verify it passes**

Run the same command and expect PASS.

### Task 2: Disable Public `ListFlights`

**Files:**
- Modify: `crates/rapidbyte-agent/src/flight.rs`
- Test: `crates/rapidbyte-agent/src/flight.rs`

**Step 1: Write the failing test**

Add a regression proving `ListFlights` is rejected instead of returning preview
tickets.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-agent flight::tests::list_flights_is_disabled -- --exact
```

Expected: FAIL because `ListFlights` currently returns active previews.

**Step 3: Write minimal implementation**

- return `Status::permission_denied` from `list_flights`
- remove the now-unused ticket-minting helper if nothing else uses it

**Step 4: Run test to verify it passes**

Run the same command and expect PASS.

### Task 3: Verify Packages

**Files:**
- No new files

**Step 1: Run targeted regressions**

```bash
cargo test -p rapidbyte-controller agent_service::tests::test_complete_task_rejects_wrong_agent -- --exact
cargo test -p rapidbyte-agent flight::tests::list_flights_is_disabled -- --exact
```

**Step 2: Run package verification**

```bash
cargo test -p rapidbyte-controller
cargo test -p rapidbyte-agent
```
