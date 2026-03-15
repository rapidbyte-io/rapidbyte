# Signal Handling for Cooperative Cancellation

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire OS signals (Ctrl-C / SIGTERM) into the cooperative cancellation system so pipelines drain cleanly instead of being killed mid-execution.

**Architecture:** The engine already has a full cooperative cancellation system (`CancellationToken` → `ensure_not_cancelled` at safe boundaries → `CANCELLED/before_commit` error). Two entry points don't wire signals into it: the CLI creates a dead token, and the agent only handles SIGINT (not SIGTERM). The fix: install signal handlers that cancel the token, letting the existing engine machinery handle graceful drain.

**Tech Stack:** Rust, `tokio::signal`, `CancellationToken` (tokio-util)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/rapidbyte-cli/src/commands/run.rs` | Modify | Wire Ctrl-C/SIGTERM to cancel token for local pipeline runs |
| `crates/rapidbyte-agent/src/worker.rs` | Modify | Add SIGTERM handler alongside existing SIGINT handler |

## Dependency Graph

```
Task 1: CLI signal handling (run.rs) — independent
Task 2: Agent SIGTERM handling (worker.rs) — independent
```

Both tasks are independent and can be done in parallel.

---

### Task 1: Wire Ctrl-C to cancel token in CLI `run` command

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:87-95`

Currently at line 91, `CancellationToken::new()` creates a dead token that nothing ever cancels. When Ctrl-C is pressed, the Tokio runtime drops and the process exits without cooperative cancellation.

- [ ] **Step 1: Create the cancel token and spawn a signal handler**

Replace the pipeline execution block (lines 87-95) with:

```rust
    // Run the pipeline with signal-driven cooperative cancellation.
    let cpu_start = process_cpu_seconds();
    let cancel_token = CancellationToken::new();
    let signal_token = cancel_token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Interrupt received, cancelling pipeline...");
        signal_token.cancel();
    });
    let outcome = orchestrator::run_pipeline(
        &config,
        &options,
        progress_tx,
        cancel_token,
        otel_guard.snapshot_reader(),
        otel_guard.meter_provider(),
    )
    .await;
```

On Unix, also handle SIGTERM (Kubernetes, systemd):
```rust
    #[cfg(unix)]
    {
        let term_token = cancel_token.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            sigterm.recv().await;
            tracing::info!("SIGTERM received, cancelling pipeline...");
            term_token.cancel();
        });
    }
```

Place the `#[cfg(unix)]` SIGTERM block right after the Ctrl-C spawn, before the `orchestrator::run_pipeline` call.

- [ ] **Step 2: Build and verify**

```bash
cargo check -p rapidbyte-cli
```
Expected: compiles. No new dependencies needed — `tokio` already has `features = ["full"]` which includes `signal`.

- [ ] **Step 3: Manual smoke test**

```bash
# Start a long-running pipeline (or use a sleep-based test plugin)
cargo run -- run tests/fixtures/slow_pipeline.yaml &
# Press Ctrl-C after 2 seconds
# Expected: "Interrupt received, cancelling pipeline..." log message
# Expected: clean exit with CANCELLED error (not a crash/panic)
```

If no slow pipeline fixture exists, skip this step — the wiring is verified by the build and by the fact that `ensure_not_cancelled` is already tested in the engine.

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(cli): wire Ctrl-C and SIGTERM to pipeline cancel token"
```

---

### Task 2: Add SIGTERM handler to agent worker

**Files:**
- Modify: `crates/rapidbyte-agent/src/worker.rs:225-240`

Currently (lines 227-240) only `tokio::signal::ctrl_c()` is handled. SIGTERM (the standard termination signal in containers) is ignored, causing unclean shutdowns in Kubernetes/systemd.

- [ ] **Step 1: Add SIGTERM to the shutdown select**

Replace the current signal handling block (lines 225-240) with:

```rust
    // Main coordinator loop with graceful shutdown.
    // Handle both SIGINT (Ctrl-C) and SIGTERM (container orchestrators).
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");

    let pool_result = tokio::select! {
        _ = &mut sigint => {
            info!("SIGINT received, stopping agent...");
            shutdown_token.cancel();
            worker_pool.await
        }
        #[cfg(unix)]
        _ = sigterm.recv() => {
            info!("SIGTERM received, stopping agent...");
            shutdown_token.cancel();
            worker_pool.await
        }
        result = &mut worker_pool => {
            shutdown_token.cancel();
            result
        }
    };
```

The `#[cfg(unix)]` gates ensure this compiles on all platforms. On non-Unix (Windows), only SIGINT/Ctrl-C is handled (same as before).

- [ ] **Step 2: Build and verify**

```bash
cargo check -p rapidbyte-agent
```

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-agent/src/worker.rs
git commit -m "feat(agent): add SIGTERM handler for clean container shutdown"
```

---

## Verification

After both tasks:

1. `cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude transform-validate` — all tests pass (signal handlers don't affect unit tests since they only fire on actual signals)
2. `cargo clippy --workspace --all-targets -- -D warnings` — no warnings
3. Manual: `kill -TERM <agent_pid>` produces "SIGTERM received, stopping agent..." log
4. Manual: Ctrl-C during `rapidbyte run` produces "Interrupt received, cancelling pipeline..." log

## What This Does NOT Cover (future work)

- **Mid-stream plugin cancellation:** Plugins running inside `spawn_blocking` can't be interrupted until the next `emit-batch`/`next-batch` boundary. A `check_cancelled` WIT host import would require a protocol v7 change.
- **Cancel timeout:** No deadline is enforced after cancel is requested. The epoch timeout (300s) is the only bound. A configurable cancel grace period could be added later.
- **Controller signal handling:** The controller is a pure gRPC server — Tonic handles shutdown internally. No changes needed.
