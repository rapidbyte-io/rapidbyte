# Watcher Leak and Late-Cancel Coverage Design

## Problem

Two gaps remain in the distributed runtime:

- `WatchRun` subscribes before confirming a run exists, so missing run IDs can leak watcher channels if the error path does not clean up.
- The recent late-cancellation regression test only asserts that a constant helper returns `true`; it does not exercise real finalization behavior with a cancelled token after stream execution.

## Decision

Keep the subscribe-first `WatchRun` flow so the existing terminal-event race fix remains intact, but explicitly remove the watcher entry when the run lookup returns `NotFound`.

For engine cancellation coverage, replace the constant post-stream helper with a boundary helper that executes a supplied finalization closure even when the token is already cancelled. Test that helper by running real finalization logic with a cancelled token and asserting the persisted run status remains the real terminal outcome.

## Testing

- Add a controller regression that calls `watch_run_after_subscribe()` for a missing run and then asserts watcher storage is empty.
- Add an engine regression that cancels the token before post-stream finalization and still observes a completed run status from `finalize_successful_run_state`.

## Scope

This is a narrow cleanup and test-hardening change. It does not alter the terminal-state replay design for `WatchRun` or broaden cooperative cancellation semantics elsewhere.
