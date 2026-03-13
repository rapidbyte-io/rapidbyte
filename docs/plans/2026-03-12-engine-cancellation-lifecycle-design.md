# Engine Cancellation Lifecycle Design

## Context

The distributed controller/agent path still has four correctness gaps:

1. `poll_task` can return an assignment whose run was concurrently cancelled after the scheduler claimed it.
2. Agent cancellation is only checked before `run_pipeline` starts, so accepted cancels become a no-op for running work.
3. Completion retries loop forever during controller outages and block clean shutdown.
4. Preview TTL does not actually reclaim memory on long-lived agents because expired spool entries are never evicted unless cleanup is called manually.

## Design

### Controller Assignment Fencing

`AgentService::poll_task` should treat the run-state transition as authoritative. If the scheduler claims a task but the run can no longer move to `Assigned`, the controller must not hand that assignment to an agent.

The safest local behavior is:

- poll from the task queue
- attempt `runs.transition(run_id, Assigned)`
- if that transition fails, immediately clear the claimed task from the scheduler and return `NoTask`

That closes the cancellation race without requiring a larger scheduler/run-store redesign.

### Engine-Level Cooperative Cancellation

Cancellation should be handled inside the engine at safe boundaries, not by dropping the `run_pipeline` future from the agent.

The engine will accept a `CancellationToken` and observe it at these boundaries:

- before retry backoff sleeps
- before destination DDL preflight starts
- before spawning each stream worker
- after source and transform stages complete but before destination write begins
- before checkpoint correlation / cursor persistence

If cancellation is observed before destination writes start, the engine should return a structured terminal error that preserves retry safety:

- `code = "CANCELLED"`
- `retryable = false`
- `safe_to_retry = true`
- `commit_state = before_commit`

If destination write has already started for a stream, the engine should keep running to the real outcome rather than fabricating a clean cancel. That avoids lying about commit safety.

### Worker Shutdown Semantics

`report_completion_until_terminal` should observe the worker shutdown token. If shutdown is requested while completion retries are still failing, the loop should stop retrying and return control to the worker so the process can terminate cleanly.

This should leave the lease entry intact rather than falsely acknowledging completion. On restart, normal lease expiry / controller reconciliation can handle the unfinished completion report.

### Preview Spool Reclamation

Expired spool entries should be evicted eagerly and periodically:

- `PreviewSpool::get` should remove expired entries instead of returning `None` while leaving memory resident
- the agent should periodically call `cleanup_expired()` from a long-lived loop such as heartbeats

That makes the configured preview TTL a real memory bound rather than a soft visibility timeout.

## Testing Strategy

- Controller regression test proving cancelled runs are not returned from `poll_task`.
- Engine/agent tests for cancellation before destination write and for preserving real outcomes after destination work has started.
- Worker test that completion retries stop when shutdown is cancelled.
- Spool tests proving expired `get()` evicts and worker cleanup actually runs.
