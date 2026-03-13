# Controller Poll Capacity Atomicity Design

**Date:** 2026-03-13

**Problem**

`poll_task` currently checks an agent's active-task count under a read lock and then claims work later under a separate task write lock. Under concurrent polls for the same agent, both requests can observe spare capacity and both can claim a task, exceeding `max_tasks`.

This is a regression in scheduler admission semantics. The controller must make capacity enforcement and task claim one atomic decision.

## Decision Summary

Make the agent-capacity check authoritative inside `try_claim_task()` while holding the task write lock that performs the claim.

Keep the existing read-side check in `poll_task()` only as an optional fast-path that can avoid unnecessary work, but do not rely on it for correctness. The write-side claim path must reject assignment when the agent is already at capacity.

## Design

### Atomic Capacity Check In Claim Path

`AgentServiceImpl::try_claim_task()` already owns the task write lock when it calls into the scheduler to claim pending work. That is the correct place to enforce capacity.

The updated flow should be:

1. acquire the task write lock
2. compute `active_tasks_for_agent(agent_id)` while still holding that lock
3. if the count is already at the configured `max_tasks`, return `Ok(None)` without peeking or polling
4. otherwise continue with the existing pending-task claim flow

This closes the race because any second concurrent poll must wait for the first claimant to release the write lock and will then see the updated active-task count before claiming.

### Method Signature

`try_claim_task()` should take the agent's `max_tasks` so it can enforce the cap under the write lock:

- current: `try_claim_task(&self, agent_id: &str)`
- new: `try_claim_task(&self, agent_id: &str, max_tasks: u32)`

That keeps the registry lookup in `poll_task()` unchanged and avoids locking the registry from inside the task scheduler path.

### Scope Boundary

Do not broaden this patch beyond the controller admission race.

Specifically:

- do not change plugin build tooling in this branch for this review item
- do not rewrite scheduler internals if the existing task write lock is already sufficient
- do not add new durable-store behavior; this is an in-memory admission atomicity fix

## Testing Strategy

Add a concurrency regression test before implementation:

- register one agent with `max_tasks = 1`
- enqueue two pending runs
- fire two `poll_task` RPCs concurrently for that same agent
- assert exactly one returns a task and the other returns `NoTask`
- assert the scheduler reports only one active task for that agent

Keep the existing serial capacity test as coverage for the ordinary non-concurrent path.

Verification after implementation:

- focused `poll_task` capacity tests
- `cargo test -p rapidbyte-controller`
- `cargo clippy --workspace --all-targets -- -D warnings`

## Rollout

Ship this fix in the branch before merge. Capacity oversubscription is a behavioral regression in scheduler fairness and can create downstream lease and recovery noise under load.
