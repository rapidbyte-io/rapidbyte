# Agent Completion And Flight Authorization Design

## Goal

Close two authorization gaps in the distributed runtime without widening the auth model:
`CompleteTask` must be bound to the agent that owns the lease, and the agent Flight
service must stop minting preview tickets for arbitrary callers.

## Approach

### Controller completion binding

The scheduler already records `assigned_agent_id` on task assignment. The controller
should use that existing ownership record when handling `CompleteTask`.

The minimal safe behavior is:

- pass `req.agent_id` into scheduler completion validation
- reject completion if the assigned agent does not match
- return `acknowledged: false` for mismatched callers so stale/forged completions are
  fenced the same way stale leases are fenced

This keeps the current RPC/API shape and avoids introducing a second agent identity
system.

### Flight ticket minting

`ListFlights` currently signs fresh preview tickets for any caller that can reach the
agent Flight port. That bypasses controller auth entirely.

The safest short-term behavior is:

- disable `ListFlights`
- keep `GetFlightInfo` and `DoGet` working for already-issued controller tickets

That preserves the actual preview download flow while removing the public ticket
minting path.

## Tests

- controller regression: a different registered agent cannot complete another agent's
  assigned task
- Flight regression: `ListFlights` is rejected instead of returning usable preview
  tickets
