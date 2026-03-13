# Architecture v1.1 Alignment Design

## Goal

Bring the distributed controller/agent/CLI implementation into alignment with
`docs/ARCHITECTUREv2.md` v1.1 while improving correctness and robustness in the
same pass.

## Scope

This is a completion pass for the existing controller/agent architecture, not a
redesign. The current controller, agent, and distributed CLI paths stay in
place. The work focuses on filling the documented v1.1 gaps and hardening the
existing implementation where the architecture already implies stricter
behavior.

The main alignment targets are:

1. preview replay identity and lifecycle correctness
2. missing distributed CLI public surface
3. incomplete run metadata and nondeterministic run listing
4. missing diagnostic Flight surface
5. preview spool durability/footprint behavior
6. TLS-capable transport wiring across control and data planes

## Current Gaps

### Preview replay is under-fenced

Controller preview tickets already sign `run_id`, `task_id`, `stream_name`,
`lease_epoch`, and expiry, but the agent preview spool is keyed only by
`task_id`. That means the signed identity is not fully enforced at replay time.

In practice this becomes dangerous when controller state restarts while an agent
keeps old preview data resident. The scheduler may reuse a task identifier, and
the agent can then serve stale preview data for a new signed ticket that happens
to reference the same `task_id`.

### CLI surface is narrower than the documented v1.1 contract

The architecture doc treats distributed `run`, `status`, `watch`, and
`list-runs` as first-class CLI operations. The current CLI only exposes
distributed `run`. The underlying controller RPCs already exist, so this is
primarily a CLI surface and formatting gap rather than a protocol gap.

### Run metadata is only partially surfaced

The run store already keeps creation/update timestamps and a `current_task_id`,
but `GetRun` and `ListRuns` still return placeholder metadata. `ListRuns`
currently truncates unsorted `HashMap` values, so it is not meaningfully
"recent runs".

### Preview spool is in-memory only

The architecture doc describes a preview spool that keeps small previews in
memory and spills large ones to temp Arrow IPC files. The current spool only
stores `DryRunResult` in memory. TTL cleanup now works, but large previews can
still occupy memory for the full TTL.

### TLS is not wired through the shipped binaries

The architecture requires TLS support for controller gRPC and Flight transport.
The current controller, agent, and CLI all use plaintext channels only.

### Diagnostic Flight listing is missing

The architecture includes `ListFlights` in v1.1 for diagnostics. The current
Flight service leaves it unimplemented.

## Design

### 1. Strengthen preview identity at the data plane

The agent spool should be keyed by the same identity the controller signs:

- `run_id`
- `task_id`
- `lease_epoch`

The stream name remains part of ticket verification and stream lookup, but not
the outer spool key.

Concretely:

- replace the spool key from raw `task_id` string to a small preview identity
  struct
- store dry-run results under that identity when the task completes
- look up previews in Flight by the full verified ticket payload, not by
  `task_id` alone

This keeps ticket verification and preview replay aligned and eliminates stale
preview reuse across controller restarts or task-ID reuse.

As part of this change, task identifiers should stop being sequential
`task-N` strings and switch to UUIDs. Full preview identity fencing is still the
real fix, but UUID task IDs remove a second unnecessary source of collision risk
in the control plane.

### 2. Complete the documented distributed CLI surface

Add three CLI commands that are thin controller clients:

- `rapidbyte status <run-id>`
- `rapidbyte watch <run-id>`
- `rapidbyte list-runs`

These commands should reuse the existing controller proto surface and the same
global `--controller` / `RAPIDBYTE_CONTROLLER` / config-file resolution logic as
distributed `run`.

Behavior:

- without a controller, the commands should fail clearly instead of silently
  falling back to standalone mode, because they have no meaningful local
  equivalent
- with a controller, they should use the existing bearer-token helper and print
  concise human-readable output

### 3. Make run metadata real and deterministic

The run store already tracks enough data to populate the documented response
shape. Wire that through rather than returning placeholders.

Changes:

- keep `created_at` / `updated_at` as the basis for `submitted_at`
- record start/completion timing in run state when transitions occur
- track current task identity on enqueue/assignment/retry
- populate `GetRun.current_task`
- populate timestamps in `GetRun` and `ListRuns`
- make `ListRuns` sort by most recent first before truncating

Because the in-memory store uses `Instant` today, this likely needs a small
shift to wall-clock timestamps (`SystemTime` or `DateTime<Utc>`) for API
serialization. That is a correctness improvement, not just polish.

### 4. Add preview spill-to-disk behind the spool abstraction

Keep the public spool API small, but change the implementation so preview
storage can choose between:

- in-memory `DryRunResult` for smaller previews
- temp Arrow IPC files for larger preview streams

A practical v1.1 implementation is:

- configurable byte threshold
- if a stream exceeds the threshold, write its batches to a temp file
- store schema/row metadata plus file path in the spool entry
- Flight `DoGet` reads from either memory-backed or file-backed preview streams

The point is not resumable streaming or fancy compaction. It is to make the
v1.1 “memory + spill-to-disk” statement true and keep long-lived agents from
holding arbitrarily large dry-run previews in RAM.

### 5. Implement diagnostic `ListFlights`

`ListFlights` should expose active preview entries on an agent for diagnostics.
This does not need full catalog richness. A minimal correct v1.1 version is:

- one `FlightInfo` per active preview stream
- signed retrievable ticket on each endpoint
- row and byte counts where available

This can reuse the same spool iteration and ticket construction primitives
already used by `GetFlightInfo`.

### 6. Add TLS-capable transport configuration

The architecture requires TLS support across control and data planes. The right
v1.1 implementation is TLS-capable binaries with optional local plaintext for
developer convenience, not a hardcoded plaintext-only stack.

Controller:

- add optional server cert/key and optional CA bundle config
- if configured, use `ServerTlsConfig`

Agent:

- add TLS config for its controller client and Flight server
- register the advertised Flight endpoint exactly as configured by the operator

CLI:

- add optional CA / domain / client-cert parameters needed to connect securely
  to controller and Flight
- preserve plaintext defaults when no TLS options are supplied

This matches the architecture’s production requirement without breaking local
test ergonomics.

## Hardening Work Included In Scope

While implementing the above, fix adjacent quality debt that otherwise leaves
the branch half-finished:

- add controller-side preview metadata cleanup loop
- update stale docs/comments that still describe unimplemented spill behavior or
  old engine signatures
- add focused regression tests for preview replay fencing, recent-run ordering,
  metadata population, Flight listing, and TLS config wiring

## Explicitly Out Of Scope

These remain deferred, consistent with the architecture:

- cross-agent dataflow
- persistent controller metadata
- remote `check` / `discover`
- resumable Flight offsets
- multi-controller HA

## Testing Strategy

This pass should stay test-first and package-verifiable.

Required regression coverage:

- controller rejects stale preview identity reuse across task-ID reuse
- `ListRuns` is newest-first and stable
- `GetRun` returns non-placeholder timestamps/current task when appropriate
- `status` / `watch` / `list-runs` CLI commands use controller correctly
- `ListFlights` returns active preview entries
- large preview spool entries spill to disk and still replay through Flight
- TLS config builders wire server/client transport settings correctly

Full package verification remains:

- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-agent`
- `cargo test -p rapidbyte-cli`
- targeted `cargo test -p rapidbyte-engine` if spool/file replay changes require
  new engine-facing coverage
