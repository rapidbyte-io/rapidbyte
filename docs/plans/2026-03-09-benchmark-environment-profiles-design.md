# Benchmark Environment Profiles Design

## Problem

The native benchmark scenarios currently commit concrete PostgreSQL connection
details directly in scenario manifests. That is the wrong boundary.

Symptoms:

- benchmark scenarios are no longer portable
- local credentials drift from the actual Docker/dev database
- `just bench --suite lab --scenario ...` fails for environment reasons that are
  not visible in the scenario model
- the benchmark runner is forced to treat environment config as scenario data

This makes the system fragile and works against the connector-agnostic benchmark
goal.

## Goals

- Remove committed credentials and host-specific connection details from
  benchmark scenarios
- Keep scenarios focused on workload, connector mode, and assertions
- Add a committed repo-supported local benchmark environment profile
- Allow local overrides without editing tracked files
- Keep the core benchmark runner focused on execution, not infrastructure
  provisioning
- Provide a one-command wrapper for local lab runs

## Non-Goals

- General Testcontainers support in this slice
- Automatic container startup inside the core benchmark runner
- Full multi-backend environment provisioning for every future connector
- Secret-management integration beyond simple environment-variable overrides

## Recommended Approach

Use committed benchmark environment profiles plus a thin wrapper command.

The system should be split into four roles:

- Scenario: defines what is being benchmarked
- Environment profile: defines where it runs
- Wrapper: prepares the local environment and invokes the runner
- Runner: merges scenario + environment and executes the benchmark

This keeps the execution model clean and makes the local developer experience
better without hardcoding Docker/Testcontainers behavior into the core runner.

## Alternatives Considered

### 1. Environment variables only

Store all runtime connection details in env vars and inject them directly into
the benchmark runner.

Pros:

- flexible
- no committed credentials

Cons:

- poor local UX
- easy to misconfigure
- weak discoverability
- harder to document reproducible local flows

Rejected because it solves secrecy but not usability or repeatability.

### 2. Provisioning inside the benchmark runner

Teach the runner to start Docker/Testcontainers automatically.

Pros:

- convenient one-command experience

Cons:

- couples runner to provisioning concerns
- worsens connector-agnostic design
- obscures benchmark timing boundaries
- grows operational complexity too early

Rejected because the runner should not own environment orchestration.

### 3. Committed profiles plus wrapper

Add committed environment profile files, keep the runner execution-focused, and
use `just` wrappers for local orchestration.

Pros:

- clean separation of concerns
- easy local UX
- reproducible repo-supported benchmark environment
- preserves a generic design for future backends/providers

Selected.

## Configuration Model

### Scenario model

Scenarios should stop embedding concrete source and destination connection
details.

Instead, they should declare a logical environment reference plus any benchmark
local data that is truly scenario-specific.

For the PostgreSQL destination benchmarks, the scenario should retain:

- environment reference id
- logical stream name
- connector options like `load_method` and `write_mode`
- workload
- assertions

Example direction:

```yaml
environment:
  ref: local-dev-postgres
  stream_name: bench_events
```

The source and destination connection blocks should move out of the scenario.

### Environment profile model

Add committed environment profiles under:

- `benchmarks/environments/`

Initial profile:

- `benchmarks/environments/local-dev-postgres.yaml`

The profile should be generic in shape, even if the first implementation only
supports PostgreSQL bindings.

Recommended structure:

```yaml
id: local-dev-postgres
provider:
  kind: docker_compose
  project_dir: .
services:
  postgres:
    kind: postgres
    host: 127.0.0.1
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
bindings:
  source:
    service: postgres
    schema: analytics
  destination:
    service: postgres
    schema: raw
```

This is intentionally broader than “just a Postgres DSN”. It leaves room for
future bindings without discarding the model.

### Override model

The committed profile should represent the repo-supported default local
environment. Overrides should come from env vars, not by editing the tracked
profile.

Initial override surface can be simple:

- `RB_BENCH_PG_HOST`
- `RB_BENCH_PG_PORT`
- `RB_BENCH_PG_USER`
- `RB_BENCH_PG_PASSWORD`
- `RB_BENCH_PG_DATABASE`
- `RB_BENCH_PG_SOURCE_SCHEMA`
- `RB_BENCH_PG_DEST_SCHEMA`

This is enough for the first slice. More granular or profile-scoped overrides
can be added later if needed.

## CLI and Runner Changes

### CLI

Add:

- `--env-profile <id-or-path>`

Behavior:

- synthetic PR benchmarks should continue to work with no environment profile
- real lab scenarios should fail with a clear error when they require an
  environment profile and none is provided

### Runner

The runner should:

1. load scenario
2. load and resolve environment profile
3. apply environment-variable overrides
4. merge the resolved environment into the scenario execution context
5. seed source, render pipeline, run benchmark, enforce assertions

The runner should not:

- start Docker
- call Testcontainers
- mutate the host environment beyond benchmark execution needs

## Wrapper Commands

Add a local wrapper in `Justfile` rather than a one-off shell script.

Recommended entrypoint:

```bash
just bench-lab scenario env="local-dev-postgres"
```

Behavior:

- `docker compose up -d --wait`
- `just build-all`
- invoke benchmark runner with `--suite lab --scenario ... --env-profile ...`

This keeps local orchestration explicit and easily discoverable.

## Error Handling

Clear failures matter here because the system is crossing scenario, environment,
and execution boundaries.

Required errors:

- missing env profile
- unknown environment reference
- unsupported provider kind
- incomplete resolved connection info
- scenario requires a real environment but none was supplied

These should fail early, before seeding or pipeline rendering.

## Testing Strategy

Add tests for:

- environment profile parsing
- environment override application
- scenario + profile merge resolution
- runner errors when real scenarios lack a profile
- wrapper docs/examples remain accurate

Existing synthetic PR benchmark tests must remain green and unchanged in
behavior.

## Migration Plan

1. Add environment profile model and parser
2. Update scenarios to reference profiles instead of hardcoded credentials
3. Update runner to resolve environments from profile + overrides
4. Add `just bench-lab`
5. Update docs

## Recommendation

Implement committed environment profiles plus a `just` wrapper now, keep the
runner execution-focused, and defer Testcontainers or broader provider support
until there is a concrete need.
