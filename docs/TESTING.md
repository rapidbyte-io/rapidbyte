# Testing

## Manual Distributed Run

This is the quickest way to test the distributed controller/agent path manually on
one machine.

### Preconditions

- Build the CLI and plugins first:

```bash
just build-all
```

- Use a pipeline that is valid for distributed execution.
  - The pipeline must use a shared state backend such as Postgres.
  - Do not use SQLite state for distributed runs.

- Pick a real signing key and auth token for local testing:

```bash
export RAPIDBYTE_PLUGIN_DIR="$PWD/target/plugins"
export RAPIDBYTE_AUTH_TOKEN="rapidbyte-local-test-token"
export RAPIDBYTE_SIGNING_KEY="rapidbyte-local-test-signing-key"
```

### 1. Start the controller

In terminal 1:

```bash
./target/release/rapidbyte -vv \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  controller \
  --listen 127.0.0.1:56090 \
  --signing-key "$RAPIDBYTE_SIGNING_KEY"
```

Notes:
- `-v` enables normal server logs.
- `-vv` enables debug logs.

### 2. Start the agent

In terminal 2:

```bash
./target/release/rapidbyte -vv \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  agent \
  --controller http://127.0.0.1:56090 \
  --flight-listen 127.0.0.1:56091 \
  --flight-advertise 127.0.0.1:56091 \
  --max-tasks 1 \
  --signing-key "$RAPIDBYTE_SIGNING_KEY"
```

The agent should register with the controller and start long-polling for work.

### 3. Submit a distributed run

In terminal 3:

```bash
./target/release/rapidbyte \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  --controller http://127.0.0.1:56090 \
  run path/to/pipeline.yaml
```

If the pipeline completes normally, the CLI will stream progress and exit on the
terminal event.

### 4. Inspect the run manually

If you want to inspect runs outside the initial `run` command:

List recent runs:

```bash
./target/release/rapidbyte \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  --controller http://127.0.0.1:56090 \
  list-runs
```

Watch a specific run:

```bash
./target/release/rapidbyte \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  --controller http://127.0.0.1:56090 \
  watch <run-id>
```

Fetch a single run status:

```bash
./target/release/rapidbyte \
  --auth-token "$RAPIDBYTE_AUTH_TOKEN" \
  --controller http://127.0.0.1:56090 \
  status <run-id>
```

### Common local issues

- `controller requires --auth-token ... or --allow-unauthenticated`
  - Set `RAPIDBYTE_AUTH_TOKEN` or pass `--auth-token`.

- `controller requires --signing-key ... or --allow-insecure-signing-key`
  - Set `RAPIDBYTE_SIGNING_KEY` or pass `--signing-key`.

- Run is rejected as not valid for distributed mode
  - Check that the pipeline uses a shared state backend, not SQLite.
