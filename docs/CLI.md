# RapidByte — Configuration & CLI Reference

> Design principle: make the 10-stream pipeline a 20-line file, make the
> 200-stream production deployment readable by a new hire in 15 minutes,
> and solve schema evolution better than anyone else.

---

## Table of Contents

1. [Quickstart](#1-quickstart)
2. [Project Structure](#2-project-structure)
3. [Configuration Files](#3-configuration-files)
   - [Pipeline files](#pipeline-files)
   - [connections.yml](#connectionsyml)
   - [rapidbyte.yml](#rapidbyteyml)
4. [Configuration Reference](#4-configuration-reference)
   - [Source block](#source-block)
   - [Destination block](#destination-block)
   - [Stream config](#stream-config)
   - [Transforms](#transforms)
   - [Assertions](#assertions)
   - [Schema evolution](#schema-evolution)
   - [Error handling](#error-handling)
   - [Scheduling](#scheduling)
   - [State](#state)
   - [Resources](#resources)
5. [Environment Variables](#5-environment-variables)
6. [CLI Reference](#6-cli-reference)
   - [Pipeline commands](#pipeline-commands)
   - [Controller commands](#server-commands)
   - [Operational commands](#operational-commands)
   - [Plugin management](#plugin-management)
   - [Project commands](#project-commands)
   - [Global flags](#global-flags)
   - [Routing](#routing)
7. [Deployment](#7-deployment)
   - [One-shot](#one-shot)
   - [Long-running server](#long-running-server)
   - [Progression](#progression)
8. [Examples](#8-examples)
   - [Minimal pipeline](#minimal-single-pipeline)
   - [Multi-stream production pipeline](#multi-stream-production-pipeline)
   - [Full production project](#full-production-project)
9. [Design Decisions Log](#9-design-decisions-log)

---

## 1. Quickstart

One file. One command.

```yaml
# pipeline.yml

version: "1.0"
pipeline: my-first-sync

source:
  use: postgres
  config:
    host: localhost
    port: 5432
    user: app
    password: ${PG_PASSWORD}
    database: mydb
  streams:
    - users
    - orders

destination:
  use: postgres
  config:
    host: warehouse.internal
    port: 5432
    user: loader
    password: ${DEST_PG_PASSWORD}
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]
```

```bash
PG_PASSWORD=secret DEST_PG_PASSWORD=secret rapidbyte sync
```

That's it. No project file, no connections file, no configuration directory.
When you outgrow a single file, read on.

---

## 2. Project Structure

### Single pipeline (getting started)

```
pipeline.yml              # that's it
```

### Multiple pipelines

```
my-project/
├── rapidbyte.yml         # project metadata + defaults
├── connections.yml       # named connections (shared across pipelines)
└── pipelines/
    ├── salesforce-crm.yml
    ├── postgres-product.yml
    └── stripe-payments.yml
```

### Large-scale (teams, many sources)

```
my-project/
├── rapidbyte.yml
├── connections.yml
└── pipelines/
    ├── crm/
    │   ├── salesforce-accounts.yml
    │   ├── salesforce-contacts.yml
    │   └── hubspot-companies.yml
    ├── payments/
    │   ├── stripe-invoices.yml
    │   └── stripe-subscriptions.yml
    └── product/
        ├── postgres-events.yml
        └── postgres-users.yml
```

RapidByte auto-discovers pipeline files recursively under the configured
pipeline path. Subdirectories are organizational only — they carry no
semantic meaning and do not affect defaults.

---

## 3. Configuration Files

### Pipeline Files

The core unit. One file per logical sync (source → destination).
A pipeline can sync one stream or many.

#### Anatomy of a pipeline file

```yaml
# pipelines/salesforce-crm.yml

version: "1.0"

pipeline: salesforce-crm
description: "Salesforce CRM objects → Snowflake raw layer"
tags: [crm, tier-1]
schedule: every 30m

# ── Source ─────────────────────────────────────────────────────────
source:
  use: salesforce                 # connector plugin name
  connection: salesforce_prod     # named connection from connections.yml
  streams:
    - accounts
    - contacts
    - opportunities
    - leads
    - name: events                # override defaults for this stream
      sync_mode: incremental
      cursor_field: created_at

# ── Destination ────────────────────────────────────────────────────
destination:
  use: snowflake
  connection: snowflake_raw
  schema: crm
  write_mode: upsert
  primary_key: [id]

# ── Schema evolution (overrides project default) ───────────────────
schema_evolution:
  new_column: add
  removed_column: soft_delete
  type_change: coerce
  alert: slack://data-alerts

# ── Assertions (overrides project default) ─────────────────────────
assert:
  freshness: { warn: 1h, error: 3h }
  not_null: [id]
  unique: [id]

# ── Error handling ─────────────────────────────────────────────────
on_error:
  data: dlq                       # dead letter queue for bad rows
  schema: alert                   # alert on schema-level errors
  max_consecutive_failures: 3     # pause pipeline after 3 failures in a row

# ── Optional metadata ─────────────────────────────────────────────
meta:
  owner: crm-team
  slack: "#crm-data"
  runbook: "https://wiki.acme.com/runbooks/salesforce-sync"
```

When starting out, skip `connection:` and use inline `config:` directly
(as shown in the [quickstart](#1-quickstart)). Move to named connections
when you have multiple pipelines sharing the same source or destination.

---

### `connections.yml`

Named, reusable connection definitions. Separates credentials from
pipeline logic. One file, all connections, all environments.

Environment switching happens at the env var level — not through a
`target` abstraction. This is how Kubernetes, Docker, CI, and every
deployment tool already works.

```yaml
# connections.yml

connections:

  # ── Sources ──────────────────────────────────────────────────────

  postgres_app:
    use: postgres
    config:
      host: ${PG_HOST}
      port: ${PG_PORT:-5432}
      user: ${PG_USER}
      password: ${PG_PASSWORD}
      database: ${PG_DATABASE:-production}
      ssl_mode: require
      pool_size: 5
      connect_timeout: 10s

  salesforce_prod:
    use: salesforce
    config:
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      instance_url: ${SF_INSTANCE_URL}
      api_version: "59.0"

  stripe_prod:
    use: stripe
    config:
      api_key: ${STRIPE_API_KEY}
      account_id: ${STRIPE_ACCOUNT_ID}

  s3_clickstream:
    use: s3
    config:
      bucket: ${S3_BUCKET}
      prefix: events/
      region: ${AWS_REGION:-eu-west-1}
      # Uses standard AWS credential chain (env vars, instance role, etc.)

  # ── Destinations ─────────────────────────────────────────────────

  snowflake_raw:
    use: snowflake
    config:
      account: ${SNOWFLAKE_ACCOUNT}
      user: ${SNOWFLAKE_USER}
      private_key_path: ${SNOWFLAKE_KEY_PATH}
      database: ${SNOWFLAKE_DB:-RAW}
      warehouse: ${SNOWFLAKE_WH:-LOADER_WH}
      role: ${SNOWFLAKE_ROLE:-LOADER_ROLE}

  bigquery_landing:
    use: bigquery
    config:
      project: ${GCP_PROJECT}
      dataset: ${BQ_DATASET:-landing}
      credentials_json: ${GCP_CREDENTIALS}
      location: EU

  # ── Internal ─────────────────────────────────────────────────────

  rapidbyte_meta:
    use: postgres
    config:
      host: ${META_PG_HOST:-localhost}
      port: 5432
      user: ${META_PG_USER:-rapidbyte}
      password: ${META_PG_PASSWORD}
      database: ${META_PG_DB:-rapidbyte}
```

---

### `rapidbyte.yml`

Project-level configuration and defaults. Optional — RapidByte runs
without it if you have a standalone `pipeline.yml`.

```yaml
# rapidbyte.yml

project: acme-data-platform
version: "1.0"

# ── Paths ──────────────────────────────────────────────────────────
# All relative to this file. These are the defaults:
paths:
  pipelines: pipelines/
  connections: connections.yml

# ── Project-wide defaults ──────────────────────────────────────────
# Every pipeline inherits these. Override per-pipeline as needed.
defaults:
  schedule: every 6h
  sync_mode: incremental
  write_mode: upsert
  compression: lz4
  checkpoint_interval: 128mb

  schema_evolution:
    new_column: add
    removed_column: soft_delete
    type_change: coerce

  assert:
    freshness: { warn: 6h, error: 24h }
    row_count: { min: 1 }

  on_error:
    data: dlq
    schema: alert

# ── State backend ──────────────────────────────────────────────────
# Where RapidByte persists sync cursors and run history.
state:
  backend: postgres               #  postgres
  connection: rapidbyte_meta      # named connection (for postgres)

# ── Observability ──────────────────────────────────────────────────
observability:
  metrics: prometheus             # prometheus | otlp | none
  metrics_port: 9090
  log_level: info                 # trace | debug | info | warn | error
  log_format: json                # json | text
```

---

## 4. Configuration Reference

### Source block

```yaml
source:
  use: <connector>                # required — connector plugin name
  connection: <name>              # named connection from connections.yml
  config:                         # OR inline config (for standalone files)
    host: localhost
    # ...

  # ── Stream selection ─────────────────────────────────────────────
  streams:
    # Simple: just list table names (takes all defaults)
    - users
    - orders
    - products

    # Override: use object form for per-stream config
    - name: events
      sync_mode: incremental
      cursor_field: created_at
      primary_key: [id]
      columns:
        include: [id, type, payload, created_at]
        # OR
        exclude: [internal_id, debug_data]
      rename:
        userId: user_id
        eventType: event_type
      transform:
        email: lowercase
        payload: { cast: jsonb }
      assert:
        not_null: [id]
        unique: [id]
      batch_size: 50000

  # ── Alternative: wildcard with exclusions ────────────────────────
  streams: "*"                    # all tables from source
  exclude_streams:
    - _migrations
    - django_*                    # glob patterns
    - pg_*

  # ── Sandbox permissions (narrows plugin-declared permissions) ────
  permissions:
    network:
      allowed_hosts: ["db.internal:5432"]
    env:
      allowed_vars: ["PG_HOST", "PG_PORT"]
    fs:
      allowed_preopens: ["/tmp/staging"]

  # ── Resource limits (narrows plugin-declared limits) ─────────────
  limits:
    max_memory: 256mb
    timeout_seconds: 300
```

### Destination block

```yaml
destination:
  use: <connector>                # required
  connection: <name>              # named connection
  schema: <schema>                # destination schema

  # ── Write behavior ───────────────────────────────────────────────
  write_mode: upsert              # append | upsert | replace | merge
  primary_key: [id]               # required for upsert/merge
  merge_key: [id]                 # optional, defaults to primary_key
  dedup: last_write_wins          # last_write_wins | first_write_wins | none

  # ── Table naming ─────────────────────────────────────────────────
  table_prefix: ""                # prepend to all table names
  table_suffix: ""                # append to all table names
  table_map:                      # explicit rename (source → destination)
    Account: accounts
    Contact: contacts

  # ── Destination-specific ─────────────────────────────────────────
  # Snowflake
  warehouse: LOADER_WH
  # BigQuery
  partition_field: _synced_at
  clustering_fields: [region, type]

  # ── Sandbox permissions & limits (same as source) ────────────────
  permissions:
    network:
      allowed_hosts: ["warehouse.internal:443"]
  limits:
    max_memory: 512mb
    timeout_seconds: 600
```

### Stream config

Full reference for per-stream configuration:

```yaml
- name: orders                    # required — source table/object name
  sync_mode: incremental          # full_refresh | incremental | cdc
  cursor_field: updated_at        # required for incremental
  primary_key: [id]               # required for cdc / upsert

  # ── Column selection (pick one) ──────────────────────────────────
  columns:
    include: [id, amount, status, created_at]
    # OR
    exclude: [internal_notes, debug_json]

  # ── Column renaming ──────────────────────────────────────────────
  rename:
    sourceColumnName: destination_column_name
    Id: id
    CreatedDate: created_at

  # ── Lightweight transforms ───────────────────────────────────────
  transform:
    email: lowercase
    phone: nullif_empty
    name: trim
    billing_country: { coalesce: "UNKNOWN" }
    created_at: { cast: timestamp_tz }
    ssn: { mask: "***-**-{last4}" }
    password_hash: { hash: sha256 }
    ip_address: { replace: { pattern: "\\d+$", with: "0" } }

  # ── Assertions ───────────────────────────────────────────────────
  assert:
    not_null: [id, amount]
    unique: [id]
    freshness: { warn: 30m, error: 2h }
    row_count: { min: 1, max_delta_pct: 50 }
    custom_sql: |
      SELECT count(*) as failures
      FROM ${stream_table}
      WHERE amount < 0 AND status = 'paid'

  # ── Per-stream overrides ─────────────────────────────────────────
  write_mode: append              # override pipeline-level write_mode
  destination_table: raw_orders   # explicit destination table name
  batch_size: 100000              # rows per batch
  partition_key: region           # partitioning hint
  schedule: every 5m              # override pipeline-level schedule

  # ── Filtering (pushed down to source when connector supports it) ─
  filter:
    created_at: { gte: "2024-01-01" }
    status: { in: [active, pending] }
    is_deleted: false
```

### Transforms

Transforms are a **closed set of operations** — ingestion hygiene, not
arbitrary SQL. If you need `JOIN`, `GROUP BY`, `CASE WHEN`, use dbt.

```yaml
transform:
  # ── String operations ────────────────────────────────────────────
  email: lowercase                         # → lower(email)
  name: uppercase                          # → upper(name)
  title: trim                              # → trim(title)
  code: { truncate: 10 }                   # → left(code, 10)
  slug: { replace: { pattern: " ", with: "-" } }

  # ── Null handling ────────────────────────────────────────────────
  country: { coalesce: "UNKNOWN" }         # → coalesce(country, 'UNKNOWN')
  notes: nullif_empty                      # → nullif(trim(notes), '')

  # ── Type casting ─────────────────────────────────────────────────
  amount: { cast: decimal(10,2) }
  created_at: { cast: timestamp_tz }
  is_active: { cast: boolean }
  metadata: { cast: jsonb }

  # ── Privacy / PII ───────────────────────────────────────────────
  ssn: { mask: "***-**-{last4}" }          # partial masking
  email: { mask: "{first2}***@{domain}" }  # user-visible masking
  password: { hash: sha256 }               # one-way hash
  ip: { hash: sha256 }                     # pseudonymize
  name: redact                             # → NULL

  # ── Computed ─────────────────────────────────────────────────────
  full_name: { concat: [first_name, " ", last_name] }
  year: { extract: { from: created_at, part: year } }
```

### Assertions

Data quality checks applied as stream properties. Assertions inherit
through the same defaults cascade as everything else:
project defaults → pipeline-level → stream-level.

```yaml
assert:
  # ── Freshness ────────────────────────────────────────────────────
  freshness:
    warn: 1h                      # warning threshold
    error: 3h                     # failure threshold

  # ── Row-level checks ─────────────────────────────────────────────
  not_null: [id, email]           # these columns must never be NULL
  unique: [id]                    # these columns must be unique
  accepted_values:
    status: [active, pending, cancelled]

  # ── Volume checks ────────────────────────────────────────────────
  row_count:
    min: 1                        # at least 1 row per sync
    max_delta_pct: 50             # alert if row count changes > 50%

  # ── PII / compliance ─────────────────────────────────────────────
  columns_masked: [email, phone, ssn]
  column_name_denylist:
    patterns: ["*password*", "*secret*", "*token*"]
    severity: error

  # ── Custom SQL (escape hatch) ────────────────────────────────────
  custom_sql: |
    SELECT count(*) as failures
    FROM ${stream_table}
    WHERE amount < 0 AND status = 'paid'
    -- returns 0 = pass, >0 = fail
```

Assertions can be defined at three levels:

```yaml
# 1. Project defaults (rapidbyte.yml) — applies to every stream
defaults:
  assert:
    freshness: { warn: 6h, error: 24h }
    row_count: { min: 1 }

# 2. Pipeline-level — applies to all streams in this pipeline
assert:
  not_null: [id]
  unique: [id]

# 3. Stream-level — overrides for a specific stream
streams:
  - name: events
    assert:
      freshness: { warn: 30m, error: 2h }  # tighter than pipeline default
```

### Schema evolution

```yaml
schema_evolution:
  # ── Column additions ─────────────────────────────────────────────
  new_column: add                 # add | ignore | fail
                                  # add:    ALTER TABLE ADD COLUMN
                                  # ignore: silently drop new columns
                                  # fail:   stop pipeline, alert

  # ── Column removals ─────────────────────────────────────────────
  removed_column: soft_delete     # soft_delete | keep | drop | fail
                                  # soft_delete: rename to _deleted_<col>_<date>
                                  # keep:        leave column, fill NULLs
                                  # drop:        ALTER TABLE DROP COLUMN
                                  # fail:        stop pipeline, alert

  # ── Type changes ─────────────────────────────────────────────────
  type_change: coerce             # coerce | widen | fail
                                  # coerce: best-effort cast
                                  # widen:  only allow widening (int→bigint)
                                  # fail:   stop pipeline, alert

  # ── Nullability changes ──────────────────────────────────────────
  nullability_change: allow       # allow | fail
                                  # allow: permit NOT NULL → NULL or vice versa
                                  # fail:  stop pipeline, alert

  # ── Alerting ─────────────────────────────────────────────────────
  alert: slack://data-alerts      # where to send schema change alerts
                                  # slack://<channel>
                                  # webhook://<url>
                                  # email://<address>
                                  # none
```

### Error handling

```yaml
on_error:
  # ── Row-level errors (bad data, type mismatches, etc.) ───────────
  data: dlq                       # dlq | skip | fail
                                  # dlq:  dead letter queue (row + error)
                                  # skip: silently drop bad rows
                                  # fail: stop pipeline on first bad row

  dlq:
    destination: s3               # where DLQ rows go (default: local file)
    connection: s3_dlq
    prefix: "dlq/${pipeline}/${stream}/"
    max_rows: 10000               # fail if DLQ exceeds this per run
    retention: 30d

  # ── Schema-level errors (unexpected schema changes) ──────────────
  schema: alert                   # alert | fail | ignore

  # ── Connection errors ────────────────────────────────────────────
  retry:
    max_attempts: 5
    backoff: exponential          # exponential | linear | fixed
    initial_delay: 1s
    max_delay: 5m

  # ── Pipeline-level circuit breaker ───────────────────────────────
  max_consecutive_failures: 3     # pause pipeline after N failures
  alert: slack://data-alerts
```

### Scheduling

```yaml
# ── Human-readable (preferred for common cases) ───────────────────
schedule: every 5m
schedule: every 30m
schedule: every 2h
schedule: every 6h
schedule: hourly
schedule: daily                   # midnight UTC
schedule: daily at 06:00          # specific time (UTC)
schedule: daily at 06:00 Europe/Warsaw  # with timezone

# ── Cron (escape hatch for complex schedules) ─────────────────────
schedule: cron(*/15 9-17 * * MON-FRI)   # every 15m during business hours
schedule: cron(0 2 * * SUN)             # 2am every Sunday

# ── Not scheduled (external orchestrator or manual) ────────────────
# Simply omit the schedule field.
# Pipeline runs only when explicitly triggered:
#   rapidbyte sync --pipeline my-pipeline
```

### State

```yaml
state:
  # ── Postgres (recommended for production / multi-node) ───────────
  backend: postgres
  connection: rapidbyte_meta      # named connection

  # ── What's stored ────────────────────────────────────────────────
  # • Sync cursors (last cursor_field value per stream)
  # • Run history (start, end, rows synced, errors, duration)
  # • DLQ metadata
  # • Schema snapshots (for evolution tracking)
  # • Checkpoint offsets (for resumable syncs)
```

### Resources

```yaml
resources:
  # ── Memory / batching ────────────────────────────────────────────
  compression: lz4                # lz4 | zstd | none
  checkpoint_interval: 128mb      # flush to destination every N bytes
  batch_size: 50000               # default rows per batch (per stream)
  max_memory: 512mb               # memory ceiling for the pipeline

  # ── Parallelism ──────────────────────────────────────────────────
  parallel_streams: 4             # concurrent streams within a pipeline
  parallel_batches: 2             # concurrent batches per stream

  # ── Network ──────────────────────────────────────────────────────
  request_timeout: 30s
  request_concurrency: 10         # max concurrent API requests (API sources)
```

---

## 5. Environment Variables

```yaml
# ── Syntax ─────────────────────────────────────────────────────────
password: ${DB_PASSWORD}                  # required — fails if not set
database: ${DB_NAME:-production}          # with default value
warehouse: ${SNOWFLAKE_WH:-LOADER_WH}    # with default value

# ── Supported everywhere ───────────────────────────────────────────
# Environment variables are resolved in:
#   • connections.yml
#   • rapidbyte.yml
#   • pipeline files
#   • CLI arguments via --env KEY=VALUE
```

```bash
# Pass env vars at runtime
rapidbyte sync --env SNOWFLAKE_DB=DEV_RAW --env PG_HOST=dev-db.internal

# Or use .env file
rapidbyte sync --env-file .env.dev

# .env.dev
SNOWFLAKE_DB=DEV_RAW
SNOWFLAKE_WH=DEV_WH
PG_HOST=dev-db.internal
PG_USER=dev_loader
```

No `profiles.yml` targets. No `target: dev` abstraction. The environment
IS the target. Same pipeline files, different env vars. This is how
every container, CI system, and Kubernetes deployment already works.

---

## 6. CLI Reference

### Pipeline commands

```bash
# ── sync (implemented) ────────────────────────────────────────────
# Run pipelines. The main command. (Renamed from `run`.)
#
# NOTE: The current CLI takes a positional pipeline path
# (e.g. `rapidbyte sync pipeline.yml`). The --pipeline flag shown
# below is the planned design for named pipeline selection.

rapidbyte sync                              # run all pipelines
rapidbyte sync pipeline.yml                 # run one file directly
rapidbyte sync --pipeline salesforce-crm    # run by name
rapidbyte sync --pipeline salesforce-crm --stream accounts  # one stream
rapidbyte sync --tag tier-1                 # all pipelines with tag
rapidbyte sync --tag tier-1 --tag crm       # intersection (AND)
rapidbyte sync --tag tier-1 --exclude stripe-payments  # subtract
rapidbyte sync --dir pipelines/crm/         # all pipelines in directory

# Mode overrides
rapidbyte sync --pipeline postgres-product --full-refresh
rapidbyte sync --pipeline postgres-product --stream users --full-refresh

# Backfill
rapidbyte sync --pipeline postgres-product \
  --stream events \
  --cursor-start "2024-01-01T00:00:00Z" \
  --cursor-end "2024-06-30T23:59:59Z"

# Dry run
rapidbyte sync --dry-run                    # resolve config, show plan, don't execute
rapidbyte sync --pipeline salesforce-crm --dry-run

# ── check (implemented) ──────────────────────────────────────────
rapidbyte check                             # validate all pipeline configs
rapidbyte check --pipeline salesforce-crm   # validate one
rapidbyte check --apply                     # provision resources after validation

# ── discover (deferred — design divergence to resolve: connection-based vs pipeline-based) ──
rapidbyte discover --connection postgres_app
rapidbyte discover --connection postgres_app --table users
# OUTPUT:
#   TABLE: public.users (est. 1,204,301 rows)
#   COLUMN           TYPE              NULLABLE   DEFAULT
#   id               bigint            NO         nextval(...)
#   email            varchar(255)      NO
#   ...
#   SUGGESTED CONFIG:
#     sync_mode: cdc
#     primary_key: [id]
#     cursor_field: updated_at

# ── compile (deferred — YAGNI, security concern with exposing resolved secrets) ──
# Resolve all env vars and defaults. Show final effective config.
rapidbyte compile --pipeline salesforce-crm

# ── diff (deferred — needs engine schema comparison support) ──────
# Show what changed since last sync (schema drift detection).
rapidbyte diff --pipeline postgres-product
# OUTPUT:
#   STREAM: users
#     + new column: phone_verified (boolean, nullable)
#     ~ type change: zip_code varchar(5) → varchar(10)
#   STREAM: orders
#     (no changes)

# ── assert (deferred — needs engine assertion execution) ──────────
# Run data quality assertions.
rapidbyte assert                            # run all assertions
rapidbyte assert --pipeline salesforce-crm  # one pipeline
rapidbyte assert --tag tier-1               # by tag

# ── teardown (implemented) ───────────────────────────────────────
# Tear down resources provisioned by a pipeline (tables, replication slots, etc.)
rapidbyte teardown --pipeline salesforce-crm
rapidbyte teardown --pipeline salesforce-crm --reason pipeline_deleted
```

### Server commands

```bash
# ── controller (long-running daemon) ──────────────────────────────
# Starts the gRPC + REST server. Mode depends on configuration:
#   - No --metadata-database-url: standalone (embedded engine, local execution)
#   - With --metadata-database-url: distributed coordinator (agents execute)

rapidbyte controller                             # standalone (embedded engine)
rapidbyte controller --port 8080                 # custom listen port
rapidbyte controller --metrics-listen 0.0.0.0:9090  # Prometheus metrics

# Distributed mode (coordinator)
rapidbyte controller --metadata-database-url postgres://...
rapidbyte controller --listen [::]:9090 --metadata-database-url postgres://...

# ── agent (distributed worker) ────────────────────────────────────
# Start a worker that connects to a controller instance via gRPC.

rapidbyte agent --controller http://coordinator:9090
rapidbyte agent --controller http://coordinator:9090 --max-tasks 4
rapidbyte agent --metrics-listen 0.0.0.0:9191
```

### Operational commands

These commands query or control a running system (`controller`).

```bash
# ── status (implemented) ─────────────────────────────────────────
# NOTE: The current CLI takes a positional <run_id> argument.
# The --pipeline flag shown below is the planned design.

rapidbyte status <run_id>
# Pipeline: salesforce-crm
# Schedule: every 30m
# Last 5 runs:
#   2025-03-19 14:30  ✓  4 streams  12,847 rows  23s
#   2025-03-19 14:00  ✓  4 streams  11,203 rows  21s
#   2025-03-19 13:30  ✗  failed     connection timeout (attempt 3/3)
#   2025-03-19 13:00  ✓  4 streams  13,102 rows  25s
#   2025-03-19 12:30  ✓  4 streams  12,558 rows  22s
# Streams:
#   accounts       incremental  cursor: 2025-03-19T14:28:00Z  4,201 rows
#   contacts       incremental  cursor: 2025-03-19T14:29:12Z  5,892 rows
#   opportunities  incremental  cursor: 2025-03-19T14:27:45Z  2,154 rows
#   events         incremental  cursor: 2025-03-19T14:30:01Z    600 rows
# DLQ: 0 rows

# ── watch (implemented) ──────────────────────────────────────────
rapidbyte watch <run_id>                    # live-follow a running sync

# ── list-runs (implemented) ──────────────────────────────────────
rapidbyte list-runs                         # recent runs
rapidbyte list-runs --limit 20             # last 20 runs
rapidbyte list-runs --state failed         # filter by state
# PIPELINE             SCHEDULE    LAST RUN     NEXT RUN    STATUS
# salesforce-crm       every 30m   12m ago      18m         ✓ healthy
# postgres-product     every 15m   3m ago       12m         ✓ healthy
# stripe-payments      every 6h    2h ago       4h          ⚠ 2 DLQ rows
# s3-clickstream       every 1h    7h ago       —           ✗ paused

# ── Retry / recovery ─────────────────────────────────────────────
rapidbyte sync --failed                     # retry all failed from last run
rapidbyte sync --stale                      # run anything past freshness SLA

# ── pause / resume ───────────────────────────────────────────────
rapidbyte pause --pipeline salesforce-crm   # pause scheduled runs
rapidbyte resume --pipeline salesforce-crm  # resume

# ── reset ─────────────────────────────────────────────────────────
rapidbyte reset --pipeline salesforce-crm   # clear state (next run = full refresh)
rapidbyte reset --pipeline salesforce-crm --stream events  # per-stream

# ── freshness ─────────────────────────────────────────────────────
rapidbyte freshness                         # check all pipelines
rapidbyte freshness --tag tier-1
# OUTPUT:
#   salesforce-crm      last sync: 12m ago   SLA: 1h    ✓ fresh
#   postgres-product    last sync: 8h ago    SLA: 6h    ✗ stale
#   stripe-payments     last sync: 45m ago   SLA: 1h    ✓ fresh

# ── logs ──────────────────────────────────────────────────────────
rapidbyte logs --pipeline salesforce-crm    # recent logs
rapidbyte logs --pipeline salesforce-crm --run-id latest
rapidbyte logs --pipeline salesforce-crm --limit 50
rapidbyte logs --follow                     # tail all pipeline logs
```

### Plugin management

```bash
# ── Connector plugins (WASM, distributed via OCI) ─────────────────

rapidbyte plugin pull <ref>                 # pull from OCI registry
rapidbyte plugin push <ref> <wasm_path>     # push to OCI registry
rapidbyte plugin inspect <ref>              # inspect metadata
rapidbyte plugin tags <ref>                 # list available tags
rapidbyte plugin list                       # list locally cached
rapidbyte plugin remove <ref>              # remove from cache
rapidbyte plugin search [query]             # search registry
rapidbyte plugin keygen                     # generate signing keypair
```

### Project commands

```bash
# ── dev (implemented) ─────────────────────────────────────────────
rapidbyte dev                               # interactive dev shell (REPL)

# ── scaffold (implemented) ───────────────────────────────────────
rapidbyte plugin scaffold <name>            # scaffold a new plugin project

# ── init (deferred — low priority, one-time use) ─────────────────
rapidbyte init                              # scaffold new project
rapidbyte init --template minimal           # just pipeline.yml
rapidbyte init --template standard          # full project structure

# ── version ────────────────────────────────────────────────────────
rapidbyte version
# rapidbyte 0.1.0 (rustc 1.82.0, wasmtime 41.0.0)
# plugins: postgres@1.2.3, salesforce@1.5.1, snowflake@2.0.0

# ── login / logout ────────────────────────────────────────────────
rapidbyte login --controller http://localhost:8080              # store auth token
rapidbyte login --controller http://localhost:8080 --token rb_tok_abc123
rapidbyte logout                                                # remove stored token
rapidbyte logout --controller http://localhost:8080             # remove token for specific controller
```

### Global flags

```bash
rapidbyte [command] \
  --controller <url> \                    # controller endpoint (RAPIDBYTE_CONTROLLER)
  --auth-token <token> \                  # bearer token (RAPIDBYTE_AUTH_TOKEN)
  --tls-ca-cert <path> \                  # TLS CA cert (RAPIDBYTE_TLS_CA_CERT)
  --tls-domain <name> \                   # TLS domain override (RAPIDBYTE_TLS_DOMAIN)
  --log-level info \                      # trace | debug | info | warn | error
  -v                                      # verbose (-vv for diagnostic)
  -q                                      # quiet (exit code only)
  --registry-url <url> \                  # OCI registry (RAPIDBYTE_REGISTRY_URL)
  --registry-insecure \                   # HTTP instead of HTTPS
  --trust-policy skip \                   # plugin trust: skip | warn | verify
  --trust-key <path> \                    # Ed25519 public key for verification
  --vault-addr <url> \                    # Vault address (VAULT_ADDR)
  --vault-token <token> \                 # Vault token (VAULT_TOKEN)
  --vault-role-id <id> \                  # Vault AppRole role ID (VAULT_ROLE_ID)
  --vault-secret-id <id>                  # Vault AppRole secret ID (VAULT_SECRET_ID)
```

### Routing

`rapidbyte sync` always executes through a controller. Without
`--controller`, an embedded controller runs in-process. With
`--controller <url>`, it connects to a running `rapidbyte controller`
instance. The execution path is identical either way.

Controller endpoint resolution order:

1. `--controller <url>` flag
2. `RAPIDBYTE_CONTROLLER` environment variable
3. `controller.url` in `~/.rapidbyte/config.yaml`
4. No controller configured → embedded (in-process)

```
                                  ┌─ embedded controller (in-process)
rapidbyte sync ──→ controller ──┤
                                  └─ remote controller (--controller <url>)
```

Same command, same output, different execution backend. Graduating
from local to distributed is just adding `--controller <url>`.

---

## 7. Deployment

Same pipelines, same config, same CLI — regardless of where the
controller runs. `rapidbyte sync` always goes through a controller;
the only question is whether it's embedded or remote.

### One-shot

Embedded controller, in-process. For development, CI/CD, and
external orchestrators (Airflow, Dagster).

```bash
rapidbyte sync pipeline.yml                 # embedded controller, run and exit
rapidbyte sync --pipeline salesforce-crm    # run by name
```

Works everywhere: laptop, CI runner, Docker, cron job.

### Long-running server

`rapidbyte controller` starts a persistent gRPC + REST server with
built-in scheduler, metrics endpoint. Pipelines can execute locally
(embedded engine) or be delegated to agents.

```bash
# Single node — embedded engine, no agents needed
rapidbyte controller --metrics-listen 0.0.0.0:9090

# Multi-node — add agents for distributed execution
rapidbyte controller --metadata-database-url postgres://... \
  --listen [::]:9090

rapidbyte agent --controller http://coordinator:9090 --max-tasks 4
```

The CLI connects to the running server — same commands either way:

```bash
rapidbyte sync pipeline.yml --controller http://localhost:9090
rapidbyte status <run_id> --controller http://localhost:9090
```

### Progression

```
rapidbyte sync              →  rapidbyte controller        →  controller + agents
(embedded controller)          (persistent controller)         (distributed)
```

Same pipeline files at every stage. No config migration.

---

## 8. Examples

### Minimal single pipeline

The absolute smallest useful config. No project file, no connections file.
One file, runs with `rapidbyte sync`.

```yaml
# pipeline.yml — that's the whole thing

version: "1.0"
pipeline: my-first-sync

source:
  use: postgres
  config:
    host: localhost
    port: 5432
    user: app
    password: ${PG_PASSWORD}
    database: mydb
  streams:
    - users
    - orders

destination:
  use: postgres
  config:
    host: warehouse.internal
    port: 5432
    user: loader
    password: ${DEST_PG_PASSWORD}
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]
```

```bash
PG_PASSWORD=secret DEST_PG_PASSWORD=secret rapidbyte sync
```

---

### Multi-stream production pipeline

Real-world Salesforce → Snowflake with per-stream overrides, transforms,
and assertions.

```yaml
# pipelines/salesforce-crm.yml

version: "1.0"

pipeline: salesforce-crm
description: "Salesforce CRM → Snowflake raw.crm"
tags: [crm, tier-1, pii]
schedule: every 30m

source:
  use: salesforce
  connection: salesforce_prod
  streams:
    # Most streams: just list them. Zero config, take all defaults.
    - leads
    - campaigns
    - campaign_members
    - tasks

    # Accounts: rename some ugly Salesforce field names
    - name: accounts
      rename:
        Id: account_id
        Name: account_name
        BillingCountry: billing_country
        AnnualRevenue: annual_revenue
      transform:
        billing_country: { coalesce: "UNKNOWN" }
        account_name: trim

    # Contacts: exclude PII we don't need, mask what we keep
    - name: contacts
      columns:
        exclude: [MailingStreet, Phone, Fax, HomePhone]
      rename:
        Id: contact_id
        Email: email
        AccountId: account_id
      transform:
        email: { mask: "{first2}***@{domain}" }
      assert:
        columns_masked: [email]

    # Events: high volume, needs tuning
    - name: events
      sync_mode: incremental
      cursor_field: CreatedDate
      batch_size: 100000
      assert:
        freshness: { warn: 15m, error: 1h }

destination:
  use: snowflake
  connection: snowflake_raw
  schema: crm
  write_mode: upsert
  primary_key: [id]

schema_evolution:
  new_column: add
  type_change: coerce
  alert: slack://crm-data

assert:
  freshness: { warn: 1h, error: 3h }
  not_null: [id]
  unique: [id]

on_error:
  data: dlq
  max_consecutive_failures: 3

meta:
  owner: crm-team
  slack: "#crm-data"
```

---

### Full production project

```
acme-data-platform/
├── rapidbyte.yml
├── connections.yml
├── pipelines/
│   ├── salesforce-crm.yml        # (as above)
│   ├── postgres-product.yml
│   ├── stripe-payments.yml
│   └── s3-clickstream.yml
```

```yaml
# rapidbyte.yml

project: acme-data-platform
version: "1.0"

defaults:
  schedule: every 6h
  sync_mode: incremental
  write_mode: upsert
  compression: lz4
  checkpoint_interval: 128mb
  schema_evolution:
    new_column: add
    removed_column: soft_delete
    type_change: coerce
  assert:
    freshness: { warn: 6h, error: 24h }
    row_count: { min: 1 }
  on_error:
    data: dlq
    retry:
      max_attempts: 3
      backoff: exponential

state:
  backend: postgres
  connection: rapidbyte_meta

observability:
  metrics: prometheus
  metrics_port: 9090
  log_level: info
  log_format: json
```

```yaml
# connections.yml

connections:
  salesforce_prod:
    use: salesforce
    config:
      client_id: ${SF_CLIENT_ID}
      client_secret: ${SF_CLIENT_SECRET}
      instance_url: ${SF_INSTANCE_URL}

  postgres_app:
    use: postgres
    config:
      host: ${PG_HOST}
      port: 5432
      user: ${PG_USER}
      password: ${PG_PASSWORD}
      database: production
      ssl_mode: require

  stripe_prod:
    use: stripe
    config:
      api_key: ${STRIPE_API_KEY}

  s3_clickstream:
    use: s3
    config:
      bucket: ${CLICKSTREAM_BUCKET}
      prefix: events/
      region: eu-west-1

  snowflake_raw:
    use: snowflake
    config:
      account: ${SNOWFLAKE_ACCOUNT}
      user: ${SNOWFLAKE_USER}
      private_key_path: ${SNOWFLAKE_KEY_PATH}
      database: ${SNOWFLAKE_DB:-RAW}
      warehouse: ${SNOWFLAKE_WH:-LOADER_WH}
      role: ${SNOWFLAKE_ROLE:-LOADER_ROLE}

  rapidbyte_meta:
    use: postgres
    config:
      host: ${META_PG_HOST}
      port: 5432
      user: rapidbyte
      password: ${META_PG_PASSWORD}
      database: rapidbyte
```

```bash
# Deploy: same files, different env vars
# Dev
rapidbyte sync --env-file .env.dev

# Production (Kubernetes)
# Env vars injected via Secret/ConfigMap — zero config change.
rapidbyte controller
```

---

## 9. Design Decisions Log

| Decision | Choice | Rationale |
|---|---|---|
| Project structure | Flat by default, split when you outgrow it | Don't force 6 directories for 3 pipelines |
| Environment switching | Env vars, not targets/profiles | Works with K8s, Docker, CI natively |
| Scheduling | `every 30m` syntax, `cron()` escape hatch | Human-readable first, power when needed |
| Transforms | Closed set of operations, not SQL | Keep boundary clean — dbt does transforms |
| Assertions | Stream properties, not separate contracts | Same inheritance as all other config. One mental model |
| Pipeline routing | Always through controller (embedded or remote) | One execution path, zero surprises scaling up |
| Deployment modes | sync (embedded) → controller → controller + agents | Same config at every stage, progressive complexity |
| REST API | Lives in controller; same surface everywhere | CLI doesn't care where pipelines execute |
| Schema evolution | First-class declarative block | THE differentiator vs Airbyte/Fivetran |
| Config inheritance | Project defaults → pipeline → stream overrides | Three levels max. No directory-level cascading |
| Connection management | Named refs in connections.yml | Solves copy-paste, env vars solve environments |
| Templating | `${ENV_VAR}` / `${ENV_VAR:-default}` only | No Jinja. Complexity magnet for marginal benefit |
| Stream listing | String list for simple, object for overrides | 90% of streams need zero config |
