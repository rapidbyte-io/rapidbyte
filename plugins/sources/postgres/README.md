# PostgreSQL Source Connector

The `source-postgres` package reads data from PostgreSQL tables using three modes:

- `FullRefresh` for snapshot reads.
- `Incremental` for cursor-based reads.
- `Cdc` for logical-replication reads through `pgoutput`.

The package discovers tables from one schema at a time, emits schema-qualified stream names for non-`public` schemas, and includes publication metadata when a publication filter is configured.

## Supported Behavior

- Discovery scans `information_schema.tables` and `information_schema.columns` for `BASE TABLE` entries.
- Primary keys are attached to discovered stream schemas when present.
- Generated columns are marked in discovery output and are skipped when suggesting a default cursor field.
- CDC reads use a logical replication slot and a PostgreSQL publication.
- CDC preflight checks cover slot type, slot/database affinity, active-slot conflicts, and publication membership before consuming WAL.

## Prerequisites

- PostgreSQL 10 or newer.
- Network access from Rapidbyte to the database host and port.
- A database user that can connect and read the target schema.
- CDC mode additionally requires logical replication support, a logical replication slot, and a publication that includes the target table.

The source `prerequisites()` lifecycle currently reports database version support and a deferred CDC-readiness note. CDC-specific checks are enforced when a stream enters the read path.

## Configuration

```yaml
source:
  type: source-postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: app_db
  schema: public
  replication_slot: rapidbyte_orders
  publication: rapidbyte_orders
```

`schema` defaults to `public`. If `replication_slot` or `publication` are omitted, the connector derives `rapidbyte_{stream_name}` at runtime.

## Example Usage

Use one stream per table. For non-`public` schemas, stream names are schema-qualified, for example `analytics.events`.

## Local Testing

Run the full package test suite:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml
```

Targeted checks are useful when changing a specific lifecycle:

```bash
cargo test --manifest-path plugins/sources/postgres/Cargo.toml prerequisites
cargo test --manifest-path plugins/sources/postgres/Cargo.toml discovery
cargo test --manifest-path plugins/sources/postgres/Cargo.toml cdc
```

## Operational Caveats

- CDC is destructive at the WAL layer: `pg_logical_slot_get_binary_changes` advances the slot and requires successful checkpointing to avoid data loss.
- The connector only discovers one schema per configuration.
- Publication filtering is applied during discovery and CDC preflight, so tables not in the selected publication are excluded or rejected early.
- Default CDC slot and publication names are derived from the stream name; override them only when you need to integrate with an existing PostgreSQL setup.

