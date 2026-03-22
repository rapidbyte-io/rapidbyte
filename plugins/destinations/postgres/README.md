# PostgreSQL Destination Connector

The `dest-postgres` package writes Arrow batches into PostgreSQL tables. It supports two load methods:

- `copy` for bulk ingest.
- `insert` for row-oriented writes.

The package keeps lifecycle responsibilities separated:

- `prerequisites()` checks connectivity and destination schema capability.
- `validate()` checks upstream schema compatibility.
- `apply()` prepares schema and table structure.
- `write()` runs the streaming write path and reuses prepared contracts when safe, with an explicit fallback through `prepare_stream_contract()` when the durable handoff is missing or stale.

## Supported Behavior

- Writes land in a single target schema, defaulting to `public`.
- `Replace` write mode uses a staging table and contract handoff.
- Schema drift is handled during apply/write through the existing DDL helpers.
- The runtime preserves contract state so the writer can reuse prepared tables when the upstream schema signature matches.

## Prerequisites

- PostgreSQL access from Rapidbyte.
- A database user that can connect to the database.
- `CREATE` privileges on the target schema, or `CREATE` on the database when the schema does not yet exist.

The prerequisite report distinguishes between an existing schema and a schema that will be created by `apply()`.

## Configuration

```yaml
destination:
  type: dest-postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: app_db
  schema: public
  load_method: copy
  copy_flush_bytes: 4194304
```

`schema` defaults to `public`. `load_method` accepts `copy` or `insert`. `copy_flush_bytes` is an advanced tuning knob.

## Example Usage

The destination expects one stream per table. `apply()` owns the normal target-schema and table preparation path before the write loop starts, and `write()` owns runtime reuse, watermark handling, and streaming ingestion.

## Local Testing

Run the full package test suite:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml
```

Targeted checks are useful when changing a specific lifecycle:

```bash
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml prerequisites
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml validate
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml apply
cargo test --manifest-path plugins/destinations/postgres/Cargo.toml writer
```

## Operational Caveats

- Validation only accepts the Arrow types supported by the write path; unsupported upstream fields are rejected before apply.
- `apply()` creates `UNLOGGED` tables, so the destination is optimized for ingestion and replay, not crash-safe durability.
- Replace mode writes to a per-stream staging table named `{stream_name}__rb_staging` and relies on the contract handoff to decide whether the staging state can be reused.
- The writer may rebuild the contract when schema signatures drift or the existing handoff is no longer safe to reuse.
