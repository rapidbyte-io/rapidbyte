# CLI Output Redesign

## Goal

Replace the current wall-of-println! output with a modern, minimal CLI experience using `indicatif` + `console`. Provide layered verbosity (`-v`, `-vv`, `-q`) and clean non-TTY behavior.

## Verbosity Levels

| Flag | During execution | Final summary |
|------|-----------------|---------------|
| `--quiet` / `-q` | Nothing | Nothing (exit code only, errors on stderr) |
| Default | Single spinner with aggregate counters | Compact block (~6 lines) |
| `-v` | Spinner + per-stream lines on completion | Compact block + per-stream table + stage timings |
| `-vv` | Same as `-v` | Full diagnostics: compression, WASM overhead, CPU/RSS, per-shard skew |

`--quiet` wins over `--verbose` if both specified. `--log-level` remains orthogonal (controls tracing output).

## Live Progress (During Execution)

### Progress Channel

Engine accepts an `Option<UnboundedSender<ProgressEvent>>`. If `None`, engine behaves as today.

```rust
enum ProgressEvent {
    PhaseChange { phase: Phase },
    BatchCompleted { stream: String, records: u64, bytes: u64 },
    StreamCompleted { stream: String },
    Error { message: String },
}

enum Phase { Resolving, Loading, Running, Finished }
```

### Spinner Rendering (Default)

```
-> Loading connectors...
```

transitions to:

```
-> Running -- 1.2M rows | 48.2 MB | 3 of 5 streams done
```

Single line, updates in-place. Numbers tick up on `BatchCompleted`. Stream count increments on `StreamCompleted`. Spinner hides on completion, replaced by final summary.

## Final Summary

### Default

```
v Pipeline completed in 3.2s

  Records     1,200,000 read -> 1,198,500 written
  Data        48.2 MB read -> 46.1 MB written
  Throughput  375K rows/s | 15.1 MB/s
  Streams     5 completed | parallelism: 4
```

Green checkmark on success, red X on failure. Numbers use comma separators, human-friendly units.

### `-v` adds

Per-stream table:

```
  Streams
  +----------+----------+----------+---------+--------+
  | Stream   | Read     | Written  | Data    | Time   |
  +----------+----------+----------+---------+--------+
  | users    | 500,000  | 499,800  | 18.2 MB | 1.1s   |
  | orders   | 620,000  | 618,700  | 24.8 MB | 1.8s   |
  | products | 80,000   | 80,000   | 5.2 MB  | 0.3s   |
  +----------+----------+----------+---------+--------+
```

Stage timing breakdown:

```
  Timing
    Source     connect 0.02s | query 0.05s | fetch 0.98s | encode 0.12s
    Dest       connect 0.01s | flush 0.84s | commit 0.03s | decode 0.08s
    Transform  0.42s (2 stages)
```

### `-vv` adds

```
  Diagnostics
    WASM overhead    0.18s
    Compression      lz4 encode 0.04s | decode 0.02s
    Host ops         emit_batch 0.06s (1,200 calls) | next_batch 0.03s
    Process          CPU 2.8s user + 0.4s sys | RSS 142 MB

  Shard skew
  +----------+-------+----------+----------+--------+
  | Stream   | Shard | Read     | Written  | Time   |
  +----------+-------+----------+----------+--------+
  | orders   | 0/2   | 310,000  | 309,400  | 0.9s   |
  | orders   | 1/2   | 310,000  | 309,300  | 0.9s   |
  +----------+-------+----------+----------+--------+
```

## Architecture

### Dependencies

```toml
# workspace Cargo.toml
indicatif = "0.17"
console = "0.15"
```

### New Modules

```
crates/rapidbyte-cli/src/output/
  mod.rs          # Verbosity enum, OutputHandler
  progress.rs     # Live spinner driven by ProgressEvent channel
  summary.rs      # Final summary formatting (default, -v, -vv)
  format.rs       # Number formatting helpers (human bytes, commas, duration)

crates/rapidbyte-engine/src/progress.rs
  # ProgressEvent enum, Phase enum (engine-side, no display deps)
```

### Changes to Existing Code

1. **`orchestrator.rs`** -- Accept `Option<UnboundedSender<ProgressEvent>>`, emit events at phase transitions and after each batch.
2. **`run.rs`** (CLI command) -- Replace all `println!` with `OutputHandler` methods. Spawn progress spinner task, pass sender to orchestrator.
3. **`main.rs`** -- Parse `-v`/`-vv`/`-q` flags, construct `OutputHandler` with correct verbosity.
4. **Other commands** (`check`, `discover`, `connectors`) -- Adopt colored output and consistent formatting, no spinner needed.

### Non-TTY Behavior

- `console::Term::stdout().is_term()` detection
- Not a TTY: no spinner, no colors, plain text final summary
- `--quiet` works the same regardless of TTY

## Other Commands

### `check`

```
v source-postgres    valid
v dest-postgres      valid
v transform-sql      valid
v state (sqlite)     valid
```

Red X with error message for failures.

### `discover`

```
v Discovered 3 streams

  +------------+-------------+------------+---------+
  | Stream     | Sync        | Cursor     | Columns |
  +------------+-------------+------------+---------+
  | users      | full        | --         | 12      |
  | orders     | incremental | updated_at | 8       |
  | products   | full        | --         | 6       |
  +------------+-------------+------------+---------+
```

`-v` shows full schema/column details per stream.

### `connectors`

```
  source-postgres    source       PostgreSQL source connector
  dest-postgres      destination  PostgreSQL destination connector
  transform-sql      transform    SQL transform (DataFusion)
```

All commands respect `--quiet` and non-TTY.

## Approach

Output layer lives entirely in `rapidbyte-cli`. Engine sends lightweight `ProgressEvent`s via an optional channel. Engine never depends on indicatif/console. Clean separation: engine = library, CLI = presentation.
