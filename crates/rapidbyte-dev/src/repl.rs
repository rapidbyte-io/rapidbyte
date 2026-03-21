//! REPL loop, command dispatch, and source plugin operations.
//!
//! Implements the interactive dev shell using reedline, with handlers for
//! all dot-commands and SQL queries.

#![allow(
    clippy::cast_possible_truncation,
    clippy::too_many_lines,
    clippy::module_name_repetitions
)]

use std::io::IsTerminal;
use std::sync::{mpsc, Arc};

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use console::style;
use reedline::{DefaultPrompt, DefaultPromptSegment, Reedline, Signal};

use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, load_plugin_manifest, resolve_plugin_path, source_bindings,
    source_error_to_sdk, ComponentHostState, Frame, LoadedComponent, WasmRuntime,
};
use rapidbyte_types::discovery::DiscoveredStream;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::schema::SchemaField;
use rapidbyte_types::schema::StreamSchema;
use rapidbyte_types::state_backend::{NoopStateBackend, StateBackend};
use rapidbyte_types::stream::{StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{PluginKind, SyncMode};

use crate::commands::{self, Command};
use crate::display;
use crate::workspace::ArrowWorkspace;

// ── State types ────────────────────────────────────────────────────

pub(crate) struct ReplState {
    pub workspace: ArrowWorkspace,
    pub source: Option<ConnectedSource>,
}

pub(crate) struct ConnectedSource {
    pub plugin_ref: String,
    pub config: serde_json::Value,
    pub streams: Vec<DiscoveredStream>,
    pub loaded_module: LoadedComponent,
    pub permissions: Option<Permissions>,
}

// ── REPL entry point ───────────────────────────────────────────────

/// Run the interactive dev shell REPL loop.
pub(crate) async fn run() -> Result<()> {
    if !std::io::stdin().is_terminal() {
        anyhow::bail!("rapidbyte dev requires an interactive terminal");
    }

    print_banner();

    let mut state = ReplState {
        workspace: ArrowWorkspace::new(),
        source: None,
    };

    let completer = crate::completer::DevCompleter::new();
    let completer_state = completer.clone();
    let highlighter = Box::new(crate::highlighter::DevHighlighter);
    let mut editor = Reedline::create()
        .with_completer(Box::new(completer))
        .with_highlighter(highlighter);
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("rb".to_string()),
        DefaultPromptSegment::Empty,
    );

    loop {
        completer_state.refresh(
            state
                .source
                .as_ref()
                .map(|source| {
                    source
                        .streams
                        .iter()
                        .map(|stream| stream.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
            state.workspace.table_names(),
        );
        match editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let Some(parsed) = commands::parse(&line) else {
                    continue;
                };
                match parsed {
                    Ok(cmd) => {
                        if let Command::Quit = &cmd {
                            break;
                        }
                        handle_command(&mut state, cmd).await;
                    }
                    Err(msg) => display::print_error(&msg),
                }
            }
            Ok(Signal::CtrlD) => break,
            Ok(Signal::CtrlC) => {}
            Err(e) => {
                display::print_error(&format!("REPL error: {e}"));
                break;
            }
        }
    }

    Ok(())
}

// ── Command dispatch ───────────────────────────────────────────────

async fn handle_command(state: &mut ReplState, cmd: Command) {
    let result = match cmd {
        Command::Source { plugin, args } => handle_source(state, &plugin, &args).await,
        Command::Tables => handle_tables(state),
        Command::Schema { table } => handle_schema(state, &table),
        Command::Stream { table, limit } => handle_stream(state, &table, limit).await,
        Command::Workspace => {
            handle_workspace(state);
            Ok(())
        }
        Command::Clear { table } => handle_clear(state, table.as_deref()),
        Command::Sql(sql) => handle_sql(state, &sql).await,
        Command::Help => {
            print_help();
            Ok(())
        }
        Command::Quit => unreachable!(),
    };
    if let Err(e) = result {
        display::print_error(&format!("{e:#}"));
    }
}

// ── .source handler ────────────────────────────────────────────────

async fn handle_source(
    state: &mut ReplState,
    plugin_name: &str,
    args: &[(String, String)],
) -> Result<()> {
    // Build config object from key-value args.
    let mut config_map = serde_json::Map::new();
    for (key, value) in args {
        // Try to parse as i64, then bool, then fallback to string.
        let json_value = if let Ok(n) = value.parse::<i64>() {
            serde_json::Value::Number(serde_json::Number::from(n))
        } else if let Ok(b) = value.parse::<bool>() {
            serde_json::Value::Bool(b)
        } else {
            serde_json::Value::String(value.clone())
        };
        config_map.insert(key.clone(), json_value);
    }
    let config = serde_json::Value::Object(config_map);

    let plugin_ref = plugin_name.to_string();

    let spinner = make_spinner("Connecting...");

    let result = connect_source(&plugin_ref, &config).await;
    spinner.finish_and_clear();
    let (streams, loaded_module, permissions) = result?;

    let stream_count = streams.len();
    display::print_success(&format!(
        "Connected -- {stream_count} stream{} discovered",
        if stream_count == 1 { "" } else { "s" }
    ));

    state.source = Some(ConnectedSource {
        plugin_ref,
        config,
        streams,
        loaded_module,
        permissions,
    });

    Ok(())
}

/// Resolve, load, and discover a source plugin.
async fn connect_source(
    plugin_ref: &str,
    config: &serde_json::Value,
) -> Result<(Vec<DiscoveredStream>, LoadedComponent, Option<Permissions>)> {
    let wasm_path = resolve_plugin_path(plugin_ref, PluginKind::Source)?;
    let manifest = load_plugin_manifest(&wasm_path)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());
    let permissions_clone = permissions.clone();

    let config = config.clone();
    let plugin_ref = plugin_ref.to_string();

    let (streams, module) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<DiscoveredStream>, LoadedComponent)> {
            let runtime = WasmRuntime::new()?;
            let module = runtime.load_module(&wasm_path)?;

            let state_backend: Arc<dyn StateBackend> = Arc::new(NoopStateBackend);

            // Build host state for discover (no batch frames needed).
            let (dummy_tx, _dummy_rx) = mpsc::sync_channel::<Frame>(1);
            let mut builder = ComponentHostState::builder()
                .pipeline("dev")
                .plugin_id(&plugin_ref)
                .stream("discover")
                .state_backend(state_backend)
                .sender(dummy_tx)
                .config(&config)
                .compression(None);
            if let Some(ref p) = permissions_clone {
                builder = builder.permissions(p);
            }
            let host_state = builder.build()?;

            let mut store = module.new_store(host_state, None);
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |s| s)?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let config_json = serde_json::to_string(&config)?;
            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let wit_streams = iface
                .call_discover(&mut store, session)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Discover failed: {e}"))?;

            let streams = wit_streams
                .into_iter()
                .map(|stream| {
                    let source_bindings::rapidbyte::plugin::types::DiscoveredStream {
                        name,
                        schema,
                        supported_sync_modes,
                        default_cursor_field,
                        estimated_row_count,
                        metadata_json,
                    } = stream;
                    let source_bindings::rapidbyte::plugin::types::StreamSchema {
                        fields,
                        primary_key,
                        partition_keys,
                        source_defined_cursor,
                        schema_id,
                    } = schema;

                    DiscoveredStream {
                        name,
                        schema: StreamSchema {
                            fields: fields
                                .into_iter()
                                .map(|field| SchemaField {
                                    name: field.name,
                                    arrow_type: field.arrow_type,
                                    nullable: field.nullable,
                                    is_primary_key: field.is_primary_key,
                                    is_generated: field.is_generated,
                                    is_partition_key: field.is_partition_key,
                                    default_value: field.default_value,
                                })
                                .collect(),
                            primary_key,
                            partition_keys,
                            source_defined_cursor,
                            schema_id,
                        },
                        supported_sync_modes: supported_sync_modes
                            .into_iter()
                            .map(|sm| match sm {
                                source_bindings::rapidbyte::plugin::types::SyncMode::FullRefresh => {
                                    SyncMode::FullRefresh
                                }
                                source_bindings::rapidbyte::plugin::types::SyncMode::Incremental => {
                                    SyncMode::Incremental
                                }
                                source_bindings::rapidbyte::plugin::types::SyncMode::Cdc => {
                                    SyncMode::Cdc
                                }
                            })
                            .collect(),
                        default_cursor_field,
                        estimated_row_count,
                        metadata_json,
                    }
                })
                .collect();

            if let Err(err) = iface.call_close(&mut store, session)? {
                tracing::warn!(
                    "Source close failed after discover: {}",
                    source_error_to_sdk(err)
                );
            }

            Ok((streams, module))
        })
        .await
        .context("Plugin task panicked")??;

    Ok((streams, module, permissions))
}

// ── .tables handler ────────────────────────────────────────────────

fn handle_tables(state: &ReplState) -> Result<()> {
    let source = require_source(state)?;

    if source.streams.is_empty() {
        display::print_hint("No streams discovered.");
        return Ok(());
    }

    eprintln!(
        "{:<40} {:<15} {}",
        style("Stream").bold().underlined(),
        style("Sync Mode").bold().underlined(),
        style("Columns").bold().underlined(),
    );

    for stream in &source.streams {
        let sync_label = stream
            .supported_sync_modes
            .iter()
            .copied()
            .map(sync_mode_label)
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!(
            "{:<40} {:<15} {}",
            stream.name,
            sync_label,
            stream.schema.fields.len()
        );
    }

    Ok(())
}

// ── .schema handler ────────────────────────────────────────────────

fn handle_schema(state: &ReplState, table: &str) -> Result<()> {
    let source = require_source(state)?;
    let stream = find_stream(&source.streams, table)?;

    eprintln!("{}", style(format!("-- {} --", stream.name)).bold());
    eprintln!(
        "{:<30} {:<20} {}",
        style("Column").bold().underlined(),
        style("Type").bold().underlined(),
        style("Nullable").bold().underlined(),
    );

    for col in &stream.schema.fields {
        let nullable_str = if col.nullable { "YES" } else { "NO" };
        eprintln!("{:<30} {:<20} {nullable_str}", col.name, col.arrow_type);
    }

    Ok(())
}

// ── .stream handler ────────────────────────────────────────────────

async fn handle_stream(state: &mut ReplState, table: &str, limit: Option<u64>) -> Result<()> {
    let source = require_source(state)?;
    let stream = find_stream(&source.streams, table)?.clone();

    let plugin_ref = source.plugin_ref.clone();
    let config = source.config.clone();
    let loaded_module = source.loaded_module.clone();
    let permissions = source.permissions.clone();

    let sync_mode = stream
        .supported_sync_modes
        .first()
        .copied()
        .unwrap_or(SyncMode::FullRefresh);

    let stream_ctx = StreamContext {
        stream_name: stream.name.clone(),
        source_stream_name: None,
        stream_index: 0,
        schema: stream.schema.clone(),
        sync_mode,
        cursor_info: None,
        limits: StreamLimits {
            max_records: limit,
            ..StreamLimits::default()
        },
        policies: StreamPolicies::default(),
        write_mode: None,
        selected_columns: None,
        partition_key: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    };

    let spinner = make_spinner("Streaming...");

    let stream_result = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
        let (tx, rx) = mpsc::sync_channel::<Frame>(64);

        let state_backend: Arc<dyn StateBackend> = Arc::new(NoopStateBackend);

        let mut builder = ComponentHostState::builder()
            .pipeline("dev")
            .plugin_id(&plugin_ref)
            .stream(&stream_ctx.stream_name)
            .state_backend(state_backend)
            .sender(tx)
            .config(&config)
            .compression(None);
        if let Some(ref p) = permissions {
            builder = builder.permissions(p);
        }
        let host_state = builder.build()?;

        let mut store = loaded_module.new_store(host_state, None);
        let linker = create_component_linker(&loaded_module.engine, "source", |linker| {
            source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |s| s)?;
            Ok(())
        })?;
        let bindings = source_bindings::RapidbyteSource::instantiate(
            &mut store,
            &loaded_module.component,
            &linker,
        )?;
        let iface = bindings.rapidbyte_plugin_source();

        let config_json = serde_json::to_string(&config)?;
        let session = iface
            .call_open(&mut store, &config_json)?
            .map_err(source_error_to_sdk)
            .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

        let wit_stream_ctx = dev_stream_context_to_wit(&stream_ctx);
        let run_request = source_bindings::rapidbyte::plugin::types::RunRequest {
            streams: vec![wit_stream_ctx],
            dry_run: false,
        };

        let run_result = iface.call_run(&mut store, session, &run_request)?;
        if let Err(err) = &run_result {
            let _ = iface.call_close(&mut store, session);
            let sdk_err = source_error_to_sdk(err.clone());
            anyhow::bail!("Source run failed: {sdk_err}");
        }

        if let Err(err) = iface.call_close(&mut store, session)? {
            tracing::warn!("Source close failed: {}", source_error_to_sdk(err));
        }

        // Drop the store to close the sender side of the channel.
        drop(store);

        // Collect all frames from the receiver.
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            match frame {
                Frame::Data { payload: bytes, .. } => {
                    let decoded = rapidbyte_types::arrow::ipc_to_record_batches(&bytes)?;
                    all_batches.extend(decoded);
                }
                Frame::EndStream => break,
            }
        }

        Ok(all_batches)
    })
    .await
    .context("Stream task panicked")?;
    spinner.finish_and_clear();
    let batches = stream_result?;

    if batches.is_empty() {
        display::print_hint("0 rows returned.");
        return Ok(());
    }

    // Derive short name: strip schema prefix (e.g., "public.users" -> "users").
    let short_name = stream.name.rsplit('.').next().unwrap_or(&stream.name);

    let schema = batches[0].schema();
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let total_bytes: u64 = batches
        .iter()
        .flat_map(|b| b.columns().iter())
        .map(|col| col.get_array_memory_size() as u64)
        .sum();
    let col_count = schema.fields().len();

    state
        .workspace
        .insert(short_name, schema, batches)
        .context("Failed to insert into workspace")?;

    display::print_success(&format!(
        "{} rows -> workspace:{short_name} ({col_count} columns, {})",
        display::format_count(total_rows),
        display::format_bytes(total_bytes),
    ));

    Ok(())
}

// ── .workspace handler ─────────────────────────────────────────────

fn handle_workspace(state: &ReplState) {
    let summaries = state.workspace.summary();
    if summaries.is_empty() {
        display::print_hint("Workspace is empty. Use .stream to load data.");
        return;
    }

    eprintln!(
        "{:<30} {:>10} {:>10} {:>12}",
        style("Table").bold().underlined(),
        style("Rows").bold().underlined(),
        style("Columns").bold().underlined(),
        style("Memory").bold().underlined(),
    );

    for s in &summaries {
        eprintln!(
            "{:<30} {:>10} {:>10} {:>12}",
            s.name,
            display::format_count(s.rows),
            s.columns,
            display::format_bytes(s.memory_bytes),
        );
    }
}

// ── .clear handler ─────────────────────────────────────────────────

fn handle_clear(state: &mut ReplState, table: Option<&str>) -> Result<()> {
    if let Some(name) = table {
        if !state.workspace.has_table(name) {
            anyhow::bail!("Table '{name}' not found in workspace");
        }
        state.workspace.clear(Some(name));
        display::print_success(&format!("Cleared table '{name}'"));
    } else {
        state.workspace.clear(None);
        display::print_success("Cleared all tables from workspace");
    }
    Ok(())
}

// ── SQL handler ────────────────────────────────────────────────────

async fn handle_sql(state: &ReplState, sql: &str) -> Result<()> {
    if state.workspace.is_empty() {
        anyhow::bail!("Workspace is empty. Use .stream to load data first.");
    }

    let batches = state
        .workspace
        .query(sql)
        .await
        .context("SQL query failed")?;

    display::print_batches(&batches);
    Ok(())
}

// ── Help + Banner ──────────────────────────────────────────────────

fn print_help() {
    let commands: &[(&str, &str)] = &[
        (
            ".source <plugin> [--key value ...]",
            "Connect to a source plugin",
        ),
        (".tables", "List discovered streams"),
        (".schema <table>", "Show columns for a stream"),
        (
            ".stream <table> [--limit N]",
            "Read a stream into the workspace",
        ),
        (".workspace / .ws", "Show workspace tables"),
        (".clear [table]", "Clear one or all workspace tables"),
        (".help / .h", "Show this help"),
        (".quit / .exit / .q", "Exit the shell"),
    ];

    eprintln!();
    eprintln!("{}", style("Commands:").bold());
    for (cmd, desc) in commands {
        eprintln!("  {cmd:<40} {desc}");
    }
    eprintln!();
    eprintln!("{}", style("SQL:").bold());
    eprintln!("  Type any SQL query to run against the workspace (DataFusion).");
    eprintln!();
}

fn print_banner() {
    eprintln!();
    eprintln!(
        "{}",
        style(format!(
            "Rapidbyte Dev Shell v{}",
            env!("CARGO_PKG_VERSION")
        ))
        .bold()
    );
    eprintln!(
        "{}",
        style("Type .help for commands, or enter SQL to query.").dim()
    );
    eprintln!();
}

// ── StreamContext → WIT converter for dev shell ────────────────────

fn dev_stream_context_to_wit(
    ctx: &StreamContext,
) -> source_bindings::rapidbyte::plugin::types::StreamContext {
    use source_bindings::rapidbyte::plugin::types as wit;

    let sync_mode = match ctx.sync_mode {
        SyncMode::FullRefresh => wit::SyncMode::FullRefresh,
        SyncMode::Incremental => wit::SyncMode::Incremental,
        SyncMode::Cdc => wit::SyncMode::Cdc,
    };

    let schema = wit::StreamSchema {
        fields: ctx
            .schema
            .fields
            .iter()
            .map(|f| wit::SchemaField {
                name: f.name.clone(),
                arrow_type: f.arrow_type.clone(),
                nullable: f.nullable,
                is_primary_key: f.is_primary_key,
                is_generated: f.is_generated,
                is_partition_key: f.is_partition_key,
                default_value: f.default_value.clone(),
            })
            .collect(),
        primary_key: ctx.schema.primary_key.clone(),
        partition_keys: ctx.schema.partition_keys.clone(),
        source_defined_cursor: ctx.schema.source_defined_cursor.clone(),
        schema_id: ctx.schema.schema_id.clone(),
    };

    wit::StreamContext {
        stream_index: ctx.stream_index,
        stream_name: ctx.stream_name.clone(),
        source_stream_name: ctx.source_stream_name.clone(),
        schema,
        sync_mode,
        cursor_info: None,
        limits: wit::StreamLimits {
            max_batch_bytes: ctx.limits.max_batch_bytes,
            max_record_bytes: ctx.limits.max_record_bytes,
            max_inflight_batches: ctx.limits.max_inflight_batches,
            max_parallel_requests: ctx.limits.max_parallel_requests,
            checkpoint_interval_bytes: ctx.limits.checkpoint_interval_bytes,
            checkpoint_interval_rows: ctx.limits.checkpoint_interval_rows,
            checkpoint_interval_seconds: ctx.limits.checkpoint_interval_seconds,
            max_records: ctx.limits.max_records,
        },
        policies: wit::StreamPolicies {
            on_data_error: wit::DataErrorPolicy::Fail,
            schema_evolution: wit::SchemaEvolutionPolicy {
                new_column: wit::ColumnPolicy::Ignore,
                removed_column: wit::ColumnPolicy::Ignore,
                type_change: wit::TypeChangePolicy::Coerce,
                nullability_change: wit::NullabilityPolicy::Allow,
            },
        },
        write_mode: None,
        selected_columns: ctx.selected_columns.clone(),
        partition_key: ctx.partition_key.clone(),
        partition_count: ctx.partition_count,
        partition_index: ctx.partition_index,
        partition_strategy: None,
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn require_source(state: &ReplState) -> Result<&ConnectedSource> {
    state
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No source connected. Use .source to connect first."))
}

fn find_stream<'a>(streams: &'a [DiscoveredStream], table: &str) -> Result<&'a DiscoveredStream> {
    // Try exact match first.
    if let Some(stream) = streams.iter().find(|s| s.name == table) {
        return Ok(stream);
    }

    // Try suffix match (e.g., "users" matching "public.users").
    let matches: Vec<_> = streams
        .iter()
        .filter(|s| s.name.rsplit('.').next() == Some(table))
        .collect();

    match matches.len() {
        0 => anyhow::bail!("Stream '{table}' not found in discovered streams"),
        1 => Ok(matches[0]),
        n => anyhow::bail!(
            "Ambiguous stream name '{table}' matches {n} streams: {}",
            matches
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn sync_mode_label(mode: SyncMode) -> &'static str {
    match mode {
        SyncMode::FullRefresh => "full_refresh",
        SyncMode::Incremental => "incremental",
        SyncMode::Cdc => "cdc",
    }
}

fn make_spinner(msg: &str) -> indicatif::ProgressBar {
    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&[
                "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}",
                "\u{2827}", "\u{2807}", "\u{280f}",
            ]),
    );
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    spinner.set_message(msg.to_string());
    spinner
}
