//! Integration tests for the WASM plugin runner.
//!
//! These tests exercise the full WASM lifecycle: load component, open, run,
//! close — using the minimal test plugins built from `plugins/tests/`.
//!
//! Gated behind `#[cfg(feature = "integration")]` and require the test plugins
//! to be pre-built. Run with:
//!
//! ```bash
//! just build-test-plugins
//! cargo test -p rapidbyte-engine --features integration --test wasm_runner_integration
//! ```

#![cfg(feature = "integration")]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rapidbyte_engine::adapter::wasm_runner::WasmPluginRunner;
use rapidbyte_engine::domain::ports::runner::{
    DestinationRunParams, DiscoverParams, PluginRunner, SourceRunParams, TransformRunParams,
    ValidateParams,
};
use rapidbyte_engine::PipelineError;
use rapidbyte_runtime::{Frame, WasmRuntime};
use rapidbyte_types::catalog::SchemaHint;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::RunStats;
use rapidbyte_types::stream::{StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{PluginKind, SyncMode};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_plugins_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("plugins/tests")
}

fn source_wasm_path() -> PathBuf {
    test_plugins_dir().join("test-source/target/wasm32-wasip2/debug/test_source.wasm")
}

fn destination_wasm_path() -> PathBuf {
    test_plugins_dir().join("test-destination/target/wasm32-wasip2/debug/test_destination.wasm")
}

fn transform_wasm_path() -> PathBuf {
    test_plugins_dir().join("test-transform/target/wasm32-wasip2/debug/test_transform.wasm")
}

fn make_runner() -> WasmPluginRunner {
    let runtime = WasmRuntime::new().expect("Failed to create WasmRuntime");
    let state_backend = rapidbyte_types::state_backend::noop_state_backend();
    WasmPluginRunner::new(runtime, state_backend)
}

fn make_stream_ctx(stream_name: &str) -> StreamContext {
    StreamContext {
        stream_name: stream_name.to_string(),
        source_stream_name: None,
        schema: SchemaHint::Columns(vec![]),
        sync_mode: SyncMode::FullRefresh,
        cursor_info: None,
        limits: StreamLimits::default(),
        policies: StreamPolicies::default(),
        write_mode: None,
        selected_columns: None,
        partition_key: None,
        partition_count: None,
        partition_index: None,
        effective_parallelism: None,
        partition_strategy: None,
        copy_flush_bytes_override: None,
    }
}

fn make_stats() -> Arc<Mutex<RunStats>> {
    Arc::new(Mutex::new(RunStats::default()))
}

fn make_dlq() -> Arc<Mutex<Vec<DlqRecord>>> {
    Arc::new(Mutex::new(Vec::new()))
}

// ---------------------------------------------------------------------------
// Test: source emits expected row count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_source_emits_expected_rows() {
    let runner = make_runner();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let stats = make_stats();

    let config = serde_json::json!({
        "row_count": 25,
        "batch_size": 10
    });

    let outcome = runner
        .run_source(SourceRunParams {
            wasm_path: source_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config,
            permissions: None,
            compression: None,
            frame_sender: tx,
            stats: stats.clone(),
            on_batch_emitted: None,
        })
        .await
        .expect("source run should succeed");

    assert_eq!(outcome.summary.records_read, 25);
    assert_eq!(outcome.summary.batches_emitted, 3); // 10 + 10 + 5

    // Verify frames were sent (data frames + EndStream)
    let mut data_frames = 0;
    let mut end_stream = false;
    while let Ok(frame) = rx.try_recv() {
        match frame {
            Frame::EndStream => end_stream = true,
            _ => data_frames += 1,
        }
    }
    assert_eq!(data_frames, 3);
    assert!(
        end_stream,
        "EndStream frame should be sent after source completes"
    );

    // Verify stats were accumulated
    let s = stats.lock().unwrap();
    assert_eq!(s.records_read, 25);
}

// ---------------------------------------------------------------------------
// Test: source failure propagates error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_source_failure_propagates_error() {
    let runner = make_runner();
    let (tx, _rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let stats = make_stats();

    let config = serde_json::json!({
        "should_fail": true
    });

    let result = runner
        .run_source(SourceRunParams {
            wasm_path: source_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config,
            permissions: None,
            compression: None,
            frame_sender: tx,
            stats,
            on_batch_emitted: None,
        })
        .await;

    match result {
        Ok(_) => panic!("expected source to fail"),
        Err(PipelineError::Plugin(err)) => {
            assert_eq!(err.code, "TEST_FAILURE");
            assert!(err.message.contains("Simulated source failure"));
        }
        Err(other) => panic!("expected PipelineError::Plugin, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test: destination consumes frames and counts rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_destination_consumes_frames() {
    let runner = make_runner();

    // First, run a source to produce frames
    let (source_tx, source_rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let source_stats = make_stats();

    let source_config = serde_json::json!({
        "row_count": 15,
        "batch_size": 5
    });

    let _source_outcome = runner
        .run_source(SourceRunParams {
            wasm_path: source_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: source_config,
            permissions: None,
            compression: None,
            frame_sender: source_tx,
            stats: source_stats,
            on_batch_emitted: None,
        })
        .await
        .expect("source run should succeed");

    // Now run the destination consuming those frames
    let dest_stats = make_stats();
    let dlq = make_dlq();

    let dest_config = serde_json::json!({});

    let dest_outcome = runner
        .run_destination(DestinationRunParams {
            wasm_path: destination_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-destination".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: dest_config,
            permissions: None,
            compression: None,
            frame_receiver: source_rx,
            dlq_records: dlq,
            stats: dest_stats.clone(),
        })
        .await
        .expect("destination run should succeed");

    assert_eq!(dest_outcome.summary.records_written, 15);
    assert_eq!(dest_outcome.summary.batches_written, 3);

    // Verify stats
    let s = dest_stats.lock().unwrap();
    assert_eq!(s.records_written, 15);
}

// ---------------------------------------------------------------------------
// Test: destination failure propagates error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_destination_failure_propagates_error() {
    let runner = make_runner();

    // Send EndStream immediately (empty pipeline) - the destination
    // will still open and try to process, but should_fail will trigger
    // before any frame processing.
    let (tx, rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let _ = tx.send(Frame::EndStream);

    let dest_config = serde_json::json!({
        "should_fail": true
    });

    let result = runner
        .run_destination(DestinationRunParams {
            wasm_path: destination_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-destination".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: dest_config,
            permissions: None,
            compression: None,
            frame_receiver: rx,
            dlq_records: make_dlq(),
            stats: make_stats(),
        })
        .await;

    match result {
        Ok(_) => panic!("expected destination to fail"),
        Err(PipelineError::Plugin(err)) => {
            assert_eq!(err.code, "TEST_FAILURE");
        }
        Err(other) => panic!("expected PipelineError::Plugin, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test: transform pass-through preserves data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transform_passthrough() {
    let runner = make_runner();

    // Run source to produce frames
    let (source_tx, source_rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let source_stats = make_stats();

    let _source_outcome = runner
        .run_source(SourceRunParams {
            wasm_path: source_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: serde_json::json!({"row_count": 20, "batch_size": 10}),
            permissions: None,
            compression: None,
            frame_sender: source_tx,
            stats: source_stats,
            on_batch_emitted: None,
        })
        .await
        .expect("source run should succeed");

    // Run transform
    let (transform_tx, transform_rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let dlq = make_dlq();

    let transform_outcome = runner
        .run_transform(TransformRunParams {
            wasm_path: transform_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-transform".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: serde_json::json!({}),
            permissions: None,
            compression: None,
            frame_receiver: source_rx,
            frame_sender: transform_tx,
            dlq_records: dlq,
            transform_index: 0,
        })
        .await
        .expect("transform run should succeed");

    assert_eq!(transform_outcome.summary.records_in, 20);
    assert_eq!(transform_outcome.summary.records_out, 20);
    assert_eq!(transform_outcome.summary.batches_processed, 2);

    // Verify frames passed through (data frames + EndStream from transform)
    let mut data_frames = 0;
    let mut end_stream = false;
    while let Ok(frame) = transform_rx.try_recv() {
        match frame {
            Frame::EndStream => end_stream = true,
            _ => data_frames += 1,
        }
    }
    assert_eq!(data_frames, 2);
    assert!(end_stream);
}

// ---------------------------------------------------------------------------
// Test: full pipeline source -> transform -> destination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_full_pipeline_source_transform_destination() {
    let runner = make_runner();

    // Source
    let (source_tx, source_rx) = std::sync::mpsc::sync_channel::<Frame>(64);
    let source_stats = make_stats();

    let source_outcome = runner
        .run_source(SourceRunParams {
            wasm_path: source_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: serde_json::json!({"row_count": 30, "batch_size": 10}),
            permissions: None,
            compression: None,
            frame_sender: source_tx,
            stats: source_stats,
            on_batch_emitted: None,
        })
        .await
        .expect("source should succeed");

    // Transform
    let (transform_tx, transform_rx) = std::sync::mpsc::sync_channel::<Frame>(64);

    let transform_outcome = runner
        .run_transform(TransformRunParams {
            wasm_path: transform_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-transform".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: serde_json::json!({}),
            permissions: None,
            compression: None,
            frame_receiver: source_rx,
            frame_sender: transform_tx,
            dlq_records: make_dlq(),
            transform_index: 0,
        })
        .await
        .expect("transform should succeed");

    // Destination
    let dest_stats = make_stats();

    let dest_outcome = runner
        .run_destination(DestinationRunParams {
            wasm_path: destination_wasm_path(),
            pipeline_name: "test-pipeline".into(),
            metric_run_label: "test".into(),
            plugin_id: "test-destination".into(),
            plugin_version: "0.1.0".into(),
            stream_ctx: make_stream_ctx("test-stream"),
            config: serde_json::json!({}),
            permissions: None,
            compression: None,
            frame_receiver: transform_rx,
            dlq_records: make_dlq(),
            stats: dest_stats,
        })
        .await
        .expect("destination should succeed");

    // Verify end-to-end data flow
    assert_eq!(source_outcome.summary.records_read, 30);
    assert_eq!(transform_outcome.summary.records_in, 30);
    assert_eq!(transform_outcome.summary.records_out, 30);
    assert_eq!(dest_outcome.summary.records_written, 30);
}

// ---------------------------------------------------------------------------
// Test: validate source plugin config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_validate_source_plugin() {
    let runner = make_runner();

    let result = runner
        .validate_plugin(&ValidateParams {
            wasm_path: source_wasm_path(),
            kind: PluginKind::Source,
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            config: serde_json::json!({"row_count": 10}),
            stream_name: "test-stream".into(),
            permissions: None,
        })
        .await
        .expect("validation should succeed");

    assert_eq!(
        result.validation.status,
        rapidbyte_types::error::ValidationStatus::Success
    );
}

// ---------------------------------------------------------------------------
// Test: validate destination plugin config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_validate_destination_plugin() {
    let runner = make_runner();

    let result = runner
        .validate_plugin(&ValidateParams {
            wasm_path: destination_wasm_path(),
            kind: PluginKind::Destination,
            plugin_id: "test-destination".into(),
            plugin_version: "0.1.0".into(),
            config: serde_json::json!({}),
            stream_name: "test-stream".into(),
            permissions: None,
        })
        .await
        .expect("validation should succeed");

    assert_eq!(
        result.validation.status,
        rapidbyte_types::error::ValidationStatus::Success
    );
}

// ---------------------------------------------------------------------------
// Test: discover returns streams from source plugin
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_discover_source_streams() {
    let runner = make_runner();

    let streams = runner
        .discover(&DiscoverParams {
            wasm_path: source_wasm_path(),
            plugin_id: "test-source".into(),
            plugin_version: "0.1.0".into(),
            config: serde_json::json!({}),
            permissions: None,
        })
        .await
        .expect("discover should succeed");

    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, "test-stream");

    // Verify the catalog JSON contains expected schema
    let catalog: serde_json::Value =
        serde_json::from_str(&streams[0].catalog_json).expect("catalog should parse");
    assert_eq!(catalog["name"], "test-stream");
    let schema = catalog["schema"]
        .as_array()
        .expect("schema should be array");
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0]["name"], "id");
    assert_eq!(schema[1]["name"], "value");
}
