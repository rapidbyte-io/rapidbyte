//! Plugin runner port trait.
//!
//! Abstracts the WASM runtime so the orchestrator can run source, transform,
//! and destination plugins through a trait boundary.

use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};

use async_trait::async_trait;
use rapidbyte_runtime::{Frame, SandboxOverrides};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::discovery::PluginSpec;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::lifecycle::{ApplyReport, TeardownReport};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{ReadSummary, TransformSummary, WriteSummary};
use rapidbyte_types::schema::StreamSchema;
use rapidbyte_types::state::RunStats;
use rapidbyte_types::stream::StreamContext;
use rapidbyte_types::validation::{PrerequisitesReport, ValidationReport};
use rapidbyte_types::wire::PluginKind;

use crate::domain::error::PipelineError;

// ---------------------------------------------------------------------------
// Param structs
// ---------------------------------------------------------------------------

/// Parameters for running a source plugin on a single stream.
pub struct SourceRunParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Pipeline name for labelling.
    pub pipeline_name: String,
    /// Metric run label for scoping `OTel` instruments.
    pub metric_run_label: String,
    /// Plugin identifier (e.g. `"rapidbyte/source-postgres"`).
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Stream context for this invocation.
    pub stream_ctx: StreamContext,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
    /// Compression codec to use for frame transport.
    pub compression: Option<String>,
    /// Channel sender for emitting frames to the next stage.
    pub frame_sender: mpsc::SyncSender<Frame>,
    /// Shared run stats accumulator.
    pub stats: Arc<Mutex<RunStats>>,
    /// Optional callback invoked after each batch is emitted.
    pub on_batch_emitted: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

/// Parameters for running a transform plugin on a single stream.
pub struct TransformRunParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Pipeline name for labelling.
    pub pipeline_name: String,
    /// Metric run label for scoping `OTel` instruments.
    pub metric_run_label: String,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Stream context for this invocation.
    pub stream_ctx: StreamContext,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
    /// Compression codec to use for frame transport.
    pub compression: Option<String>,
    /// Channel receiver for incoming frames.
    pub frame_receiver: mpsc::Receiver<Frame>,
    /// Channel sender for outgoing frames.
    pub frame_sender: mpsc::SyncSender<Frame>,
    /// Shared DLQ record accumulator.
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    /// Zero-based index of this transform in the pipeline.
    pub transform_index: usize,
}

/// Parameters for running a destination plugin on a single stream.
pub struct DestinationRunParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Pipeline name for labelling.
    pub pipeline_name: String,
    /// Metric run label for scoping `OTel` instruments.
    pub metric_run_label: String,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Stream context for this invocation.
    pub stream_ctx: StreamContext,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
    /// Compression codec to use for frame transport.
    pub compression: Option<String>,
    /// Channel receiver for incoming frames.
    pub frame_receiver: mpsc::Receiver<Frame>,
    /// Shared DLQ record accumulator.
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    /// Shared run stats accumulator.
    pub stats: Arc<Mutex<RunStats>>,
}

/// Parameters for validating a plugin configuration.
pub struct ValidateParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin kind (source, destination, transform).
    pub kind: PluginKind,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Stream name for validation context.
    pub stream_name: String,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
    /// Upstream schema for schema negotiation (e.g. source schema passed to destination).
    pub upstream_schema: Option<StreamSchema>,
}

/// Parameters for discovering available streams from a source plugin.
pub struct DiscoverParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
}

/// Parameters for plugin spec retrieval.
#[derive(Debug, Clone)]
pub struct SpecParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin kind (source, destination, transform).
    pub kind: PluginKind,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
}

/// Parameters for prerequisites check.
#[derive(Debug, Clone)]
pub struct PrerequisitesParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin kind (source, destination, transform).
    pub kind: PluginKind,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
}

/// Parameters for apply (resource provisioning).
#[derive(Debug, Clone)]
pub struct ApplyParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin kind (source, destination, transform).
    pub kind: PluginKind,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Stream contexts for schema apply.
    pub streams: Vec<StreamContext>,
    /// If `true`, report planned actions without executing them.
    pub dry_run: bool,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
}

/// Parameters for teardown (resource cleanup).
#[derive(Debug, Clone)]
pub struct TeardownParams {
    /// Path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin kind (source, destination, transform).
    pub kind: PluginKind,
    /// Plugin identifier.
    pub plugin_id: String,
    /// Plugin version string.
    pub plugin_version: String,
    /// Plugin configuration as JSON.
    pub config: serde_json::Value,
    /// Names of streams to tear down.
    pub streams: Vec<String>,
    /// Human-readable reason for the teardown.
    pub reason: String,
    /// Manifest-declared permissions.
    pub permissions: Option<Permissions>,
    /// Pipeline-level sandbox overrides (permissions/limits from YAML config).
    pub sandbox_overrides: Option<SandboxOverrides>,
}

// ---------------------------------------------------------------------------
// Outcome structs
// ---------------------------------------------------------------------------

/// Result of running a source plugin for a single stream.
pub struct SourceOutcome {
    /// Wall-clock duration of the source phase in seconds.
    pub duration_secs: f64,
    /// Aggregate read metrics.
    pub summary: ReadSummary,
    /// Checkpoints emitted during the source phase.
    pub checkpoints: Vec<Checkpoint>,
}

/// Result of running a transform plugin for a single stream.
pub struct TransformOutcome {
    /// Wall-clock duration of the transform phase in seconds.
    pub duration_secs: f64,
    /// Aggregate transform metrics.
    pub summary: TransformSummary,
}

/// Result of running a destination plugin for a single stream.
pub struct DestinationOutcome {
    /// Wall-clock duration of the destination phase in seconds.
    pub duration_secs: f64,
    /// Aggregate write metrics.
    pub summary: WriteSummary,
    /// WASM instantiation overhead in seconds.
    pub wasm_instantiation_secs: f64,
    /// Time spent receiving frames from the channel in seconds.
    pub frame_receive_secs: f64,
    /// Checkpoints emitted during the destination phase.
    pub checkpoints: Vec<Checkpoint>,
}

/// Status of a single component after a `check` / validation run.
#[derive(Debug, Clone)]
pub struct CheckComponentStatus {
    /// Validation result from the plugin.
    pub validation: ValidationReport,
}

/// A stream discovered by a source plugin.
#[derive(Debug, Clone)]
pub struct DiscoveredStream {
    /// Fully-qualified stream name (e.g. `"public.users"`).
    pub name: String,
    /// JSON-serialized catalog returned by the plugin.
    pub catalog_json: String,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Port for executing plugin operations (source, transform, destination,
/// validate, discover).
///
/// The `run_*` methods take params by value because they consume channels
/// (receivers are not cloneable).
///
/// Implemented by the WASM runtime adapter in the infrastructure layer.
#[async_trait]
pub trait PluginRunner: Send + Sync {
    /// Run a source plugin for a single stream.
    async fn run_source(&self, params: SourceRunParams) -> Result<SourceOutcome, PipelineError>;

    /// Run a transform plugin for a single stream.
    async fn run_transform(
        &self,
        params: TransformRunParams,
    ) -> Result<TransformOutcome, PipelineError>;

    /// Run a destination plugin for a single stream.
    async fn run_destination(
        &self,
        params: DestinationRunParams,
    ) -> Result<DestinationOutcome, PipelineError>;

    /// Validate a plugin configuration.
    async fn validate_plugin(
        &self,
        params: &ValidateParams,
    ) -> Result<CheckComponentStatus, PipelineError>;

    /// Discover available streams from a source plugin.
    async fn discover(
        &self,
        params: &DiscoverParams,
    ) -> Result<Vec<DiscoveredStream>, PipelineError>;

    /// Retrieve the plugin's spec (protocol version, config schema, features).
    async fn spec(&self, params: &SpecParams) -> Result<PluginSpec, PipelineError> {
        let _ = params;
        Ok(PluginSpec::from_manifest())
    }

    /// Run prerequisite checks against the plugin (connectivity, permissions, etc.).
    async fn prerequisites(
        &self,
        params: &PrerequisitesParams,
    ) -> Result<PrerequisitesReport, PipelineError> {
        let _ = params;
        Ok(PrerequisitesReport::passed())
    }

    /// Apply schema changes for streams (create/alter tables, etc.).
    async fn apply(&self, params: &ApplyParams) -> Result<ApplyReport, PipelineError> {
        let _ = params;
        Ok(ApplyReport::noop())
    }

    /// Tear down resources for streams (drop tables, clean up state, etc.).
    async fn teardown(&self, params: &TeardownParams) -> Result<TeardownReport, PipelineError> {
        let _ = params;
        Ok(TeardownReport::noop())
    }
}
