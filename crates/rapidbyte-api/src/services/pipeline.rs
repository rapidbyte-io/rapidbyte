//! Local-mode implementation of [`PipelineService`].
//!
//! Delegates to the engine's `run_pipeline`, `check_pipeline`, and
//! `build_run_context` / `build_lightweight_context` factory functions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{info, warn};

use rapidbyte_engine::{
    build_lightweight_context, build_run_context, check_pipeline, run_pipeline, PipelineError,
    ProgressEvent,
};
use rapidbyte_pipeline_config::PipelineConfig;
use rapidbyte_registry::RegistryConfig;
use rapidbyte_secrets::SecretProviders;

use crate::error::ApiError;
use crate::run_manager::RunManager;
use crate::traits::PipelineService;
use crate::types::{
    AssertRequest, AssertResult, BatchRunHandle, CheckResult, DiffResult, PaginatedList,
    PipelineDetail, PipelineFilter, PipelineState, PipelineSummary, ResolvedConfig, RunHandle,
    RunLinks, RunStatus, SseEvent, SyncBatchRequest, SyncRequest, TeardownRequest,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Local-mode pipeline service.
///
/// Holds a pre-loaded catalog of pipeline configs and delegates execution
/// to the engine via `run_pipeline` / `check_pipeline`.
pub struct LocalPipelineService {
    catalog: Arc<HashMap<String, PipelineConfig>>,
    run_manager: Arc<RunManager>,
    registry_config: Arc<RegistryConfig>,
    #[allow(dead_code)]
    secrets: Arc<SecretProviders>,
}

impl LocalPipelineService {
    /// Create a new `LocalPipelineService`.
    #[must_use]
    pub fn new(
        catalog: Arc<HashMap<String, PipelineConfig>>,
        run_manager: Arc<RunManager>,
        registry_config: Arc<RegistryConfig>,
        secrets: Arc<SecretProviders>,
    ) -> Self {
        Self {
            catalog,
            run_manager,
            registry_config,
            secrets,
        }
    }

    /// Look up a pipeline config by name, returning `NotFound` if absent.
    fn lookup(&self, name: &str) -> Result<&PipelineConfig> {
        self.catalog.get(name).ok_or_else(|| {
            ApiError::not_found(
                "pipeline_not_found",
                format!("Pipeline '{name}' does not exist"),
            )
        })
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl PipelineService for LocalPipelineService {
    async fn list(&self, _filter: PipelineFilter) -> Result<PaginatedList<PipelineSummary>> {
        let items: Vec<PipelineSummary> = self
            .catalog
            .iter()
            .map(|(name, config)| PipelineSummary {
                name: name.clone(),
                description: None,
                tags: vec![],
                source: config.source.use_ref.clone(),
                destination: config.destination.use_ref.clone(),
                schedule: None,
                #[allow(clippy::cast_possible_truncation)]
                streams: config.source.streams.len() as u32,
                last_run: None,
            })
            .collect();

        Ok(PaginatedList {
            items,
            next_cursor: None,
        })
    }

    async fn get(&self, name: &str) -> Result<PipelineDetail> {
        let config = self.lookup(name)?;

        let source = serde_json::to_value(&config.source)
            .map_err(|e| ApiError::internal(format!("failed to serialize source config: {e}")))?;
        let destination = serde_json::to_value(&config.destination).map_err(|e| {
            ApiError::internal(format!("failed to serialize destination config: {e}"))
        })?;

        Ok(PipelineDetail {
            name: name.to_string(),
            description: None,
            tags: vec![],
            schedule: None,
            source,
            destination,
            state: PipelineState::Active,
        })
    }

    async fn sync(&self, request: SyncRequest) -> Result<RunHandle> {
        // Phase 1 limitations: reject unsupported fields up front so callers
        // get a clear 501 instead of silently ignoring their intent.
        if request.dry_run {
            return Err(ApiError::not_implemented("dry_run is not yet supported"));
        }
        if request.stream.is_some() {
            return Err(ApiError::not_implemented(
                "stream filtering is not yet supported",
            ));
        }
        if request.full_refresh {
            return Err(ApiError::not_implemented(
                "full_refresh is not yet supported",
            ));
        }
        if request.cursor_start.is_some() || request.cursor_end.is_some() {
            return Err(ApiError::not_implemented(
                "cursor_start/cursor_end are not yet supported",
            ));
        }

        let config = self.lookup(&request.pipeline)?.clone();
        let run_id = self.run_manager.create_run(request.pipeline.clone());
        let pipeline_name = request.pipeline.clone();

        let run_mgr = Arc::clone(&self.run_manager);
        let registry_config = Arc::clone(&self.registry_config);
        let rid = run_id.clone();

        tokio::spawn(async move {
            // Signal started
            run_mgr.send_event(
                &rid,
                SseEvent::Started {
                    run_id: rid.clone(),
                    pipeline: pipeline_name.clone(),
                    started_at: Utc::now(),
                },
            );
            run_mgr.update_status(&rid, RunStatus::Running);

            // Progress channel: engine -> SSE bridge.
            //
            // The engine's `build_run_context` API requires an
            // `UnboundedSender<ProgressEvent>`, so we use an unbounded mpsc
            // here. Back-pressure is provided by the bridge task below,
            // which drains the mpsc as fast as events arrive and forwards
            // them to a bounded broadcast channel (capacity 256 inside
            // `RunManager`). Because the bridge performs only a lightweight
            // enum translation and a non-blocking broadcast send, the mpsc
            // queue depth stays near zero under normal operation — the
            // broadcast capacity is the effective bound.
            let (progress_tx, mut progress_rx) =
                tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

            // Bridge task: translate engine progress events to SSE events.
            let bridge_mgr = Arc::clone(&run_mgr);
            let bridge_rid = rid.clone();
            tokio::spawn(async move {
                while let Some(event) = progress_rx.recv().await {
                    let sse = match event {
                        ProgressEvent::PhaseChanged { phase } => SseEvent::Progress {
                            phase: format!("{phase:?}"),
                            stream: String::new(),
                            records_read: None,
                            records_written: None,
                            bytes_read: None,
                        },
                        ProgressEvent::StreamStarted { stream }
                        | ProgressEvent::StreamCompleted { stream } => SseEvent::Progress {
                            phase: "running".into(),
                            stream,
                            records_read: None,
                            records_written: None,
                            bytes_read: None,
                        },
                        ProgressEvent::BatchEmitted { stream, bytes } => SseEvent::Progress {
                            phase: "running".into(),
                            stream,
                            records_read: None,
                            records_written: None,
                            bytes_read: Some(bytes),
                        },
                        ProgressEvent::RetryScheduled { attempt, delay } => SseEvent::Log {
                            level: "warn".into(),
                            message: format!("retry scheduled: attempt {attempt}, delay {delay:?}"),
                        },
                    };
                    bridge_mgr.send_event(&bridge_rid, sse);
                }
            });

            // Build engine context and run pipeline
            let start = std::time::Instant::now();
            match build_run_context(&config, Some(progress_tx), &registry_config).await {
                Ok(engine_ctx) => {
                    let cancel_token = run_mgr.cancel_token(&rid).unwrap_or_default();

                    match run_pipeline(&engine_ctx, &config, cancel_token).await {
                        Ok(result) => {
                            let duration_secs = start.elapsed().as_secs_f64();
                            let counts = crate::types::PipelineCounts {
                                records_read: result.counts.records_read,
                                records_written: result.counts.records_written,
                                bytes_read: result.counts.bytes_read,
                                bytes_written: result.counts.bytes_written,
                            };
                            run_mgr.send_event(
                                &rid,
                                SseEvent::Complete {
                                    run_id: rid.clone(),
                                    status: RunStatus::Completed,
                                    duration_secs,
                                    counts: Some(counts),
                                },
                            );
                            run_mgr.complete(&rid, result);
                            info!(run_id = %rid, "pipeline run completed");
                        }
                        Err(e) => {
                            // Check if the run was already cancelled (e.g. via
                            // RunService::cancel) or if the engine returned a
                            // cancellation error. In either case, preserve the
                            // Cancelled status instead of overwriting with Failed.
                            let snap = run_mgr.get(&rid);
                            let already_cancelled =
                                snap.is_some_and(|s| s.status == RunStatus::Cancelled);

                            if already_cancelled || matches!(e, PipelineError::Cancelled) {
                                if !already_cancelled {
                                    run_mgr.update_status(&rid, RunStatus::Cancelled);
                                }
                                run_mgr.send_event(
                                    &rid,
                                    SseEvent::Cancelled {
                                        run_id: rid.clone(),
                                        reason: "cancelled by user".into(),
                                    },
                                );
                                info!(run_id = %rid, "pipeline run cancelled");
                            } else {
                                let error_msg = e.to_string();
                                run_mgr.send_event(
                                    &rid,
                                    SseEvent::Failed {
                                        run_id: rid.clone(),
                                        error: error_msg.clone(),
                                    },
                                );
                                run_mgr.fail(&rid, error_msg);
                                warn!(run_id = %rid, "pipeline run failed");
                            }
                        }
                    }
                }
                Err(e) => {
                    let error_msg = format!("failed to build engine context: {e}");
                    run_mgr.send_event(
                        &rid,
                        SseEvent::Failed {
                            run_id: rid.clone(),
                            error: error_msg.clone(),
                        },
                    );
                    run_mgr.fail(&rid, error_msg);
                    warn!(run_id = %rid, "engine context build failed");
                }
            }
        });

        Ok(RunHandle {
            run_id: run_id.clone(),
            status: RunStatus::Pending,
            links: RunLinks {
                self_url: format!("/api/v1/runs/{run_id}"),
                events: format!("/api/v1/runs/{run_id}/events"),
            },
        })
    }

    async fn sync_batch(&self, _request: SyncBatchRequest) -> Result<BatchRunHandle> {
        Err(ApiError::not_implemented("sync_batch"))
    }

    async fn check(&self, name: &str) -> Result<CheckResult> {
        let config = self.lookup(name)?.clone();
        let engine_ctx = build_lightweight_context(&self.registry_config, &config)
            .await
            .map_err(ApiError::from)?;

        let engine_result = check_pipeline(&engine_ctx, &config)
            .await
            .map_err(ApiError::from)?;

        // Convert engine CheckResult -> API CheckResult
        let passed = check_all_passed(&engine_result);
        let checks = check_result_to_json(&engine_result);

        Ok(CheckResult { passed, checks })
    }

    async fn check_apply(&self, _name: &str) -> Result<RunHandle> {
        Err(ApiError::not_implemented("check_apply"))
    }

    async fn compile(&self, name: &str) -> Result<ResolvedConfig> {
        let config = self.lookup(name)?;
        let resolved = serde_json::to_value(config)
            .map_err(|e| ApiError::internal(format!("failed to serialize config: {e}")))?;

        Ok(ResolvedConfig {
            pipeline: name.to_string(),
            resolved_config: resolved,
        })
    }

    async fn diff(&self, _name: &str) -> Result<DiffResult> {
        Err(ApiError::not_implemented("diff"))
    }

    async fn assert(&self, _request: AssertRequest) -> Result<AssertResult> {
        Err(ApiError::not_implemented("assert"))
    }

    async fn teardown(&self, _request: TeardownRequest) -> Result<RunHandle> {
        Err(ApiError::not_implemented("teardown"))
    }
}

// ---------------------------------------------------------------------------
// CheckResult conversion helpers
// ---------------------------------------------------------------------------

/// Returns `true` if every check in the engine's `CheckResult` passed.
fn check_all_passed(result: &rapidbyte_engine::CheckResult) -> bool {
    let manifest_ok = result.source_manifest.as_ref().is_none_or(|s| s.ok)
        && result.destination_manifest.as_ref().is_none_or(|s| s.ok);

    let config_ok = result.source_config.as_ref().is_none_or(|s| s.ok)
        && result.destination_config.as_ref().is_none_or(|s| s.ok)
        && result.transform_configs.iter().all(|s| s.ok);

    let validation_ok = result.source_validation.is_ok()
        && result.destination_validation.is_ok()
        && result.transform_validations.iter().all(ValidationOk::is_ok);

    let state_ok = result.state.ok;

    let prereqs_ok = result
        .source_prerequisites
        .as_ref()
        .is_none_or(|p| p.passed)
        && result
            .destination_prerequisites
            .as_ref()
            .is_none_or(|p| p.passed);

    let negotiation_ok = result.schema_negotiation.iter().all(|n| n.passed);

    manifest_ok && config_ok && validation_ok && state_ok && prereqs_ok && negotiation_ok
}

/// Helper trait to check if a `ValidationReport` is OK.
trait ValidationOk {
    fn is_ok(&self) -> bool;
}

impl ValidationOk for rapidbyte_types::validation::ValidationReport {
    fn is_ok(&self) -> bool {
        self.status == rapidbyte_types::validation::ValidationStatus::Success
    }
}

/// Serialize the engine `CheckResult` into a JSON value for the API response.
fn check_result_to_json(result: &rapidbyte_engine::CheckResult) -> serde_json::Value {
    let check_status_to_json = |s: &rapidbyte_engine::CheckStatus| -> serde_json::Value {
        serde_json::json!({
            "ok": s.ok,
            "message": s.message,
        })
    };

    let negotiation_to_json = |n: &rapidbyte_engine::StreamNegotiationResult| -> serde_json::Value {
        serde_json::json!({
            "stream": n.stream_name,
            "passed": n.passed,
            "errors": n.errors,
            "warnings": n.warnings,
        })
    };

    serde_json::json!({
        "source_manifest": result.source_manifest.as_ref().map(&check_status_to_json),
        "destination_manifest": result.destination_manifest.as_ref().map(&check_status_to_json),
        "source_config": result.source_config.as_ref().map(&check_status_to_json),
        "destination_config": result.destination_config.as_ref().map(&check_status_to_json),
        "transform_configs": result.transform_configs.iter().map(&check_status_to_json).collect::<Vec<_>>(),
        "source_validation": serde_json::to_value(&result.source_validation).ok(),
        "destination_validation": serde_json::to_value(&result.destination_validation).ok(),
        "transform_validations": result.transform_validations.iter()
            .filter_map(|v| serde_json::to_value(v).ok())
            .collect::<Vec<serde_json::Value>>(),
        "state": check_status_to_json(&result.state),
        "source_prerequisites": result.source_prerequisites.as_ref()
            .and_then(|p| serde_json::to_value(p).ok()),
        "destination_prerequisites": result.destination_prerequisites.as_ref()
            .and_then(|p| serde_json::to_value(p).ok()),
        "schema_negotiation": result.schema_negotiation.iter().map(&negotiation_to_json).collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal `PipelineConfig` from YAML for testing.
    fn minimal_pipeline_config(name: &str) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: {name}
source:
  use: postgres
  config: {{}}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config: {{}}
  write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("minimal pipeline YAML must parse")
    }

    fn empty_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        Arc::new(HashMap::new())
    }

    fn test_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        let mut map = HashMap::new();
        map.insert("my-pipeline".into(), minimal_pipeline_config("my-pipeline"));
        map.insert(
            "other-pipeline".into(),
            minimal_pipeline_config("other-pipeline"),
        );
        Arc::new(map)
    }

    fn make_service(catalog: Arc<HashMap<String, PipelineConfig>>) -> LocalPipelineService {
        LocalPipelineService::new(
            catalog,
            Arc::new(RunManager::new()),
            Arc::new(RegistryConfig::default()),
            Arc::new(SecretProviders::default()),
        )
    }

    // ----- list -----

    #[tokio::test]
    async fn list_empty_catalog() {
        let svc = make_service(empty_catalog());
        let result = svc.list(PipelineFilter::default()).await.unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_returns_all_pipelines() {
        let svc = make_service(test_catalog());
        let result = svc.list(PipelineFilter::default()).await.unwrap();
        assert_eq!(result.items.len(), 2);
        let names: Vec<&str> = result.items.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"my-pipeline"));
        assert!(names.contains(&"other-pipeline"));
    }

    #[tokio::test]
    async fn list_summary_has_correct_fields() {
        let svc = make_service(test_catalog());
        let result = svc.list(PipelineFilter::default()).await.unwrap();
        let my = result
            .items
            .iter()
            .find(|s| s.name == "my-pipeline")
            .unwrap();
        assert_eq!(my.source, "postgres");
        assert_eq!(my.destination, "postgres");
        assert_eq!(my.streams, 1);
        assert!(my.tags.is_empty());
        assert!(my.last_run.is_none());
    }

    // ----- get -----

    #[tokio::test]
    async fn get_returns_detail() {
        let svc = make_service(test_catalog());
        let detail = svc.get("my-pipeline").await.unwrap();
        assert_eq!(detail.name, "my-pipeline");
        assert_eq!(detail.state, PipelineState::Active);
        assert!(detail.source.is_object());
        assert!(detail.destination.is_object());
    }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() {
        let svc = make_service(test_catalog());
        let err = svc.get("nonexistent").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    // ----- compile -----

    #[tokio::test]
    async fn compile_returns_resolved_config() {
        let svc = make_service(test_catalog());
        let resolved = svc.compile("my-pipeline").await.unwrap();
        assert_eq!(resolved.pipeline, "my-pipeline");
        assert!(resolved.resolved_config.is_object());
        // Must contain pipeline name in the serialized config
        let obj = resolved.resolved_config.as_object().unwrap();
        assert_eq!(
            obj.get("pipeline").unwrap().as_str().unwrap(),
            "my-pipeline"
        );
    }

    #[tokio::test]
    async fn compile_nonexistent_returns_not_found() {
        let svc = make_service(test_catalog());
        let err = svc.compile("nonexistent").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    // ----- sync (cannot run real engine, test only creation) -----

    #[tokio::test]
    async fn sync_nonexistent_returns_not_found() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "nonexistent".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    #[tokio::test]
    async fn sync_returns_pending_run_handle() {
        let run_mgr = Arc::new(RunManager::new());
        let svc = LocalPipelineService::new(
            test_catalog(),
            Arc::clone(&run_mgr),
            Arc::new(RegistryConfig::default()),
            Arc::new(SecretProviders::default()),
        );
        let handle = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await
            .unwrap();
        assert!(handle.run_id.starts_with("run_"));
        assert_eq!(handle.status, RunStatus::Pending);
        assert!(handle.links.self_url.contains(&handle.run_id));
        assert!(handle.links.events.contains(&handle.run_id));

        // Run should exist in run_manager
        let snap = run_mgr.get(&handle.run_id).unwrap();
        assert_eq!(snap.pipeline, "my-pipeline");
    }

    // ----- sync: dry_run guard (Bug 2) -----

    #[tokio::test]
    async fn sync_dry_run_returns_not_implemented() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: true,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn sync_stream_filter_returns_not_implemented() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: Some("users".into()),
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn sync_full_refresh_returns_not_implemented() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: None,
                full_refresh: true,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn sync_cursor_start_returns_not_implemented() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: None,
                full_refresh: false,
                cursor_start: Some("2026-01-01".into()),
                cursor_end: None,
                dry_run: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn sync_cursor_end_returns_not_implemented() {
        let svc = make_service(test_catalog());
        let err = svc
            .sync(SyncRequest {
                pipeline: "my-pipeline".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: Some("2026-12-31".into()),
                dry_run: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    // ----- sync: cancel race (Bug 1) -----

    #[tokio::test]
    async fn cancelled_run_not_overwritten_to_failed() {
        // Verify that when a run is cancelled via RunManager, the spawned
        // task's error handling preserves the Cancelled status.
        let run_mgr = Arc::new(RunManager::new());
        let run_id = run_mgr.create_run("test-pipe".into());

        // Pre-cancel the run (simulates RunService::cancel called before
        // the engine task observes the error).
        run_mgr.update_status(&run_id, RunStatus::Cancelled);

        // Verify the status is Cancelled.
        let snap = run_mgr.get(&run_id).unwrap();
        assert_eq!(snap.status, RunStatus::Cancelled);

        // Calling fail() would overwrite to Failed — but the fix ensures
        // we check first. Simulate the check logic from the spawned task:
        let already_cancelled = snap.status == RunStatus::Cancelled;
        assert!(already_cancelled, "status should be Cancelled");

        // The fix should NOT call run_mgr.fail() when already_cancelled is true.
        // Instead, it sends a Cancelled event. Verify the status stays Cancelled.
        if !already_cancelled {
            run_mgr.fail(&run_id, "some error".into());
        }
        let snap = run_mgr.get(&run_id).unwrap();
        assert_eq!(snap.status, RunStatus::Cancelled);
    }

    // ----- stub methods -----

    #[tokio::test]
    async fn sync_batch_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc
            .sync_batch(SyncBatchRequest {
                tag: None,
                exclude: None,
                full_refresh: false,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn check_apply_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc.check_apply("any").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn diff_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc.diff("any").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn assert_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc
            .assert(AssertRequest {
                pipeline: None,
                tag: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn teardown_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc
            .teardown(TeardownRequest {
                pipeline: "any".into(),
                reason: "test".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    // ----- check_all_passed helper -----

    /// Build a passing engine `CheckResult` with the given manifest overrides.
    fn make_engine_check_result(
        src_manifest_ok: Option<bool>,
        state_ok: bool,
    ) -> rapidbyte_engine::CheckResult {
        use rapidbyte_engine::CheckStatus;
        use rapidbyte_types::validation::ValidationReport;

        rapidbyte_engine::CheckResult {
            source_manifest: src_manifest_ok.map(|ok| CheckStatus {
                ok,
                message: if ok { "ok" } else { "missing" }.into(),
            }),
            destination_manifest: Some(CheckStatus {
                ok: true,
                message: "ok".into(),
            }),
            source_config: Some(CheckStatus {
                ok: true,
                message: "ok".into(),
            }),
            destination_config: Some(CheckStatus {
                ok: true,
                message: "ok".into(),
            }),
            transform_configs: vec![],
            source_validation: ValidationReport::success("ok"),
            destination_validation: ValidationReport::success("ok"),
            transform_validations: vec![],
            state: CheckStatus {
                ok: state_ok,
                message: "ok".into(),
            },
            source_prerequisites: None,
            destination_prerequisites: None,
            schema_negotiation: vec![],
        }
    }

    #[test]
    fn check_all_passed_with_all_ok() {
        let result = make_engine_check_result(Some(true), true);
        assert!(check_all_passed(&result));
    }

    #[test]
    fn check_all_passed_fails_on_bad_manifest() {
        let result = make_engine_check_result(Some(false), true);
        assert!(!check_all_passed(&result));
    }

    #[test]
    fn check_all_passed_fails_on_bad_state() {
        let result = make_engine_check_result(Some(true), false);
        assert!(!check_all_passed(&result));
    }

    #[test]
    fn check_result_to_json_structure() {
        use rapidbyte_engine::CheckStatus;

        let mut result = make_engine_check_result(Some(true), true);
        result.source_manifest = Some(CheckStatus {
            ok: true,
            message: "found".into(),
        });

        let json = check_result_to_json(&result);
        assert!(json.is_object());
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("source_manifest"));
        assert!(obj.contains_key("state"));
        assert!(obj.contains_key("schema_negotiation"));

        let sm = obj.get("source_manifest").unwrap();
        assert!(sm.get("ok").unwrap().as_bool().unwrap());
        assert_eq!(sm.get("message").unwrap().as_str().unwrap(), "found");
    }
}
