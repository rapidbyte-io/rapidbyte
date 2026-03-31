use async_trait::async_trait;

use crate::application::submit;
use crate::domain::ports::pipeline_source::PipelineSourceError;
use crate::domain::task::TaskOperation;

fn source_error_to_service(e: PipelineSourceError) -> ServiceError {
    match e {
        PipelineSourceError::NotFound(name) => ServiceError::NotFound {
            resource: "pipeline".into(),
            id: name,
        },
        other => ServiceError::Internal {
            message: other.to_string(),
        },
    }
}
use crate::traits::pipeline::{
    AssertRequest, AssertResult, BatchRunHandle, BatchRunRef, CheckResult, DiffResult,
    PipelineDetail, PipelineFilter, PipelineService, PipelineSummary, ResolvedConfig, RunHandle,
    StreamDiff, SyncBatchRequest, SyncRequest, TeardownRequest,
};
use crate::traits::{PaginatedList, ServiceError};

use super::{app_error_to_service, AppServices};

#[async_trait]
impl PipelineService for AppServices {
    async fn list(
        &self,
        filter: PipelineFilter,
    ) -> Result<PaginatedList<PipelineSummary>, ServiceError> {
        // Reject unsupported filter options so callers get a clear error.
        if filter.tag.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "tag-filtered pipeline list".into(),
            });
        }
        if filter.cursor.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "cursor-paginated pipeline list".into(),
            });
        }

        let pipelines =
            self.ctx
                .pipeline_source
                .list()
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;

        // Apply limit: clamp to [1, 100]; a limit of 0 means "use default 20".
        let effective_limit = if filter.limit == 0 {
            20
        } else {
            filter.limit.clamp(1, 100)
        } as usize;

        let items: Vec<PipelineSummary> = pipelines
            .into_iter()
            .take(effective_limit)
            .map(|p| PipelineSummary {
                name: p.name,
                description: None,
                tags: vec![],
                source: String::new(),
                destination: String::new(),
                schedule: None,
                streams: 0,
            })
            .collect();

        Ok(PaginatedList {
            items,
            next_cursor: None,
        })
    }

    async fn get(&self, name: &str) -> Result<PipelineDetail, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .get(name)
            .await
            .map_err(|e| match e {
                PipelineSourceError::NotFound(n) => ServiceError::NotFound {
                    resource: "pipeline".into(),
                    id: n,
                },
                other => ServiceError::Internal {
                    message: other.to_string(),
                },
            })?;

        let value: serde_yaml::Value =
            serde_yaml::from_str(&yaml).map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(PipelineDetail {
            name: name.to_string(),
            description: value
                .get("description")
                .and_then(serde_yaml::Value::as_str)
                .map(String::from),
            tags: vec![],
            schedule: value
                .get("schedule")
                .and_then(serde_yaml::Value::as_str)
                .map(String::from),
            source: serde_json::to_value(value.get("source")).unwrap_or_default(),
            destination: serde_json::to_value(value.get("destination")).unwrap_or_default(),
            state: "active".into(),
        })
    }

    async fn sync(&self, request: SyncRequest) -> Result<RunHandle, ServiceError> {
        // Reject unsupported options that would change execution behavior.
        if request.dry_run {
            return Err(ServiceError::NotImplemented {
                feature: "dry run mode".into(),
            });
        }
        if request.stream.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "single stream sync".into(),
            });
        }
        if request.cursor_start.is_some() || request.cursor_end.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "cursor range sync".into(),
            });
        }
        if request.full_refresh {
            return Err(ServiceError::NotImplemented {
                feature: "full refresh sync".into(),
            });
        }

        // Resolve pipeline YAML from PipelineSource
        let yaml = self
            .ctx
            .pipeline_source
            .get(&request.pipeline)
            .await
            .map_err(|e| match e {
                PipelineSourceError::NotFound(name) => ServiceError::NotFound {
                    resource: "pipeline".into(),
                    id: name,
                },
                other => ServiceError::Internal {
                    message: other.to_string(),
                },
            })?;

        // Submit as async task
        let result = submit::submit_pipeline(
            &self.ctx,
            None, // no idempotency key from REST
            yaml,
            0,    // default max_retries
            None, // no timeout
            TaskOperation::Sync,
            None,
        )
        .await
        .map_err(app_error_to_service)?;

        Ok(RunHandle {
            run_id: result.run_id,
            status: "pending".into(),
            links: None,
        })
    }

    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle, ServiceError> {
        if request.full_refresh {
            return Err(ServiceError::NotImplemented {
                feature: "full refresh batch sync".into(),
            });
        }
        if request.tag.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "tag-filtered batch sync".into(),
            });
        }

        let all = self
            .ctx
            .pipeline_source
            .list()
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let mut runs = Vec::new();
        let mut attempted = 0usize;
        for pipeline in &all {
            // Skip excluded pipelines — not a failure
            if let Some(ref exclude) = request.exclude {
                if exclude.contains(&pipeline.name) {
                    continue;
                }
            }

            attempted += 1;

            let yaml = match self.ctx.pipeline_source.get(&pipeline.name).await {
                Ok(y) => y,
                Err(e) => {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "skipping pipeline in batch sync due to error");
                    continue;
                }
            };
            let result = match submit::submit_pipeline(
                &self.ctx,
                None,
                yaml,
                0,
                None,
                TaskOperation::Sync,
                None,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(pipeline = %pipeline.name, error = %e, "failed to submit pipeline in batch sync");
                    continue;
                }
            };
            runs.push(BatchRunRef {
                pipeline: pipeline.name.clone(),
                run_id: result.run_id,
            });
        }

        // Only error if we attempted submissions and all of them failed.
        // An empty result because everything was excluded (or no pipelines exist) is fine.
        if runs.is_empty() && attempted > 0 {
            return Err(ServiceError::Internal {
                message: "all pipeline submissions failed".into(),
            });
        }

        Ok(BatchRunHandle {
            batch_id: format!("batch_{}", uuid::Uuid::new_v4()),
            runs,
            links: None,
        })
    }

    async fn check(&self, name: &str) -> Result<CheckResult, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .get(name)
            .await
            .map_err(source_error_to_service)?;

        let resolved =
            self.ctx
                .secrets
                .resolve(&yaml)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;

        let output = self
            .ctx
            .pipeline_inspector
            .check(&resolved)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(CheckResult {
            passed: output.passed,
            checks: output.checks,
        })
    }

    async fn check_apply(&self, name: &str) -> Result<RunHandle, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .get(name)
            .await
            .map_err(source_error_to_service)?;

        let result = submit::submit_pipeline(
            &self.ctx,
            None,
            yaml,
            0,
            None,
            TaskOperation::CheckApply,
            None,
        )
        .await
        .map_err(app_error_to_service)?;

        Ok(RunHandle {
            run_id: result.run_id,
            status: "pending".into(),
            links: None,
        })
    }

    async fn compile(&self, name: &str) -> Result<ResolvedConfig, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .get(name)
            .await
            .map_err(|e| match e {
                PipelineSourceError::NotFound(n) => ServiceError::NotFound {
                    resource: "pipeline".into(),
                    id: n,
                },
                other => ServiceError::Internal {
                    message: other.to_string(),
                },
            })?;

        let resolved =
            self.ctx
                .secrets
                .resolve(&yaml)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;

        let value: serde_json::Value =
            serde_yaml::from_str(&resolved).map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(ResolvedConfig {
            pipeline: name.to_string(),
            resolved_config: value,
        })
    }

    async fn diff(&self, name: &str) -> Result<DiffResult, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .get(name)
            .await
            .map_err(source_error_to_service)?;

        let resolved =
            self.ctx
                .secrets
                .resolve(&yaml)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;

        let output = self
            .ctx
            .pipeline_inspector
            .diff(&resolved)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(DiffResult {
            streams: output
                .streams
                .into_iter()
                .map(|s| StreamDiff {
                    stream_name: s.stream_name,
                    changes: s.changes,
                })
                .collect(),
        })
    }

    async fn assert(&self, request: AssertRequest) -> Result<AssertResult, ServiceError> {
        if request.tag.is_some() {
            return Err(ServiceError::NotImplemented {
                feature: "tag-filtered assertions".into(),
            });
        }

        // Determine which pipelines to assert
        let pipelines = if let Some(ref name) = request.pipeline {
            vec![name.clone()]
        } else {
            // No specific pipeline specified — assert all pipelines
            let all =
                self.ctx
                    .pipeline_source
                    .list()
                    .await
                    .map_err(|e| ServiceError::Internal {
                        message: e.to_string(),
                    })?;
            all.into_iter().map(|p| p.name).collect()
        };

        if pipelines.is_empty() {
            // No pipelines to assert — vacuously true
            return Ok(AssertResult {
                passed: true,
                results: vec![],
            });
        }

        let batch = request.pipeline.is_none();
        let mut results = Vec::new();
        for name in &pipelines {
            let yaml = match self.ctx.pipeline_source.get(name).await {
                Ok(y) => y,
                Err(e) if batch => {
                    tracing::warn!(pipeline = %name, error = %e, "skipping pipeline in batch assert due to error");
                    continue;
                }
                Err(e) => return Err(source_error_to_service(e)),
            };
            let result = match submit::submit_pipeline(
                &self.ctx,
                None,
                yaml,
                0,
                None,
                TaskOperation::Assert,
                None,
            )
            .await
            {
                Ok(r) => r,
                Err(e) if batch => {
                    tracing::warn!(pipeline = %name, error = %e, "failed to submit pipeline in batch assert");
                    continue;
                }
                Err(e) => return Err(app_error_to_service(e)),
            };
            results.push(serde_json::json!({
                "pipeline": name,
                "run_id": result.run_id,
                "status": "pending",
            }));
        }

        if results.is_empty() && batch && !pipelines.is_empty() {
            return Err(ServiceError::Internal {
                message: "all pipeline assert submissions failed".into(),
            });
        }

        Ok(AssertResult {
            passed: false, // unknown — execution pending
            results,
        })
    }

    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle, ServiceError> {
        tracing::info!(
            pipeline = %request.pipeline,
            reason = %request.reason,
            "teardown requested"
        );
        let yaml = self
            .ctx
            .pipeline_source
            .get(&request.pipeline)
            .await
            .map_err(source_error_to_service)?;

        let metadata = serde_json::json!({ "reason": request.reason });
        let result = submit::submit_pipeline(
            &self.ctx,
            None,
            yaml,
            0,
            None,
            TaskOperation::Teardown,
            Some(metadata),
        )
        .await
        .map_err(app_error_to_service)?;

        Ok(RunHandle {
            run_id: result.run_id,
            status: "pending".into(),
            links: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::application::services::AppServices;
    use crate::application::testing::{fake_app_services, fake_context, FakePipelineSource};

    #[tokio::test]
    async fn list_returns_empty() {
        let services = fake_app_services();

        let result = services.list(PipelineFilter::default()).await.unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_returns_pipelines_from_source() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("pipe-a", "pipeline: pipe-a\nversion: '1.0'")
                .with_pipeline("pipe-b", "pipeline: pipe-b\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.list(PipelineFilter::default()).await.unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_applies_limit() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("pipe-a", "pipeline: pipe-a\nversion: '1.0'")
                .with_pipeline("pipe-b", "pipeline: pipe-b\nversion: '1.0'")
                .with_pipeline("pipe-c", "pipeline: pipe-c\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services
            .list(PipelineFilter {
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_zero_limit_uses_default_20() {
        let mut tc = fake_context();
        // Add 25 pipelines
        let mut source = FakePipelineSource::new();
        for i in 0..25 {
            source = source.with_pipeline(
                &format!("pipe-{i}"),
                &format!("pipeline: pipe-{i}\nversion: '1.0'"),
            );
        }
        tc.ctx.pipeline_source = Arc::new(source);
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        // limit=0 means default of 20
        let result = services
            .list(PipelineFilter {
                limit: 0,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.items.len(), 20);
    }

    #[tokio::test]
    async fn list_tag_filter_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .list(PipelineFilter {
                tag: Some(vec!["production".into()]),
                ..Default::default()
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn list_cursor_filter_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .list(PipelineFilter {
                cursor: Some("some-cursor".into()),
                ..Default::default()
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn get_known_pipeline_returns_detail() {
        let yaml = "pipeline: pipe\nversion: '1.0'\nsource:\n  plugin: postgres\ndestination:\n  plugin: s3";
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(FakePipelineSource::new().with_pipeline("pipe", yaml));
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let detail = services.get("pipe").await.unwrap();
        assert_eq!(detail.name, "pipe");
        assert_eq!(detail.state, "active");
        // source and destination are populated from YAML
        assert!(detail.source.is_object() || !detail.source.is_null());
        assert!(detail.destination.is_object() || !detail.destination.is_null());
    }

    #[tokio::test]
    async fn get_unknown_pipeline_returns_not_found() {
        let services = fake_app_services();

        let result = services.get("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn compile_resolves_and_returns_json() {
        let yaml = "pipeline: pipe\nversion: '1.0'\nsource:\n  plugin: postgres";
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(FakePipelineSource::new().with_pipeline("pipe", yaml));
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.compile("pipe").await.unwrap();
        assert_eq!(result.pipeline, "pipe");
        assert!(result.resolved_config.is_object());
        assert_eq!(result.resolved_config["pipeline"], "pipe");
    }

    #[tokio::test]
    async fn sync_known_pipeline_returns_handle() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("test-pipe", "pipeline: test-pipe\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services
            .sync(SyncRequest {
                pipeline: "test-pipe".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let handle = result.unwrap();
        assert_eq!(handle.status, "pending");
        assert!(!handle.run_id.is_empty());
    }

    #[tokio::test]
    async fn sync_unknown_pipeline_returns_not_found() {
        // fake_context uses FakePipelineSource::new() — empty, no pipelines
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "nonexistent".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn check_known_pipeline_returns_passed() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("check-pipe", "pipeline: check-pipe\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.check("check-pipe").await.unwrap();
        assert!(result.passed);
        assert!(result.checks.is_object());
    }

    #[tokio::test]
    async fn check_unknown_pipeline_returns_not_found() {
        let services = fake_app_services();

        let result = services.check("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn check_apply_known_pipeline_returns_handle() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("apply-pipe", "pipeline: apply-pipe\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.check_apply("apply-pipe").await.unwrap();
        assert_eq!(result.status, "pending");
        assert!(!result.run_id.is_empty());
    }

    #[tokio::test]
    async fn check_apply_unknown_pipeline_returns_not_found() {
        let services = fake_app_services();

        let result = services.check_apply("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn sync_dry_run_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "any-pipe".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: true,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_stream_filter_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "any-pipe".into(),
                stream: Some("users".into()),
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_cursor_range_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "any-pipe".into(),
                stream: None,
                full_refresh: false,
                cursor_start: Some("2024-01-01".into()),
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_full_refresh_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "test-pipe".into(),
                stream: None,
                full_refresh: true,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_batch_full_refresh_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync_batch(SyncBatchRequest {
                tag: None,
                exclude: None,
                full_refresh: true,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_batch_tag_filter_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .sync_batch(SyncBatchRequest {
                tag: Some(vec!["production".into()]),
                exclude: None,
                full_refresh: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_batch_exclude_all_returns_empty_ok() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("pipe-a", "pipeline: pipe-a\nversion: '1.0'")
                .with_pipeline("pipe-b", "pipeline: pipe-b\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services
            .sync_batch(SyncBatchRequest {
                tag: None,
                exclude: Some(vec!["pipe-a".into(), "pipe-b".into()]),
                full_refresh: false,
            })
            .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let handle = result.unwrap();
        assert!(
            handle.runs.is_empty(),
            "expected empty runs when all pipelines excluded"
        );
    }

    #[tokio::test]
    async fn assert_tag_filter_returns_not_implemented() {
        let services = fake_app_services();

        let result = services
            .assert(AssertRequest {
                pipeline: None,
                tag: Some(vec!["production".into()]),
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }
}
