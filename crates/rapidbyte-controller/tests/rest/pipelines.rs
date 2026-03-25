use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::{parse_json, test_app};

#[tokio::test]
async fn list_pipelines_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["next_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn list_pipelines_with_tag_query_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines?tag=production")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["items"], serde_json::json!([]));
}

#[tokio::test]
async fn get_pipeline_returns_501_not_implemented() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // get() stub returns ServiceError::NotImplemented
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}

#[tokio::test]
async fn sync_pipeline_with_plain_name_returns_202() {
    // The current PipelineService::sync passes the pipeline name as YAML to
    // submit_pipeline. A plain string like "test-pipeline" is valid YAML;
    // extract_pipeline_name falls back to "unknown" when no `pipeline:` key
    // is present. The submit succeeds and returns 202 Accepted.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/test-pipeline/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = parse_json(resp).await;
    assert!(!body["run_id"].as_str().unwrap_or("").is_empty());
    assert_eq!(body["status"], "pending");
}

#[tokio::test]
async fn sync_pipeline_returns_202() {
    // Use URL-encoded YAML as the pipeline name path segment so submit_pipeline
    // can extract a valid pipeline name from it.
    //
    // Axum decodes %XX sequences in path params, so
    // "pipeline%3A%20test%0Aversion%3A%20'1.0'" becomes "pipeline: test\nversion: '1.0'",
    // which is valid YAML with a `pipeline:` key.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/pipeline%3A%20test%0Aversion%3A%20'1.0'/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = parse_json(resp).await;
    let run_id = body["run_id"].as_str().unwrap_or("");
    assert!(!run_id.is_empty());
    assert_eq!(body["status"], "pending");
    // links populated by the REST handler
    assert_eq!(
        body["links"]["self"],
        serde_json::json!(format!("/api/v1/runs/{run_id}"))
    );
    assert_eq!(
        body["links"]["events"],
        serde_json::json!(format!("/api/v1/runs/{run_id}/events"))
    );
}

#[tokio::test]
async fn check_pipeline_returns_501_not_implemented() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipeline/check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // check() stub returns ServiceError::NotImplemented
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}

#[tokio::test]
async fn compile_pipeline_returns_501_not_implemented() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/compiled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}

#[tokio::test]
async fn diff_pipeline_returns_501_not_implemented() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}
