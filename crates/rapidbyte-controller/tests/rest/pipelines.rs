use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::{parse_json, test_app, test_app_with_pipeline};

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
async fn sync_known_pipeline_returns_202() {
    // test_app_with_pipeline pre-loads "test-pipeline" into FakePipelineSource.
    let app = test_app_with_pipeline("test-pipeline");
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
    assert!(body["run_id"].is_string(), "expected run_id in body");
    assert_eq!(body["status"], "pending");
}

#[tokio::test]
async fn sync_unknown_pipeline_returns_404() {
    // Default test_app uses an empty FakePipelineSource — no pipelines registered.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/nonexistent/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
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
