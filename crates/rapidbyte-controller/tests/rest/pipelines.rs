use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use rapidbyte_controller::application::testing::{fake_context, FakePipelineSource};

use super::helpers::{parse_json, test_app, test_app_with_pipeline};

fn test_app_with_two_pipelines() -> axum::Router {
    use std::net::SocketAddr;

    use rapidbyte_controller::adapter::rest::extractors::RestState;
    use rapidbyte_controller::adapter::rest::router;
    use rapidbyte_controller::application::services::AppServices;
    use rapidbyte_controller::config::AuthConfig;

    let mut tc = fake_context();
    tc.ctx.pipeline_source = Arc::new(
        FakePipelineSource::new()
            .with_pipeline("pipe-a", "pipeline: pipe-a\nversion: '1.0'")
            .with_pipeline("pipe-b", "pipeline: pipe-b\nversion: '1.0'"),
    );
    let listen_addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
    let services = Arc::new(AppServices::new(
        Arc::new(tc.ctx),
        chrono::Utc::now(),
        listen_addr,
    ));
    let auth_config = AuthConfig {
        tokens: vec![],
        allow_unauthenticated: true,
        ..Default::default()
    };
    let state = RestState {
        services,
        auth_config,
    };
    router(state)
}

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
async fn get_pipeline_returns_404_not_found() {
    // Default test_app uses an empty FakePipelineSource — no pipelines registered.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
}

#[tokio::test]
async fn get_pipeline_known_returns_200() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["name"], "my-pipeline");
    assert_eq!(body["state"], "active");
}

#[tokio::test]
async fn list_pipelines_with_data_returns_items() {
    let app = test_app_with_two_pipelines();
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
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
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
async fn check_pipeline_returns_404_when_not_found() {
    // Default test_app uses an empty FakePipelineSource — no pipelines registered.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipeline/check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
}

#[tokio::test]
async fn check_known_pipeline_returns_200() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipeline/check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["passed"], true);
    assert!(body["checks"].is_object());
}

#[tokio::test]
async fn compile_pipeline_unknown_returns_404() {
    // Default test_app uses an empty FakePipelineSource — no pipelines registered.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/compiled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
}

#[tokio::test]
async fn compile_pipeline_known_returns_200() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/compiled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "my-pipeline");
    assert!(body["resolved_config"].is_object());
}

#[tokio::test]
async fn diff_unknown_pipeline_returns_404() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
}

#[tokio::test]
async fn diff_known_pipeline_returns_200() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines/my-pipeline/diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert!(body["streams"].is_array());
}

#[tokio::test]
async fn teardown_known_pipeline_returns_202() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipeline/teardown")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"test teardown"}"#))
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
async fn teardown_unknown_pipeline_returns_404() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/nonexistent/teardown")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"test teardown"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "pipeline_not_found");
}

#[tokio::test]
async fn assert_known_pipeline_returns_200() {
    let app = test_app_with_pipeline("my-pipeline");
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipeline/assert")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    // Assertions are async: passed is false (unknown/pending) and results
    // contains a pending entry with the run_id to track execution.
    assert_eq!(body["passed"], false);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["status"], "pending");
    assert!(results[0]["run_id"].is_string());
}

#[tokio::test]
async fn sync_batch_returns_202() {
    let app = test_app_with_two_pipelines();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/sync")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = parse_json(resp).await;
    assert!(body["batch_id"].is_string(), "expected batch_id in body");
    assert_eq!(body["runs"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn sync_batch_with_exclude_skips_pipeline() {
    let app = test_app_with_two_pipelines();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"exclude":["pipe-b"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body = parse_json(resp).await;
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["pipeline"], "pipe-a");
}
