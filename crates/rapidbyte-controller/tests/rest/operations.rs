use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::parse_json;
use super::helpers::test_app;

#[tokio::test]
async fn status_returns_empty_list() {
    let app = test_app();
    let resp = app
        .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body, serde_json::json!([]));
}

#[tokio::test]
async fn pipeline_status_unknown_returns_404() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/status/unknown-pipeline")
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
async fn reset_pipeline_returns_result() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/test-pipeline/reset")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "test-pipeline");
    assert_eq!(body["cursors_cleared"], 0);
    assert_eq!(body["next_sync_mode"], "full_refresh");
}

#[tokio::test]
async fn reset_pipeline_with_stream_returns_stream_in_result() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipe/reset")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"stream":"orders"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "my-pipe");
    assert_eq!(body["streams_reset"], serde_json::json!(["orders"]));
}

#[tokio::test]
async fn pause_returns_paused_state() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipe/pause")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "my-pipe");
    assert_eq!(body["state"], "paused");
    assert!(body["paused_at"].is_string());
}

#[tokio::test]
async fn resume_returns_active_state() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipe/resume")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "my-pipe");
    assert_eq!(body["state"], "active");
    assert!(body["resumed_at"].is_string());
}

#[tokio::test]
async fn freshness_returns_empty_list() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/freshness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body, serde_json::json!([]));
}

#[tokio::test]
async fn logs_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/logs?pipeline=test")
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
