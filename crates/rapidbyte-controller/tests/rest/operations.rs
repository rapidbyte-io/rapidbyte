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
async fn pipeline_status_unknown_returns_200_with_empty_streams() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/status/unknown-pipeline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "unknown-pipeline");
    assert_eq!(body["state"], "active");
    assert_eq!(body["streams"], serde_json::json!([]));
}

#[tokio::test]
async fn pause_then_pipeline_status_shows_paused() {
    // Pause, then check status reflects the paused state
    let app = test_app();
    // First pause
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/v1/pipelines/my-pipe/pause")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Then check status
    let resp = app
        .oneshot(
            Request::get("/api/v1/status/my-pipe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["pipeline"], "my-pipe");
    assert_eq!(body["state"], "paused");
}

#[tokio::test]
async fn logs_limit_is_clamped() {
    // Requesting a very large limit should not cause issues —
    // the service clamps it to 100.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/logs?pipeline=test&limit=999999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    // Empty result but no error — limit was clamped, not rejected
    assert_eq!(body["items"], serde_json::json!([]));
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

#[tokio::test]
async fn logs_invalid_cursor_returns_200_empty() {
    // The FakeLogStore always returns empty, so an invalid cursor doesn't
    // reach the Postgres parser. This test confirms the endpoint doesn't
    // crash on malformed cursor values. The real PgLogStore cursor parsing
    // is covered by unit tests in adapter::postgres::log_store::tests.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/logs?pipeline=test&cursor=not-a-valid-cursor")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // FakeLogStore ignores cursor, returns empty — no crash
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn internal_error_does_not_leak_details() {
    // Verify that a 500 response uses a generic message, not the raw error.
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use rapidbyte_controller::adapter::rest::error::RestError;
    use rapidbyte_controller::traits::ServiceError;

    let err = RestError::from(ServiceError::Internal {
        message: "SELECT * FROM secret_table; connection string: postgres://user:pass@host/db"
            .into(),
    });
    let resp = err.into_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // Must NOT contain the raw error details
    assert_eq!(body["error"]["message"], "An internal error occurred");
    assert!(!body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("postgres://"));
}
