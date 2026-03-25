use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::parse_json;
use super::helpers::test_app;

#[tokio::test]
async fn list_runs_returns_empty_paginated_list() {
    let app = test_app();
    let resp = app
        .oneshot(Request::get("/api/v1/runs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["next_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn get_run_not_found_returns_404() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/runs/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "run_not_found");
}

#[tokio::test]
async fn cancel_not_found_returns_404() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/runs/nonexistent/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_runs_invalid_status_returns_422() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/runs?status=bogus")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "validation_failed");
    assert_eq!(body["error"]["details"][0]["field"], "status");
}
