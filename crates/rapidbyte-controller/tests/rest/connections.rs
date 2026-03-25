use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::{parse_json, test_app};

#[tokio::test]
async fn list_connections_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections")
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
async fn get_connection_returns_not_found() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "connection_not_found");
}

#[tokio::test]
async fn test_connection_returns_not_found() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/connections/nonexistent/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "connection_not_found");
}

#[tokio::test]
async fn discover_connection_returns_not_found() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections/nonexistent/discover")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "connection_not_found");
}
