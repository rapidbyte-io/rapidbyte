use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::helpers::{test_app, test_app_with_auth};

#[tokio::test]
async fn health_returns_200_without_auth() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["status"], "healthy");
    assert!(body["uptime_secs"].is_number());
}

#[tokio::test]
async fn version_returns_401_without_token() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], "unauthorized");
}

#[tokio::test]
async fn version_returns_200_with_valid_token() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/version")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body["version"].is_string());
}

#[tokio::test]
async fn config_returns_auth_required_flag() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/config")
                .header("authorization", "Bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["auth_required"], true);
}

#[tokio::test]
async fn lowercase_bearer_accepted_e2e() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/version")
                .header("authorization", "bearer test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn mixed_case_bearer_accepted_e2e() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/server/version")
                .header("authorization", "BEARER test-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn runs_endpoint_requires_auth() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(Request::get("/api/v1/runs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn pipelines_endpoint_requires_auth() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn connections_endpoint_requires_auth() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn plugins_endpoint_requires_auth() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(Request::get("/api/v1/plugins").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn status_endpoint_requires_auth() {
    let app = test_app_with_auth();
    let resp = app
        .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
