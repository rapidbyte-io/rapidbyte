use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use rapidbyte_controller::adapter::rest::extractors::RestState;
use rapidbyte_controller::adapter::rest::router;
use rapidbyte_controller::application::services::AppServices;
use rapidbyte_controller::application::testing::fake_context;
use rapidbyte_controller::config::AuthConfig;

fn test_app() -> axum::Router {
    let tc = fake_context();
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

fn test_app_with_auth() -> axum::Router {
    let tc = fake_context();
    let listen_addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
    // Override allow_unauthenticated so auth_required is reflected correctly
    let mut ctx = tc.ctx;
    ctx.config.allow_unauthenticated = false;
    let services = Arc::new(AppServices::new(
        Arc::new(ctx),
        chrono::Utc::now(),
        listen_addr,
    ));
    let auth_config = AuthConfig {
        tokens: vec!["test-token".to_string()],
        allow_unauthenticated: false,
        ..Default::default()
    };
    let state = RestState {
        services,
        auth_config,
    };
    router(state)
}

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
