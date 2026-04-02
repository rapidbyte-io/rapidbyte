use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use rapidbyte_controller::adapter::rest::extractors::RestState;
use rapidbyte_controller::adapter::rest::router;
use rapidbyte_controller::application::services::AppServices;
use rapidbyte_controller::application::testing::{fake_context, FakePipelineSource};
use rapidbyte_controller::config::AuthConfig;

use super::helpers::{parse_json, test_app};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CONNECTIONS_YAML: &str = "\
connections:
  my_pg:
    use: postgres
    config:
      host: localhost
      port: 5432
      password: hunter2
  my_redis:
    use: redis
    config:
      host: redis-host
";

fn test_app_with_connections() -> axum::Router {
    let mut tc = fake_context();
    tc.ctx.pipeline_source =
        Arc::new(FakePipelineSource::new().with_connections_yaml(CONNECTIONS_YAML));
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

// ---------------------------------------------------------------------------
// Tests — no connections.yml (default fake)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests — with connections.yml content
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_connections_returns_entries_from_yaml() {
    let app = test_app_with_connections();
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
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    // Names may arrive in any order; collect them for assertion.
    let names: Vec<&str> = items.iter().map(|i| i["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"my_pg"));
    assert!(names.contains(&"my_redis"));
}

#[tokio::test]
async fn get_connection_returns_detail_with_redacted_password() {
    let app = test_app_with_connections();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections/my_pg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["name"], "my_pg");
    assert_eq!(body["connector"], "postgres");
    assert_eq!(body["config"]["config"]["password"], "***REDACTED***");
    assert_eq!(body["config"]["config"]["host"], "localhost");
}

#[tokio::test]
async fn get_connection_not_found_in_yaml() {
    let app = test_app_with_connections();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections/does_not_exist")
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
async fn test_connection_succeeds_with_fake_tester() {
    let app = test_app_with_connections();
    let resp = app
        .oneshot(
            Request::post("/api/v1/connections/my_pg/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["name"], "my_pg");
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn discover_connection_succeeds_with_fake_tester() {
    let app = test_app_with_connections();
    let resp = app
        .oneshot(
            Request::get("/api/v1/connections/my_pg/discover")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["connection"], "my_pg");
    assert!(body["streams"].is_array());
}
