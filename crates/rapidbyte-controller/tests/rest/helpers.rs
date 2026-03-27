use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::Response;
use http_body_util::BodyExt;

use rapidbyte_controller::adapter::rest::extractors::RestState;
use rapidbyte_controller::adapter::rest::router;
use rapidbyte_controller::application::services::AppServices;
use rapidbyte_controller::application::testing::{fake_context, FakePipelineSource};
use rapidbyte_controller::config::AuthConfig;

pub fn test_app() -> axum::Router {
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

/// Build a test app whose `PipelineSource` is pre-loaded with `pipeline_name`
/// so that `POST /api/v1/pipelines/{name}/sync` resolves successfully.
pub fn test_app_with_pipeline(pipeline_name: &str) -> axum::Router {
    let mut tc = fake_context();
    let yaml = format!("pipeline: {pipeline_name}\nversion: '1.0'");
    tc.ctx.pipeline_source =
        Arc::new(FakePipelineSource::new().with_pipeline(pipeline_name, &yaml));
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

pub fn test_app_with_auth() -> axum::Router {
    let tc = fake_context();
    let listen_addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
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

pub async fn parse_json(resp: Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}
