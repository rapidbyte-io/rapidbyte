pub mod error;
pub mod extract;
pub mod middleware;
pub mod routes;
pub mod sse;

use std::sync::Arc;

use axum::middleware as axum_middleware;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use rapidbyte_api::{ApiContext, ApiServerConfig, DeploymentMode};

/// Build the full axum router with all routes, auth middleware, and CORS.
pub fn build_router(ctx: ApiContext, auth_config: middleware::AuthConfig) -> Router {
    let cors = if auth_config.required {
        CorsLayer::new()
    } else {
        CorsLayer::permissive()
    };

    let auth_config = Arc::new(auth_config);

    let api = Router::new()
        // Pipelines
        .route("/pipelines", get(routes::pipelines::list))
        .route("/pipelines/sync", post(routes::pipelines::sync_batch))
        .route("/pipelines/assert", post(routes::pipelines::assert_all))
        .route("/pipelines/{name}", get(routes::pipelines::get))
        .route("/pipelines/{name}/sync", post(routes::pipelines::sync))
        .route("/pipelines/{name}/check", post(routes::pipelines::check))
        .route(
            "/pipelines/{name}/check-apply",
            post(routes::pipelines::check_apply),
        )
        .route(
            "/pipelines/{name}/compiled",
            get(routes::pipelines::compile),
        )
        .route("/pipelines/{name}/diff", get(routes::pipelines::diff))
        .route(
            "/pipelines/{name}/assert",
            post(routes::pipelines::assert_one),
        )
        .route(
            "/pipelines/{name}/teardown",
            post(routes::pipelines::teardown),
        )
        .route("/pipelines/{name}/pause", post(routes::operations::pause))
        .route("/pipelines/{name}/resume", post(routes::operations::resume))
        .route("/pipelines/{name}/reset", post(routes::operations::reset))
        // Runs
        .route("/runs", get(routes::runs::list))
        .route("/runs/{id}", get(routes::runs::get))
        .route("/runs/{id}/events", get(routes::runs::events))
        .route("/runs/{id}/cancel", post(routes::runs::cancel))
        // Batches
        .route("/batches/{id}", get(routes::runs::get_batch))
        .route("/batches/{id}/events", get(routes::runs::batch_events))
        // Connections
        .route("/connections", get(routes::connections::list))
        .route("/connections/{name}", get(routes::connections::get))
        .route("/connections/{name}/test", post(routes::connections::test))
        .route(
            "/connections/{name}/discover",
            get(routes::connections::discover),
        )
        // Plugins
        .route("/plugins", get(routes::plugins::list))
        .route("/plugins/search", get(routes::plugins::search))
        .route("/plugins/install", post(routes::plugins::install))
        .route(
            "/plugins/{org}/{name}",
            get(routes::plugins::info).delete(routes::plugins::remove),
        )
        // Operations
        .route("/status", get(routes::operations::status))
        .route("/status/{name}", get(routes::operations::status_detail))
        .route("/freshness", get(routes::operations::freshness))
        .route("/logs", get(routes::operations::logs))
        .route("/logs/stream", get(routes::operations::logs_stream))
        // Server
        .route("/server/health", get(routes::server::health))
        .route("/server/version", get(routes::server::version))
        .route("/server/config", get(routes::server::config))
        .layer(axum_middleware::from_fn_with_state(
            Arc::clone(&auth_config),
            middleware::auth::auth_middleware,
        ))
        .layer(cors)
        .with_state(Arc::new(ctx));

    Router::new().nest("/api/v1", api)
}

/// Execute the serve command — starts the REST API server.
pub async fn execute(
    listen: &str,
    allow_unauthenticated: bool,
    auth_token: Option<&str>,
    registry_config: rapidbyte_registry::RegistryConfig,
    secrets: rapidbyte_secrets::SecretProviders,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;

    let auth_required = !allow_unauthenticated;

    // Parse the port from the listen address (e.g., "0.0.0.0:8080" -> 8080).
    let port = listen
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(8080);

    let server_config = ApiServerConfig {
        port,
        auth_required,
    };

    let ctx = ApiContext::from_project(
        &cwd,
        DeploymentMode::Local,
        secrets,
        registry_config,
        server_config,
    )
    .await?;

    let tokens: Vec<String> = auth_token
        .map(|t| {
            t.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    let auth_config = middleware::AuthConfig {
        required: auth_required,
        tokens,
    };

    let app = build_router(ctx, auth_config);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("Listening on http://{listen}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("Shutting down...");
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::time::Instant;

    use axum::body::Body;
    use http::{Request, StatusCode};
    use tower::ServiceExt;

    use rapidbyte_api::run_manager::RunManager;
    use rapidbyte_api::services::{
        LocalConnectionService, LocalOperationsService, LocalPipelineService, LocalPluginService,
        LocalRunService, LocalServerService,
    };
    use rapidbyte_api::ApiContext;

    /// Build an `ApiContext` with an empty catalog for testing.
    fn test_api_context() -> ApiContext {
        let catalog = Arc::new(HashMap::new());
        let run_manager = Arc::new(RunManager::new());
        let registry_config = Arc::new(rapidbyte_registry::RegistryConfig::default());
        let secrets = Arc::new(rapidbyte_secrets::SecretProviders::default());

        ApiContext {
            pipelines: Arc::new(LocalPipelineService::new(
                Arc::clone(&catalog),
                Arc::clone(&run_manager),
                Arc::clone(&registry_config),
                Arc::clone(&secrets),
            )),
            runs: Arc::new(LocalRunService::new(Arc::clone(&run_manager))),
            connections: Arc::new(LocalConnectionService::new(Arc::clone(&catalog))),
            plugins: Arc::new(LocalPluginService),
            operations: Arc::new(LocalOperationsService::new(
                Arc::clone(&catalog),
                Arc::clone(&run_manager),
            )),
            server: Arc::new(LocalServerService::new(Instant::now(), 8080, false)),
        }
    }

    fn test_app() -> Router {
        let ctx = test_api_context();
        build_router(
            ctx,
            middleware::AuthConfig {
                required: false,
                tokens: vec![],
            },
        )
    }

    fn test_app_with_auth() -> Router {
        let ctx = test_api_context();
        build_router(
            ctx,
            middleware::AuthConfig {
                required: true,
                tokens: vec!["test-secret".into()],
            },
        )
    }

    // ------------------------------------------------------------------
    // Server endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn health_returns_200() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/server/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn version_returns_200() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/server/version")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ------------------------------------------------------------------
    // Pipeline endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn list_pipelines_returns_200_empty() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_nonexistent_pipeline_returns_404() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/pipelines/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sync_batch_returns_501() {
        let resp = test_app()
            .oneshot(
                Request::post("/api/v1/pipelines/sync")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    // ------------------------------------------------------------------
    // Auth middleware integration
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn auth_required_rejects_without_token() {
        let resp = test_app_with_auth()
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
    async fn auth_required_allows_with_valid_token() {
        let resp = test_app_with_auth()
            .oneshot(
                Request::get("/api/v1/pipelines")
                    .header("Authorization", "Bearer test-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_skips_auth() {
        let resp = test_app_with_auth()
            .oneshot(
                Request::get("/api/v1/server/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ------------------------------------------------------------------
    // Error envelope format
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn error_response_json_format() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/pipelines/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["error"]["code"].is_string());
        assert!(json["error"]["message"].is_string());
    }

    // ------------------------------------------------------------------
    // Operations endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn status_returns_200() {
        let resp = test_app()
            .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pause_returns_501() {
        let resp = test_app()
            .oneshot(
                Request::post("/api/v1/pipelines/test/pause")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    // ------------------------------------------------------------------
    // Connection endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn connections_returns_200() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/connections")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ------------------------------------------------------------------
    // Plugin endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn plugins_returns_200() {
        let resp = test_app()
            .oneshot(Request::get("/api/v1/plugins").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn plugin_info_returns_501() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/plugins/myorg/myplug")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn plugin_remove_returns_501() {
        let resp = test_app()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/plugins/myorg/myplug")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    // ------------------------------------------------------------------
    // Auth: empty / trailing-comma token handling (Bug 3)
    // ------------------------------------------------------------------

    /// Build an app whose token list comes from comma-separated parsing
    /// (mirrors the logic in `execute`).
    fn test_app_with_parsed_tokens(raw: &str) -> Router {
        let tokens: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let ctx = test_api_context();
        build_router(
            ctx,
            middleware::AuthConfig {
                required: true,
                tokens,
            },
        )
    }

    #[tokio::test]
    async fn trailing_comma_does_not_create_empty_token() {
        // "secret," should produce ["secret"], not ["secret", ""]
        let app = test_app_with_parsed_tokens("secret,");
        // Empty bearer should be rejected.
        let resp = app
            .oneshot(
                Request::get("/api/v1/pipelines")
                    .header("Authorization", "Bearer ")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn trailing_comma_valid_token_still_works() {
        let app = test_app_with_parsed_tokens("secret,");
        let resp = app
            .oneshot(
                Request::get("/api/v1/pipelines")
                    .header("Authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ------------------------------------------------------------------
    // Run endpoints
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn runs_list_returns_200() {
        let resp = test_app()
            .oneshot(Request::get("/api/v1/runs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_nonexistent_run_returns_404() {
        let resp = test_app()
            .oneshot(
                Request::get("/api/v1/runs/run_nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
