use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use super::helpers::{parse_json, test_app};

#[tokio::test]
async fn list_plugins_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(Request::get("/api/v1/plugins").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = parse_json(resp).await;
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["next_cursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn search_plugins_returns_empty() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/plugins/search?q=postgres&type=source")
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
async fn install_plugin_returns_error() {
    let app = test_app();
    let resp = app
        .oneshot(
            Request::post("/api/v1/plugins/install")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({
                        "plugin_ref": "rapidbyte/source-postgres:1.2.3"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}

#[tokio::test]
async fn get_plugin_info_returns_404_for_unknown() {
    // FakePluginRegistry returns NotFound for unknown plugins.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::get("/api/v1/plugins/rapidbyte/source-postgres:1.0.0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "plugin_not_found");
}

#[tokio::test]
async fn remove_plugin_returns_501() {
    // FakePluginRegistry::remove returns Unavailable.
    let app = test_app();
    let resp = app
        .oneshot(
            Request::delete("/api/v1/plugins/rapidbyte/source-postgres:1.0.0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    let body = parse_json(resp).await;
    assert_eq!(body["error"]["code"], "not_implemented");
}
