//! Discover-plugin use case.
//!
//! Resolves a source plugin and runs stream discovery, returning the
//! list of streams the plugin can sync.

use crate::application::context::EngineContext;
use crate::application::{build_sandbox_overrides, extract_permissions, parse_plugin_id};
use crate::domain::error::PipelineError;
use crate::domain::ports::runner::{DiscoverParams, DiscoveredStream};
use rapidbyte_types::wire::PluginKind;

/// Resolve a source plugin and discover available streams.
///
/// This is the simplest use case: resolve the plugin reference to a
/// WASM module path, then invoke the plugin's discover operation.
///
/// # Errors
///
/// Returns `PipelineError` if resolution or discovery fails.
pub async fn discover_plugin(
    ctx: &EngineContext,
    source_ref: &str,
    config_json: Option<&serde_json::Value>,
) -> Result<Vec<DiscoveredStream>, PipelineError> {
    let resolved = ctx
        .resolver
        .resolve(source_ref, PluginKind::Source, config_json)
        .await?;

    let permissions = extract_permissions(&resolved);
    // Apply manifest-declared resource limits (no pipeline YAML overrides
    // since discover operates outside a pipeline context).
    let sandbox_overrides = build_sandbox_overrides(None, None, resolved.manifest.as_ref())?;
    let (plugin_id, plugin_version) = parse_plugin_id(source_ref);

    let params = DiscoverParams {
        wasm_path: resolved.wasm_path,
        plugin_id,
        plugin_version,
        config: config_json.cloned().unwrap_or(serde_json::Value::Null),
        permissions,
        sandbox_overrides,
    };

    ctx.runner.discover(&params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::{fake_context, test_resolved_plugin};

    #[tokio::test]
    async fn discover_resolves_and_discovers() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            catalog_json: "{}".into(),
            schema: None,
        }]));

        let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "users");
    }

    #[tokio::test]
    async fn discover_returns_error_on_resolution_failure() {
        let tc = fake_context();
        // Don't register any plugin — resolver will return an error.
        let result = discover_plugin(&tc.ctx, "nonexistent", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn discover_returns_error_on_runner_failure() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner
            .enqueue_discover(Err(PipelineError::infra("discover failed")));

        let result = discover_plugin(&tc.ctx, "source-pg", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("discover failed"));
    }

    #[tokio::test]
    async fn discover_passes_config_through() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner.enqueue_discover(Ok(vec![
            DiscoveredStream {
                name: "orders".into(),
                catalog_json: r#"{"schema":"public"}"#.into(),
                schema: None,
            },
            DiscoveredStream {
                name: "products".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
        ]));

        let config = serde_json::json!({"host": "localhost"});
        let streams = discover_plugin(&tc.ctx, "source-pg", Some(&config))
            .await
            .unwrap();
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].name, "orders");
        assert_eq!(streams[1].name, "products");
    }

    #[test]
    fn parse_plugin_id_with_version() {
        let (id, ver) = parse_plugin_id("rapidbyte/source-postgres:1.2.0");
        assert_eq!(id, "rapidbyte/source-postgres");
        assert_eq!(ver, "1.2.0");
    }

    #[test]
    fn parse_plugin_id_bare_name() {
        let (id, ver) = parse_plugin_id("postgres");
        assert_eq!(id, "postgres");
        assert_eq!(ver, "0.0.0");
    }

    // -------------------------------------------------------------------
    // Test: Discover returns multiple streams
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn discover_returns_multiple_streams() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner.enqueue_discover(Ok(vec![
            DiscoveredStream {
                name: "users".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
            DiscoveredStream {
                name: "orders".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
            DiscoveredStream {
                name: "products".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
            DiscoveredStream {
                name: "invoices".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
            DiscoveredStream {
                name: "payments".into(),
                catalog_json: "{}".into(),
                schema: None,
            },
        ]));

        let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
        assert_eq!(streams.len(), 5);
        assert_eq!(streams[0].name, "users");
        assert_eq!(streams[4].name, "payments");
    }

    // -------------------------------------------------------------------
    // Test: Discover with no config
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn discover_with_no_config() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "public.events".into(),
            catalog_json: "{}".into(),
            schema: None,
        }]));

        // Pass None for config
        let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "public.events");
    }
}
