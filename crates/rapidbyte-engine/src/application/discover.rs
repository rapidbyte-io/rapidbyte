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
    use rapidbyte_types::schema::{SchemaField, StreamSchema};
    use rapidbyte_types::wire::SyncMode;

    #[tokio::test]
    async fn discover_resolves_and_discovers() {
        let tc = fake_context();
        tc.resolver.register("source-pg", test_resolved_plugin());
        tc.runner.enqueue_discover(Ok(vec![DiscoveredStream {
            name: "users".into(),
            schema: StreamSchema {
                fields: vec![SchemaField::new("id", "int64", false)],
                primary_key: vec!["id".into()],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: Some(42),
            metadata_json: Some(r#"{"source":"test"}"#.into()),
        }]));

        let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "users");
        assert_eq!(streams[0].schema.fields[0].name, "id");
        assert_eq!(streams[0].supported_sync_modes, vec![SyncMode::FullRefresh]);
        assert_eq!(streams[0].estimated_row_count, Some(42));
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
                schema: StreamSchema {
                    fields: vec![SchemaField::new("order_id", "int64", false)],
                    primary_key: vec!["order_id".into()],
                    partition_keys: vec![],
                    source_defined_cursor: Some("updated_at".into()),
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::Incremental],
                default_cursor_field: Some("updated_at".into()),
                estimated_row_count: None,
                metadata_json: Some(r#"{"schema":"public"}"#.into()),
            },
            DiscoveredStream {
                name: "products".into(),
                schema: StreamSchema {
                    fields: vec![SchemaField::new("sku", "utf8", false)],
                    primary_key: vec!["sku".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::FullRefresh],
                default_cursor_field: None,
                estimated_row_count: Some(3),
                metadata_json: None,
            },
        ]));

        let config = serde_json::json!({"host": "localhost"});
        let streams = discover_plugin(&tc.ctx, "source-pg", Some(&config))
            .await
            .unwrap();
        assert_eq!(streams.len(), 2);
        assert_eq!(streams[0].name, "orders");
        assert_eq!(streams[1].name, "products");
        assert_eq!(
            streams[0].default_cursor_field.as_deref(),
            Some("updated_at")
        );
        assert_eq!(streams[1].estimated_row_count, Some(3));
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
                schema: StreamSchema {
                    fields: vec![SchemaField::new("id", "int64", false)],
                    primary_key: vec!["id".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::FullRefresh],
                default_cursor_field: None,
                estimated_row_count: None,
                metadata_json: None,
            },
            DiscoveredStream {
                name: "orders".into(),
                schema: StreamSchema {
                    fields: vec![SchemaField::new("order_id", "int64", false)],
                    primary_key: vec!["order_id".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::Incremental],
                default_cursor_field: Some("updated_at".into()),
                estimated_row_count: None,
                metadata_json: None,
            },
            DiscoveredStream {
                name: "products".into(),
                schema: StreamSchema {
                    fields: vec![SchemaField::new("sku", "utf8", false)],
                    primary_key: vec!["sku".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::FullRefresh],
                default_cursor_field: None,
                estimated_row_count: None,
                metadata_json: None,
            },
            DiscoveredStream {
                name: "invoices".into(),
                schema: StreamSchema {
                    fields: vec![SchemaField::new("invoice_id", "int64", false)],
                    primary_key: vec!["invoice_id".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::FullRefresh],
                default_cursor_field: None,
                estimated_row_count: None,
                metadata_json: None,
            },
            DiscoveredStream {
                name: "payments".into(),
                schema: StreamSchema {
                    fields: vec![SchemaField::new("payment_id", "int64", false)],
                    primary_key: vec!["payment_id".into()],
                    partition_keys: vec![],
                    source_defined_cursor: None,
                    schema_id: None,
                },
                supported_sync_modes: vec![SyncMode::FullRefresh],
                default_cursor_field: None,
                estimated_row_count: None,
                metadata_json: None,
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
            schema: StreamSchema {
                fields: vec![SchemaField::new("event_id", "int64", false)],
                primary_key: vec!["event_id".into()],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }]));

        // Pass None for config
        let streams = discover_plugin(&tc.ctx, "source-pg", None).await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "public.events");
    }
}
