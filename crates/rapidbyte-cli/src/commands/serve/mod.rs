pub mod error;
pub mod extract;
pub mod middleware;
pub mod routes;
pub mod sse;

/// Execute the serve command — starts the REST API server.
pub async fn execute(
    _listen: &str,
    _allow_unauthenticated: bool,
    _auth_token: Option<&str>,
    _registry_config: &rapidbyte_registry::RegistryConfig,
    _secrets: &rapidbyte_secrets::SecretProviders,
) -> anyhow::Result<()> {
    tracing::info!("serve command not yet fully implemented");
    Ok(())
}
