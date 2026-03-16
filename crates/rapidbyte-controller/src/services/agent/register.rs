//! Handler for the `register_agent` RPC.

use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{RegisterAgentRequest, RegisterAgentResponse};

pub(crate) async fn handle_register(
    handler: &super::AgentHandler,
    req: RegisterAgentRequest,
) -> Result<Response<RegisterAgentResponse>, Status> {
    let agent_id = uuid::Uuid::new_v4().to_string();

    let bundle_hash = req.plugin_bundle_hash.clone();

    let mut registry = handler.state.registry.write().await;

    // Log bundle hash mismatch warnings before registering
    if !bundle_hash.is_empty() {
        for other in registry.list() {
            if !other.plugin_bundle_hash.is_empty() && other.plugin_bundle_hash != bundle_hash {
                tracing::warn!(
                    new_agent = agent_id,
                    existing_agent = other.agent_id,
                    new_hash = bundle_hash,
                    existing_hash = other.plugin_bundle_hash,
                    "Bundle hash mismatch across agent pool"
                );
                break;
            }
        }
    }

    registry.register(
        agent_id.clone(),
        req.max_tasks,
        req.flight_advertise_endpoint,
        req.plugin_bundle_hash,
        req.available_plugins,
        req.memory_bytes,
    );
    drop(registry);

    if let Err(error) = handler.state.persist_agent(&agent_id).await {
        handler.state.registry.write().await.remove(&agent_id);
        return Err(Status::internal(error.to_string()));
    }

    tracing::info!(agent_id, "Agent registered");
    rapidbyte_metrics::instruments::controller::active_agents().add(1, &[]);
    Ok(Response::new(RegisterAgentResponse {
        agent_id,
        registry_url: handler.registry_url.clone(),
        registry_insecure: handler.registry_insecure,
        trust_policy: handler.trust_policy.clone(),
        trusted_key_pems: handler.trusted_key_pems.clone(),
    }))
}
