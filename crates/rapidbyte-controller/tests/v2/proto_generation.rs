use rapidbyte_controller::proto::rapidbyte::v2::{
    agent_session_client, control_plane_client, AgentMessage, SubmitRunRequest,
};

#[test]
fn proto_module_is_generated() {
    assert_eq!(SubmitRunRequest::default().idempotency_key, None);
    let agent_message = AgentMessage::default();
    assert!(agent_message.agent_id.is_empty());

    let control_plane_path = std::any::type_name::<
        control_plane_client::ControlPlaneClient<tonic::transport::Channel>,
    >();
    let agent_session_path = std::any::type_name::<
        agent_session_client::AgentSessionClient<tonic::transport::Channel>,
    >();
    assert!(control_plane_path.contains("ControlPlaneClient"));
    assert!(agent_session_path.contains("AgentSessionClient"));
}
