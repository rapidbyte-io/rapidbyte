use rapidbyte_controller::proto::rapidbyte::v2::{
    agent_session_client, control_plane_client, AgentMessage, SubmitRunRequest,
};

#[test]
fn v2_proto_module_is_generated() {
    let _ = SubmitRunRequest::default();
    let _ = AgentMessage::default();
    let _control_plane_path = std::any::type_name::<
        control_plane_client::ControlPlaneClient<tonic::transport::Channel>,
    >();
    let _agent_session_path = std::any::type_name::<
        agent_session_client::AgentSessionClient<tonic::transport::Channel>,
    >();
}
