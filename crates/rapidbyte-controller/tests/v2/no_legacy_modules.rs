#[test]
fn v1_service_modules_are_removed() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let services = [
        "src/services/agent/mod.rs",
        "src/services/agent/register.rs",
        "src/services/agent/poll.rs",
        "src/services/agent/heartbeat.rs",
        "src/services/agent/complete.rs",
        "src/services/agent/dispatch.rs",
        "src/services/agent/secret.rs",
        "src/services/pipeline/mod.rs",
        "src/services/pipeline/submit.rs",
        "src/services/pipeline/query.rs",
        "src/services/pipeline/cancel.rs",
        "src/services/pipeline/convert.rs",
        "src/services/mod.rs",
    ];

    for relative in services {
        assert!(
            !root.join(relative).exists(),
            "legacy module should be removed: {relative}"
        );
    }

    assert!(
        !root
            .join("../../proto/rapidbyte/v1/controller.proto")
            .exists(),
        "legacy proto should be removed: proto/rapidbyte/v1/controller.proto"
    );
}
