#[test]
fn agent_runtime_and_codegen_do_not_reference_v1_proto() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let worker = std::fs::read_to_string(root.join("src/worker.rs"))
        .expect("worker source should be readable");
    let proto = std::fs::read_to_string(root.join("src/proto.rs"))
        .expect("proto module should be readable");
    let build =
        std::fs::read_to_string(root.join("build.rs")).expect("build script should be readable");

    assert!(!worker.contains("proto::rapidbyte::v1"));
    assert!(!proto.contains("rapidbyte.v1"));
    assert!(!build.contains("rapidbyte/v1/controller.proto"));
}
