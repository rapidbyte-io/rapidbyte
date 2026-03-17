#[test]
fn controller_v2_surface_removes_insecure_auth_escape_hatches() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let controller_config = std::fs::read_to_string(root.join("src/config.rs"))
        .expect("controller config source should be readable");
    let cli_controller =
        std::fs::read_to_string(root.join("../rapidbyte-cli/src/commands/controller.rs"))
            .expect("cli controller command source should be readable");
    let cli_main = std::fs::read_to_string(root.join("../rapidbyte-cli/src/main.rs"))
        .expect("cli main source should be readable");

    assert!(!controller_config.contains("allow_unauthenticated"));
    assert!(!controller_config.contains("allow_insecure_default_signing_key"));
    assert!(!cli_controller.contains("allow_unauthenticated"));
    assert!(!cli_controller.contains("allow_insecure_signing_key"));
    assert!(!cli_main.contains("allow_unauthenticated"));
    assert!(!cli_main.contains("allow-unauthenticated"));
}
