#[test]
fn controller_crate_does_not_publicly_export_legacy_services_module() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let lib = std::fs::read_to_string(root.join("src/lib.rs"))
        .expect("controller lib should be readable");

    assert!(!lib.contains("pub mod services;"));
}
