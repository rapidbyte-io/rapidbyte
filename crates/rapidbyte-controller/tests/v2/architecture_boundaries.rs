use std::path::Path;

#[test]
fn domain_module_exists_and_avoids_transport_storage_imports() {
    let domain_mod = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/domain/mod.rs");
    assert!(
        domain_mod.exists(),
        "expected v2 domain module at {}",
        domain_mod.display()
    );

    let domain = std::fs::read_to_string(domain_mod).expect("domain module should be readable");
    assert!(!domain.contains("tonic::"));
    assert!(!domain.contains("tokio_postgres::"));
}
