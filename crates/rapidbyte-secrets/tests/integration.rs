//! Integration tests requiring a running Vault at localhost:8200.
//! Start with: `just dev-up`
//! Run with: `cargo test -p rapidbyte-secrets --test integration -- --ignored`

use rapidbyte_secrets::{SecretProvider, VaultAuth, VaultConfig, VaultProvider};

#[tokio::test]
#[ignore]
async fn read_seeded_postgres_secret() {
    let provider = VaultProvider::new(&VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .expect("should create Vault provider");

    let password = provider
        .read_secret("secret/postgres", "password")
        .await
        .expect("should read seeded secret");

    assert_eq!(password, "postgres");
}

#[tokio::test]
#[ignore]
async fn read_missing_path_returns_error() {
    let provider = VaultProvider::new(&VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .unwrap();

    let result = provider.read_secret("secret/nonexistent", "key").await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn read_missing_key_returns_error() {
    let provider = VaultProvider::new(&VaultConfig {
        address: "http://127.0.0.1:8200".into(),
        auth: VaultAuth::Token("rapidbyte-dev-vault-token".into()),
    })
    .unwrap();

    let result = provider
        .read_secret("secret/postgres", "nonexistent_key")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, rapidbyte_secrets::SecretError::NotFound(_)),
        "expected NotFound, got: {err}"
    );
    assert!(err.to_string().contains("not found"));
}
