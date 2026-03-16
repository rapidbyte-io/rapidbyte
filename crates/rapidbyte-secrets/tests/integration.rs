//! Integration tests requiring a running Vault at localhost:8200.
//! Start with: `just dev-up`
//! Run with: `cargo test -p rapidbyte-secrets --test integration -- --ignored`

use rapidbyte_secrets::{SecretProvider, VaultAuth, VaultConfig, VaultProvider};

const VAULT_ADDR: &str = "http://127.0.0.1:8200";
const ROOT_TOKEN: &str = "rapidbyte-dev-vault-token";

#[tokio::test]
#[ignore]
async fn read_seeded_postgres_secret() {
    let provider = VaultProvider::new(&VaultConfig {
        address: VAULT_ADDR.into(),
        auth: VaultAuth::Token(ROOT_TOKEN.into()),
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
        address: VAULT_ADDR.into(),
        auth: VaultAuth::Token(ROOT_TOKEN.into()),
    })
    .unwrap();

    let result = provider.read_secret("secret/nonexistent", "key").await;
    assert!(result.is_err());
}

#[tokio::test]
#[ignore]
async fn read_missing_key_returns_error() {
    let provider = VaultProvider::new(&VaultConfig {
        address: VAULT_ADDR.into(),
        auth: VaultAuth::Token(ROOT_TOKEN.into()),
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

/// Helper: create a root-token `VaultClient` for admin setup operations.
fn admin_client() -> vaultrs::client::VaultClient {
    vaultrs::client::VaultClient::new(
        vaultrs::client::VaultClientSettingsBuilder::default()
            .address(VAULT_ADDR)
            .token(ROOT_TOKEN)
            .build()
            .expect("valid settings"),
    )
    .expect("should create admin client")
}

/// Integration test: AppRole re-auth after token revocation.
///
/// 1. Enable AppRole auth, create a role, generate a secret_id
/// 2. Create a VaultProvider with AppRole creds
/// 3. Read a secret (triggers lazy AppRole login)
/// 4. Revoke the current token via admin client
/// 5. Read again — should transparently re-authenticate and succeed
#[tokio::test]
#[ignore]
async fn approle_reauth_after_token_revocation() {
    let admin = admin_client();

    // Enable AppRole auth (ignore error if already enabled).
    let _ = vaultrs::sys::auth::enable(&admin, "approle", "approle", None).await;

    // Create a role with a short TTL.
    vaultrs::auth::approle::role::set(
        &admin,
        "approle",
        "test-reauth-role",
        Some(
            &mut vaultrs::api::auth::approle::requests::SetAppRoleRequest::builder()
                .token_ttl("10s"),
        ),
    )
    .await
    .expect("should create AppRole role");

    // Read the role_id.
    let role_info = vaultrs::auth::approle::role::read_id(&admin, "approle", "test-reauth-role")
        .await
        .expect("should read role_id");
    let role_id = role_info.role_id;

    // Generate a secret_id.
    let secret_resp = vaultrs::auth::approle::role::secret::custom(
        &admin,
        "approle",
        "test-reauth-role",
        "test-secret-id",
        None,
    )
    .await
    .expect("should create custom secret_id");
    let secret_id = secret_resp.secret_id;

    // Create provider with AppRole auth.
    let provider = VaultProvider::new(&VaultConfig {
        address: VAULT_ADDR.into(),
        auth: VaultAuth::AppRole {
            role_id: role_id.clone(),
            secret_id: secret_id.clone(),
        },
    })
    .expect("should create provider");

    // First read — triggers lazy AppRole login.
    let password = provider
        .read_secret("secret/postgres", "password")
        .await
        .expect("first read should succeed");
    assert_eq!(password, "postgres");

    // Look up the current token and revoke it to simulate expiry.
    let client = provider.client_for_test();
    let token_info = vaultrs::token::lookup_self(client)
        .await
        .expect("should lookup self token");
    vaultrs::token::revoke(&admin, &token_info.id)
        .await
        .expect("should revoke token");

    // Second read — should detect 403 on the expired token,
    // re-authenticate with AppRole, and succeed.
    let password = provider
        .read_secret("secret/postgres", "password")
        .await
        .expect("second read after token revocation should succeed via re-auth");
    assert_eq!(password, "postgres");
}
