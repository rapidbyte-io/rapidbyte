use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::protocol::SyncMode;

fn main() {
    ManifestBuilder::source("rapidbyte/source-postgres")
        .name("PostgreSQL Source")
        .description("Reads data from PostgreSQL tables using streaming queries")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
