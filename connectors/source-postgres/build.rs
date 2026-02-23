use rapidbyte_sdk::build::ManifestEmitter;
use rapidbyte_sdk::protocol::SyncMode;

fn main() {
    ManifestEmitter::source("rapidbyte/source-postgres")
        .name("PostgreSQL Source")
        .description("Reads data from PostgreSQL tables using streaming queries")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .config_schema_file("config_schema.json")
        .emit();
}
