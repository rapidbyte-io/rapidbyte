use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::{Feature, SyncMode};

fn main() {
    ManifestBuilder::source("rapidbyte/source-postgres")
        .name("PostgreSQL Source")
        .description("Reads data from PostgreSQL tables using streaming queries")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
        .source_features(vec![Feature::Cdc, Feature::PartitionedRead])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
