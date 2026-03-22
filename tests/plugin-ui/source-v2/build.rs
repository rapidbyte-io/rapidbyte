use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::{Feature, SyncMode};

fn main() {
    ManifestBuilder::source("tests/source-v2")
        .name("Source V2 Fixture")
        .version("0.1.0")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Cdc])
        .source_features(vec![Feature::PartitionedRead, Feature::Cdc])
        .emit();
}
