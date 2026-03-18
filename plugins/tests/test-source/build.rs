use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {
    ManifestBuilder::source("rapidbyte/test-source")
        .name("Test Source")
        .description("Minimal test source plugin for integration tests")
        .sync_modes(&[SyncMode::FullRefresh])
        .emit();
}
