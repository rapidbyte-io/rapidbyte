use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {
    ManifestBuilder::source("tests/source-old-context")
        .name("Source Old Context Fixture")
        .version("0.1.0")
        .sync_modes(&[SyncMode::FullRefresh])
        .emit();
}
