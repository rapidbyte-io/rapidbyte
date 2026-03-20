use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::WriteMode;

fn main() {
    ManifestBuilder::destination("rapidbyte/test-destination")
        .name("Test Destination")
        .description("Minimal test destination plugin for integration tests")
        .write_modes(&[WriteMode::Append])
        .emit();
}
