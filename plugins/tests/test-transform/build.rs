use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("rapidbyte/test-transform")
        .name("Test Transform")
        .description("Minimal pass-through transform plugin for integration tests")
        .emit();
}
