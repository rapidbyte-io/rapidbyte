use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("rapidbyte/transform-validate")
        .name("Validation Transform")
        .description("Applies data quality assertion rules to Arrow batches")
        .emit();
}
