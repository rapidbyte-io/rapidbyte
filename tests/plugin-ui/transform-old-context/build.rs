use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("tests/transform-old-context")
        .name("Transform Old Context Fixture")
        .version("0.1.0")
        .emit();
}
