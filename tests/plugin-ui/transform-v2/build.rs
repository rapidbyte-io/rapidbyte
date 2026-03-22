use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("tests/transform-v2")
        .name("Transform V2 Fixture")
        .version("0.1.0")
        .emit();
}
