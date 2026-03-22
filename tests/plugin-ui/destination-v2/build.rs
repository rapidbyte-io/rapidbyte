use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::WriteMode;

fn main() {
    ManifestBuilder::destination("tests/destination-v2")
        .name("Destination V2 Fixture")
        .version("0.1.0")
        .write_modes(&[WriteMode::Append])
        .emit();
}
