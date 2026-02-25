use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("rapidbyte/transform-sql")
        .name("SQL Transform")
        .description("Executes SQL queries on Arrow batches using Apache DataFusion")
        .emit();
}
