use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::manifest::DestinationFeature;
use rapidbyte_sdk::protocol::WriteMode;

fn main() {
    ManifestBuilder::destination("rapidbyte/dest-postgres")
        .name("PostgreSQL Destination")
        .description("Writes data to PostgreSQL using INSERT or COPY")
        .write_modes(&[
            WriteMode::Append,
            WriteMode::Replace,
            WriteMode::Upsert {
                primary_key: vec![],
            },
        ])
        .dest_features(vec![DestinationFeature::BulkLoadCopy])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
