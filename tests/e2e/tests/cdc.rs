#[tokio::test]
async fn cdc_reads_insert_update_delete_changes() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("cdc")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        let run = context
            .run_cdc_pipeline(&schemas, &state_conn)
            .await
            .expect("cdc pipeline should succeed");

        assert!(run.records_read >= 1);
        assert!(run.records_written >= 1);

        let written = context
            .table_row_count(&schemas.destination_schema, &schemas.source_users_table)
            .await
            .expect("destination cdc row count query must succeed");
        assert!(written >= 1);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
