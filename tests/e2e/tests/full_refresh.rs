#[tokio::test]
async fn full_refresh_copies_users_and_orders_into_destination_schema() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("full_refresh")
        .await
        .expect("schema allocation must succeed");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        let run = context
            .run_full_refresh_pipeline(&schemas)
            .await
            .expect("pipeline should run");
        assert_eq!(run.records_read, 6);
        assert_eq!(run.records_written, 6);

        let user_count = context
            .table_row_count(&schemas.destination_schema, &schemas.source_users_table)
            .await
            .expect("destination users count query must succeed");
        let order_count = context
            .table_row_count(&schemas.destination_schema, &schemas.source_orders_table)
            .await
            .expect("destination orders count query must succeed");

        assert_eq!(user_count, 3);
        assert_eq!(order_count, 3);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");

    result
}
