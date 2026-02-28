use rstest::rstest;

#[rstest]
#[case("append")]
#[case("replace")]
#[case("upsert")]
#[tokio::test]
async fn full_refresh_write_mode_matrix(#[case] write_mode: &str) {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("mode_matrix")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("mode_matrix_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_pipeline(&schemas, "full_refresh", write_mode, &state_path)
            .await
            .expect("first run should succeed");
        context
            .run_pipeline(&schemas, "full_refresh", write_mode, &state_path)
            .await
            .expect("second run should succeed");

        let users = context
            .table_row_count(&schemas.destination_schema, &schemas.source_users_table)
            .await
            .expect("destination user count query must succeed");
        if write_mode == "append" {
            assert!(users >= 3);
        } else {
            assert_eq!(users, 3);
        }
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}

#[tokio::test]
async fn incremental_sync_only_appends_new_rows_on_second_run() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("incremental")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("incremental_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        let first = context
            .run_pipeline(&schemas, "incremental", "append", &state_path)
            .await
            .expect("incremental first run should succeed");
        assert_eq!(first.records_read, 6);

        context
            .insert_source_user(&schemas, "Dave", "dave@example.com")
            .await
            .expect("source mutation should succeed");

        let second = context
            .run_pipeline(&schemas, "incremental", "append", &state_path)
            .await
            .expect("incremental second run should succeed");
        assert!(second.records_read < first.records_read);

        let users_after_second = context
            .table_row_count(&schemas.destination_schema, &schemas.source_users_table)
            .await
            .expect("destination user count query must succeed");
        assert_eq!(users_after_second, 4);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}

#[rstest]
#[case(None)]
#[case(Some("lz4"))]
#[case(Some("zstd"))]
#[tokio::test]
async fn full_refresh_compression_matrix(#[case] compression: Option<&str>) {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("compression")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("compression_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_pipeline_with_compression(
                &schemas,
                "full_refresh",
                "replace",
                &state_path,
                compression,
            )
            .await
            .expect("compressed pipeline run should succeed");

        let users = context
            .table_row_count(&schemas.destination_schema, &schemas.source_users_table)
            .await
            .expect("destination user count query must succeed");
        assert_eq!(users, 3);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
