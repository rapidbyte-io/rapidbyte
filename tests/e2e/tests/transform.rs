#[tokio::test]
async fn sql_transform_filters_and_projects_expected_rows() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("transform")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("transform_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_transform_pipeline(
                &schemas,
                r#"SELECT id, UPPER(name) AS name_upper FROM input WHERE id % 2 = 1"#,
                &state_path,
            )
            .await
            .expect("transform pipeline should succeed");

        let snapshot = context
            .table_rows_snapshot(
                &schemas.destination_schema,
                &schemas.source_users_table,
                &["id", "name_upper"],
                "id",
            )
            .await
            .expect("snapshot query should succeed");

        insta::assert_snapshot!(snapshot);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
