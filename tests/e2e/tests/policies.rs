use rstest::rstest;

#[rstest]
#[case("fail")]
#[case("skip")]
#[case("dlq")]
#[tokio::test]
async fn on_data_error_policy_matrix_accepts_all_enums(#[case] on_data_error: &str) {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("policy_on_data_error")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("policy_on_data_error_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_pipeline_with_policies(
                &schemas,
                "full_refresh",
                "append",
                &state_path,
                None,
                Some(on_data_error),
                None,
            )
            .await
            .expect("policy-configured pipeline should succeed");

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

#[tokio::test]
async fn schema_evolution_new_column_fail_rejects_source_drift() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("policy_schema_evo_fail")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("policy_schema_evo_fail_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_pipeline_with_policies(
                &schemas,
                "full_refresh",
                "append",
                &state_path,
                None,
                None,
                None,
            )
            .await
            .expect("baseline pipeline should succeed");

        context
            .add_source_user_column(&schemas, "nickname")
            .await
            .expect("source schema drift mutation should succeed");

        let err = context
            .run_pipeline_with_policies(
                &schemas,
                "full_refresh",
                "append",
                &state_path,
                None,
                None,
                Some("    new_column: fail"),
            )
            .await
            .expect_err("schema evolution fail policy should reject new source column");

        let err_text = format!("{err:#}");
        assert!(
            err_text.contains("new column") || err_text.contains("schema evolution"),
            "unexpected schema evolution error: {err_text}"
        );
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
