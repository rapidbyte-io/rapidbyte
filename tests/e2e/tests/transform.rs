#[tokio::test]
async fn sql_transform_filters_and_projects_expected_rows() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("transform")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_transform_pipeline(
                &schemas,
                &format!(
                    r#"SELECT id, UPPER(name) AS name_upper FROM "{}" WHERE id % 2 = 1"#,
                    schemas.source_users_table
                ),
                &state_conn,
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

#[tokio::test]
async fn sql_transform_rejects_legacy_input_table_name() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("transform_invalid_query")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        let run = context
            .run_transform_pipeline(&schemas, r#"SELECT * FROM input"#, &state_conn)
            .await;

        assert!(
            run.is_err(),
            "pipeline should fail when query uses the legacy input table name"
        );
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}

#[tokio::test]
async fn validate_transform_fails_pipeline_when_on_data_error_is_fail() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("validate_fail")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");
        context
            .set_source_user_email_null(&schemas, "Bob")
            .await
            .expect("should null one email");

        let rules = r#"
- assert_not_null: [email]
- assert_regex:
    email: "^.+@.+\\..+$"
"#;

        let run = context
            .run_validate_transform_pipeline(&schemas, rules, "fail", &state_conn)
            .await;

        assert!(run.is_err(), "pipeline should fail when invalid rows are present");
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}

#[tokio::test]
async fn validate_transform_skip_writes_only_valid_rows() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("validate_skip")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");
        context
            .set_source_user_email_null(&schemas, "Bob")
            .await
            .expect("should null one email");

        let rules = r#"
- assert_not_null: [email]
- assert_regex:
    email: "^.+@.+\\..+$"
"#;

        context
            .run_validate_transform_pipeline(&schemas, rules, "skip", &state_conn)
            .await
            .expect("pipeline should succeed in skip mode");

        let snapshot = context
            .table_rows_snapshot(
                &schemas.destination_schema,
                &schemas.source_users_table,
                &["id", "name", "email"],
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

#[tokio::test]
async fn validate_transform_dlq_continues_and_writes_valid_rows() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("validate_dlq")
        .await
        .expect("schema allocation must succeed");
    let state_conn = context.state_connection();

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");
        context
            .set_source_user_email_null(&schemas, "Bob")
            .await
            .expect("should null one email");

        let rules = r#"
- assert_not_null: [email]
- assert_regex:
    email: "^.+@.+\\..+$"
"#;

        let run = context
            .run_validate_transform_pipeline(&schemas, rules, "dlq", &state_conn)
            .await
            .expect("pipeline should succeed in dlq mode");

        assert_eq!(run.records_read, 3);
        assert_eq!(run.records_written, 2);

        let dlq_rows = context
            .read_dlq_rows(&state_conn, "e2e_validate_transform")
            .await
            .expect("should read persisted dlq rows");
        assert_eq!(dlq_rows.len(), 1);
        assert_eq!(dlq_rows[0].stream_name, schemas.source_users_table);
        assert_eq!(dlq_rows[0].error_category, "data");
        assert!(dlq_rows[0].record_json.contains("\"name\":\"Bob\""));
        assert!(dlq_rows[0].error_message.contains("assert_not_null(email)"));
        assert!(dlq_rows[0].error_message.contains("assert_regex(email,"));
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
