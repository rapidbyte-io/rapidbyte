use rapidbyte_e2e::harness::AutotuneOptions;

#[tokio::test]
async fn output_equivalence_with_autotune_enabled_and_disabled() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");

    let enabled_schemas = context
        .allocate_schema_pair("autotune_enabled")
        .await
        .expect("schema allocation must succeed");
    let disabled_schemas = context
        .allocate_schema_pair("autotune_disabled")
        .await
        .expect("schema allocation must succeed");

    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let enabled_state = temp.path().join("autotune_enabled.db");
    let disabled_state = temp.path().join("autotune_disabled.db");

    let result = async {
        context
            .seed_basic_source_data(&enabled_schemas)
            .await
            .expect("source seed should succeed");
        context
            .seed_basic_source_data(&disabled_schemas)
            .await
            .expect("source seed should succeed");

        context
            .run_pipeline_with_autotune(
                &enabled_schemas,
                "full_refresh",
                "replace",
                &enabled_state,
                None,
            )
            .await
            .expect("default autotune run should succeed");

        let autotune_disabled = AutotuneOptions {
            enabled: Some(false),
            ..AutotuneOptions::default()
        };
        context
            .run_pipeline_with_autotune(
                &disabled_schemas,
                "full_refresh",
                "replace",
                &disabled_state,
                Some(&autotune_disabled),
            )
            .await
            .expect("autotune-disabled run should succeed");

        let enabled_users = context
            .table_rows_snapshot(
                &enabled_schemas.destination_schema,
                &enabled_schemas.source_users_table,
                &["id", "name", "email"],
                "id",
            )
            .await
            .expect("enabled users snapshot query must succeed");
        let disabled_users = context
            .table_rows_snapshot(
                &disabled_schemas.destination_schema,
                &disabled_schemas.source_users_table,
                &["id", "name", "email"],
                "id",
            )
            .await
            .expect("disabled users snapshot query must succeed");
        assert_eq!(enabled_users, disabled_users);

        let enabled_orders = context
            .table_rows_snapshot(
                &enabled_schemas.destination_schema,
                &enabled_schemas.source_orders_table,
                &["id", "user_id", "amount_cents", "status"],
                "id",
            )
            .await
            .expect("enabled orders snapshot query must succeed");
        let disabled_orders = context
            .table_rows_snapshot(
                &disabled_schemas.destination_schema,
                &disabled_schemas.source_orders_table,
                &["id", "user_id", "amount_cents", "status"],
                "id",
            )
            .await
            .expect("disabled orders snapshot query must succeed");
        assert_eq!(enabled_orders, disabled_orders);
    }
    .await;

    context
        .drop_schema_pair(&enabled_schemas)
        .await
        .expect("schema cleanup must succeed");
    context
        .drop_schema_pair(&disabled_schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}

#[tokio::test]
async fn manual_autotune_pins_run_successfully() {
    let context = rapidbyte_e2e::harness::bootstrap()
        .await
        .expect("bootstrap must initialize test harness");
    let schemas = context
        .allocate_schema_pair("autotune_pins")
        .await
        .expect("schema allocation must succeed");
    let temp = tempfile::tempdir().expect("must create tempdir for sqlite state");
    let state_path = temp.path().join("autotune_pins_state.db");

    let result = async {
        context
            .seed_basic_source_data(&schemas)
            .await
            .expect("source seed should succeed");

        let pinned = AutotuneOptions {
            enabled: Some(true),
            pin_parallelism: Some(1),
            pin_source_partition_mode: Some("mod".to_string()),
            pin_copy_flush_bytes: Some(4 * 1024 * 1024),
        };

        let summary = context
            .run_pipeline_with_autotune(
                &schemas,
                "full_refresh",
                "replace",
                &state_path,
                Some(&pinned),
            )
            .await
            .expect("pinned autotune run should succeed");

        assert_eq!(summary.records_read, 6);
        assert_eq!(summary.records_written, 6);
    }
    .await;

    context
        .drop_schema_pair(&schemas)
        .await
        .expect("schema cleanup must succeed");
    result
}
