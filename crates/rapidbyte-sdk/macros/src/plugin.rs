//! `#[plugin(source|destination|transform)]` attribute macro implementation.
//!
//! Generates all WIT bindings, component glue, manifest embedding, and config
//! schema embedding that previously required three separate declarative macros.

use std::path::PathBuf;

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, ItemStruct, Result};

struct ManifestFeatures {
    has_partitioned_read: bool,
    has_cdc: bool,
    has_bulk_load: bool,
}

fn read_manifest_features(kind: &PluginKind) -> Option<ManifestFeatures> {
    let out_dir = std::env::var("OUT_DIR").ok()?;
    let path = PathBuf::from(out_dir).join("rapidbyte_manifest.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&json).ok()?;

    let features_key = match kind {
        PluginKind::Source => "source",
        PluginKind::Destination => "destination",
        PluginKind::Transform => {
            return Some(ManifestFeatures {
                has_partitioned_read: false,
                has_cdc: false,
                has_bulk_load: false,
            })
        }
    };

    let features: Vec<String> = manifest
        .get("roles")
        .and_then(|r| r.get(features_key))
        .and_then(|s| s.get("features"))
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default();

    Some(ManifestFeatures {
        has_partitioned_read: features.iter().any(|f| f == "partitioned_read"),
        has_cdc: features.iter().any(|f| f == "cdc"),
        has_bulk_load: features.iter().any(|f| f == "bulk_load"),
    })
}

fn gen_feature_assertions(
    kind: &PluginKind,
    struct_name: &Ident,
    features: &ManifestFeatures,
) -> TokenStream {
    let mut assertions = Vec::new();

    match kind {
        PluginKind::Source => {
            if features.has_partitioned_read {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_partitioned_source<T: ::rapidbyte_sdk::features::PartitionedSource>() {}
                        fn __check() { __assert_partitioned_source::<#struct_name>(); }
                    };
                });
            }
            if features.has_cdc {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_cdc_source<T: ::rapidbyte_sdk::features::CdcSource>() {}
                        fn __check() { __assert_cdc_source::<#struct_name>(); }
                    };
                });
            }
        }
        PluginKind::Destination => {
            if features.has_bulk_load {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_bulk_destination<T: ::rapidbyte_sdk::features::BulkDestination>() {}
                        fn __check() { __assert_bulk_destination::<#struct_name>(); }
                    };
                });
            }
        }
        PluginKind::Transform => {}
    }

    quote! { #(#assertions)* }
}

/// The plugin kind parsed from the attribute argument.
pub enum PluginKind {
    Source,
    Destination,
    Transform,
}

impl Parse for PluginKind {
    fn parse(input: ParseStream) -> Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "source" => Ok(PluginKind::Source),
            "destination" => Ok(PluginKind::Destination),
            "transform" => Ok(PluginKind::Transform),
            other => Err(syn::Error::new(
                ident.span(),
                format!(
                    "expected `source`, `destination`, or `transform`, found `{}`",
                    other
                ),
            )),
        }
    }
}

/// Main expansion entry point.
pub fn expand(kind: PluginKind, input: ItemStruct) -> Result<TokenStream> {
    let struct_name = &input.ident;

    let bindings_mod = quote! { __rb_bindings };
    let (world_name, trait_path) = match kind {
        PluginKind::Source => (
            "rapidbyte-source",
            quote! { ::rapidbyte_sdk::plugin::Source },
        ),
        PluginKind::Destination => (
            "rapidbyte-destination",
            quote! { ::rapidbyte_sdk::plugin::Destination },
        ),
        PluginKind::Transform => (
            "rapidbyte-transform",
            quote! { ::rapidbyte_sdk::plugin::Transform },
        ),
    };

    let features = read_manifest_features(&kind);
    let wit_bindings = gen_wit_bindings(world_name);
    let common = gen_common(struct_name);
    let guest_impl = gen_guest_impl(&kind, struct_name, &trait_path, features.as_ref());
    let embeds = gen_embeds(struct_name, &trait_path);
    let feature_assertions = features
        .as_ref()
        .map(|f| gen_feature_assertions(&kind, struct_name, f))
        .unwrap_or_default();

    Ok(quote! {
        #input

        #wit_bindings
        #common
        #guest_impl

        #feature_assertions
        #bindings_mod::export!(RapidbyteComponent with_types_in #bindings_mod);

        #embeds

        fn main() {}
    })
}

/// Generate the WIT bindings module.
fn gen_wit_bindings(world_name: &str) -> TokenStream {
    // We must produce a literal string for the world name inside the
    // wit_bindgen::generate! invocation.  Because the inner macro is a
    // declarative `macro_rules!` expansion the world name must be a string
    // literal, not an ident.
    let world_lit = syn::LitStr::new(world_name, proc_macro2::Span::call_site());
    quote! {
        mod __rb_bindings {
            ::rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../../wit",
                world: #world_lit,
            });
        }
    }
}

/// Generate statics, helpers, and conversion functions shared by all roles.
fn gen_common(struct_name: &Ident) -> TokenStream {
    quote! {
        use std::cell::RefCell;
        use std::sync::OnceLock;

        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        static CONFIG_JSON: OnceLock<String> = OnceLock::new();

        fn get_runtime() -> &'static tokio::runtime::Runtime {
            RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create guest tokio runtime")
            })
        }

        struct SyncRefCell(RefCell<Option<#struct_name>>);
        unsafe impl Sync for SyncRefCell {}

        static PLUGIN: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<#struct_name>> {
            &PLUGIN.get_or_init(|| SyncRefCell(RefCell::new(None))).0
        }

        fn to_component_error(
            error: ::rapidbyte_sdk::error::PluginError,
        ) -> __rb_bindings::rapidbyte::plugin::types::PluginError {
            use __rb_bindings::rapidbyte::plugin::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                PluginError as CPluginError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };
            use ::rapidbyte_sdk::error::{
                BackoffClass, CommitState, ErrorCategory, ErrorScope,
            };

            CPluginError {
                category: match error.category {
                    ErrorCategory::Config => CErrorCategory::Config,
                    ErrorCategory::Auth => CErrorCategory::Auth,
                    ErrorCategory::Permission => CErrorCategory::Permission,
                    ErrorCategory::RateLimit => CErrorCategory::RateLimit,
                    ErrorCategory::TransientNetwork => CErrorCategory::TransientNetwork,
                    ErrorCategory::TransientDb => CErrorCategory::TransientDb,
                    ErrorCategory::Data => CErrorCategory::Data,
                    ErrorCategory::Schema => CErrorCategory::Schema,
                    ErrorCategory::Frame => CErrorCategory::Frame,
                    ErrorCategory::Cancelled => CErrorCategory::Cancelled,
                    ErrorCategory::Internal | _ => CErrorCategory::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => CErrorScope::PerStream,
                    ErrorScope::Batch => CErrorScope::PerBatch,
                    ErrorScope::Record => CErrorScope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBackoffClass::Fast,
                    BackoffClass::Normal => CBackoffClass::Normal,
                    BackoffClass::Slow => CBackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    CommitState::BeforeCommit => CCommitState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CCommitState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CCommitState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|value| value.to_string()),
            }
        }

        fn parse_config<T: serde::de::DeserializeOwned>(
            config_json: &str,
        ) -> Result<T, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            serde_json::from_str(config_json).map_err(|e| {
                to_component_error(::rapidbyte_sdk::error::PluginError::config(
                    "INVALID_CONFIG",
                    format!("Config parse error: {}", e),
                ))
            })
        }

        fn parse_saved_config<T: serde::de::DeserializeOwned>(
        ) -> Result<T, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let json = CONFIG_JSON.get().expect("open must be called before validate");
            parse_config(json)
        }

        // ── Schema converters: WIT → SDK ────────────────────────────

        fn from_component_schema_field(
            f: __rb_bindings::rapidbyte::plugin::types::SchemaField,
        ) -> ::rapidbyte_sdk::schema::SchemaField {
            ::rapidbyte_sdk::schema::SchemaField {
                name: f.name,
                arrow_type: f.arrow_type,
                nullable: f.nullable,
                is_primary_key: f.is_primary_key,
                is_generated: f.is_generated,
                is_partition_key: f.is_partition_key,
                default_value: f.default_value,
            }
        }

        fn from_component_stream_schema(
            s: __rb_bindings::rapidbyte::plugin::types::StreamSchema,
        ) -> ::rapidbyte_sdk::schema::StreamSchema {
            ::rapidbyte_sdk::schema::StreamSchema {
                fields: s.fields.into_iter().map(from_component_schema_field).collect(),
                primary_key: s.primary_key,
                partition_keys: s.partition_keys,
                source_defined_cursor: s.source_defined_cursor,
                schema_id: s.schema_id,
            }
        }

        // ── Schema converters: SDK → WIT ────────────────────────────

        fn to_component_schema_field(
            f: ::rapidbyte_sdk::schema::SchemaField,
        ) -> __rb_bindings::rapidbyte::plugin::types::SchemaField {
            __rb_bindings::rapidbyte::plugin::types::SchemaField {
                name: f.name,
                arrow_type: f.arrow_type,
                nullable: f.nullable,
                is_primary_key: f.is_primary_key,
                is_generated: f.is_generated,
                is_partition_key: f.is_partition_key,
                default_value: f.default_value,
            }
        }

        fn to_component_stream_schema(
            s: ::rapidbyte_sdk::schema::StreamSchema,
        ) -> __rb_bindings::rapidbyte::plugin::types::StreamSchema {
            __rb_bindings::rapidbyte::plugin::types::StreamSchema {
                fields: s.fields.into_iter().map(to_component_schema_field).collect(),
                primary_key: s.primary_key,
                partition_keys: s.partition_keys,
                source_defined_cursor: s.source_defined_cursor,
                schema_id: s.schema_id,
            }
        }

        // ── Validation converters ───────────────────────────────────

        fn to_component_field_constraint(
            c: ::rapidbyte_sdk::schema::FieldConstraint,
        ) -> __rb_bindings::rapidbyte::plugin::types::FieldConstraint {
            use __rb_bindings::rapidbyte::plugin::types::FieldConstraint as CFieldConstraint;
            use ::rapidbyte_sdk::schema::FieldConstraint;
            match c {
                FieldConstraint::FieldRequired => CFieldConstraint::FieldRequired,
                FieldConstraint::FieldOptional => CFieldConstraint::FieldOptional,
                FieldConstraint::FieldForbidden => CFieldConstraint::FieldForbidden,
                FieldConstraint::FieldRecommended => CFieldConstraint::FieldRecommended,
                FieldConstraint::TypeIncompatible => CFieldConstraint::TypeIncompatible,
            }
        }

        fn to_component_field_requirement(
            r: ::rapidbyte_sdk::schema::FieldRequirement,
        ) -> __rb_bindings::rapidbyte::plugin::types::FieldRequirement {
            __rb_bindings::rapidbyte::plugin::types::FieldRequirement {
                field_name: r.field_name,
                constraint: to_component_field_constraint(r.constraint),
                reason: r.reason,
                accepted_types: r.accepted_types,
            }
        }

        fn to_component_validation(
            result: ::rapidbyte_sdk::validation::ValidationReport,
        ) -> __rb_bindings::rapidbyte::plugin::types::ValidationReport {
            use __rb_bindings::rapidbyte::plugin::types::{
                ValidationReport as CValidationReport, ValidationStatus as CValidationStatus,
            };
            use ::rapidbyte_sdk::validation::ValidationStatus;

            CValidationReport {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
                warnings: result.warnings,
                output_schema: result.output_schema.map(to_component_stream_schema),
                field_requirements: result.field_requirements.map(|reqs|
                    reqs.into_iter().map(to_component_field_requirement).collect()
                ),
            }
        }

        // ── SyncMode converters ─────────────────────────────────────

        fn from_component_sync_mode(
            m: __rb_bindings::rapidbyte::plugin::types::SyncMode,
        ) -> ::rapidbyte_sdk::wire::SyncMode {
            use __rb_bindings::rapidbyte::plugin::types::SyncMode as CSyncMode;
            match m {
                CSyncMode::FullRefresh => ::rapidbyte_sdk::wire::SyncMode::FullRefresh,
                CSyncMode::Incremental => ::rapidbyte_sdk::wire::SyncMode::Incremental,
                CSyncMode::Cdc => ::rapidbyte_sdk::wire::SyncMode::Cdc,
            }
        }

        fn to_component_sync_mode(
            m: ::rapidbyte_sdk::wire::SyncMode,
        ) -> __rb_bindings::rapidbyte::plugin::types::SyncMode {
            use __rb_bindings::rapidbyte::plugin::types::SyncMode as CSyncMode;
            match m {
                ::rapidbyte_sdk::wire::SyncMode::FullRefresh => CSyncMode::FullRefresh,
                ::rapidbyte_sdk::wire::SyncMode::Incremental => CSyncMode::Incremental,
                ::rapidbyte_sdk::wire::SyncMode::Cdc => CSyncMode::Cdc,
            }
        }

        // ── WriteMode converters ────────────────────────────────────

        fn from_component_write_mode(
            m: __rb_bindings::rapidbyte::plugin::types::WriteMode,
            schema_primary_key: &[String],
        ) -> ::rapidbyte_sdk::wire::WriteMode {
            use __rb_bindings::rapidbyte::plugin::types::WriteMode as CWriteMode;
            match m {
                CWriteMode::Append => ::rapidbyte_sdk::wire::WriteMode::Append,
                CWriteMode::Replace => ::rapidbyte_sdk::wire::WriteMode::Replace,
                CWriteMode::Upsert => ::rapidbyte_sdk::wire::WriteMode::Upsert {
                    primary_key: schema_primary_key.to_vec(),
                },
            }
        }

        fn to_component_write_mode(
            m: ::rapidbyte_sdk::wire::WriteMode,
        ) -> __rb_bindings::rapidbyte::plugin::types::WriteMode {
            use __rb_bindings::rapidbyte::plugin::types::WriteMode as CWriteMode;
            match m {
                ::rapidbyte_sdk::wire::WriteMode::Append => CWriteMode::Append,
                ::rapidbyte_sdk::wire::WriteMode::Replace => CWriteMode::Replace,
                ::rapidbyte_sdk::wire::WriteMode::Upsert { .. } => CWriteMode::Upsert,
            }
        }

        // ── PartitionStrategy converters ────────────────────────────

        fn from_component_partition_strategy(
            s: __rb_bindings::rapidbyte::plugin::types::PartitionStrategy,
        ) -> ::rapidbyte_sdk::stream::PartitionStrategy {
            use __rb_bindings::rapidbyte::plugin::types::PartitionStrategy as CPS;
            match s {
                CPS::ModHash => ::rapidbyte_sdk::stream::PartitionStrategy::ModHash,
                CPS::Range => ::rapidbyte_sdk::stream::PartitionStrategy::Range,
            }
        }

        fn to_component_partition_strategy(
            s: ::rapidbyte_sdk::stream::PartitionStrategy,
        ) -> __rb_bindings::rapidbyte::plugin::types::PartitionStrategy {
            use __rb_bindings::rapidbyte::plugin::types::PartitionStrategy as CPS;
            match s {
                ::rapidbyte_sdk::stream::PartitionStrategy::ModHash => CPS::ModHash,
                ::rapidbyte_sdk::stream::PartitionStrategy::Range => CPS::Range,
            }
        }

        // ── DataErrorPolicy converters ──────────────────────────────

        fn from_component_data_error_policy(
            p: __rb_bindings::rapidbyte::plugin::types::DataErrorPolicy,
        ) -> ::rapidbyte_sdk::stream::DataErrorPolicy {
            use __rb_bindings::rapidbyte::plugin::types::DataErrorPolicy as CDP;
            match p {
                CDP::Fail => ::rapidbyte_sdk::stream::DataErrorPolicy::Fail,
                CDP::Skip => ::rapidbyte_sdk::stream::DataErrorPolicy::Skip,
                CDP::Dlq => ::rapidbyte_sdk::stream::DataErrorPolicy::Dlq,
            }
        }

        // ── ColumnPolicy converters ─────────────────────────────────

        fn from_component_column_policy(
            p: __rb_bindings::rapidbyte::plugin::types::ColumnPolicy,
        ) -> ::rapidbyte_sdk::stream::ColumnPolicy {
            use __rb_bindings::rapidbyte::plugin::types::ColumnPolicy as CCP;
            match p {
                CCP::Add => ::rapidbyte_sdk::stream::ColumnPolicy::Add,
                CCP::Ignore => ::rapidbyte_sdk::stream::ColumnPolicy::Ignore,
                CCP::Fail => ::rapidbyte_sdk::stream::ColumnPolicy::Fail,
            }
        }

        // ── TypeChangePolicy converters ─────────────────────────────

        fn from_component_type_change_policy(
            p: __rb_bindings::rapidbyte::plugin::types::TypeChangePolicy,
        ) -> ::rapidbyte_sdk::stream::TypeChangePolicy {
            use __rb_bindings::rapidbyte::plugin::types::TypeChangePolicy as CTP;
            match p {
                CTP::Coerce => ::rapidbyte_sdk::stream::TypeChangePolicy::Coerce,
                CTP::Fail => ::rapidbyte_sdk::stream::TypeChangePolicy::Fail,
                CTP::NullOut => ::rapidbyte_sdk::stream::TypeChangePolicy::Null,
            }
        }

        // ── NullabilityPolicy converters ────────────────────────────

        fn from_component_nullability_policy(
            p: __rb_bindings::rapidbyte::plugin::types::NullabilityPolicy,
        ) -> ::rapidbyte_sdk::stream::NullabilityPolicy {
            use __rb_bindings::rapidbyte::plugin::types::NullabilityPolicy as CNP;
            match p {
                CNP::Allow => ::rapidbyte_sdk::stream::NullabilityPolicy::Allow,
                CNP::Fail => ::rapidbyte_sdk::stream::NullabilityPolicy::Fail,
            }
        }

        // ── StreamContext converter: WIT → SDK ──────────────────────

        fn from_component_cursor_info(
            c: __rb_bindings::rapidbyte::plugin::types::CursorInfo,
        ) -> ::rapidbyte_sdk::cursor::CursorInfo {
            // cursor_type is a snake_case string in the WIT (e.g., "int64", "utf8").
            // Deserialize it via serde to map to the SDK CursorType enum.
            let quoted = format!("\"{}\"", c.cursor_type);
            let cursor_type: ::rapidbyte_sdk::cursor::CursorType =
                serde_json::from_str(&quoted)
                    .unwrap_or(::rapidbyte_sdk::cursor::CursorType::Utf8);
            let last_value = c.last_value_json.map(|json| {
                serde_json::from_str(&json).unwrap_or(::rapidbyte_sdk::cursor::CursorValue::Null)
            });
            ::rapidbyte_sdk::cursor::CursorInfo {
                cursor_field: c.cursor_field,
                tie_breaker_field: c.tie_breaker_field,
                cursor_type,
                last_value,
            }
        }

        fn from_component_stream_limits(
            l: __rb_bindings::rapidbyte::plugin::types::StreamLimits,
        ) -> ::rapidbyte_sdk::stream::StreamLimits {
            ::rapidbyte_sdk::stream::StreamLimits {
                max_batch_bytes: l.max_batch_bytes,
                max_record_bytes: l.max_record_bytes,
                max_inflight_batches: l.max_inflight_batches,
                max_parallel_requests: l.max_parallel_requests,
                checkpoint_interval_bytes: l.checkpoint_interval_bytes,
                checkpoint_interval_rows: l.checkpoint_interval_rows,
                checkpoint_interval_seconds: l.checkpoint_interval_seconds,
                max_records: l.max_records,
            }
        }

        fn from_component_stream_policies(
            p: __rb_bindings::rapidbyte::plugin::types::StreamPolicies,
        ) -> ::rapidbyte_sdk::stream::StreamPolicies {
            ::rapidbyte_sdk::stream::StreamPolicies {
                on_data_error: from_component_data_error_policy(p.on_data_error),
                schema_evolution: ::rapidbyte_sdk::stream::SchemaEvolutionPolicy {
                    new_column: from_component_column_policy(p.schema_evolution.new_column),
                    removed_column: from_component_column_policy(p.schema_evolution.removed_column),
                    type_change: from_component_type_change_policy(p.schema_evolution.type_change),
                    nullability_change: from_component_nullability_policy(p.schema_evolution.nullability_change),
                },
            }
        }

        fn from_component_stream_context(
            ctx: __rb_bindings::rapidbyte::plugin::types::StreamContext,
        ) -> ::rapidbyte_sdk::stream::StreamContext {
            // Extract primary_key before moving schema so we can populate
            // WriteMode::Upsert { primary_key } (the WIT enum does not carry it).
            let schema_pk = ctx.schema.primary_key.clone();
            ::rapidbyte_sdk::stream::StreamContext {
                stream_index: ctx.stream_index,
                stream_name: ctx.stream_name,
                source_stream_name: ctx.source_stream_name,
                schema: from_component_stream_schema(ctx.schema),
                sync_mode: from_component_sync_mode(ctx.sync_mode),
                cursor_info: ctx.cursor_info.map(from_component_cursor_info),
                limits: from_component_stream_limits(ctx.limits),
                policies: from_component_stream_policies(ctx.policies),
                write_mode: ctx.write_mode.map(|m| from_component_write_mode(m, &schema_pk)),
                selected_columns: ctx.selected_columns,
                partition_key: ctx.partition_key,
                partition_count: ctx.partition_count,
                partition_index: ctx.partition_index,
                effective_parallelism: ctx.effective_parallelism,
                partition_strategy: ctx.partition_strategy.map(from_component_partition_strategy),
                copy_flush_bytes_override: None,
            }
        }

        // ── StreamContext converter: SDK → WIT ──────────────────────

        fn to_component_cursor_info(
            c: ::rapidbyte_sdk::cursor::CursorInfo,
        ) -> __rb_bindings::rapidbyte::plugin::types::CursorInfo {
            // Serialize cursor_type via serde to get the snake_case string
            // (e.g., CursorType::Int64 → "int64").  Strip surrounding quotes.
            let cursor_type_str = serde_json::to_string(&c.cursor_type)
                .unwrap_or_else(|_| "\"utf8\"".to_string());
            let cursor_type = cursor_type_str.trim_matches('"').to_string();
            __rb_bindings::rapidbyte::plugin::types::CursorInfo {
                cursor_field: c.cursor_field,
                tie_breaker_field: c.tie_breaker_field,
                cursor_type,
                last_value_json: c.last_value.map(|v| serde_json::to_string(&v).unwrap_or_default()),
            }
        }

        fn to_component_stream_limits(
            l: ::rapidbyte_sdk::stream::StreamLimits,
        ) -> __rb_bindings::rapidbyte::plugin::types::StreamLimits {
            __rb_bindings::rapidbyte::plugin::types::StreamLimits {
                max_batch_bytes: l.max_batch_bytes,
                max_record_bytes: l.max_record_bytes,
                max_inflight_batches: l.max_inflight_batches,
                max_parallel_requests: l.max_parallel_requests,
                checkpoint_interval_bytes: l.checkpoint_interval_bytes,
                checkpoint_interval_rows: l.checkpoint_interval_rows,
                checkpoint_interval_seconds: l.checkpoint_interval_seconds,
                max_records: l.max_records,
            }
        }

        fn to_component_data_error_policy(
            p: ::rapidbyte_sdk::stream::DataErrorPolicy,
        ) -> __rb_bindings::rapidbyte::plugin::types::DataErrorPolicy {
            use __rb_bindings::rapidbyte::plugin::types::DataErrorPolicy as CDP;
            match p {
                ::rapidbyte_sdk::stream::DataErrorPolicy::Fail => CDP::Fail,
                ::rapidbyte_sdk::stream::DataErrorPolicy::Skip => CDP::Skip,
                ::rapidbyte_sdk::stream::DataErrorPolicy::Dlq => CDP::Dlq,
            }
        }

        fn to_component_column_policy(
            p: ::rapidbyte_sdk::stream::ColumnPolicy,
        ) -> __rb_bindings::rapidbyte::plugin::types::ColumnPolicy {
            use __rb_bindings::rapidbyte::plugin::types::ColumnPolicy as CCP;
            match p {
                ::rapidbyte_sdk::stream::ColumnPolicy::Add => CCP::Add,
                ::rapidbyte_sdk::stream::ColumnPolicy::Ignore => CCP::Ignore,
                ::rapidbyte_sdk::stream::ColumnPolicy::Fail => CCP::Fail,
            }
        }

        fn to_component_type_change_policy(
            p: ::rapidbyte_sdk::stream::TypeChangePolicy,
        ) -> __rb_bindings::rapidbyte::plugin::types::TypeChangePolicy {
            use __rb_bindings::rapidbyte::plugin::types::TypeChangePolicy as CTP;
            match p {
                ::rapidbyte_sdk::stream::TypeChangePolicy::Coerce => CTP::Coerce,
                ::rapidbyte_sdk::stream::TypeChangePolicy::Fail => CTP::Fail,
                ::rapidbyte_sdk::stream::TypeChangePolicy::Null => CTP::NullOut,
            }
        }

        fn to_component_nullability_policy(
            p: ::rapidbyte_sdk::stream::NullabilityPolicy,
        ) -> __rb_bindings::rapidbyte::plugin::types::NullabilityPolicy {
            use __rb_bindings::rapidbyte::plugin::types::NullabilityPolicy as CNP;
            match p {
                ::rapidbyte_sdk::stream::NullabilityPolicy::Allow => CNP::Allow,
                ::rapidbyte_sdk::stream::NullabilityPolicy::Fail => CNP::Fail,
            }
        }

        fn to_component_stream_policies(
            p: ::rapidbyte_sdk::stream::StreamPolicies,
        ) -> __rb_bindings::rapidbyte::plugin::types::StreamPolicies {
            __rb_bindings::rapidbyte::plugin::types::StreamPolicies {
                on_data_error: to_component_data_error_policy(p.on_data_error),
                schema_evolution: __rb_bindings::rapidbyte::plugin::types::SchemaEvolutionPolicy {
                    new_column: to_component_column_policy(p.schema_evolution.new_column),
                    removed_column: to_component_column_policy(p.schema_evolution.removed_column),
                    type_change: to_component_type_change_policy(p.schema_evolution.type_change),
                    nullability_change: to_component_nullability_policy(p.schema_evolution.nullability_change),
                },
            }
        }

        fn to_component_stream_context(
            ctx: ::rapidbyte_sdk::stream::StreamContext,
        ) -> __rb_bindings::rapidbyte::plugin::types::StreamContext {
            __rb_bindings::rapidbyte::plugin::types::StreamContext {
                stream_index: ctx.stream_index,
                stream_name: ctx.stream_name,
                source_stream_name: ctx.source_stream_name,
                schema: to_component_stream_schema(ctx.schema),
                sync_mode: to_component_sync_mode(ctx.sync_mode),
                cursor_info: ctx.cursor_info.map(to_component_cursor_info),
                limits: to_component_stream_limits(ctx.limits),
                policies: to_component_stream_policies(ctx.policies),
                write_mode: ctx.write_mode.map(to_component_write_mode),
                selected_columns: ctx.selected_columns,
                partition_key: ctx.partition_key,
                partition_count: ctx.partition_count,
                partition_index: ctx.partition_index,
                effective_parallelism: ctx.effective_parallelism,
                partition_strategy: ctx.partition_strategy.map(to_component_partition_strategy),
            }
        }

        // ── PluginSpec converter: SDK → WIT ─────────────────────────

        fn to_component_plugin_spec(
            spec: ::rapidbyte_sdk::discovery::PluginSpec,
        ) -> __rb_bindings::rapidbyte::plugin::types::PluginSpec {
            __rb_bindings::rapidbyte::plugin::types::PluginSpec {
                protocol_version: spec.protocol_version,
                config_schema_json: spec.config_schema_json,
                resource_schema_json: spec.resource_schema_json,
                documentation_url: spec.documentation_url,
                features: spec.features,
                supported_sync_modes: spec.supported_sync_modes.into_iter().map(to_component_sync_mode).collect(),
                supported_write_modes: spec.supported_write_modes.map(|modes|
                    modes.into_iter().map(to_component_write_mode).collect()
                ),
            }
        }

        // ── DiscoveredStream converter: SDK → WIT ───────────────────

        fn to_component_discovered_stream(
            s: ::rapidbyte_sdk::discovery::DiscoveredStream,
        ) -> __rb_bindings::rapidbyte::plugin::types::DiscoveredStream {
            __rb_bindings::rapidbyte::plugin::types::DiscoveredStream {
                name: s.name,
                schema: to_component_stream_schema(s.schema),
                supported_sync_modes: s.supported_sync_modes.into_iter().map(to_component_sync_mode).collect(),
                default_cursor_field: s.default_cursor_field,
                estimated_row_count: s.estimated_row_count,
                metadata_json: s.metadata_json,
            }
        }

        // ── Prerequisites converters ────────────────────────────────

        fn to_component_prerequisite_severity(
            s: ::rapidbyte_sdk::validation::PrerequisiteSeverity,
        ) -> __rb_bindings::rapidbyte::plugin::types::PrerequisiteSeverity {
            use __rb_bindings::rapidbyte::plugin::types::PrerequisiteSeverity as CPS;
            use ::rapidbyte_sdk::validation::PrerequisiteSeverity;
            match s {
                PrerequisiteSeverity::Error => CPS::Error,
                PrerequisiteSeverity::Warning => CPS::Warning,
                PrerequisiteSeverity::Info => CPS::Info,
            }
        }

        fn to_component_prerequisite_check(
            c: ::rapidbyte_sdk::validation::PrerequisiteCheck,
        ) -> __rb_bindings::rapidbyte::plugin::types::PrerequisiteCheck {
            __rb_bindings::rapidbyte::plugin::types::PrerequisiteCheck {
                name: c.name,
                passed: c.passed,
                severity: to_component_prerequisite_severity(c.severity),
                message: c.message,
                fix_hint: c.fix_hint,
            }
        }

        fn to_component_prerequisites_report(
            r: ::rapidbyte_sdk::validation::PrerequisitesReport,
        ) -> __rb_bindings::rapidbyte::plugin::types::PrerequisitesReport {
            __rb_bindings::rapidbyte::plugin::types::PrerequisitesReport {
                passed: r.passed,
                checks: r.checks.into_iter().map(to_component_prerequisite_check).collect(),
            }
        }

        // ── Apply converters ────────────────────────────────────────

        fn from_component_apply_request(
            r: __rb_bindings::rapidbyte::plugin::types::ApplyRequest,
        ) -> ::rapidbyte_sdk::lifecycle::ApplyRequest {
            ::rapidbyte_sdk::lifecycle::ApplyRequest {
                streams: r.streams.into_iter().map(from_component_stream_context).collect(),
                dry_run: r.dry_run,
            }
        }

        fn to_component_apply_action(
            a: ::rapidbyte_sdk::lifecycle::ApplyAction,
        ) -> __rb_bindings::rapidbyte::plugin::types::ApplyAction {
            __rb_bindings::rapidbyte::plugin::types::ApplyAction {
                stream_name: a.stream_name,
                description: a.description,
                ddl_executed: a.ddl_executed,
            }
        }

        fn to_component_apply_report(
            r: ::rapidbyte_sdk::lifecycle::ApplyReport,
        ) -> __rb_bindings::rapidbyte::plugin::types::ApplyReport {
            __rb_bindings::rapidbyte::plugin::types::ApplyReport {
                actions: r.actions.into_iter().map(to_component_apply_action).collect(),
            }
        }

        // ── Teardown converters ─────────────────────────────────────

        fn from_component_teardown_request(
            r: __rb_bindings::rapidbyte::plugin::types::TeardownRequest,
        ) -> ::rapidbyte_sdk::lifecycle::TeardownRequest {
            ::rapidbyte_sdk::lifecycle::TeardownRequest {
                streams: r.streams,
                reason: r.reason,
            }
        }

        fn to_component_teardown_report(
            r: ::rapidbyte_sdk::lifecycle::TeardownReport,
        ) -> __rb_bindings::rapidbyte::plugin::types::TeardownReport {
            __rb_bindings::rapidbyte::plugin::types::TeardownReport {
                actions: r.actions,
            }
        }

        // ── RunRequest / RunSummary converters ──────────────────────

        fn from_component_run_request(
            r: __rb_bindings::rapidbyte::plugin::types::RunRequest,
        ) -> ::rapidbyte_sdk::run::RunRequest {
            ::rapidbyte_sdk::run::RunRequest {
                streams: r.streams.into_iter().map(from_component_stream_context).collect(),
                dry_run: r.dry_run,
            }
        }

        fn to_component_stream_result(
            r: ::rapidbyte_sdk::run::StreamResult,
        ) -> __rb_bindings::rapidbyte::plugin::types::StreamResult {
            __rb_bindings::rapidbyte::plugin::types::StreamResult {
                stream_index: r.stream_index,
                stream_name: r.stream_name,
                outcome_json: r.outcome_json,
                succeeded: r.succeeded,
            }
        }

        fn to_component_run_summary(
            s: ::rapidbyte_sdk::run::RunSummary,
        ) -> __rb_bindings::rapidbyte::plugin::types::RunSummary {
            __rb_bindings::rapidbyte::plugin::types::RunSummary {
                results: s.results.into_iter().map(to_component_stream_result).collect(),
            }
        }

        /// Build a single-stream RunSummary from a ReadSummary.
        fn read_summary_to_run_summary(
            stream: &::rapidbyte_sdk::stream::StreamContext,
            summary: ::rapidbyte_sdk::metric::ReadSummary,
        ) -> ::rapidbyte_sdk::run::RunSummary {
            let stream_failed =
                ::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream.stream_index);
            let outcome = serde_json::json!({
                "records_read": summary.records_read,
                "bytes_read": summary.bytes_read,
                "batches_emitted": summary.batches_emitted,
                "checkpoint_count": summary.checkpoint_count,
                "records_skipped": summary.records_skipped,
            });
            ::rapidbyte_sdk::run::RunSummary {
                results: vec![::rapidbyte_sdk::run::StreamResult {
                    stream_index: stream.stream_index,
                    stream_name: stream.stream_name.clone(),
                    outcome_json: outcome.to_string(),
                    succeeded: !stream_failed,
                }],
            }
        }

        /// Build a single-stream RunSummary from a WriteSummary.
        fn write_summary_to_run_summary(
            stream: &::rapidbyte_sdk::stream::StreamContext,
            summary: ::rapidbyte_sdk::metric::WriteSummary,
            succeeded: bool,
        ) -> ::rapidbyte_sdk::run::RunSummary {
            let outcome = serde_json::json!({
                "records_written": summary.records_written,
                "bytes_written": summary.bytes_written,
                "batches_written": summary.batches_written,
                "checkpoint_count": summary.checkpoint_count,
                "records_failed": summary.records_failed,
            });
            ::rapidbyte_sdk::run::RunSummary {
                results: vec![::rapidbyte_sdk::run::StreamResult {
                    stream_index: stream.stream_index,
                    stream_name: stream.stream_name.clone(),
                    outcome_json: outcome.to_string(),
                    succeeded,
                }],
            }
        }

        /// Build a single-stream RunSummary from a TransformSummary.
        fn transform_summary_to_run_summary(
            stream: &::rapidbyte_sdk::stream::StreamContext,
            summary: ::rapidbyte_sdk::metric::TransformSummary,
            succeeded: bool,
        ) -> ::rapidbyte_sdk::run::RunSummary {
            let outcome = serde_json::json!({
                "records_in": summary.records_in,
                "records_out": summary.records_out,
                "bytes_in": summary.bytes_in,
                "bytes_out": summary.bytes_out,
                "batches_processed": summary.batches_processed,
            });
            ::rapidbyte_sdk::run::RunSummary {
                results: vec![::rapidbyte_sdk::run::StreamResult {
                    stream_index: stream.stream_index,
                    stream_name: stream.stream_name.clone(),
                    outcome_json: outcome.to_string(),
                    succeeded,
                }],
            }
        }
    }
}

/// Generate the Guest impl with lifecycle + role-specific methods.
fn gen_guest_impl(
    kind: &PluginKind,
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let lifecycle = gen_lifecycle_methods(struct_name, trait_path);

    let (guest_trait_path, role_methods) = match kind {
        PluginKind::Source => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::source::Guest },
            gen_source_methods(struct_name, trait_path, features),
        ),
        PluginKind::Destination => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::destination::Guest },
            gen_dest_methods(struct_name, trait_path, features),
        ),
        PluginKind::Transform => (
            quote! { __rb_bindings::exports::rapidbyte::plugin::transform::Guest },
            gen_transform_methods(struct_name, trait_path),
        ),
    };

    quote! {
        struct RapidbyteComponent;

        impl #guest_trait_path for RapidbyteComponent {
            #lifecycle
            #role_methods
        }
    }
}

/// Generate spec, open, validate, close methods (shared by all roles).
fn gen_lifecycle_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn spec() -> Result<
            __rb_bindings::rapidbyte::plugin::types::PluginSpec,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let sdk_spec = <#struct_name as #trait_path>::spec();
            Ok(to_component_plugin_spec(sdk_spec))
        }

        fn open(
            input: __rb_bindings::rapidbyte::plugin::types::OpenInput,
        ) -> Result<u64, __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let config_json = input.config_json;
            let _ = CONFIG_JSON.set(config_json.clone());

            let config: <#struct_name as #trait_path>::Config =
                parse_config(&config_json)?;

            let rt = get_runtime();
            let instance = rt
                .block_on(<#struct_name as #trait_path>::init(
                    config,
                    ::rapidbyte_sdk::plugin::InitInput::new(),
                ))
                .map_err(to_component_error)?;
            *get_state().borrow_mut() = Some(instance);
            Ok(1)
        }

        fn validate(
            input: __rb_bindings::rapidbyte::plugin::types::ValidateInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::ValidationReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let upstream = input.stream_schema.map(from_component_stream_schema);

            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            rt.block_on(<#struct_name as #trait_path>::validate(
                conn,
                ::rapidbyte_sdk::plugin::ValidateInput::new(upstream.as_ref())
                    .with_stream_name(input.stream_name.as_deref()),
            ))
                .map(to_component_validation)
                .map_err(to_component_error)
        }

        fn close(
            _input: __rb_bindings::rapidbyte::plugin::types::CloseInput,
        ) -> Result<(), __rb_bindings::rapidbyte::plugin::types::PluginError> {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            if let Some(conn) = state_ref.as_ref() {
                rt.block_on(<#struct_name as #trait_path>::close(
                    conn,
                    ::rapidbyte_sdk::plugin::CloseInput::new(),
                ))
                    .map_err(to_component_error)?;
            }
            drop(state_ref);
            *get_state().borrow_mut() = None;
            Ok(())
        }
    }
}

/// Build the read dispatch body based on declared features.
///
/// Conditionally inserts `if` branches for PartitionedRead and Cdc,
/// falling back to `Source::read` in the else branch.
fn gen_read_dispatch(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let has_partitioned = features.is_some_and(|f| f.has_partitioned_read);
    let has_cdc = features.is_some_and(|f| f.has_cdc);

    if !has_partitioned && !has_cdc {
        return quote! {
            rt.block_on(<#struct_name as #trait_path>::read(
                conn,
                ::rapidbyte_sdk::plugin::ReadInput::with_dry_run(stream.clone(), dry_run),
            ))
                .map_err(to_component_error)?
        };
    }

    let partition_branch = has_partitioned.then(|| quote! {
        if let Some(partition) = stream.partition_coordinates_typed() {
            return <#struct_name as ::rapidbyte_sdk::features::PartitionedSource>::read_partition(
                conn,
                ::rapidbyte_sdk::plugin::PartitionedReadInput::with_capabilities(
                    stream,
                    partition,
                    dry_run,
                    ::rapidbyte_sdk::capabilities::Emit,
                    ::rapidbyte_sdk::capabilities::Cancel,
                    ::rapidbyte_sdk::capabilities::State,
                    ::rapidbyte_sdk::capabilities::Checkpoints,
                    ::rapidbyte_sdk::capabilities::Metrics,
                    ::rapidbyte_sdk::capabilities::Log,
                ),
            ).await;
        }
    });

    let cdc_branch = has_cdc.then(|| {
        quote! {
            if stream.sync_mode == ::rapidbyte_sdk::wire::SyncMode::Cdc {
                let resume = stream.cdc_resume_token().unwrap_or(
                    ::rapidbyte_sdk::stream::CdcResumeToken {
                        value: None,
                        cursor_type: ::rapidbyte_sdk::cursor::CursorType::Utf8,
                    }
                );
                return <#struct_name as ::rapidbyte_sdk::features::CdcSource>::read_changes(
                    conn,
                    ::rapidbyte_sdk::plugin::CdcReadInput::with_capabilities(
                        stream,
                        resume,
                        dry_run,
                        ::rapidbyte_sdk::capabilities::Emit,
                        ::rapidbyte_sdk::capabilities::Cancel,
                        ::rapidbyte_sdk::capabilities::State,
                        ::rapidbyte_sdk::capabilities::Checkpoints,
                        ::rapidbyte_sdk::capabilities::Metrics,
                        ::rapidbyte_sdk::capabilities::Log,
                    ),
                ).await;
            }
        }
    });

    quote! {
        rt.block_on(async {
            #partition_branch
            #cdc_branch
            <#struct_name as #trait_path>::read(
                conn,
                ::rapidbyte_sdk::plugin::ReadInput::with_dry_run(stream.clone(), dry_run),
            ).await
        }).map_err(to_component_error)?
    }
}

/// Generate source-specific methods: discover, prerequisites, apply, run, teardown.
fn gen_source_methods(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let read_dispatch = gen_read_dispatch(struct_name, trait_path, features);

    quote! {
        fn prerequisites(
            _input: __rb_bindings::rapidbyte::plugin::types::PrerequisitesInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::PrerequisitesReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            rt.block_on(<#struct_name as #trait_path>::prerequisites(
                conn,
                ::rapidbyte_sdk::plugin::PrerequisitesInput::new(),
            ))
                .map(to_component_prerequisites_report)
                .map_err(to_component_error)
        }

        fn discover(
            _input: __rb_bindings::rapidbyte::plugin::types::DiscoverInput,
        ) -> Result<
            Vec<__rb_bindings::rapidbyte::plugin::types::DiscoveredStream>,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            let streams = rt
                .block_on(<#struct_name as #trait_path>::discover(
                    conn,
                    ::rapidbyte_sdk::plugin::DiscoverInput::new(),
                ))
                .map_err(to_component_error)?;

            Ok(streams.into_iter().map(to_component_discovered_stream).collect())
        }

        fn apply(
            input: __rb_bindings::rapidbyte::plugin::types::ApplyInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::ApplyReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            let sdk_request = from_component_apply_request(input.request);
            rt.block_on(<#struct_name as #trait_path>::apply(
                conn,
                ::rapidbyte_sdk::plugin::ApplyInput::new(sdk_request),
            ))
                .map(to_component_apply_report)
                .map_err(to_component_error)
        }

        fn run(
            input: __rb_bindings::rapidbyte::plugin::types::RunInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let sdk_request = from_component_run_request(input.request);
            let dry_run = sdk_request.dry_run;
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            // Process each stream and collect results
            let mut results = Vec::new();
            for stream in sdk_request.streams {
                let stream_idx = stream.stream_index;
                let stream_nm = stream.stream_name.clone();
                let _ = ::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream_idx);
                let summary = #read_dispatch;
                let stream_failed =
                    ::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream_idx);
                let outcome = serde_json::json!({
                    "records_read": summary.records_read,
                    "bytes_read": summary.bytes_read,
                    "batches_emitted": summary.batches_emitted,
                    "checkpoint_count": summary.checkpoint_count,
                    "records_skipped": summary.records_skipped,
                });
                results.push(::rapidbyte_sdk::run::StreamResult {
                    stream_index: stream_idx,
                    stream_name: stream_nm,
                    outcome_json: outcome.to_string(),
                    succeeded: !stream_failed,
                });
            }

            Ok(to_component_run_summary(::rapidbyte_sdk::run::RunSummary { results }))
        }

        fn teardown(
            input: __rb_bindings::rapidbyte::plugin::types::TeardownInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::TeardownReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            let sdk_request = from_component_teardown_request(input.request);
            rt.block_on(<#struct_name as #trait_path>::teardown(
                conn,
                ::rapidbyte_sdk::plugin::TeardownInput::new(sdk_request),
            ))
                .map(to_component_teardown_report)
                .map_err(to_component_error)
        }
    }
}

/// Generate destination-specific methods: prerequisites, apply, run, teardown.
fn gen_dest_methods(
    struct_name: &Ident,
    trait_path: &TokenStream,
    features: Option<&ManifestFeatures>,
) -> TokenStream {
    let write_dispatch = if features.is_some_and(|f| f.has_bulk_load) {
        quote! {
            rt.block_on(<#struct_name as ::rapidbyte_sdk::features::BulkDestination>::write_bulk(
                conn,
                ::rapidbyte_sdk::plugin::BulkWriteInput::with_capabilities(
                    stream.clone(),
                    dry_run,
                    ::rapidbyte_sdk::capabilities::Reader,
                    ::rapidbyte_sdk::capabilities::Cancel,
                    ::rapidbyte_sdk::capabilities::State,
                    ::rapidbyte_sdk::capabilities::Checkpoints,
                ),
            ))
            .map_err(to_component_error)?
        }
    } else {
        quote! {
            rt.block_on(<#struct_name as #trait_path>::write(
                conn,
                ::rapidbyte_sdk::plugin::WriteInput::with_dry_run(stream.clone(), dry_run),
            ))
                .map_err(to_component_error)?
        }
    };

    quote! {
        fn prerequisites(
            _input: __rb_bindings::rapidbyte::plugin::types::PrerequisitesInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::PrerequisitesReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            rt.block_on(<#struct_name as #trait_path>::prerequisites(
                conn,
                ::rapidbyte_sdk::plugin::PrerequisitesInput::new(),
            ))
                .map(to_component_prerequisites_report)
                .map_err(to_component_error)
        }

        fn apply(
            input: __rb_bindings::rapidbyte::plugin::types::ApplyInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::ApplyReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            let sdk_request = from_component_apply_request(input.request);
            rt.block_on(<#struct_name as #trait_path>::apply(
                conn,
                ::rapidbyte_sdk::plugin::ApplyInput::new(sdk_request),
            ))
                .map(to_component_apply_report)
                .map_err(to_component_error)
        }

        fn run(
            input: __rb_bindings::rapidbyte::plugin::types::RunInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let sdk_request = from_component_run_request(input.request);
            let dry_run = sdk_request.dry_run;
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            // Process each stream and collect results
            let mut results = Vec::new();
            for stream in sdk_request.streams {
                let _ = ::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream.stream_index);

                let summary = #write_dispatch;

                let succeeded =
                    !::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream.stream_index);
                let run_sum = write_summary_to_run_summary(&stream, summary, succeeded);
                results.extend(run_sum.results);
            }

            Ok(to_component_run_summary(::rapidbyte_sdk::run::RunSummary { results }))
        }

        fn teardown(
            input: __rb_bindings::rapidbyte::plugin::types::TeardownInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::TeardownReport,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            let sdk_request = from_component_teardown_request(input.request);
            rt.block_on(<#struct_name as #trait_path>::teardown(
                conn,
                ::rapidbyte_sdk::plugin::TeardownInput::new(sdk_request),
            ))
                .map(to_component_teardown_report)
                .map_err(to_component_error)
        }
    }
}

/// Generate transform-specific methods: run.
fn gen_transform_methods(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        fn run(
            input: __rb_bindings::rapidbyte::plugin::types::RunInput,
        ) -> Result<
            __rb_bindings::rapidbyte::plugin::types::RunSummary,
            __rb_bindings::rapidbyte::plugin::types::PluginError,
        > {
            let sdk_request = from_component_run_request(input.request);
            let dry_run = sdk_request.dry_run;
            let rt = get_runtime();
            let state_cell = get_state();
            let state_ref = state_cell.borrow();
            let conn = state_ref.as_ref().expect("Plugin not opened");

            // Process each stream and collect results
            let mut results = Vec::new();
            for stream in sdk_request.streams {
                let _ = ::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream.stream_index);

                let summary = rt
                    .block_on(<#struct_name as #trait_path>::transform(
                        conn,
                        ::rapidbyte_sdk::plugin::TransformInput::with_runtime(
                            stream.clone(),
                            input.plugin_id.as_str(),
                            dry_run,
                        ),
                    ))
                    .map_err(to_component_error)?;

                let succeeded =
                    !::rapidbyte_sdk::host_ffi::take_reported_stream_error(stream.stream_index);
                let run_sum = transform_summary_to_run_summary(&stream, summary, succeeded);
                results.extend(run_sum.results);
            }

            Ok(to_component_run_summary(::rapidbyte_sdk::run::RunSummary { results }))
        }
    }
}

/// Generate manifest and config schema embeds.
fn gen_embeds(struct_name: &Ident, trait_path: &TokenStream) -> TokenStream {
    quote! {
        // Embed the manifest JSON as a `rapidbyte_manifest_v1` custom section.
        // Only on wasm32 — native builds don't support this link section format.
        #[cfg(target_arch = "wasm32")]
        include!(concat!(env!("OUT_DIR"), "/rapidbyte_manifest_embed.rs"));

        // Embed the config schema as a `rapidbyte_config_schema_v1` Wasm custom section.
        #[cfg(target_arch = "wasm32")]
        const __RB_SCHEMA_BYTES: &[u8] =
            <<#struct_name as #trait_path>::Config as ::rapidbyte_sdk::ConfigSchema>::SCHEMA_JSON
                .as_bytes();

        #[cfg(target_arch = "wasm32")]
        #[link_section = "rapidbyte_config_schema_v1"]
        #[used]
        static __RAPIDBYTE_CONFIG_SCHEMA: [u8; { __RB_SCHEMA_BYTES.len() }] = {
            let mut arr = [0u8; __RB_SCHEMA_BYTES.len()];
            let mut i = 0;
            while i < __RB_SCHEMA_BYTES.len() {
                arr[i] = __RB_SCHEMA_BYTES[i];
                i += 1;
            }
            arr
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quote::quote;
    use syn::parse_quote;

    #[test]
    fn source_export_uses_v2_typed_inputs_and_feature_traits() {
        let struct_name: Ident = parse_quote!(TestTransform);
        let trait_path = quote!(::rapidbyte_sdk::plugin::Source);
        let features = ManifestFeatures {
            has_partitioned_read: true,
            has_cdc: true,
            has_bulk_load: false,
        };

        let generated = gen_guest_impl(
            &PluginKind::Source,
            &struct_name,
            &trait_path,
            Some(&features),
        )
        .to_string();

        assert!(generated.contains("InitInput :: new"));
        assert!(generated.contains("ReadInput :: with_dry_run"));
        assert!(generated.contains("PartitionedReadInput :: with_capabilities"));
        assert!(generated.contains("CdcReadInput :: with_capabilities"));
        assert!(generated.contains("PartitionedSource"));
        assert!(generated.contains("CdcSource"));
        assert!(!generated.contains("Context"));
    }

    #[test]
    fn destination_and_transform_exports_use_v2_inputs() {
        let struct_name: Ident = parse_quote!(TestDestination);
        let destination_trait = quote!(::rapidbyte_sdk::plugin::Destination);
        let destination_generated = gen_guest_impl(
            &PluginKind::Destination,
            &struct_name,
            &destination_trait,
            None,
        )
        .to_string();

        assert!(destination_generated.contains("InitInput :: new"));
        assert!(destination_generated.contains("WriteInput :: with_dry_run"));
        assert!(destination_generated.contains("PrerequisitesInput :: new"));
        assert!(destination_generated.contains("ApplyInput :: new"));
        assert!(destination_generated.contains("TeardownInput :: new"));
        assert!(!destination_generated.contains("Context"));

        let struct_name: Ident = parse_quote!(TestTransform);
        let transform_trait = quote!(::rapidbyte_sdk::plugin::Transform);
        let transform_generated =
            gen_guest_impl(&PluginKind::Transform, &struct_name, &transform_trait, None)
                .to_string();

        assert!(transform_generated.contains("InitInput :: new"));
        assert!(transform_generated.contains("TransformInput :: with_runtime"));
        assert!(transform_generated.contains("ValidateInput :: new"));
        assert!(transform_generated.contains("CloseInput :: new"));
        assert!(!transform_generated.contains("Context"));
    }

    #[test]
    fn common_run_summary_helpers_consult_reported_stream_errors() {
        let struct_name: Ident = parse_quote!(TestSource);

        let generated = gen_common(&struct_name).to_string();

        assert!(generated.contains("take_reported_stream_error"));
        assert!(generated.contains("succeeded : ! stream_failed"));
    }

    #[test]
    fn destination_run_dispatches_through_write() {
        let struct_name: Ident = parse_quote!(TestDestination);
        let trait_path = quote!(::rapidbyte_sdk::plugin::Destination);

        let generated = gen_dest_methods(&struct_name, &trait_path, None).to_string();

        assert!(generated.contains(":: write"));
        assert!(generated.contains("write_summary_to_run_summary"));
    }

    #[test]
    fn destination_run_dispatches_bulk_feature_through_write_bulk() {
        let struct_name: Ident = parse_quote!(TestDestination);
        let trait_path = quote!(::rapidbyte_sdk::plugin::Destination);
        let features = ManifestFeatures {
            has_partitioned_read: false,
            has_cdc: false,
            has_bulk_load: true,
        };

        let generated = gen_dest_methods(&struct_name, &trait_path, Some(&features)).to_string();

        assert!(generated.contains("BulkDestination"));
        assert!(generated.contains("write_bulk"));
    }

    #[test]
    fn destination_and_transform_runs_preclear_and_helpers_are_pure() {
        let struct_name: Ident = parse_quote!(TestTransform);
        let destination_trait = quote!(::rapidbyte_sdk::plugin::Destination);
        let transform_trait = quote!(::rapidbyte_sdk::plugin::Transform);

        let destination_generated =
            gen_dest_methods(&struct_name, &destination_trait, None).to_string();
        let transform_generated = gen_transform_methods(&struct_name, &transform_trait).to_string();
        let common_generated = gen_common(&struct_name).to_string();

        assert!(destination_generated
            .contains("let _ = :: rapidbyte_sdk :: host_ffi :: take_reported_stream_error"));
        assert!(transform_generated
            .contains("let _ = :: rapidbyte_sdk :: host_ffi :: take_reported_stream_error"));
        assert!(common_generated.contains("fn write_summary_to_run_summary"));
        assert!(common_generated
            .contains("summary : :: rapidbyte_sdk :: metric :: WriteSummary , succeeded : bool"));
    }

    #[test]
    fn feature_assertions_use_current_partitioned_source_trait_names() {
        let features = ManifestFeatures {
            has_partitioned_read: true,
            has_cdc: true,
            has_bulk_load: false,
        };

        let generated =
            gen_feature_assertions(&PluginKind::Source, &parse_quote!(TestSource), &features)
                .to_string();

        assert!(generated.contains("PartitionedSource"));
        assert!(generated.contains("CdcSource"));
        assert!(!generated.contains("PartitionPlanner"));
    }

    #[test]
    fn old_context_shaped_signatures_are_absent_from_generated_exports() {
        let struct_name: Ident = parse_quote!(TestSource);
        let trait_path = quote!(::rapidbyte_sdk::plugin::Source);
        let features = ManifestFeatures {
            has_partitioned_read: true,
            has_cdc: true,
            has_bulk_load: false,
        };

        let generated = gen_guest_impl(
            &PluginKind::Source,
            &struct_name,
            &trait_path,
            Some(&features),
        )
        .to_string();

        assert!(generated.contains("InitInput :: new"));
        assert!(generated.contains("DiscoverInput :: new"));
        assert!(generated.contains("ValidateInput :: new"));
        assert!(generated.contains("ReadInput :: with_dry_run"));
        assert!(!generated.contains("Context"));
    }

    #[test]
    fn stream_context_converters_preserve_effective_parallelism() {
        let struct_name: Ident = parse_quote!(TestTransform);
        let generated = gen_common(&struct_name).to_string();

        assert!(generated.contains("effective_parallelism : ctx . effective_parallelism"));
    }
}
