//! WIT binding glue: generated Wasmtime component bindings, Host trait impls,
//! and error/validation type converters for all three plugin worlds.

#![allow(clippy::needless_pass_by_value)]

// --- Generated bindings from WIT ---

pub mod source_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-source",
    });
}

pub mod dest_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-destination",
    });
}

pub mod transform_bindings {
    wasmtime::component::bindgen!({
        path: "../../wit",
        world: "rapidbyte-transform",
    });
}

// --- Error + validation converters + Host trait impls ---

use rapidbyte_types::batch::BatchMetadata;
use rapidbyte_types::checkpoint::{CheckpointKind, CursorUpdate, StateMutation, StateScope};
use rapidbyte_types::error::{BackoffClass, CommitState, ErrorCategory, ErrorScope, PluginError};
use rapidbyte_types::schema::{FieldConstraint, FieldRequirement, SchemaField, StreamSchema};
use rapidbyte_types::validation::{ValidationReport, ValidationStatus};

use crate::host_state::ComponentHostState;
use crate::socket::{SocketReadResult, SocketWriteResult};

macro_rules! for_each_world {
    ($macro_name:ident) => {
        $macro_name!(
            source_bindings,
            to_source_error,
            source_error_to_sdk,
            source_validation_to_sdk
        );
        $macro_name!(
            dest_bindings,
            to_dest_error,
            dest_error_to_sdk,
            dest_validation_to_sdk
        );
        $macro_name!(
            transform_bindings,
            to_transform_error,
            transform_error_to_sdk,
            transform_validation_to_sdk
        );
    };
}

macro_rules! define_error_converters {
    ($to_world_fn:ident, $from_world_fn:ident, $module:ident) => {
        fn $to_world_fn(error: PluginError) -> $module::rapidbyte::plugin::types::PluginError {
            use $module::rapidbyte::plugin::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                ErrorCategory as CErrorCategory, ErrorScope as CErrorScope,
                PluginError as CPluginError,
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
                    _ => CErrorCategory::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => CErrorScope::PerStream,
                    ErrorScope::Batch => CErrorScope::PerBatch,
                    ErrorScope::Record => CErrorScope::PerRecord,
                },
                code: error.code.to_string(),
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
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        pub fn $from_world_fn(
            error: $module::rapidbyte::plugin::types::PluginError,
        ) -> PluginError {
            PluginError {
                category: match error.category {
                    $module::rapidbyte::plugin::types::ErrorCategory::Config => {
                        ErrorCategory::Config
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Auth => ErrorCategory::Auth,
                    $module::rapidbyte::plugin::types::ErrorCategory::Permission => {
                        ErrorCategory::Permission
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::RateLimit => {
                        ErrorCategory::RateLimit
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::TransientNetwork => {
                        ErrorCategory::TransientNetwork
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::TransientDb => {
                        ErrorCategory::TransientDb
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Data => ErrorCategory::Data,
                    $module::rapidbyte::plugin::types::ErrorCategory::Schema => {
                        ErrorCategory::Schema
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Internal => {
                        ErrorCategory::Internal
                    }
                    $module::rapidbyte::plugin::types::ErrorCategory::Frame => ErrorCategory::Frame,
                    $module::rapidbyte::plugin::types::ErrorCategory::Cancelled => {
                        ErrorCategory::Cancelled
                    }
                },
                scope: match error.scope {
                    $module::rapidbyte::plugin::types::ErrorScope::PerStream => ErrorScope::Stream,
                    $module::rapidbyte::plugin::types::ErrorScope::PerBatch => ErrorScope::Batch,
                    $module::rapidbyte::plugin::types::ErrorScope::PerRecord => ErrorScope::Record,
                },
                code: error.code.into(),
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    $module::rapidbyte::plugin::types::BackoffClass::Fast => BackoffClass::Fast,
                    $module::rapidbyte::plugin::types::BackoffClass::Normal => BackoffClass::Normal,
                    $module::rapidbyte::plugin::types::BackoffClass::Slow => BackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    $module::rapidbyte::plugin::types::CommitState::BeforeCommit => {
                        CommitState::BeforeCommit
                    }
                    $module::rapidbyte::plugin::types::CommitState::AfterCommitUnknown => {
                        CommitState::AfterCommitUnknown
                    }
                    $module::rapidbyte::plugin::types::CommitState::AfterCommitConfirmed => {
                        CommitState::AfterCommitConfirmed
                    }
                }),
                details: error
                    .details_json
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
            }
        }
    };
}

macro_rules! impl_host_trait_for_world {
    ($module:ident, $to_world_error:ident, $from_world_fn:ident) => {
        impl $module::rapidbyte::plugin::host::Host for ComponentHostState {
            fn emit_batch(
                &mut self,
                handle: u64,
                metadata: $module::rapidbyte::plugin::types::BatchMetadata,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                let meta = BatchMetadata {
                    stream_index: metadata.stream_index,
                    schema_fingerprint: metadata.schema_fingerprint,
                    sequence_number: metadata.sequence_number,
                    compression: metadata.compression,
                    record_count: metadata.record_count,
                    byte_count: metadata.byte_count,
                };
                self.emit_batch_impl(handle, meta).map_err($to_world_error)
            }

            fn next_batch(
                &mut self,
            ) -> std::result::Result<Option<u64>, $module::rapidbyte::plugin::types::PluginError>
            {
                self.next_batch_impl().map_err($to_world_error)
            }

            fn current_stream_name(&mut self) -> String {
                self.current_stream().to_string()
            }

            fn next_batch_metadata(
                &mut self,
                handle: u64,
            ) -> std::result::Result<
                $module::rapidbyte::plugin::types::BatchMetadata,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.next_batch_metadata_impl(handle)
                    .map(|bm| $module::rapidbyte::plugin::types::BatchMetadata {
                        stream_index: bm.stream_index,
                        schema_fingerprint: bm.schema_fingerprint,
                        sequence_number: bm.sequence_number,
                        compression: bm.compression,
                        record_count: bm.record_count,
                        byte_count: bm.byte_count,
                    })
                    .map_err($to_world_error)
            }

            fn log(&mut self, level: u32, msg: String) {
                self.log_impl(level, msg);
            }

            fn checkpoint_begin(
                &mut self,
                kind: $module::rapidbyte::plugin::types::CheckpointKind,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError> {
                let kind = match kind {
                    $module::rapidbyte::plugin::types::CheckpointKind::Source => {
                        CheckpointKind::Source
                    }
                    $module::rapidbyte::plugin::types::CheckpointKind::Destination => {
                        CheckpointKind::Dest
                    }
                    $module::rapidbyte::plugin::types::CheckpointKind::Transform => {
                        CheckpointKind::Transform
                    }
                };
                self.checkpoint_begin_impl(kind).map_err($to_world_error)
            }

            fn checkpoint_set_cursor(
                &mut self,
                txn: u64,
                cursor: $module::rapidbyte::plugin::types::CursorUpdate,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                let cursor = CursorUpdate {
                    stream_name: cursor.stream_name,
                    cursor_field: cursor.cursor_field,
                    cursor_value_json: cursor.cursor_value_json,
                };
                self.checkpoint_set_cursor_impl(txn, cursor)
                    .map_err($to_world_error)
            }

            fn checkpoint_set_state(
                &mut self,
                txn: u64,
                mutation: $module::rapidbyte::plugin::types::StateMutation,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                let scope = match mutation.scope {
                    $module::rapidbyte::plugin::types::StateScope::Pipeline => StateScope::Pipeline,
                    $module::rapidbyte::plugin::types::StateScope::PerStream => StateScope::Stream,
                    $module::rapidbyte::plugin::types::StateScope::PluginInstance => {
                        StateScope::PluginInstance
                    }
                };
                let mutation = StateMutation {
                    scope,
                    key: mutation.key,
                    value: mutation.value,
                };
                self.checkpoint_set_state_impl(txn, mutation)
                    .map_err($to_world_error)
            }

            fn checkpoint_commit(
                &mut self,
                txn: u64,
                records_processed: u64,
                bytes_processed: u64,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.checkpoint_commit_impl(txn, records_processed, bytes_processed)
                    .map_err($to_world_error)
            }

            fn checkpoint_abort(&mut self, txn: u64) {
                self.checkpoint_abort_impl(txn);
            }

            fn is_cancelled(&mut self) -> bool {
                self.is_cancelled_impl()
            }

            fn stream_error(
                &mut self,
                stream_index: u32,
                error: $module::rapidbyte::plugin::types::PluginError,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                let sdk_error = $from_world_fn(error);
                self.stream_error_impl(stream_index, sdk_error)
                    .map_err($to_world_error)
            }

            fn counter_add(
                &mut self,
                name: String,
                value: u64,
                labels_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.counter_add_impl(name, value, labels_json)
                    .map_err($to_world_error)
            }

            fn gauge_set(
                &mut self,
                name: String,
                value: f64,
                labels_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.gauge_set_impl(name, value, labels_json)
                    .map_err($to_world_error)
            }

            fn histogram_record(
                &mut self,
                name: String,
                value: f64,
                labels_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.histogram_record_impl(name, value, labels_json)
                    .map_err($to_world_error)
            }

            fn emit_dlq_record(
                &mut self,
                stream_name: String,
                record_json: String,
                error_message: String,
                error_category: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.emit_dlq_record_impl(stream_name, record_json, error_message, error_category)
                    .map_err($to_world_error)
            }

            fn state_get(
                &mut self,
                scope: $module::rapidbyte::plugin::types::StateScope,
                key: String,
            ) -> std::result::Result<Option<String>, $module::rapidbyte::plugin::types::PluginError>
            {
                let scope = match scope {
                    $module::rapidbyte::plugin::types::StateScope::Pipeline => StateScope::Pipeline,
                    $module::rapidbyte::plugin::types::StateScope::PerStream => StateScope::Stream,
                    $module::rapidbyte::plugin::types::StateScope::PluginInstance => {
                        StateScope::PluginInstance
                    }
                };
                self.state_get_impl(scope, key).map_err($to_world_error)
            }

            fn state_put(
                &mut self,
                scope: $module::rapidbyte::plugin::types::StateScope,
                key: String,
                val: String,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                let scope = match scope {
                    $module::rapidbyte::plugin::types::StateScope::Pipeline => StateScope::Pipeline,
                    $module::rapidbyte::plugin::types::StateScope::PerStream => StateScope::Stream,
                    $module::rapidbyte::plugin::types::StateScope::PluginInstance => {
                        StateScope::PluginInstance
                    }
                };
                self.state_put_impl(scope, key, val)
                    .map_err($to_world_error)
            }

            fn frame_new(
                &mut self,
                capacity: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError> {
                Ok(self.frame_new_impl(capacity))
            }

            fn frame_write(
                &mut self,
                handle: u64,
                chunk: Vec<u8>,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError> {
                self.frame_write_impl(handle, chunk)
                    .map_err($to_world_error)
            }

            fn frame_seal(
                &mut self,
                handle: u64,
            ) -> std::result::Result<(), $module::rapidbyte::plugin::types::PluginError> {
                self.frame_seal_impl(handle).map_err($to_world_error)
            }

            fn frame_len(
                &mut self,
                handle: u64,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError> {
                self.frame_len_impl(handle).map_err($to_world_error)
            }

            fn frame_read(
                &mut self,
                handle: u64,
                offset: u64,
                len: u64,
            ) -> std::result::Result<Vec<u8>, $module::rapidbyte::plugin::types::PluginError> {
                self.frame_read_impl(handle, offset, len)
                    .map_err($to_world_error)
            }

            fn frame_drop(&mut self, handle: u64) {
                self.frame_drop_impl(handle);
            }

            fn connect_tcp(
                &mut self,
                host: String,
                port: u16,
            ) -> std::result::Result<u64, $module::rapidbyte::plugin::types::PluginError> {
                self.connect_tcp_impl(host, port).map_err($to_world_error)
            }

            fn socket_read(
                &mut self,
                handle: u64,
                len: u64,
            ) -> std::result::Result<
                $module::rapidbyte::plugin::types::SocketReadResult,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.socket_read_impl(handle, len)
                    .map(|result| match result {
                        SocketReadResult::Data(data) => {
                            $module::rapidbyte::plugin::types::SocketReadResult::Data(data)
                        }
                        SocketReadResult::Eof => {
                            $module::rapidbyte::plugin::types::SocketReadResult::Eof
                        }
                        SocketReadResult::WouldBlock => {
                            $module::rapidbyte::plugin::types::SocketReadResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_write(
                &mut self,
                handle: u64,
                data: Vec<u8>,
            ) -> std::result::Result<
                $module::rapidbyte::plugin::types::SocketWriteResult,
                $module::rapidbyte::plugin::types::PluginError,
            > {
                self.socket_write_impl(handle, data)
                    .map(|result| match result {
                        SocketWriteResult::Written(n) => {
                            $module::rapidbyte::plugin::types::SocketWriteResult::Written(n)
                        }
                        SocketWriteResult::WouldBlock => {
                            $module::rapidbyte::plugin::types::SocketWriteResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_close(&mut self, handle: u64) {
                self.socket_close_impl(handle);
            }
        }
    };
}

macro_rules! define_validation_to_sdk {
    ($fn_name:ident, $module:ident) => {
        pub fn $fn_name(
            value: $module::rapidbyte::plugin::types::ValidationReport,
        ) -> ValidationReport {
            ValidationReport {
                status: match value.status {
                    $module::rapidbyte::plugin::types::ValidationStatus::Success => {
                        ValidationStatus::Success
                    }
                    $module::rapidbyte::plugin::types::ValidationStatus::Failed => {
                        ValidationStatus::Failed
                    }
                    $module::rapidbyte::plugin::types::ValidationStatus::Warning => {
                        ValidationStatus::Warning
                    }
                },
                message: value.message,
                warnings: value.warnings,
                output_schema: value.output_schema.map(|s| StreamSchema {
                    fields: s
                        .fields
                        .into_iter()
                        .map(|f| SchemaField {
                            name: f.name,
                            arrow_type: f.arrow_type,
                            nullable: f.nullable,
                            is_primary_key: f.is_primary_key,
                            is_generated: f.is_generated,
                            is_partition_key: f.is_partition_key,
                            default_value: f.default_value,
                        })
                        .collect(),
                    primary_key: s.primary_key,
                    partition_keys: s.partition_keys,
                    source_defined_cursor: s.source_defined_cursor,
                    schema_id: s.schema_id,
                }),
                field_requirements: value.field_requirements.map(|reqs| {
                    reqs.into_iter()
                        .map(|r| FieldRequirement {
                            field_name: r.field_name,
                            constraint: match r.constraint {
                                $module::rapidbyte::plugin::types::FieldConstraint::FieldRequired => {
                                    FieldConstraint::FieldRequired
                                }
                                $module::rapidbyte::plugin::types::FieldConstraint::FieldOptional => {
                                    FieldConstraint::FieldOptional
                                }
                                $module::rapidbyte::plugin::types::FieldConstraint::FieldForbidden => {
                                    FieldConstraint::FieldForbidden
                                }
                                $module::rapidbyte::plugin::types::FieldConstraint::FieldRecommended => {
                                    FieldConstraint::FieldRecommended
                                }
                                $module::rapidbyte::plugin::types::FieldConstraint::TypeIncompatible => {
                                    FieldConstraint::TypeIncompatible
                                }
                            },
                            reason: r.reason,
                            accepted_types: r.accepted_types,
                        })
                        .collect()
                }),
            }
        }
    };
}

macro_rules! define_world_glue {
    ($module:ident, $to_world_error:ident, $from_world_error:ident, $validation_fn:ident) => {
        define_error_converters!($to_world_error, $from_world_error, $module);
        impl_host_trait_for_world!($module, $to_world_error, $from_world_error);
        impl $module::rapidbyte::plugin::types::Host for ComponentHostState {}
        define_validation_to_sdk!($validation_fn, $module);
    };
}

for_each_world!(define_world_glue);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v7_world_bindings_exist() {
        let _ = std::any::TypeId::of::<source_bindings::RapidbyteSource>();
        let _ = std::any::TypeId::of::<dest_bindings::RapidbyteDestination>();
        let _ = std::any::TypeId::of::<transform_bindings::RapidbyteTransform>();

        let _: Option<source_bindings::rapidbyte::plugin::types::RunRequest> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::RunSummary> = None;
    }

    #[test]
    fn v2_lifecycle_binding_types_exist() {
        let _: Option<source_bindings::rapidbyte::plugin::types::OpenInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::PrerequisitesInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::DiscoverInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::ValidateInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::ApplyInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::RunInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::CloseInput> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::TeardownInput> = None;

        let _: Option<source_bindings::rapidbyte::plugin::types::FrameHandle> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::SocketHandle> = None;
        let _: Option<source_bindings::rapidbyte::plugin::types::CheckpointTxn> = None;
    }

    #[test]
    fn source_validation_conversion_preserves_warnings() {
        let report = source_bindings::rapidbyte::plugin::types::ValidationReport {
            status: source_bindings::rapidbyte::plugin::types::ValidationStatus::Warning,
            message: "validation incomplete".to_string(),
            warnings: vec![
                "missing live check".to_string(),
                "using defaults".to_string(),
            ],
            output_schema: None,
            field_requirements: None,
        };

        let converted = source_validation_to_sdk(report);

        assert_eq!(converted.status, ValidationStatus::Warning);
        assert_eq!(converted.message, "validation incomplete");
        assert_eq!(
            converted.warnings,
            vec![
                "missing live check".to_string(),
                "using defaults".to_string()
            ]
        );
    }

    #[test]
    fn source_validation_conversion_maps_output_schema() {
        let report = source_bindings::rapidbyte::plugin::types::ValidationReport {
            status: source_bindings::rapidbyte::plugin::types::ValidationStatus::Success,
            message: "ok".to_string(),
            warnings: vec![],
            output_schema: Some(source_bindings::rapidbyte::plugin::types::StreamSchema {
                fields: vec![source_bindings::rapidbyte::plugin::types::SchemaField {
                    name: "id".to_string(),
                    arrow_type: "int64".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    is_generated: false,
                    is_partition_key: false,
                    default_value: None,
                }],
                primary_key: vec!["id".to_string()],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            }),
            field_requirements: None,
        };

        let converted = source_validation_to_sdk(report);
        let schema = converted.output_schema.unwrap();
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name, "id");
        assert!(schema.fields[0].is_primary_key);
    }

    #[test]
    fn source_validation_conversion_maps_field_requirements() {
        let report = source_bindings::rapidbyte::plugin::types::ValidationReport {
            status: source_bindings::rapidbyte::plugin::types::ValidationStatus::Warning,
            message: "constraints".to_string(),
            warnings: vec![],
            output_schema: None,
            field_requirements: Some(vec![
                source_bindings::rapidbyte::plugin::types::FieldRequirement {
                    field_name: "email".to_string(),
                    constraint:
                        source_bindings::rapidbyte::plugin::types::FieldConstraint::FieldRequired,
                    reason: "destination needs email".to_string(),
                    accepted_types: Some(vec!["utf8".to_string()]),
                },
            ]),
        };

        let converted = source_validation_to_sdk(report);
        let reqs = converted.field_requirements.unwrap();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].field_name, "email");
        assert_eq!(reqs[0].constraint, FieldConstraint::FieldRequired);
    }

    #[test]
    fn error_category_cancelled_roundtrips() {
        let err = PluginError::cancelled("TEST_CANCEL", "test cancellation");
        let wit_err = to_source_error(err);
        assert!(matches!(
            wit_err.category,
            source_bindings::rapidbyte::plugin::types::ErrorCategory::Cancelled
        ));

        let back = source_error_to_sdk(wit_err);
        assert_eq!(back.category, ErrorCategory::Cancelled);
    }
}
