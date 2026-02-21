//! WIT binding glue: error type converters between SDK types and world-specific WIT types,
//! `Host` trait impls for all three worlds, and validation result converters.

use rapidbyte_sdk::errors::{
    BackoffClass, CommitState, ConnectorError, ErrorCategory, ErrorScope, ValidationResult,
    ValidationStatus,
};

use super::component_runtime::{
    dest_bindings, source_bindings, transform_bindings, ComponentHostState,
};
use super::host_socket::{SocketReadResultInternal, SocketWriteResultInternal};

macro_rules! define_error_converters {
    ($to_world_fn:ident, $from_world_fn:ident, $module:ident) => {
        fn $to_world_fn(
            error: ConnectorError,
        ) -> $module::rapidbyte::connector::types::ConnectorError {
            use $module::rapidbyte::connector::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                ConnectorError as CConnectorError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };

            CConnectorError {
                category: match error.category {
                    ErrorCategory::Config => CErrorCategory::Config,
                    ErrorCategory::Auth => CErrorCategory::Auth,
                    ErrorCategory::Permission => CErrorCategory::Permission,
                    ErrorCategory::RateLimit => CErrorCategory::RateLimit,
                    ErrorCategory::TransientNetwork => CErrorCategory::TransientNetwork,
                    ErrorCategory::TransientDb => CErrorCategory::TransientDb,
                    ErrorCategory::Data => CErrorCategory::Data,
                    ErrorCategory::Schema => CErrorCategory::Schema,
                    ErrorCategory::Internal => CErrorCategory::Internal,
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
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        pub fn $from_world_fn(
            error: $module::rapidbyte::connector::types::ConnectorError,
        ) -> ConnectorError {
            ConnectorError {
                category: match error.category {
                    $module::rapidbyte::connector::types::ErrorCategory::Config => {
                        ErrorCategory::Config
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Auth => {
                        ErrorCategory::Auth
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Permission => {
                        ErrorCategory::Permission
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::RateLimit => {
                        ErrorCategory::RateLimit
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::TransientNetwork => {
                        ErrorCategory::TransientNetwork
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::TransientDb => {
                        ErrorCategory::TransientDb
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Data => {
                        ErrorCategory::Data
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Schema => {
                        ErrorCategory::Schema
                    }
                    $module::rapidbyte::connector::types::ErrorCategory::Internal => {
                        ErrorCategory::Internal
                    }
                },
                scope: match error.scope {
                    $module::rapidbyte::connector::types::ErrorScope::PerStream => {
                        ErrorScope::Stream
                    }
                    $module::rapidbyte::connector::types::ErrorScope::PerBatch => ErrorScope::Batch,
                    $module::rapidbyte::connector::types::ErrorScope::PerRecord => {
                        ErrorScope::Record
                    }
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    $module::rapidbyte::connector::types::BackoffClass::Fast => BackoffClass::Fast,
                    $module::rapidbyte::connector::types::BackoffClass::Normal => {
                        BackoffClass::Normal
                    }
                    $module::rapidbyte::connector::types::BackoffClass::Slow => BackoffClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| match state {
                    $module::rapidbyte::connector::types::CommitState::BeforeCommit => {
                        CommitState::BeforeCommit
                    }
                    $module::rapidbyte::connector::types::CommitState::AfterCommitUnknown => {
                        CommitState::AfterCommitUnknown
                    }
                    $module::rapidbyte::connector::types::CommitState::AfterCommitConfirmed => {
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

define_error_converters!(to_source_error, source_error_to_sdk, source_bindings);
define_error_converters!(to_dest_error, dest_error_to_sdk, dest_bindings);
define_error_converters!(
    to_transform_error,
    transform_error_to_sdk,
    transform_bindings
);

macro_rules! impl_host_trait_for_world {
    ($module:ident, $to_world_error:ident) => {
        impl $module::rapidbyte::connector::host::Host for ComponentHostState {
            fn emit_batch(
                &mut self,
                batch: Vec<u8>,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.emit_batch_impl(batch).map_err($to_world_error)
            }

            fn next_batch(
                &mut self,
            ) -> std::result::Result<
                Option<Vec<u8>>,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.next_batch_impl().map_err($to_world_error)
            }

            fn log(&mut self, level: u32, msg: String) {
                self.log_impl(level, msg);
            }

            fn checkpoint(
                &mut self,
                kind: u32,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.checkpoint_impl(kind, payload_json)
                    .map_err($to_world_error)
            }

            fn metric(
                &mut self,
                payload_json: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.metric_impl(payload_json).map_err($to_world_error)
            }

            fn state_get(
                &mut self,
                scope: u32,
                key: String,
            ) -> std::result::Result<
                Option<String>,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.state_get_impl(scope, key).map_err($to_world_error)
            }

            fn state_put(
                &mut self,
                scope: u32,
                key: String,
                val: String,
            ) -> std::result::Result<(), $module::rapidbyte::connector::types::ConnectorError> {
                self.state_put_impl(scope, key, val)
                    .map_err($to_world_error)
            }

            fn state_cas(
                &mut self,
                scope: u32,
                key: String,
                expected: Option<String>,
                new_val: String,
            ) -> std::result::Result<bool, $module::rapidbyte::connector::types::ConnectorError>
            {
                self.state_cas_impl(scope, key, expected, new_val)
                    .map_err($to_world_error)
            }

            fn connect_tcp(
                &mut self,
                host: String,
                port: u16,
            ) -> std::result::Result<u64, $module::rapidbyte::connector::types::ConnectorError>
            {
                self.connect_tcp_impl(host, port).map_err($to_world_error)
            }

            fn socket_read(
                &mut self,
                handle: u64,
                len: u64,
            ) -> std::result::Result<
                $module::rapidbyte::connector::types::SocketReadResult,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.socket_read_impl(handle, len)
                    .map(|result| match result {
                        SocketReadResultInternal::Data(data) => {
                            $module::rapidbyte::connector::types::SocketReadResult::Data(data)
                        }
                        SocketReadResultInternal::Eof => {
                            $module::rapidbyte::connector::types::SocketReadResult::Eof
                        }
                        SocketReadResultInternal::WouldBlock => {
                            $module::rapidbyte::connector::types::SocketReadResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_write(
                &mut self,
                handle: u64,
                data: Vec<u8>,
            ) -> std::result::Result<
                $module::rapidbyte::connector::types::SocketWriteResult,
                $module::rapidbyte::connector::types::ConnectorError,
            > {
                self.socket_write_impl(handle, data)
                    .map(|result| match result {
                        SocketWriteResultInternal::Written(n) => {
                            $module::rapidbyte::connector::types::SocketWriteResult::Written(n)
                        }
                        SocketWriteResultInternal::WouldBlock => {
                            $module::rapidbyte::connector::types::SocketWriteResult::WouldBlock
                        }
                    })
                    .map_err($to_world_error)
            }

            fn socket_close(&mut self, handle: u64) {
                self.socket_close_impl(handle)
            }
        }
    };
}

impl_host_trait_for_world!(source_bindings, to_source_error);
impl_host_trait_for_world!(dest_bindings, to_dest_error);
impl_host_trait_for_world!(transform_bindings, to_transform_error);

impl source_bindings::rapidbyte::connector::types::Host for ComponentHostState {}
impl dest_bindings::rapidbyte::connector::types::Host for ComponentHostState {}
impl transform_bindings::rapidbyte::connector::types::Host for ComponentHostState {}

pub fn source_validation_to_sdk(
    value: source_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            source_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            source_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            source_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}

pub fn dest_validation_to_sdk(
    value: dest_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            dest_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}

pub fn transform_validation_to_sdk(
    value: transform_bindings::rapidbyte::connector::types::ValidationResult,
) -> ValidationResult {
    ValidationResult {
        status: match value.status {
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Success => {
                ValidationStatus::Success
            }
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Failed => {
                ValidationStatus::Failed
            }
            transform_bindings::rapidbyte::connector::types::ValidationStatus::Warning => {
                ValidationStatus::Warning
            }
        },
        message: value.message,
    }
}
