//! Standard connector traits.
//!
//! Rapidbyte connectors implement one or more traits below.
//! The `*_connector_main!` macros export component-model bindings
//! for the corresponding WIT world.

use crate::errors::{ConnectorError, ValidationResult};
use crate::protocol::{
    Catalog, OpenContext, OpenInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};

/// Source connector lifecycle.
pub trait SourceConnector: Default {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError>;
    fn discover(&mut self) -> Result<Catalog, ConnectorError>;
    fn validate(&mut self) -> Result<ValidationResult, ConnectorError>;
    fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Destination connector lifecycle.
pub trait DestinationConnector: Default {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError>;
    fn validate(&mut self) -> Result<ValidationResult, ConnectorError>;
    fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Transform connector lifecycle.
pub trait TransformConnector: Default {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError>;
    fn validate(&mut self) -> Result<ValidationResult, ConnectorError>;
    fn transform(&mut self, ctx: StreamContext) -> Result<TransformSummary, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Export a source connector component for `rapidbyte-source` world.
#[macro_export]
macro_rules! source_connector_main {
    ($connector_type:ty) => {
        mod __rb_source_bindings {
            rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../wit",
                world: "rapidbyte-source",
            });
        }

        use std::cell::RefCell;
        use std::sync::OnceLock;

        use rapidbyte_sdk::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};

        struct SyncRefCell(RefCell<$connector_type>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_connector() -> &'static RefCell<$connector_type> {
            &CONNECTOR
                .get_or_init(|| SyncRefCell(RefCell::new(<$connector_type>::default())))
                .0
        }

        fn to_component_error(
            error: rapidbyte_sdk::errors::ConnectorError,
        ) -> __rb_source_bindings::rapidbyte::connector::types::ConnectorError {
            use __rb_source_bindings::rapidbyte::connector::types::{
                BackoffClass as CBClass, CommitState as CState, ConnectorError as CErr,
                ErrorCategory as Cat, ErrorScope as Scope,
            };

            CErr {
                category: match error.category {
                    ErrorCategory::Config => Cat::Config,
                    ErrorCategory::Auth => Cat::Auth,
                    ErrorCategory::Permission => Cat::Permission,
                    ErrorCategory::RateLimit => Cat::RateLimit,
                    ErrorCategory::TransientNetwork => Cat::TransientNetwork,
                    ErrorCategory::TransientDb => Cat::TransientDb,
                    ErrorCategory::Data => Cat::Data,
                    ErrorCategory::Schema => Cat::Schema,
                    ErrorCategory::Internal => Cat::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => Scope::PerStream,
                    ErrorScope::Batch => Scope::PerBatch,
                    ErrorScope::Record => Scope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBClass::Fast,
                    BackoffClass::Normal => CBClass::Normal,
                    BackoffClass::Slow => CBClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|s| match s {
                    CommitState::BeforeCommit => CState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        fn parse_open_context(
            config_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::OpenContext,
            __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let config_value: serde_json::Value = serde_json::from_str(&config_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_CONFIG_JSON",
                    format!("Invalid config JSON: {}", e),
                ))
            })?;

            Ok(rapidbyte_sdk::protocol::OpenContext {
                config: rapidbyte_sdk::protocol::ConfigBlob::Json(config_value),
                connector_id: env!("CARGO_PKG_NAME").to_string(),
                connector_version: env!("CARGO_PKG_VERSION").to_string(),
            })
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::StreamContext,
            __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn to_component_validation(
            result: rapidbyte_sdk::errors::ValidationResult,
        ) -> __rb_source_bindings::rapidbyte::connector::types::ValidationResult {
            use __rb_source_bindings::rapidbyte::connector::types::{
                ValidationResult as CValidationResult, ValidationStatus as CValidationStatus,
            };
            use rapidbyte_sdk::errors::ValidationStatus;

            CValidationResult {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
            }
        }

        struct RapidbyteSourceComponent;

        impl __rb_source_bindings::exports::rapidbyte::connector::source_connector::Guest
            for RapidbyteSourceComponent
        {
            fn open(
                config_json: String,
            ) -> Result<(), __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let open_ctx = parse_open_context(config_json)?;
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::SourceConnector>::open(
                    &mut *conn,
                    open_ctx,
                )
                .map(|_| ())
                .map_err(to_component_error)
            }

            fn discover(
            ) -> Result<String, __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let mut conn = get_connector().borrow_mut();
                let catalog = <$connector_type as rapidbyte_sdk::connector::SourceConnector>::discover(
                    &mut *conn,
                )
                .map_err(to_component_error)?;

                serde_json::to_string(&catalog).map_err(|e| {
                    to_component_error(rapidbyte_sdk::errors::ConnectorError::internal(
                        "SERIALIZE_CATALOG",
                        e.to_string(),
                    ))
                })
            }

            fn validate(
            ) -> Result<
                __rb_source_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::SourceConnector>::validate(&mut *conn)
                    .map(to_component_validation)
                    .map_err(to_component_error)
            }

            fn run_read(
                ctx_json: String,
            ) -> Result<
                __rb_source_bindings::rapidbyte::connector::types::ReadSummary,
                __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let ctx = parse_stream_context(ctx_json)?;
                let mut conn = get_connector().borrow_mut();
                let summary = <$connector_type as rapidbyte_sdk::connector::SourceConnector>::read(
                    &mut *conn,
                    ctx,
                )
                .map_err(to_component_error)?;

                Ok(__rb_source_bindings::rapidbyte::connector::types::ReadSummary {
                    records_read: summary.records_read,
                    bytes_read: summary.bytes_read,
                    batches_emitted: summary.batches_emitted,
                    checkpoint_count: summary.checkpoint_count,
                    records_skipped: summary.records_skipped,
                })
            }

            fn close(
            ) -> Result<(), __rb_source_bindings::rapidbyte::connector::types::ConnectorError> {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::SourceConnector>::close(&mut *conn)
                    .map_err(to_component_error)
            }
        }

        __rb_source_bindings::export!(
            RapidbyteSourceComponent with_types_in __rb_source_bindings
        );

        fn main() {}
    };
}

/// Export a destination connector component for `rapidbyte-destination` world.
#[macro_export]
macro_rules! dest_connector_main {
    ($connector_type:ty) => {
        mod __rb_dest_bindings {
            rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../wit",
                world: "rapidbyte-destination",
            });
        }

        use std::cell::RefCell;
        use std::sync::OnceLock;

        use rapidbyte_sdk::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};

        struct SyncRefCell(RefCell<$connector_type>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_connector() -> &'static RefCell<$connector_type> {
            &CONNECTOR
                .get_or_init(|| SyncRefCell(RefCell::new(<$connector_type>::default())))
                .0
        }

        fn to_component_error(
            error: rapidbyte_sdk::errors::ConnectorError,
        ) -> __rb_dest_bindings::rapidbyte::connector::types::ConnectorError {
            use __rb_dest_bindings::rapidbyte::connector::types::{
                BackoffClass as CBClass, CommitState as CState, ConnectorError as CErr,
                ErrorCategory as Cat, ErrorScope as Scope,
            };

            CErr {
                category: match error.category {
                    ErrorCategory::Config => Cat::Config,
                    ErrorCategory::Auth => Cat::Auth,
                    ErrorCategory::Permission => Cat::Permission,
                    ErrorCategory::RateLimit => Cat::RateLimit,
                    ErrorCategory::TransientNetwork => Cat::TransientNetwork,
                    ErrorCategory::TransientDb => Cat::TransientDb,
                    ErrorCategory::Data => Cat::Data,
                    ErrorCategory::Schema => Cat::Schema,
                    ErrorCategory::Internal => Cat::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => Scope::PerStream,
                    ErrorScope::Batch => Scope::PerBatch,
                    ErrorScope::Record => Scope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBClass::Fast,
                    BackoffClass::Normal => CBClass::Normal,
                    BackoffClass::Slow => CBClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|s| match s {
                    CommitState::BeforeCommit => CState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        fn parse_open_context(
            config_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::OpenContext,
            __rb_dest_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let config_value: serde_json::Value = serde_json::from_str(&config_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_CONFIG_JSON",
                    format!("Invalid config JSON: {}", e),
                ))
            })?;

            Ok(rapidbyte_sdk::protocol::OpenContext {
                config: rapidbyte_sdk::protocol::ConfigBlob::Json(config_value),
                connector_id: env!("CARGO_PKG_NAME").to_string(),
                connector_version: env!("CARGO_PKG_VERSION").to_string(),
            })
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::StreamContext,
            __rb_dest_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn to_component_validation(
            result: rapidbyte_sdk::errors::ValidationResult,
        ) -> __rb_dest_bindings::rapidbyte::connector::types::ValidationResult {
            use __rb_dest_bindings::rapidbyte::connector::types::{
                ValidationResult as CValidationResult, ValidationStatus as CValidationStatus,
            };
            use rapidbyte_sdk::errors::ValidationStatus;

            CValidationResult {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
            }
        }

        struct RapidbyteDestinationComponent;

        impl __rb_dest_bindings::exports::rapidbyte::connector::dest_connector::Guest
            for RapidbyteDestinationComponent
        {
            fn open(
                config_json: String,
            ) -> Result<(), __rb_dest_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let open_ctx = parse_open_context(config_json)?;
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::open(
                    &mut *conn,
                    open_ctx,
                )
                .map(|_| ())
                .map_err(to_component_error)
            }

            fn validate(
            ) -> Result<
                __rb_dest_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_dest_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::validate(
                    &mut *conn,
                )
                .map(to_component_validation)
                .map_err(to_component_error)
            }

            fn run_write(
                ctx_json: String,
            ) -> Result<
                __rb_dest_bindings::rapidbyte::connector::types::WriteSummary,
                __rb_dest_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let ctx = parse_stream_context(ctx_json)?;
                let mut conn = get_connector().borrow_mut();
                let summary = <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::write(
                    &mut *conn,
                    ctx,
                )
                .map_err(to_component_error)?;

                Ok(__rb_dest_bindings::rapidbyte::connector::types::WriteSummary {
                    records_written: summary.records_written,
                    bytes_written: summary.bytes_written,
                    batches_written: summary.batches_written,
                    checkpoint_count: summary.checkpoint_count,
                    records_failed: summary.records_failed,
                })
            }

            fn close(
            ) -> Result<(), __rb_dest_bindings::rapidbyte::connector::types::ConnectorError> {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::close(
                    &mut *conn,
                )
                .map_err(to_component_error)
            }
        }

        __rb_dest_bindings::export!(
            RapidbyteDestinationComponent with_types_in __rb_dest_bindings
        );

        fn main() {}
    };
}

/// Export a transform connector component for `rapidbyte-transform` world.
#[macro_export]
macro_rules! transform_connector_main {
    ($connector_type:ty) => {
        mod __rb_transform_bindings {
            rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../wit",
                world: "rapidbyte-transform",
            });
        }

        use std::cell::RefCell;
        use std::sync::OnceLock;

        use rapidbyte_sdk::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};

        struct SyncRefCell(RefCell<$connector_type>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_connector() -> &'static RefCell<$connector_type> {
            &CONNECTOR
                .get_or_init(|| SyncRefCell(RefCell::new(<$connector_type>::default())))
                .0
        }

        fn to_component_error(
            error: rapidbyte_sdk::errors::ConnectorError,
        ) -> __rb_transform_bindings::rapidbyte::connector::types::ConnectorError {
            use __rb_transform_bindings::rapidbyte::connector::types::{
                BackoffClass as CBClass, CommitState as CState, ConnectorError as CErr,
                ErrorCategory as Cat, ErrorScope as Scope,
            };

            CErr {
                category: match error.category {
                    ErrorCategory::Config => Cat::Config,
                    ErrorCategory::Auth => Cat::Auth,
                    ErrorCategory::Permission => Cat::Permission,
                    ErrorCategory::RateLimit => Cat::RateLimit,
                    ErrorCategory::TransientNetwork => Cat::TransientNetwork,
                    ErrorCategory::TransientDb => Cat::TransientDb,
                    ErrorCategory::Data => Cat::Data,
                    ErrorCategory::Schema => Cat::Schema,
                    ErrorCategory::Internal => Cat::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => Scope::PerStream,
                    ErrorScope::Batch => Scope::PerBatch,
                    ErrorScope::Record => Scope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBClass::Fast,
                    BackoffClass::Normal => CBClass::Normal,
                    BackoffClass::Slow => CBClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|s| match s {
                    CommitState::BeforeCommit => CState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        fn parse_open_context(
            config_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::OpenContext,
            __rb_transform_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            let config_value: serde_json::Value = serde_json::from_str(&config_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_CONFIG_JSON",
                    format!("Invalid config JSON: {}", e),
                ))
            })?;

            Ok(rapidbyte_sdk::protocol::OpenContext {
                config: rapidbyte_sdk::protocol::ConfigBlob::Json(config_value),
                connector_id: env!("CARGO_PKG_NAME").to_string(),
                connector_version: env!("CARGO_PKG_VERSION").to_string(),
            })
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::StreamContext,
            __rb_transform_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn to_component_validation(
            result: rapidbyte_sdk::errors::ValidationResult,
        ) -> __rb_transform_bindings::rapidbyte::connector::types::ValidationResult {
            use __rb_transform_bindings::rapidbyte::connector::types::{
                ValidationResult as CValidationResult, ValidationStatus as CValidationStatus,
            };
            use rapidbyte_sdk::errors::ValidationStatus;

            CValidationResult {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
            }
        }

        struct RapidbyteTransformComponent;

        impl __rb_transform_bindings::exports::rapidbyte::connector::transform_connector::Guest
            for RapidbyteTransformComponent
        {
            fn open(
                config_json: String,
            ) -> Result<(), __rb_transform_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let open_ctx = parse_open_context(config_json)?;
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::TransformConnector>::open(
                    &mut *conn,
                    open_ctx,
                )
                .map(|_| ())
                .map_err(to_component_error)
            }

            fn validate(
            ) -> Result<
                __rb_transform_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_transform_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::TransformConnector>::validate(
                    &mut *conn,
                )
                .map(to_component_validation)
                .map_err(to_component_error)
            }

            fn run_transform(
                ctx_json: String,
            ) -> Result<
                __rb_transform_bindings::rapidbyte::connector::types::TransformSummary,
                __rb_transform_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let ctx = parse_stream_context(ctx_json)?;
                let mut conn = get_connector().borrow_mut();
                let summary = <$connector_type as rapidbyte_sdk::connector::TransformConnector>::transform(
                    &mut *conn,
                    ctx,
                )
                .map_err(to_component_error)?;

                Ok(
                    __rb_transform_bindings::rapidbyte::connector::types::TransformSummary {
                        records_in: summary.records_in,
                        records_out: summary.records_out,
                        bytes_in: summary.bytes_in,
                        bytes_out: summary.bytes_out,
                        batches_processed: summary.batches_processed,
                    },
                )
            }

            fn close(
            ) -> Result<(), __rb_transform_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let mut conn = get_connector().borrow_mut();
                <$connector_type as rapidbyte_sdk::connector::TransformConnector>::close(
                    &mut *conn,
                )
                .map_err(to_component_error)
            }
        }

        __rb_transform_bindings::export!(
            RapidbyteTransformComponent with_types_in __rb_transform_bindings
        );

        fn main() {}
    };
}
