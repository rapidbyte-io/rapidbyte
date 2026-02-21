//! Async-first connector traits.
//!
//! Connectors implement one of the traits below. The `*_connector_main!`
//! macros handle Tokio runtime, config deserialization, and WIT bindings.

use serde::de::DeserializeOwned;

use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
use crate::protocol::{
    Catalog, OpenInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};

/// Source connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait SourceConnector: Sized {
    type Config: DeserializeOwned;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    async fn validate(_config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError>;

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Destination connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait DestinationConnector: Sized {
    type Config: DeserializeOwned;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    async fn validate(_config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Transform connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait TransformConnector: Sized {
    type Config: DeserializeOwned;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    async fn validate(_config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    async fn transform(&mut self, ctx: StreamContext) -> Result<TransformSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
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

        // Note: These statics live inside the macro expansion, so each connector
        // type that invokes the macro gets its own distinct static variables.
        // This is correct and thread-safe within the WASI single-threaded environment.
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

        struct SyncRefCell(RefCell<Option<$connector_type>>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<$connector_type>> {
            &CONNECTOR.get_or_init(|| SyncRefCell(RefCell::new(None))).0
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
                // Store raw JSON for validate() to re-parse later
                let _ = CONFIG_JSON.set(config_json.clone());

                let config: <$connector_type as rapidbyte_sdk::connector::SourceConnector>::Config =
                    serde_json::from_str(&config_json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                let (instance, _open_info) = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::connect(
                            config,
                        ),
                    )
                    .map_err(to_component_error)?;

                // Store just the initialized connector instance
                let state_cell = get_state();
                *state_cell.borrow_mut() = Some(instance);

                Ok(())
            }

            fn discover(
            ) -> Result<String, __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let catalog = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::discover(
                            conn,
                        ),
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
                let json = CONFIG_JSON.get().expect("open must be called before validate");
                let config: <$connector_type as rapidbyte_sdk::connector::SourceConnector>::Config =
                    serde_json::from_str(json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                rt.block_on(
                    <$connector_type as rapidbyte_sdk::connector::SourceConnector>::validate(
                        &config,
                    ),
                )
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let summary = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::read(
                            conn,
                            ctx,
                        ),
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                if let Some(conn) = state_ref.as_mut() {
                    rt.block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::close(
                            conn,
                        ),
                    )
                    .map_err(to_component_error)?;
                }
                *state_ref = None;
                Ok(())
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

        // Note: These statics live inside the macro expansion, so each connector
        // type that invokes the macro gets its own distinct static variables.
        // This is correct and thread-safe within the WASI single-threaded environment.
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

        struct SyncRefCell(RefCell<Option<$connector_type>>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<$connector_type>> {
            &CONNECTOR.get_or_init(|| SyncRefCell(RefCell::new(None))).0
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

        struct RapidbyteDestComponent;

        impl __rb_dest_bindings::exports::rapidbyte::connector::dest_connector::Guest
            for RapidbyteDestComponent
        {
            fn open(
                config_json: String,
            ) -> Result<(), __rb_dest_bindings::rapidbyte::connector::types::ConnectorError>
            {
                // Store raw JSON for validate() to re-parse later
                let _ = CONFIG_JSON.set(config_json.clone());

                let config: <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::Config =
                    serde_json::from_str(&config_json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                let (instance, _open_info) = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::connect(
                            config,
                        ),
                    )
                    .map_err(to_component_error)?;

                // Store just the initialized connector instance
                let state_cell = get_state();
                *state_cell.borrow_mut() = Some(instance);

                Ok(())
            }

            fn validate(
            ) -> Result<
                __rb_dest_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_dest_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let json = CONFIG_JSON.get().expect("open must be called before validate");
                let config: <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::Config =
                    serde_json::from_str(json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                rt.block_on(
                    <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::validate(
                        &config,
                    ),
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let summary = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::write(
                            conn,
                            ctx,
                        ),
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                if let Some(conn) = state_ref.as_mut() {
                    rt.block_on(
                        <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::close(
                            conn,
                        ),
                    )
                    .map_err(to_component_error)?;
                }
                *state_ref = None;
                Ok(())
            }
        }

        __rb_dest_bindings::export!(
            RapidbyteDestComponent with_types_in __rb_dest_bindings
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

        // Note: These statics live inside the macro expansion, so each connector
        // type that invokes the macro gets its own distinct static variables.
        // This is correct and thread-safe within the WASI single-threaded environment.
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

        struct SyncRefCell(RefCell<Option<$connector_type>>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<$connector_type>> {
            &CONNECTOR.get_or_init(|| SyncRefCell(RefCell::new(None))).0
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
                // Store raw JSON for validate() to re-parse later
                let _ = CONFIG_JSON.set(config_json.clone());

                let config: <$connector_type as rapidbyte_sdk::connector::TransformConnector>::Config =
                    serde_json::from_str(&config_json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                let (instance, _open_info) = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::TransformConnector>::connect(
                            config,
                        ),
                    )
                    .map_err(to_component_error)?;

                // Store just the initialized connector instance
                let state_cell = get_state();
                *state_cell.borrow_mut() = Some(instance);

                Ok(())
            }

            fn validate(
            ) -> Result<
                __rb_transform_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_transform_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let json = CONFIG_JSON.get().expect("open must be called before validate");
                let config: <$connector_type as rapidbyte_sdk::connector::TransformConnector>::Config =
                    serde_json::from_str(json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                rt.block_on(
                    <$connector_type as rapidbyte_sdk::connector::TransformConnector>::validate(
                        &config,
                    ),
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let summary = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::TransformConnector>::transform(
                            conn,
                            ctx,
                        ),
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
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                if let Some(conn) = state_ref.as_mut() {
                    rt.block_on(
                        <$connector_type as rapidbyte_sdk::connector::TransformConnector>::close(
                            conn,
                        ),
                    )
                    .map_err(to_component_error)?;
                }
                *state_ref = None;
                Ok(())
            }
        }

        __rb_transform_bindings::export!(
            RapidbyteTransformComponent with_types_in __rb_transform_bindings
        );

        fn main() {}
    };
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
    use crate::protocol::{Catalog, OpenInfo, ReadSummary, StreamContext, WriteSummary, TransformSummary};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        host: String,
    }

    struct TestSource {
        config: TestConfig,
    }

    impl SourceConnector for TestSource {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
            Ok(Catalog { streams: vec![] })
        }

        async fn read(&mut self, _ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
            Ok(ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            })
        }
    }

    struct TestDest {
        config: TestConfig,
    }

    impl DestinationConnector for TestDest {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn write(&mut self, _ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
            Ok(WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            })
        }
    }

    struct TestTransform {
        config: TestConfig,
    }

    impl TransformConnector for TestTransform {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(&mut self, _ctx: StreamContext) -> Result<TransformSummary, ConnectorError> {
            Ok(TransformSummary {
                records_in: 0,
                records_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                batches_processed: 0,
            })
        }
    }

    #[test]
    fn test_trait_shapes_compile() {
        fn assert_source<T: SourceConnector>() {}
        fn assert_dest<T: DestinationConnector>() {}
        fn assert_transform<T: TransformConnector>() {}
        assert_source::<TestSource>();
        assert_dest::<TestDest>();
        assert_transform::<TestTransform>();
    }
}
