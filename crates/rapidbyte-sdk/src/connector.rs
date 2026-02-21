//! Async-first connector traits and component export macros.

use serde::de::DeserializeOwned;

use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
use crate::protocol::{
    Catalog, ConnectorInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};

/// Default validation response for connectors that do not implement validation.
pub fn default_validation<C>(_config: &C) -> Result<ValidationResult, ConnectorError> {
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not implemented".to_string(),
    })
}

/// Default close implementation.
pub async fn default_close() -> Result<(), ConnectorError> {
    Ok(())
}

/// Source connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Source: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        default_validation(config)
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError>;

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        default_close().await
    }
}

/// Destination connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Destination: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        default_validation(config)
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        default_close().await
    }
}

/// Transform connector lifecycle.
#[allow(async_fn_in_trait)]
pub trait Transform: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        default_validation(config)
    }

    async fn transform(&mut self, ctx: StreamContext) -> Result<TransformSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        default_close().await
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __rb_map_enum_variants {
    ($value:expr, $src:path => $dst:path, { $( $src_variant:ident => $dst_variant:ident ),+ $(,)? }) => {
        match $value {
            $( <$src>::$src_variant => <$dst>::$dst_variant, )+
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __rb_generate_bindings {
    ($bindings_mod:ident, $world:literal) => {
        mod $bindings_mod {
            $crate::wit_bindgen::generate!({
                path: "../../wit",
                world: $world,
            });
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __rb_connector_common {
    ($connector_type:ty, $bindings_mod:ident) => {
        use std::cell::RefCell;
        use std::sync::OnceLock;

        use $crate::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};

        // These statics live inside the macro expansion, so each connector type
        // gets its own distinct static variables.
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
            error: $crate::errors::ConnectorError,
        ) -> $bindings_mod::rapidbyte::connector::types::ConnectorError {
            use $bindings_mod::rapidbyte::connector::types::{
                BackoffClass as CBackoffClass, CommitState as CCommitState,
                ConnectorError as CConnectorError, ErrorCategory as CErrorCategory,
                ErrorScope as CErrorScope,
            };

            CConnectorError {
                category: $crate::__rb_map_enum_variants!(
                    error.category,
                    ErrorCategory => CErrorCategory,
                    {
                        Config => Config,
                        Auth => Auth,
                        Permission => Permission,
                        RateLimit => RateLimit,
                        TransientNetwork => TransientNetwork,
                        TransientDb => TransientDb,
                        Data => Data,
                        Schema => Schema,
                        Internal => Internal
                    }
                ),
                scope: $crate::__rb_map_enum_variants!(
                    error.scope,
                    ErrorScope => CErrorScope,
                    {
                        Stream => PerStream,
                        Batch => PerBatch,
                        Record => PerRecord
                    }
                ),
                code: error.code.0,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: $crate::__rb_map_enum_variants!(
                    error.backoff_class,
                    BackoffClass => CBackoffClass,
                    {
                        Fast => Fast,
                        Normal => Normal,
                        Slow => Slow
                    }
                ),
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|state| {
                    $crate::__rb_map_enum_variants!(
                        state,
                        CommitState => CCommitState,
                        {
                            BeforeCommit => BeforeCommit,
                            AfterCommitUnknown => AfterCommitUnknown,
                            AfterCommitConfirmed => AfterCommitConfirmed
                        }
                    )
                }),
                details_json: error.details.map(|value| value.to_string()),
            }
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            $crate::protocol::StreamContext,
            $bindings_mod::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error($crate::errors::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn parse_config<T: serde::de::DeserializeOwned>(
            config_json: &str,
        ) -> Result<T, $bindings_mod::rapidbyte::connector::types::ConnectorError> {
            serde_json::from_str(config_json).map_err(|e| {
                to_component_error($crate::errors::ConnectorError::config(
                    "INVALID_CONFIG",
                    format!("Config parse error: {}", e),
                ))
            })
        }

        fn parse_saved_config<T: serde::de::DeserializeOwned>(
        ) -> Result<T, $bindings_mod::rapidbyte::connector::types::ConnectorError> {
            let json = CONFIG_JSON.get().expect("open must be called before validate");
            parse_config(json)
        }

        fn to_component_validation(
            result: $crate::errors::ValidationResult,
        ) -> $bindings_mod::rapidbyte::connector::types::ValidationResult {
            use $bindings_mod::rapidbyte::connector::types::{
                ValidationResult as CValidationResult, ValidationStatus as CValidationStatus,
            };
            use $crate::errors::ValidationStatus;

            CValidationResult {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __rb_guest_lifecycle_methods {
    ($connector_type:ty, $bindings_mod:ident, $connector_trait:path) => {
        fn open(
            config_json: String,
        ) -> Result<(), $bindings_mod::rapidbyte::connector::types::ConnectorError> {
            // Store raw JSON for validate() to re-parse later.
            let _ = CONFIG_JSON.set(config_json.clone());

            let config: <$connector_type as $connector_trait>::Config = parse_config(&config_json)?;

            let rt = get_runtime();
            let (instance, _connector_info) = rt
                .block_on(<$connector_type as $connector_trait>::init(config))
                .map_err(to_component_error)?;

            *get_state().borrow_mut() = Some(instance);
            Ok(())
        }

        fn validate() -> Result<
            $bindings_mod::rapidbyte::connector::types::ValidationResult,
            $bindings_mod::rapidbyte::connector::types::ConnectorError,
        > {
            let config: <$connector_type as $connector_trait>::Config = parse_saved_config()?;

            let rt = get_runtime();
            rt.block_on(<$connector_type as $connector_trait>::validate(&config))
                .map(to_component_validation)
                .map_err(to_component_error)
        }

        fn close() -> Result<(), $bindings_mod::rapidbyte::connector::types::ConnectorError> {
            let rt = get_runtime();
            let state_cell = get_state();
            let mut state_ref = state_cell.borrow_mut();
            if let Some(conn) = state_ref.as_mut() {
                rt.block_on(<$connector_type as $connector_trait>::close(conn))
                    .map_err(to_component_error)?;
            }
            *state_ref = None;
            Ok(())
        }
    };
}

/// Export a connector component.
///
/// ```ignore
/// connector_main!(source, MySource);
/// connector_main!(destination, MyDest);
/// connector_main!(transform, MyTransform);
/// ```
#[macro_export]
macro_rules! connector_main {
    (source, $connector_type:ty) => {
        $crate::__rb_generate_bindings!(__rb_source_bindings, "rapidbyte-source");
        $crate::__rb_connector_common!($connector_type, __rb_source_bindings);

        struct RapidbyteSourceComponent;

        impl __rb_source_bindings::exports::rapidbyte::connector::source_connector::Guest
            for RapidbyteSourceComponent
        {
            $crate::__rb_guest_lifecycle_methods!(
                $connector_type,
                __rb_source_bindings,
                $crate::connector::Source
            );

            fn discover(
            ) -> Result<String, __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let catalog = rt
                    .block_on(<$connector_type as $crate::connector::Source>::discover(conn))
                    .map_err(to_component_error)?;

                serde_json::to_string(&catalog).map_err(|e| {
                    to_component_error($crate::errors::ConnectorError::internal(
                        "SERIALIZE_CATALOG",
                        e.to_string(),
                    ))
                })
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
                    .block_on(<$connector_type as $crate::connector::Source>::read(conn, ctx))
                    .map_err(to_component_error)?;

                Ok(__rb_source_bindings::rapidbyte::connector::types::ReadSummary {
                    records_read: summary.records_read,
                    bytes_read: summary.bytes_read,
                    batches_emitted: summary.batches_emitted,
                    checkpoint_count: summary.checkpoint_count,
                    records_skipped: summary.records_skipped,
                })
            }
        }

        __rb_source_bindings::export!(
            RapidbyteSourceComponent with_types_in __rb_source_bindings
        );

        fn main() {}
    };

    (destination, $connector_type:ty) => {
        $crate::__rb_generate_bindings!(__rb_dest_bindings, "rapidbyte-destination");
        $crate::__rb_connector_common!($connector_type, __rb_dest_bindings);

        struct RapidbyteDestComponent;

        impl __rb_dest_bindings::exports::rapidbyte::connector::dest_connector::Guest
            for RapidbyteDestComponent
        {
            $crate::__rb_guest_lifecycle_methods!(
                $connector_type,
                __rb_dest_bindings,
                $crate::connector::Destination
            );

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
                    .block_on(<$connector_type as $crate::connector::Destination>::write(
                        conn, ctx,
                    ))
                    .map_err(to_component_error)?;

                Ok(__rb_dest_bindings::rapidbyte::connector::types::WriteSummary {
                    records_written: summary.records_written,
                    bytes_written: summary.bytes_written,
                    batches_written: summary.batches_written,
                    checkpoint_count: summary.checkpoint_count,
                    records_failed: summary.records_failed,
                })
            }
        }

        __rb_dest_bindings::export!(RapidbyteDestComponent with_types_in __rb_dest_bindings);

        fn main() {}
    };

    (transform, $connector_type:ty) => {
        $crate::__rb_generate_bindings!(__rb_transform_bindings, "rapidbyte-transform");
        $crate::__rb_connector_common!($connector_type, __rb_transform_bindings);

        struct RapidbyteTransformComponent;

        impl __rb_transform_bindings::exports::rapidbyte::connector::transform_connector::Guest
            for RapidbyteTransformComponent
        {
            $crate::__rb_guest_lifecycle_methods!(
                $connector_type,
                __rb_transform_bindings,
                $crate::connector::Transform
            );

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
                    .block_on(<$connector_type as $crate::connector::Transform>::transform(
                        conn, ctx,
                    ))
                    .map_err(to_component_error)?;

                Ok(__rb_transform_bindings::rapidbyte::connector::types::TransformSummary {
                    records_in: summary.records_in,
                    records_out: summary.records_out,
                    bytes_in: summary.bytes_in,
                    bytes_out: summary.bytes_out,
                    batches_processed: summary.batches_processed,
                })
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
    use crate::protocol::{
        Catalog, ConnectorInfo, ProtocolVersion, ReadSummary, StreamContext, TransformSummary,
        WriteSummary,
    };
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        host: String,
    }

    struct TestSource {
        config: TestConfig,
    }

    impl Source for TestSource {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V2,
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

    impl Destination for TestDest {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V2,
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

    impl Transform for TestTransform {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
                    protocol_version: ProtocolVersion::V2,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(
            &mut self,
            _ctx: StreamContext,
        ) -> Result<TransformSummary, ConnectorError> {
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
        fn assert_source<T: Source>() {}
        fn assert_dest<T: Destination>() {}
        fn assert_transform<T: Transform>() {}
        assert_source::<TestSource>();
        assert_dest::<TestDest>();
        assert_transform::<TestTransform>();
    }
}
