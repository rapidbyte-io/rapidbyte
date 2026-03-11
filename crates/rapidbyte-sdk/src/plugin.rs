//! Async-first plugin traits and component export macros.

use serde::de::DeserializeOwned;

use crate::catalog::Catalog;
use crate::context::Context;
use crate::error::{PluginError, ValidationResult, ValidationStatus};
use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
use crate::stream::StreamContext;
use crate::wire::PluginInfo;

/// Default validation response for plugins that do not implement validation.
pub fn default_validation<C>(_config: &C, _ctx: &Context) -> Result<ValidationResult, PluginError> {
    Ok(ValidationResult {
        status: ValidationStatus::Warning,
        message: "Validation not implemented".to_string(),
        warnings: Vec::new(),
    })
}

/// Default close implementation.
pub async fn default_close(_ctx: &Context) -> Result<(), PluginError> {
    Ok(())
}

/// Source plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Source: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, PluginError>;

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, PluginError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

/// Destination plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Destination: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;

    async fn write_bulk(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        self.write(ctx, stream).await
    }

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

/// Transform plugin lifecycle.
#[allow(async_fn_in_trait)]
pub trait Transform: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError>;

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        default_validation(config, ctx)
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError>;

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        default_close(ctx).await
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::context::Context;
    use crate::error::{PluginError, ValidationResult, ValidationStatus};
    use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
    use crate::stream::{StreamContext, StreamLimits, StreamPolicies};
    use crate::wire::{PluginInfo, ProtocolVersion};
    use rapidbyte_types::catalog::SchemaHint;
    use rapidbyte_types::wire::SyncMode;
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

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self { config },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, PluginError> {
            Ok(Catalog { streams: vec![] })
        }

        async fn read(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<ReadSummary, PluginError> {
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
        write_calls: usize,
    }

    impl Destination for TestDest {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self {
                    config,
                    write_calls: 0,
                },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn write(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<WriteSummary, PluginError> {
            self.write_calls += 1;
            Ok(WriteSummary {
                records_written: 42,
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

        async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
            Ok((
                Self { config },
                PluginInfo {
                    protocol_version: ProtocolVersion::V5,
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(
            &mut self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<TransformSummary, PluginError> {
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

    #[test]
    fn default_validation_returns_warning_status() {
        let ctx = Context::new("test-plugin", "test-stream");
        let result = default_validation(
            &TestConfig {
                host: "localhost".to_string(),
            },
            &ctx,
        )
        .expect("default validation");

        assert_eq!(result.status, ValidationStatus::Warning);
        assert_eq!(result.message, "Validation not implemented");
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn destination_write_bulk_defaults_to_write() {
        let ctx = Context::new("test-plugin", "users");
        let stream = StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(Vec::new()),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        };
        let mut dest = TestDest {
            config: TestConfig {
                host: "localhost".to_string(),
            },
            write_calls: 0,
        };

        let summary = futures::executor::block_on(<TestDest as Destination>::write_bulk(
            &mut dest, &ctx, stream,
        ))
        .expect("write_bulk should succeed");

        assert_eq!(dest.write_calls, 1);
        assert_eq!(summary.records_written, 42);
    }
}
