//! Async-first plugin traits and component export macros.
//!
//! Defines the v7 plugin lifecycle for [`Source`], [`Destination`], and
//! [`Transform`] plugins. All methods take `&self` (not `&mut self`);
//! interior mutability is the plugin author's responsibility.

use serde::de::DeserializeOwned;

use crate::context::Context;
use crate::discovery::{DiscoveredStream, PluginSpec};
use crate::error::PluginError;
use crate::lifecycle::{ApplyReport, ApplyRequest, TeardownReport, TeardownRequest};
use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
use crate::schema::StreamSchema;
use crate::stream::StreamContext;
use crate::validation::{PrerequisitesReport, ValidationReport};

/// Source plugin lifecycle.
///
/// # v7 changes
///
/// - `init()` returns `Result<Self, PluginError>` (no `PluginInfo`).
/// - `spec()` is a static method with a default implementation.
/// - `validate()` takes `&self` and an optional upstream `StreamSchema`.
/// - `discover()` returns typed `Vec<DiscoveredStream>`.
/// - New lifecycle hooks: `prerequisites()`, `apply()`, `teardown()`.
/// - All instance methods take `&self`, not `&mut self`.
#[allow(async_fn_in_trait)]
pub trait Source: Sized {
    type Config: DeserializeOwned;

    /// Return plugin spec. Called before open, no instance needed.
    fn spec() -> PluginSpec {
        PluginSpec::from_manifest()
    }

    /// Initialize the plugin with parsed config.
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    /// Pre-flight checks (wal_level, permissions, etc).
    async fn prerequisites(&self, _ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    /// Discover available streams and their schemas.
    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError>;

    /// Validate config, optionally against an upstream schema.
    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Create/prepare external resources before data flows.
    async fn apply(
        &self,
        _ctx: &Context,
        _request: ApplyRequest,
    ) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    /// Read data from a single stream.
    async fn read(&self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }

    /// Tear down persistent resources (replication slots, etc).
    async fn teardown(
        &self,
        _ctx: &Context,
        _request: TeardownRequest,
    ) -> Result<TeardownReport, PluginError> {
        Ok(TeardownReport::noop())
    }
}

/// Destination plugin lifecycle.
///
/// # v7 changes
///
/// - `init()` returns `Result<Self, PluginError>` (no `PluginInfo`).
/// - `spec()` is a static method with a default implementation.
/// - `validate()` takes `&self` and an optional upstream `StreamSchema`.
/// - New lifecycle hooks: `prerequisites()`, `apply()`, `teardown()`.
/// - All instance methods take `&self`, not `&mut self`.
/// - `write_bulk` removed (moved to `BulkDestination` feature trait).
#[allow(async_fn_in_trait)]
pub trait Destination: Sized {
    type Config: DeserializeOwned;

    /// Return plugin spec. Called before open, no instance needed.
    fn spec() -> PluginSpec {
        PluginSpec::from_manifest()
    }

    /// Initialize the plugin with parsed config.
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    /// Pre-flight checks (connectivity, permissions, etc).
    async fn prerequisites(&self, _ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    /// Validate config, optionally against an upstream schema.
    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Create/prepare external resources (tables, schemas) before data flows.
    async fn apply(
        &self,
        _ctx: &Context,
        _request: ApplyRequest,
    ) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    /// Write data to a single stream.
    async fn write(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }

    /// Tear down persistent resources.
    async fn teardown(
        &self,
        _ctx: &Context,
        _request: TeardownRequest,
    ) -> Result<TeardownReport, PluginError> {
        Ok(TeardownReport::noop())
    }
}

/// Transform plugin lifecycle.
///
/// # v7 changes
///
/// - `init()` returns `Result<Self, PluginError>` (no `PluginInfo`).
/// - `spec()` is a static method with a default implementation.
/// - `validate()` takes `&self` and an optional upstream `StreamSchema`.
/// - All instance methods take `&self`, not `&mut self`.
#[allow(async_fn_in_trait)]
pub trait Transform: Sized {
    type Config: DeserializeOwned;

    /// Return plugin spec. Called before open, no instance needed.
    fn spec() -> PluginSpec {
        PluginSpec::from_manifest()
    }

    /// Initialize the plugin with parsed config.
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    /// Validate config, optionally against an upstream schema.
    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Transform data from a single stream.
    async fn transform(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use crate::context::Context;
    use crate::discovery::{DiscoveredStream, PluginSpec};
    use crate::error::PluginError;
    use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
    use crate::schema::StreamSchema;
    use crate::stream::{StreamContext, StreamLimits, StreamPolicies};
    use crate::validation::{ValidationReport, ValidationStatus};
    use rapidbyte_types::wire::SyncMode;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        host: String,
    }

    struct TestSource {
        _config: TestConfig,
    }

    impl Source for TestSource {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn discover(&self, _ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError> {
            Ok(vec![])
        }

        async fn read(
            &self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<ReadSummary, PluginError> {
            Ok(ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            })
        }
    }

    struct TestDest {
        _config: TestConfig,
    }

    impl Destination for TestDest {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn write(
            &self,
            _ctx: &Context,
            _stream: StreamContext,
        ) -> Result<WriteSummary, PluginError> {
            Ok(WriteSummary {
                records_written: 42,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
            })
        }
    }

    struct TestTransform {
        _config: TestConfig,
    }

    impl Transform for TestTransform {
        type Config = TestConfig;

        async fn init(config: Self::Config) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn transform(
            &self,
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
    fn spec_returns_default_v7() {
        let spec = TestSource::spec();
        assert_eq!(spec.protocol_version, 7);
        assert_eq!(spec.config_schema_json, "{}");
        assert!(spec.features.is_empty());
        assert!(spec.supported_sync_modes.is_empty());
        assert!(spec.supported_write_modes.is_none());
    }

    #[test]
    fn default_validation_returns_success() {
        let ctx = Context::new("test-plugin", "test-stream");
        let source = futures::executor::block_on(TestSource::init(TestConfig {
            host: "localhost".to_string(),
        }))
        .expect("init");

        let result =
            futures::executor::block_on(source.validate(&ctx, None)).expect("default validation");

        assert_eq!(result.status, ValidationStatus::Success);
        assert_eq!(result.message, "Validation not implemented");
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn default_prerequisites_passes() {
        let ctx = Context::new("test-plugin", "test-stream");
        let source = futures::executor::block_on(TestSource::init(TestConfig {
            host: "localhost".to_string(),
        }))
        .expect("init");

        let report =
            futures::executor::block_on(source.prerequisites(&ctx)).expect("prerequisites");
        assert!(report.passed);
        assert!(report.checks.is_empty());
    }

    #[test]
    fn default_apply_is_noop() {
        use crate::lifecycle::ApplyRequest;

        let ctx = Context::new("test-plugin", "test-stream");
        let source = futures::executor::block_on(TestSource::init(TestConfig {
            host: "localhost".to_string(),
        }))
        .expect("init");

        let request = ApplyRequest {
            streams: vec![],
            dry_run: false,
        };
        let report = futures::executor::block_on(source.apply(&ctx, request)).expect("apply");
        assert!(report.actions.is_empty());
    }

    #[test]
    fn default_teardown_is_noop() {
        use crate::lifecycle::TeardownRequest;

        let ctx = Context::new("test-plugin", "test-stream");
        let source = futures::executor::block_on(TestSource::init(TestConfig {
            host: "localhost".to_string(),
        }))
        .expect("init");

        let request = TeardownRequest {
            streams: vec![],
            reason: "test".into(),
        };
        let report = futures::executor::block_on(source.teardown(&ctx, request)).expect("teardown");
        assert!(report.actions.is_empty());
    }
}
