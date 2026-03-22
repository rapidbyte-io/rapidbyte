//! Async-first plugin traits and component export macros.
//!
//! Defines the plugin lifecycle for [`Source`], [`Destination`], and
//! [`Transform`] plugins using typed lifecycle inputs.

use core::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::discovery::{DiscoveredStream, PluginSpec};
use crate::error::PluginError;
use crate::lifecycle::{ApplyReport, ApplyRequest, TeardownReport, TeardownRequest};
use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
use crate::schema::StreamSchema;
use crate::stream::{CdcResumeToken, PartitionCoordinates, StreamContext};
use crate::validation::{PrerequisitesReport, ValidationReport};

/// Marker input passed to [`Source::init`] and [`Destination::init`].
#[derive(Debug, Clone, Copy, Default)]
pub struct InitInput<'a> {
    _marker: PhantomData<&'a ()>,
}

impl<'a> InitInput<'a> {
    /// Create an empty init input.
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Marker input passed to pre-flight checks.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrerequisitesInput<'a> {
    _marker: PhantomData<&'a ()>,
}

impl<'a> PrerequisitesInput<'a> {
    /// Create an empty prerequisites input.
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Marker input passed to discovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiscoverInput<'a> {
    _marker: PhantomData<&'a ()>,
}

impl<'a> DiscoverInput<'a> {
    /// Create an empty discovery input.
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Marker input passed to validation.
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidateInput<'a> {
    /// Optional upstream schema being validated against.
    pub upstream: Option<&'a StreamSchema>,
}

impl<'a> ValidateInput<'a> {
    /// Create a validation input.
    pub const fn new(upstream: Option<&'a StreamSchema>) -> Self {
        Self { upstream }
    }
}

/// Input passed to schema-apply hooks.
#[derive(Debug, Clone)]
pub struct ApplyInput<'a> {
    /// Streams being prepared or applied.
    pub request: ApplyRequest,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ApplyInput<'a> {
    /// Create an apply input from a request.
    pub fn new(request: ApplyRequest) -> Self {
        Self {
            request,
            _marker: PhantomData,
        }
    }
}

/// Input passed to read hooks.
#[derive(Debug, Clone)]
pub struct ReadInput<'a> {
    /// Stream being read.
    pub stream: StreamContext,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ReadInput<'a> {
    /// Create a read input for a single stream.
    pub fn new(stream: StreamContext) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

/// Input passed to destination write hooks.
#[derive(Debug, Clone)]
pub struct WriteInput<'a> {
    /// Stream being written.
    pub stream: StreamContext,
    _marker: PhantomData<&'a ()>,
}

impl<'a> WriteInput<'a> {
    /// Create a write input for a single stream.
    pub fn new(stream: StreamContext) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

/// Input passed to transform hooks.
#[derive(Debug, Clone)]
pub struct TransformInput<'a> {
    /// Stream being transformed.
    pub stream: StreamContext,
    _marker: PhantomData<&'a ()>,
}

impl<'a> TransformInput<'a> {
    /// Create a transform input for a single stream.
    pub fn new(stream: StreamContext) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

/// Input passed to close hooks.
#[derive(Debug, Clone, Copy, Default)]
pub struct CloseInput<'a> {
    _marker: PhantomData<&'a ()>,
}

impl<'a> CloseInput<'a> {
    /// Create an empty close input.
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Input passed to teardown hooks.
#[derive(Debug, Clone)]
pub struct TeardownInput<'a> {
    /// Streams being torn down.
    pub request: TeardownRequest,
    _marker: PhantomData<&'a ()>,
}

impl<'a> TeardownInput<'a> {
    /// Create a teardown input from a request.
    pub fn new(request: TeardownRequest) -> Self {
        Self {
            request,
            _marker: PhantomData,
        }
    }
}

/// Input passed to partitioned source reads.
#[derive(Debug, Clone)]
pub struct PartitionedReadInput<'a> {
    pub stream: StreamContext,
    pub partition: PartitionCoordinates,
    _marker: PhantomData<&'a ()>,
}

impl<'a> PartitionedReadInput<'a> {
    pub fn new(stream: StreamContext, partition: PartitionCoordinates) -> Self {
        Self {
            stream,
            partition,
            _marker: PhantomData,
        }
    }
}

/// Input passed to CDC source reads.
#[derive(Debug, Clone)]
pub struct CdcReadInput<'a> {
    pub stream: StreamContext,
    pub resume: CdcResumeToken,
    _marker: PhantomData<&'a ()>,
}

impl<'a> CdcReadInput<'a> {
    pub fn new(stream: StreamContext, resume: CdcResumeToken) -> Self {
        Self {
            stream,
            resume,
            _marker: PhantomData,
        }
    }
}

/// Input passed to multi-stream source reads.
#[derive(Debug, Clone)]
pub struct MultiStreamReadInput<'a> {
    pub streams: Vec<StreamContext>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> MultiStreamReadInput<'a> {
    pub fn new(streams: Vec<StreamContext>) -> Self {
        Self {
            streams,
            _marker: PhantomData,
        }
    }
}

/// Input passed to bulk destination writes.
#[derive(Debug, Clone)]
pub struct BulkWriteInput<'a> {
    pub stream: StreamContext,
    _marker: PhantomData<&'a ()>,
}

impl<'a> BulkWriteInput<'a> {
    pub fn new(stream: StreamContext) -> Self {
        Self {
            stream,
            _marker: PhantomData,
        }
    }
}

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
    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError>;

    /// Pre-flight checks (wal_level, permissions, etc).
    async fn prerequisites(
        &self,
        _input: PrerequisitesInput<'_>,
    ) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    /// Discover available streams and their schemas.
    async fn discover(
        &self,
        _input: DiscoverInput<'_>,
    ) -> Result<Vec<DiscoveredStream>, PluginError>;

    /// Validate config, optionally against an upstream schema.
    async fn validate(&self, _input: ValidateInput<'_>) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Create/prepare external resources before data flows.
    async fn apply(&self, _input: ApplyInput<'_>) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    /// Read data from a single stream.
    async fn read(&self, input: ReadInput<'_>) -> Result<ReadSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _input: CloseInput<'_>) -> Result<(), PluginError> {
        Ok(())
    }

    /// Tear down persistent resources (replication slots, etc).
    async fn teardown(&self, _input: TeardownInput<'_>) -> Result<TeardownReport, PluginError> {
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
    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError>;

    /// Pre-flight checks (connectivity, permissions, etc).
    async fn prerequisites(
        &self,
        _input: PrerequisitesInput<'_>,
    ) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    /// Validate config, optionally against an upstream schema.
    async fn validate(&self, _input: ValidateInput<'_>) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Create/prepare external resources (tables, schemas) before data flows.
    async fn apply(&self, _input: ApplyInput<'_>) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    /// Write data to a single stream.
    async fn write(&self, input: WriteInput<'_>) -> Result<WriteSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _input: CloseInput<'_>) -> Result<(), PluginError> {
        Ok(())
    }

    /// Tear down persistent resources.
    async fn teardown(&self, _input: TeardownInput<'_>) -> Result<TeardownReport, PluginError> {
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
    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError>;

    /// Validate config, optionally against an upstream schema.
    async fn validate(&self, _input: ValidateInput<'_>) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Transform data from a single stream.
    async fn transform(&self, input: TransformInput<'_>) -> Result<TransformSummary, PluginError>;

    /// Clean up session resources.
    async fn close(&self, _input: CloseInput<'_>) -> Result<(), PluginError> {
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

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn discover(
            &self,
            _input: DiscoverInput<'_>,
        ) -> Result<Vec<DiscoveredStream>, PluginError> {
            Ok(vec![])
        }

        async fn read(&self, _input: ReadInput<'_>) -> Result<ReadSummary, PluginError> {
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

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn write(&self, _input: WriteInput<'_>) -> Result<WriteSummary, PluginError> {
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

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            Ok(Self { _config: config })
        }

        async fn transform(
            &self,
            _input: TransformInput<'_>,
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
        let source = futures::executor::block_on(TestSource::init(
            TestConfig {
                host: "localhost".to_string(),
            },
            InitInput::new(),
        ))
        .expect("init");

        let result = futures::executor::block_on(source.validate(ValidateInput::new(None)))
            .expect("default validation");

        assert_eq!(result.status, ValidationStatus::Success);
        assert_eq!(result.message, "Validation not implemented");
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn default_prerequisites_passes() {
        let source = futures::executor::block_on(TestSource::init(
            TestConfig {
                host: "localhost".to_string(),
            },
            InitInput::new(),
        ))
        .expect("init");

        let report = futures::executor::block_on(source.prerequisites(PrerequisitesInput::new()))
            .expect("prerequisites");
        assert!(report.passed);
        assert!(report.checks.is_empty());
    }

    #[test]
    fn default_apply_is_noop() {
        let source = futures::executor::block_on(TestSource::init(
            TestConfig {
                host: "localhost".to_string(),
            },
            InitInput::new(),
        ))
        .expect("init");

        let request = ApplyRequest {
            streams: vec![],
            dry_run: false,
        };
        let report =
            futures::executor::block_on(source.apply(ApplyInput::new(request))).expect("apply");
        assert!(report.actions.is_empty());
    }

    #[test]
    fn default_teardown_is_noop() {
        let source = futures::executor::block_on(TestSource::init(
            TestConfig {
                host: "localhost".to_string(),
            },
            InitInput::new(),
        ))
        .expect("init");

        let request = TeardownRequest {
            streams: vec![],
            reason: "test".into(),
        };
        let report = futures::executor::block_on(source.teardown(TeardownInput::new(request)))
            .expect("teardown");
        assert!(report.actions.is_empty());
    }
}
