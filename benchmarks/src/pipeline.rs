use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_yaml::{Mapping, Value as YamlValue};

use crate::adapters::prepare_pipeline_components;
use crate::scenario::{BenchmarkKind, ScenarioManifest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedPipeline {
    pub path: PathBuf,
    pub yaml: String,
}

#[cfg(test)]
fn render_pipeline_yaml_from_scenario(scenario: &ScenarioManifest) -> Result<String> {
    let env = scenario
        .environment
        .postgres
        .as_ref()
        .with_context(|| format!("scenario {} is missing postgres environment", scenario.id))?;
    render_pipeline_yaml(scenario, env)
}

fn render_pipeline_yaml(
    scenario: &ScenarioManifest,
    env: &crate::scenario::PostgresBenchmarkEnvironment,
) -> Result<String> {
    if scenario.kind != BenchmarkKind::Pipeline {
        bail!("scenario {} is not a pipeline benchmark", scenario.id);
    }

    let prepared = prepare_pipeline_components(scenario, env)?;

    let mut root = Mapping::new();
    root.insert(str_key("version"), YamlValue::String("1.0".to_string()));
    root.insert(
        str_key("pipeline"),
        YamlValue::String(format!("benchmark_{}", scenario.id)),
    );
    root.insert(str_key("source"), YamlValue::Mapping(prepared.source));
    root.insert(
        str_key("destination"),
        YamlValue::Mapping(prepared.destination),
    );
    if !prepared.transforms.is_empty() {
        root.insert(
            str_key("transforms"),
            YamlValue::Sequence(
                prepared
                    .transforms
                    .into_iter()
                    .map(YamlValue::Mapping)
                    .collect(),
            ),
        );
    }
    root.insert(str_key("state"), render_state_mapping(scenario, env));

    serde_yaml::to_string(&root).context("failed to serialize rendered pipeline")
}

pub fn write_rendered_pipeline(
    scenario: &ScenarioManifest,
    env: &crate::scenario::PostgresBenchmarkEnvironment,
    temp_root: &Path,
) -> Result<RenderedPipeline> {
    fs::create_dir_all(temp_root).with_context(|| {
        format!(
            "failed to create pipeline temp root {}",
            temp_root.display()
        )
    })?;
    let yaml = render_pipeline_yaml(scenario, env)?;
    let path = temp_root.join(format!("{}.yaml", scenario.id));
    fs::write(&path, &yaml)
        .with_context(|| format!("failed to write rendered pipeline {}", path.display()))?;
    Ok(RenderedPipeline { path, yaml })
}

fn render_state_mapping(
    _scenario: &ScenarioManifest,
    env: &crate::scenario::PostgresBenchmarkEnvironment,
) -> YamlValue {
    let mut state = Mapping::new();
    // Both distributed and non-distributed use postgres:// URL format.
    // The engine's PgBackend::connect() uses sqlx which expects URL format.
    state.insert(
        str_key("connection"),
        YamlValue::String(format!(
            "postgres://{}:{}@{}:{}/{}",
            env.source.user,
            env.source.password,
            env.source.host,
            env.source.port,
            env.source.database,
        )),
    );
    YamlValue::Mapping(state)
}

fn str_key(value: &str) -> YamlValue {
    YamlValue::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rapidbyte_pipeline_config::types::{PipelineConfig, PipelineWriteMode};
    use rapidbyte_types::wire::SyncMode;

    fn parse_pipeline_sync(yaml: &str) -> PipelineConfig {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(rapidbyte_pipeline_config::parser::parse_pipeline(
                yaml,
                &rapidbyte_secrets::SecretProviders::new(),
            ))
            .expect("pipeline YAML must parse")
    }

    use super::*;
    use crate::scenario::{
        BenchmarkExecutionMode, BenchmarkKind, ConnectorOptions, DestinationConnectorOptions,
        EnvironmentConfig, ExecutionProfile, PostgresBenchmarkEnvironment,
        PostgresConnectionProfile, ScenarioAssertions, ScenarioConnectorRef, ScenarioManifest,
        SourceConnectorOptions, TransformConnectorOptions, WorkloadProfile,
    };
    use crate::workload::WorkloadFamily;

    #[test]
    fn renders_valid_postgres_pipeline_yaml() {
        let scenario = sample_postgres_pipeline("insert");

        let yaml = render_pipeline_yaml_from_scenario(&scenario).expect("render pipeline yaml");
        let parsed = parse_pipeline_sync(&yaml);

        assert_eq!(parsed.pipeline, "benchmark_pg_dest_insert");
        assert_eq!(parsed.source.use_ref, "postgres");
        assert_eq!(parsed.destination.use_ref, "postgres");
        assert_eq!(parsed.destination.write_mode, PipelineWriteMode::Append);
        assert_eq!(parsed.source.streams.len(), 1);
        assert_eq!(parsed.source.streams[0].name, "bench_events");
        assert_eq!(parsed.source.streams[0].sync_mode, SyncMode::FullRefresh);
    }

    #[test]
    fn renders_destination_load_method_override() {
        let scenario = sample_postgres_pipeline("copy");

        let yaml = render_pipeline_yaml_from_scenario(&scenario).expect("render pipeline yaml");
        let parsed = parse_pipeline_sync(&yaml);

        assert_eq!(parsed.destination.config["load_method"], "copy");
        assert_eq!(parsed.destination.config["schema"], "raw");
    }

    #[test]
    fn writes_rendered_pipeline_with_environment_connection_data() {
        let scenario = sample_postgres_pipeline("insert");
        let temp_root = temp_dir("pipeline-render");

        let env = scenario.environment.postgres.clone().expect("env");
        let rendered =
            write_rendered_pipeline(&scenario, &env, &temp_root).expect("write pipeline");
        let yaml = fs::read_to_string(&rendered.path).expect("read rendered pipeline");
        let parsed = parse_pipeline_sync(&yaml);

        assert!(rendered.path.starts_with(&temp_root));
        assert!(rendered.path.exists());
        assert_eq!(parsed.source.config["host"], "source-db");
        assert_eq!(parsed.source.config["database"], "bench_source");
        assert_eq!(parsed.destination.config["host"], "dest-db");
        assert_eq!(parsed.destination.config["database"], "bench_dest");
        assert_eq!(parsed.destination.config["schema"], "raw");
    }

    #[test]
    fn writes_rendered_pipeline_with_postgres_state_connection() {
        let scenario = sample_postgres_pipeline("insert");
        let temp_root = temp_dir("pipeline-state");

        let env = scenario.environment.postgres.clone().expect("env");
        let rendered =
            write_rendered_pipeline(&scenario, &env, &temp_root).expect("write pipeline");
        let yaml = fs::read_to_string(&rendered.path).expect("read rendered pipeline");
        let parsed = parse_pipeline_sync(&yaml);

        let connection = parsed
            .state
            .connection
            .expect("rendered pipeline should have postgres state connection");
        assert!(
            connection.starts_with("postgres://"),
            "state connection should be a postgres URL, got: {connection}"
        );
        assert!(connection.contains("bench_source"));
    }

    #[test]
    fn writes_distributed_pipeline_with_postgres_state_backend() {
        let mut scenario = sample_postgres_pipeline("copy");
        scenario.benchmark.execution_mode = BenchmarkExecutionMode::Distributed;
        let temp_root = temp_dir("pipeline-distributed-state");

        let env = scenario.environment.postgres.clone().expect("env");
        let rendered =
            write_rendered_pipeline(&scenario, &env, &temp_root).expect("write pipeline");
        let yaml = fs::read_to_string(&rendered.path).expect("read rendered pipeline");
        let parsed = parse_pipeline_sync(&yaml);

        let connection = parsed
            .state
            .connection
            .expect("distributed pipeline should pin postgres state connection");
        assert!(connection.starts_with("postgres://"));
        assert!(connection.contains("source-db"));
        assert!(connection.contains("bench_source"));
    }

    #[test]
    fn logical_environment_reference_renders_with_resolved_profile_data() {
        let scenario = sample_logical_pipeline("copy");
        let resolved = PostgresBenchmarkEnvironment {
            stream_name: "bench_events".to_string(),
            source: PostgresConnectionProfile {
                host: "source-db".to_string(),
                port: 5433,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                database: "rapidbyte_test".to_string(),
                schema: "public".to_string(),
            },
            destination: PostgresConnectionProfile {
                host: "dest-db".to_string(),
                port: 5433,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                database: "rapidbyte_test".to_string(),
                schema: "raw".to_string(),
            },
        };

        let yaml = render_pipeline_yaml(&scenario, &resolved).expect("render pipeline");
        let parsed = parse_pipeline_sync(&yaml);

        assert_eq!(parsed.source.config["host"], "source-db");
        assert_eq!(parsed.destination.config["host"], "dest-db");
        assert_eq!(parsed.destination.config["load_method"], "copy");
    }

    #[test]
    fn pipeline_components_are_materialized_via_adapters() {
        let scenario = sample_postgres_pipeline("copy");
        let env = scenario.environment.postgres.clone().expect("env");

        let prepared = prepare_pipeline_components(&scenario, &env).expect("prepare pipeline");

        assert_eq!(
            prepared.source["use"],
            YamlValue::String("postgres".to_string())
        );
        assert_eq!(
            prepared.destination["config"]["load_method"],
            YamlValue::String("copy".to_string())
        );
    }

    #[test]
    fn renders_transform_sections_for_pipeline_benchmarks() {
        let mut scenario = sample_postgres_pipeline("copy");
        scenario.connectors.push(ScenarioConnectorRef {
            kind: "transform".to_string(),
            plugin: "sql".to_string(),
        });
        scenario.connector_options.transforms = vec![TransformConnectorOptions {
            config: std::collections::BTreeMap::from([(
                "query".to_string(),
                YamlValue::String("SELECT * FROM bench_events".to_string()),
            )]),
        }];

        let yaml = render_pipeline_yaml_from_scenario(&scenario).expect("render pipeline yaml");
        let parsed = parse_pipeline_sync(&yaml);

        assert_eq!(parsed.transforms.len(), 1);
        assert_eq!(parsed.transforms[0].use_ref, "sql");
        assert_eq!(
            parsed.transforms[0].config["query"],
            "SELECT * FROM bench_events"
        );
    }

    fn sample_postgres_pipeline(load_method: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: format!("pg_dest_{load_method}"),
            name: format!("Postgres destination via {load_method}"),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string(), "postgres".to_string()],
            connectors: vec![
                ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                },
                ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                },
            ],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 1_000_000,
            },
            execution: ExecutionProfile {
                iterations: 3,
                warmups: 1,
            },
            benchmark: Default::default(),
            environment: EnvironmentConfig {
                reference: None,
                stream_name: None,
                postgres: Some(PostgresBenchmarkEnvironment {
                    stream_name: "bench_events".to_string(),
                    source: PostgresConnectionProfile {
                        host: "source-db".to_string(),
                        port: 5432,
                        user: "postgres".to_string(),
                        password: "postgres".to_string(),
                        database: "bench_source".to_string(),
                        schema: "public".to_string(),
                    },
                    destination: PostgresConnectionProfile {
                        host: "dest-db".to_string(),
                        port: 5432,
                        user: "postgres".to_string(),
                        password: "postgres".to_string(),
                        database: "bench_dest".to_string(),
                        schema: "raw".to_string(),
                    },
                }),
            },
            connector_options: ConnectorOptions {
                source: SourceConnectorOptions {
                    sync_mode: Some(SyncMode::FullRefresh),
                    config: Default::default(),
                },
                destination: DestinationConnectorOptions {
                    load_method: Some(load_method.to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
                transforms: vec![],
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(1_000_000),
                expected_records_written: Some(1_000_000),
            },
        }
    }

    fn sample_logical_pipeline(load_method: &str) -> ScenarioManifest {
        ScenarioManifest {
            id: format!("pg_dest_{load_method}"),
            name: format!("Postgres destination via {load_method}"),
            suite: "lab".to_string(),
            kind: BenchmarkKind::Pipeline,
            tags: vec!["lab".to_string(), "postgres".to_string()],
            connectors: vec![
                ScenarioConnectorRef {
                    kind: "source".to_string(),
                    plugin: "postgres".to_string(),
                },
                ScenarioConnectorRef {
                    kind: "destination".to_string(),
                    plugin: "postgres".to_string(),
                },
            ],
            requires: vec![],
            workload: WorkloadProfile {
                family: WorkloadFamily::NarrowAppend,
                rows: 1_000_000,
            },
            execution: ExecutionProfile {
                iterations: 3,
                warmups: 1,
            },
            benchmark: Default::default(),
            environment: EnvironmentConfig {
                reference: Some("local-bench-postgres".to_string()),
                stream_name: Some("bench_events".to_string()),
                postgres: None,
            },
            connector_options: ConnectorOptions {
                source: SourceConnectorOptions {
                    sync_mode: Some(SyncMode::FullRefresh),
                    config: Default::default(),
                },
                destination: DestinationConnectorOptions {
                    load_method: Some(load_method.to_string()),
                    write_mode: Some("append".to_string()),
                    config: Default::default(),
                },
                transforms: vec![],
            },
            assertions: ScenarioAssertions {
                expected_records_read: Some(1_000_000),
                expected_records_written: Some(1_000_000),
            },
        }
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rapidbyte-benchmarks-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
