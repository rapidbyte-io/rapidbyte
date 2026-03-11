# Benchmark Environment Lifecycle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make benchmark environment profiles own infrastructure lifecycle so `bench run` provisions the selected environment and always tears it down afterward, including on benchmark failure.

**Architecture:** Extend the benchmark environment module with a typed provider lifecycle layer and an `EnvironmentSession` that owns the loaded profile plus provider teardown. Refactor the benchmark runner to create one session per `bench run`, execute warmups and measured iterations inside that session, and always finalize teardown with combined error reporting when both execution and teardown fail.

**Tech Stack:** Rust, Serde YAML, `std::process::Command`, Clap, Cargo, Markdown docs.

---

### Task 1: Add failing tests for environment provider lifecycle dispatch

**Files:**
- Modify: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing tests**

Add unit tests in `benchmarks/src/environment.rs` that assert:

- `docker_compose` profiles require `provider.project_dir`
- project directories resolve relative to the repo root
- provider lifecycle builds `docker compose up -d` for provision
- provider lifecycle builds `docker compose down -v` for teardown

Use a fake command executor seam instead of invoking real Docker.

```rust
#[test]
fn docker_compose_provider_requires_project_dir() {
    let profile = EnvironmentProfile {
        id: "local-dev-postgres".to_string(),
        provider: EnvironmentProvider {
            kind: "docker_compose".to_string(),
            project_dir: None,
        },
        services: BTreeMap::new(),
        bindings: sample_bindings(),
    };

    let err = EnvironmentSession::provision(
        Path::new("/repo"),
        profile,
        &RecordingExecutor::default(),
    )
    .expect_err("docker compose provider should require project_dir");

    assert!(err.to_string().contains("project_dir"));
}

#[test]
fn docker_compose_provider_uses_repo_relative_project_dir_for_up_and_down() {
    let mut executor = RecordingExecutor::default();
    let session = EnvironmentSession::provision(
        Path::new("/repo"),
        sample_docker_compose_profile(),
        &executor,
    )
    .expect("provision session");

    assert_eq!(
        executor.calls()[0],
        CommandCall::new("/repo", vec!["docker", "compose", "up", "-d"])
            .with_cwd("/repo")
    );

    session.finish().expect("teardown succeeds");

    assert_eq!(
        executor.calls()[1],
        CommandCall::new("/repo", vec!["docker", "compose", "down", "-v"])
            .with_cwd("/repo")
    );
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_requires_project_dir
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_uses_repo_relative_project_dir_for_up_and_down
```

Expected:
- FAIL because `environment.rs` does not yet model provider lifecycle or a testable command executor seam

**Step 3: Write minimal implementation**

Update `benchmarks/src/environment.rs` to add:

- a small provider dispatch enum or match on `EnvironmentProvider.kind`
- an `EnvironmentSession` type that owns the resolved profile
- a provider executor seam that records or runs commands
- `EnvironmentSession::provision(...)`
- `EnvironmentSession::finish(...)`

Keep the first slice minimal and typed:

```rust
pub struct EnvironmentSession {
    profile: EnvironmentProfile,
    teardown: Option<ProviderTeardown>,
}

impl EnvironmentSession {
    pub fn provision(
        repo_root: &Path,
        profile: EnvironmentProfile,
        executor: &dyn ProviderCommandExecutor,
    ) -> Result<Self> {
        match profile.provider.kind.as_str() {
            "docker_compose" => provision_docker_compose(repo_root, &profile, executor),
            other => bail!("unsupported environment provider kind {other}"),
        }
    }

    pub fn finish(mut self, executor: &dyn ProviderCommandExecutor) -> Result<()> {
        if let Some(teardown) = self.teardown.take() {
            teardown.run(executor)?;
        }
        Ok(())
    }
}
```

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_requires_project_dir
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_uses_repo_relative_project_dir_for_up_and_down
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs
git commit -m "feat(bench): add environment provider lifecycle session"
```

### Task 2: Add failing runner tests for mandatory teardown

**Files:**
- Modify: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add focused runner tests that assert:

- teardown runs after a successful benchmark run
- teardown still runs when benchmark execution fails
- when benchmark execution and teardown both fail, the benchmark error remains primary and teardown failure is attached as context

Introduce a small runner seam so tests can inject a fake environment session and a fake scenario executor.

```rust
#[test]
fn emit_scenario_artifacts_tears_down_environment_after_success() {
    let harness = RecordingHarness::successful();

    let written = emit_scenario_artifacts_with_harness(
        Path::new("/repo"),
        &sample_scenarios(),
        sample_emit_options(),
        &harness,
    )
    .expect("run succeeds");

    assert_eq!(written, 1);
    assert_eq!(harness.events(), vec!["provision", "execute", "teardown"]);
}

#[test]
fn emit_scenario_artifacts_tears_down_environment_after_execution_failure() {
    let harness = RecordingHarness::with_execution_error("boom");

    let err = emit_scenario_artifacts_with_harness(
        Path::new("/repo"),
        &sample_scenarios(),
        sample_emit_options(),
        &harness,
    )
    .expect_err("run should fail");

    assert!(err.to_string().contains("boom"));
    assert_eq!(harness.events(), vec!["provision", "execute", "teardown"]);
}

#[test]
fn emit_scenario_artifacts_reports_execution_failure_with_teardown_context() {
    let harness = RecordingHarness::with_execution_and_teardown_error(
        "benchmark failed",
        "teardown failed",
    );

    let err = emit_scenario_artifacts_with_harness(
        Path::new("/repo"),
        &sample_scenarios(),
        sample_emit_options(),
        &harness,
    )
    .expect_err("run should fail");

    let rendered = err.to_string();
    assert!(rendered.contains("benchmark failed"));
    assert!(rendered.contains("teardown failed"));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_tears_down_environment_after_success
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_tears_down_environment_after_execution_failure
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_reports_execution_failure_with_teardown_context
```

Expected:
- FAIL because the runner currently has no environment session lifecycle and no guaranteed teardown path

**Step 3: Write minimal implementation**

Refactor `benchmarks/src/runner.rs` to separate:

- environment session acquisition
- scenario execution loop
- session finalization

Keep the public entrypoint intact and add a narrow test-only seam:

```rust
pub fn emit_scenario_artifacts(
    root: &Path,
    scenarios: &[ScenarioManifest],
    options: EmitArtifactsOptions<'_>,
) -> Result<usize> {
    let session = BenchmarkEnvironmentHandle::acquire(root, scenarios, options.env_profile)?;
    emit_scenario_artifacts_with_session(root, scenarios, options, session)
}

fn emit_scenario_artifacts_with_session(
    root: &Path,
    scenarios: &[ScenarioManifest],
    options: EmitArtifactsOptions<'_>,
    mut session: BenchmarkEnvironmentHandle,
) -> Result<usize> {
    let run_result = run_selected_scenarios(root, scenarios, options, session.profile());
    let teardown_result = session.finish();

    match (run_result, teardown_result) {
        (Ok(written), Ok(())) => Ok(written),
        (Ok(_), Err(teardown_err)) => Err(teardown_err),
        (Err(run_err), Ok(())) => Err(run_err),
        (Err(run_err), Err(teardown_err)) => Err(run_err.context(teardown_err)),
    }
}
```

Do not change benchmark measurement logic or scenario filtering in this task.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_tears_down_environment_after_success
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_tears_down_environment_after_execution_failure
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_reports_execution_failure_with_teardown_context
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/runner.rs benchmarks/src/environment.rs
git commit -m "fix(bench): always teardown benchmark environments"
```

### Task 3: Wire session-backed environment resolution into benchmark execution

**Files:**
- Modify: `benchmarks/src/environment.rs`
- Modify: `benchmarks/src/runner.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing test**

Add a runner test that proves environment resolution happens from the acquired
session profile rather than from a second ad hoc profile load during scenario
execution.

```rust
#[test]
fn emit_scenario_artifacts_reuses_acquired_environment_session_profile() {
    let harness = RecordingHarness::successful_with_profile(sample_profile());

    emit_scenario_artifacts_with_harness(
        Path::new("/repo"),
        &sample_env_profile_scenarios(),
        sample_emit_options(),
        &harness,
    )
    .expect("run succeeds");

    assert_eq!(harness.profile_load_count(), 1);
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_reuses_acquired_environment_session_profile
```

Expected:
- FAIL because environment profile resolution is still tied directly to scenario execution

**Step 3: Write minimal implementation**

Adjust environment and runner plumbing so the acquired session exposes the
resolved profile or the already derived `PostgresBenchmarkEnvironment`, and the
scenario loop consumes that value without reloading provider state.

Keep the API small:

```rust
impl EnvironmentSession {
    pub fn profile(&self) -> &EnvironmentProfile {
        &self.profile
    }
}

pub fn resolve_postgres_environment_from_profile(
    scenario: &ScenarioManifest,
    profile: Option<&EnvironmentProfile>,
) -> Result<Option<PostgresBenchmarkEnvironment>> { /* ... */ }
```

Use the existing logical environment validation rules unchanged.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_reuses_acquired_environment_session_profile
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs benchmarks/src/runner.rs
git commit -m "refactor(bench): resolve logical environments from session state"
```

### Task 4: Document benchmark-owned environment behavior

**Files:**
- Modify: `benchmarks/scenarios/README.md`
- Modify: `benchmarks/environments/local-dev-postgres.yaml`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing test**

Add or extend a profile parsing test to assert the committed
`local-dev-postgres` profile continues to parse with the required
`docker_compose` project directory field.

```rust
#[test]
fn committed_local_dev_profile_parses_with_docker_compose_project_dir() {
    let profile_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("environments/local-dev-postgres.yaml");

    let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

    assert_eq!(profile.provider.kind, "docker_compose");
    assert_eq!(profile.provider.project_dir.as_deref(), Some("."));
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_dev_profile_parses_with_docker_compose_project_dir
```

Expected:
- FAIL if the committed profile or tests do not yet document the required lifecycle field explicitly

**Step 3: Write minimal implementation**

Update:

- `benchmarks/scenarios/README.md` to state that `bench run` provisions and
  tears down the environment profile automatically
- `benchmarks/environments/local-dev-postgres.yaml` only if the provider
  metadata needs clarification or comments
- `benchmarks/src/environment.rs` tests to keep the committed profile contract
  explicit

Keep the docs concise and operational.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_dev_profile_parses_with_docker_compose_project_dir
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/scenarios/README.md benchmarks/environments/local-dev-postgres.yaml benchmarks/src/environment.rs
git commit -m "docs(bench): document benchmark-owned environment lifecycle"
```

### Task 5: Full verification before completion

**Files:**
- Modify: none
- Test: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Run targeted benchmark crate tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::
cargo test -p rapidbyte-benchmarks runner::tests::
```

Expected:
- PASS with the new lifecycle coverage included

**Step 2: Run the full benchmark crate test suite**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected:
- PASS

**Step 3: Run a real local benchmark lifecycle smoke check**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- run --suite lab --scenario pg_dest_copy_release --env-profile local-dev-postgres --output /tmp/bench-env-lifecycle.jsonl
```

Expected:
- benchmark artifacts are written successfully
- the Docker Compose benchmark environment is torn down after the command exits

**Step 4: Inspect resulting summary artifact**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- summary /tmp/bench-env-lifecycle.jsonl
```

Expected:
- summary renders normally, confirming lifecycle changes did not break artifact output

**Step 5: Commit any final documentation-only touchups if needed**

```bash
git status --short
```

Expected:
- no unexpected changes remain before merge or review handoff
