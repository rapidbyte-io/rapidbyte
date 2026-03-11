# Isolated Benchmark Stack Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move benchmark-owned Docker lifecycle off the shared repo-root dev stack and onto an isolated benchmark-specific Compose stack and profile.

**Architecture:** Add a dedicated benchmark Compose definition and a new `local-bench-postgres` environment profile, then update benchmark lifecycle code to provision that isolated project with a stable benchmark-specific Compose project name. Keep destructive teardown for benchmark-owned profiles, but route wrapper commands and docs to the isolated profile instead of the shared `local-dev-postgres` stack.

**Tech Stack:** Rust, Serde YAML, Docker Compose, Just, Bash, Markdown docs.

---

### Task 1: Add the isolated benchmark profile and Compose definition

**Files:**
- Create: `benchmarks/docker-compose.yml`
- Create: `benchmarks/environments/local-bench-postgres.yaml`
- Modify: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing tests**

Add environment tests that assert:

- the committed `local-bench-postgres` profile parses successfully
- it uses `provider.kind: docker_compose`
- it points at the benchmark-local project directory
- it contains the benchmark-specific provider metadata required for isolated
  ownership

Example test target:

```rust
#[test]
fn committed_local_bench_profile_parses_with_isolated_compose_metadata() {
    let profile_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("environments/local-bench-postgres.yaml");

    let profile = EnvironmentProfile::from_path(&profile_path).expect("parse profile");

    assert_eq!(profile.id, "local-bench-postgres");
    assert_eq!(profile.provider.kind, "docker_compose");
    assert_eq!(profile.provider.project_dir.as_deref(), Some("."));
    assert_eq!(profile.provider.project_name.as_deref(), Some("rapidbyte-bench"));
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_bench_profile_parses_with_isolated_compose_metadata
```

Expected:
- FAIL because the isolated benchmark profile and provider metadata do not exist yet

**Step 3: Write minimal implementation**

Create:

- `benchmarks/docker-compose.yml`
- `benchmarks/environments/local-bench-postgres.yaml`

Update `benchmarks/src/environment.rs` to support the minimal additional
provider metadata, for example:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvironmentProvider {
    pub kind: String,
    #[serde(default)]
    pub project_dir: Option<String>,
    #[serde(default)]
    pub project_name: Option<String>,
}
```

Keep `local-dev-postgres` intact for now unless the test requires an explicit
shared/non-owned classification.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_bench_profile_parses_with_isolated_compose_metadata
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/docker-compose.yml benchmarks/environments/local-bench-postgres.yaml benchmarks/src/environment.rs
git commit -m "feat(bench): add isolated benchmark compose profile"
```

### Task 2: Make provider commands target the isolated benchmark Compose project

**Files:**
- Modify: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing tests**

Add focused tests that assert:

- provision uses the configured benchmark-specific Compose project name
- provision uses `up -d --wait`
- teardown uses the same project name plus `down -v`

Example shape:

```rust
#[test]
fn docker_compose_provider_uses_project_name_for_owned_benchmark_stack() {
    let executor = RecordingCommandExecutor::default();
    let session = EnvironmentSession::provision(
        Path::new("/repo"),
        sample_profile_with_project_name("rapidbyte-bench"),
        &executor,
    )
    .expect("provision session");

    assert_eq!(
        executor.commands.borrow().as_slice(),
        &[RecordedCommand {
            cwd: PathBuf::from("/repo/benchmarks"),
            program: "docker".to_string(),
            args: vec![
                "compose".to_string(),
                "-p".to_string(),
                "rapidbyte-bench".to_string(),
                "up".to_string(),
                "-d".to_string(),
                "--wait".to_string(),
            ],
        }]
    );

    session.finish(&executor).expect("teardown");

    assert_eq!(
        executor.commands.borrow()[1].args,
        vec![
            "compose".to_string(),
            "-p".to_string(),
            "rapidbyte-bench".to_string(),
            "down".to_string(),
            "-v".to_string(),
        ]
    );
}
```

**Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_uses_project_name_for_owned_benchmark_stack
```

Expected:
- FAIL because the provider currently runs plain `docker compose up -d` and `down -v`

**Step 3: Write minimal implementation**

Update `benchmarks/src/environment.rs` so owned Docker profiles:

- require `provider.project_name`
- run `docker compose -p <project> up -d --wait`
- run `docker compose -p <project> down -v`

Keep the cleanup-after-failed-provision behavior already implemented.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::docker_compose_provider_uses_project_name_for_owned_benchmark_stack
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs
git commit -m "feat(bench): namespace owned docker compose lifecycle"
```

### Task 3: Repoint benchmark wrappers and docs to the owned profile

**Files:**
- Modify: `Justfile`
- Modify: `scripts/bench-env-up.sh`
- Modify: `benchmarks/scenarios/README.md`
- Modify: `docs/BENCHMARKING.md`
- Test: `benchmarks/src/environment.rs`

**Step 1: Write the failing test**

Add or extend a committed-profile test that asserts both:

- `local-dev-postgres` remains the shared/manual profile
- `local-bench-postgres` is the default benchmark-owned profile referenced by
  wrapper docs and command defaults

If a direct test on `Justfile` is too awkward, make the docs/profile contract
explicit in unit tests and verify wrapper defaults by inspection after the code
change.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_profiles_distinguish_shared_and_owned_use
```

Expected:
- FAIL because the shared-vs-owned split is not yet encoded clearly

**Step 3: Write minimal implementation**

Update:

- `Justfile`
  - change `bench-lab` default env from `local-dev-postgres` to `local-bench-postgres`
  - change `bench-pr` default env likewise
- `scripts/bench-env-up.sh`
  - point it at the benchmark-local Compose directory/project name, or remove
    its shared-stack assumptions if it remains as a helper
- `benchmarks/scenarios/README.md`
  - describe the owned benchmark profile
- `docs/BENCHMARKING.md`
  - describe `local-bench-postgres` as auto-managed and `local-dev-postgres` as
    shared/manual

Keep wording explicit about ownership to avoid future ambiguity.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::committed_local_profiles_distinguish_shared_and_owned_use
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add Justfile scripts/bench-env-up.sh benchmarks/scenarios/README.md docs/BENCHMARKING.md benchmarks/src/environment.rs
git commit -m "docs(bench): default wrappers to owned benchmark profile"
```

### Task 4: Verify destructive teardown is isolated to the benchmark-owned stack

**Files:**
- Modify: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Write the failing tests**

Add tests that assert:

- owned benchmark Docker profiles keep destructive teardown
- shared/manual profiles do not enter the owned Docker lifecycle path
- empty scenario selections still skip environment acquisition for the new
  default owned profile

Example targeted commands:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::owned_benchmark_profile_uses_destructive_teardown
cargo test -p rapidbyte-benchmarks environment::tests::shared_profile_does_not_require_owned_docker_lifecycle_metadata
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_skips_environment_acquisition_when_no_scenarios_match
```

Expected:
- FAIL where the ownership distinction is not yet encoded or tested

**Step 2: Run tests to verify they fail**

Run the commands above.

Expected:
- FAIL for the new ownership-specific assertions

**Step 3: Write minimal implementation**

Adjust the environment profile model only as much as needed to distinguish:

- shared/manual existing profile behavior
- owned Docker lifecycle behavior

Keep `down -v` only on the owned benchmark profile/provider path. Avoid adding
heuristics tied to whether a stack was already running.

**Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p rapidbyte-benchmarks environment::tests::owned_benchmark_profile_uses_destructive_teardown
cargo test -p rapidbyte-benchmarks environment::tests::shared_profile_does_not_require_owned_docker_lifecycle_metadata
cargo test -p rapidbyte-benchmarks runner::tests::emit_scenario_artifacts_skips_environment_acquisition_when_no_scenarios_match
```

Expected:
- PASS

**Step 5: Commit**

```bash
git add benchmarks/src/environment.rs benchmarks/src/runner.rs
git commit -m "fix(bench): isolate destructive teardown to owned stack"
```

### Task 5: Full verification and local smoke validation

**Files:**
- Modify: none
- Test: `benchmarks/src/environment.rs`
- Test: `benchmarks/src/runner.rs`

**Step 1: Run benchmark crate tests**

Run:

```bash
cargo test -p rapidbyte-benchmarks
```

Expected:
- PASS

**Step 2: Run the benchmark-owned local stack smoke flow**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- run --suite lab --scenario pg_dest_copy_release --env-profile local-bench-postgres --output /tmp/bench-owned-stack.jsonl
```

Expected:
- the owned benchmark stack is provisioned
- benchmark execution proceeds or fails for benchmark reasons only
- the owned stack is torn down afterward

**Step 3: Confirm the shared dev stack is untouched**

Run:

```bash
docker ps --format '{{.Names}}'
```

Expected:
- benchmark-owned containers use only the benchmark project namespace
- repo-root shared dev containers are not implicitly reused or destroyed

**Step 4: Inspect output summary if artifact exists**

Run:

```bash
cargo run --manifest-path benchmarks/Cargo.toml -- summary /tmp/bench-owned-stack.jsonl
```

Expected:
- summary renders normally if the benchmark run produced an artifact

**Step 5: Final status check**

Run:

```bash
git status --short
```

Expected:
- only intended tracked changes remain before review or merge
