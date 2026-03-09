CREATE TABLE IF NOT EXISTS benchmark_runs (
    scenario_id TEXT NOT NULL,
    suite_id TEXT NOT NULL,
    git_sha TEXT,
    hardware_class TEXT NOT NULL,
    build_mode TEXT NOT NULL,
    execution_flags JSON,
    canonical_metrics JSON NOT NULL,
    connector_metrics JSON,
    correctness JSON NOT NULL
);
