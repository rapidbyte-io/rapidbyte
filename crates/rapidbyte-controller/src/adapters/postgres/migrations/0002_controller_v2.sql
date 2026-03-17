CREATE TABLE IF NOT EXISTS controller_v2_runs (
  run_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  retryable BOOLEAN NOT NULL DEFAULT FALSE,
  idempotency_key TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_controller_v2_runs_created_at
  ON controller_v2_runs (created_at DESC);
