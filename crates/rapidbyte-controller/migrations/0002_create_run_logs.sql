CREATE TABLE run_logs (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    pipeline    TEXT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    level       TEXT NOT NULL,
    message     TEXT NOT NULL,
    fields      JSONB
);

CREATE INDEX idx_run_logs_pipeline ON run_logs(pipeline, "timestamp" DESC);
CREATE INDEX idx_run_logs_run_id ON run_logs(run_id, "timestamp" DESC);
