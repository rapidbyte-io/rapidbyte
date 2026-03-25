CREATE TABLE pipeline_states (
    pipeline    TEXT PRIMARY KEY,
    state       TEXT NOT NULL DEFAULT 'active',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
