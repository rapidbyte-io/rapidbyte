-- Benchmark seed: creates bench_events table
-- Row count controlled by bench_rows variable (default 1000)
-- Usage: psql -v bench_rows=10000 -f bench_seed.sql

-- Drop and recreate for clean benchmark runs
DROP TABLE IF EXISTS public.bench_events;

CREATE TABLE public.bench_events (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,
    user_id     INTEGER NOT NULL,
    amount      INTEGER NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    payload     TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO public.bench_events (event_type, user_id, amount, is_active, payload, created_at)
SELECT
    CASE (i % 5)
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'purchase'
        WHEN 2 THEN 'view'
        WHEN 3 THEN 'signup'
        WHEN 4 THEN 'logout'
    END,
    (i % 1000) + 1,
    (i * 7 % 10000),
    (i % 3 != 0),
    'payload-' || i || '-' || repeat('x', 50),
    NOW() - make_interval(secs => (i * 37) % 86400)
FROM generate_series(1, :'bench_rows') AS s(i);
