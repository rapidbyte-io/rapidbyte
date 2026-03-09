-- Benchmark seed: small profile (~500 B/row event stream)
-- 14 columns: UUID, INET, BOOLEAN, BIGINT, TEXT with realistic distributions
-- Row count controlled by :bench_rows psql variable
-- Usage: psql -v bench_rows=100000 -f bench_seed_small.sql

DROP TABLE IF EXISTS public.bench_events;

CREATE TABLE public.bench_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    event_type      TEXT NOT NULL,
    user_id         BIGINT NOT NULL,
    session_id      BIGINT,
    device_type     TEXT NOT NULL,
    ip_address      INET NOT NULL,
    country_code    TEXT NOT NULL,
    page_url        TEXT NOT NULL,
    referrer_url    TEXT,
    utm_source      TEXT,
    utm_medium      TEXT,
    duration_ms     INTEGER,
    is_converted    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.bench_events (
    event_id, event_type, user_id, session_id, device_type, ip_address,
    country_code, page_url, referrer_url, utm_source, utm_medium,
    duration_ms, is_converted, created_at
)
SELECT
    gen_random_uuid(),
    (ARRAY['click','purchase','view','signup','logout','scroll','hover','submit'])[1 + (i % 8)],
    (i % 100000) + 1,
    CASE WHEN i % 10 < 7 THEN (i * 31) % 500000 ELSE NULL END,
    (ARRAY['mobile','desktop','tablet'])[1 + (i % 3)],
    ('10.' || (i % 256) || '.' || (i / 256 % 256) || '.' || (i % 253 + 1))::inet,
    (ARRAY['US','GB','DE','FR','JP','CA','AU','BR','IN','MX',
           'KR','ES','IT','NL','SE','NO','DK','FI','PL','PT'])[1 + (i % 20)],
    '/section-' || (i % 12) || '/page-' || (i % 500),
    CASE WHEN i % 10 < 6 THEN 'https://ref-' || (i % 50) || '.example.com/campaign/' || (i % 200) ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['google','facebook','twitter','linkedin'])[1 + (i % 4)] ELSE NULL END,
    CASE WHEN i % 10 < 4 THEN (ARRAY['cpc','organic','social','email'])[1 + (i % 4)] ELSE NULL END,
    (i * 7) % 30000,
    (i % 5 = 0),
    NOW() - make_interval(secs => (i * 37) % 86400)
FROM generate_series(1, :'bench_rows') AS s(i);
