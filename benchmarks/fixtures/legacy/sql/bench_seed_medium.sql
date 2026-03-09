-- Benchmark seed: medium profile (~4 KB/row SaaS orders)
-- 22 columns: UUID, NUMERIC, JSONB (~1.5 KB), multiple TEXT with realistic data
-- Row count controlled by :bench_rows psql variable
-- Usage: psql -v bench_rows=50000 -f bench_seed_medium.sql

DROP TABLE IF EXISTS public.bench_events;

CREATE TABLE public.bench_events (
    id                  BIGSERIAL PRIMARY KEY,
    order_id            UUID NOT NULL DEFAULT gen_random_uuid(),
    customer_id         BIGINT NOT NULL,
    organization_id     BIGINT NOT NULL,
    status              TEXT NOT NULL,
    currency            TEXT NOT NULL,
    subtotal            NUMERIC(12,2) NOT NULL,
    tax_amount          NUMERIC(12,2) NOT NULL,
    total_amount        NUMERIC(12,2) NOT NULL,
    shipping_name       TEXT NOT NULL,
    shipping_street     TEXT NOT NULL,
    shipping_city       TEXT NOT NULL,
    shipping_state      TEXT,
    shipping_zip        TEXT NOT NULL,
    shipping_country    TEXT NOT NULL,
    billing_name        TEXT NOT NULL,
    billing_email       TEXT NOT NULL,
    notes               TEXT,
    tags                TEXT NOT NULL,
    metadata            JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.bench_events (
    order_id, customer_id, organization_id, status, currency,
    subtotal, tax_amount, total_amount,
    shipping_name, shipping_street, shipping_city, shipping_state,
    shipping_zip, shipping_country,
    billing_name, billing_email, notes, tags, metadata,
    created_at, updated_at
)
SELECT
    gen_random_uuid(),
    (i % 50000) + 1,
    (i % 2000) + 1,
    (ARRAY['pending','processing','shipped','delivered','cancelled','refunded'])[1 + (i % 6)],
    (ARRAY['USD','EUR','GBP','JPY','CAD'])[1 + (i % 5)],
    ((i * 13 % 50000) + 100)::numeric / 100,
    ((i * 13 % 50000) + 100)::numeric / 100 * 0.08,
    ((i * 13 % 50000) + 100)::numeric / 100 * 1.08,

    -- shipping_name (~25 chars)
    (ARRAY['James','Maria','Robert','Sarah','Michael','Jennifer','William','Linda',
           'David','Elizabeth','Richard','Barbara','Joseph','Susan','Thomas','Jessica',
           'Charles','Karen','Daniel','Nancy'])[1 + (i % 20)]
    || ' ' ||
    (ARRAY['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
           'Rodriguez','Martinez','Hernandez','Lopez','Wilson','Anderson','Taylor','Thomas',
           'Moore','Jackson','Martin','Lee'])[1 + ((i / 20) % 20)],

    -- shipping_street (~40 chars)
    ((i % 9999) + 1)::text || ' ' ||
    (ARRAY['Oak','Maple','Cedar','Elm','Pine','Birch','Walnut','Cherry',
           'Spruce','Ash','Willow','Poplar','Hickory','Cypress','Magnolia','Redwood'])[1 + (i % 16)]
    || ' ' ||
    (ARRAY['Street','Avenue','Boulevard','Drive','Lane','Road','Court','Place'])[1 + (i % 8)],

    -- shipping_city
    (ARRAY['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia',
           'San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville',
           'Fort Worth','Columbus','Charlotte','Indianapolis'])[1 + (i % 16)],

    -- shipping_state (nullable ~80%)
    CASE WHEN i % 5 < 4 THEN
        (ARRAY['California','Texas','Florida','New York','Pennsylvania','Illinois',
               'Ohio','Georgia','Michigan','North Carolina','New Jersey','Virginia'])[1 + (i % 12)]
    ELSE NULL END,

    -- shipping_zip
    lpad(((i * 7) % 99999 + 1)::text, 5, '0'),

    -- shipping_country
    (ARRAY['United States','Canada','United Kingdom','Germany','France',
           'Australia','Japan','Brazil','Mexico','India'])[1 + (i % 10)],

    -- billing_name (same pool, different offset)
    (ARRAY['James','Maria','Robert','Sarah','Michael','Jennifer','William','Linda',
           'David','Elizabeth','Richard','Barbara','Joseph','Susan','Thomas','Jessica',
           'Charles','Karen','Daniel','Nancy'])[1 + ((i + 7) % 20)]
    || ' ' ||
    (ARRAY['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
           'Rodriguez','Martinez','Hernandez','Lopez','Wilson','Anderson','Taylor','Thomas',
           'Moore','Jackson','Martin','Lee'])[1 + (((i + 7) / 20) % 20)],

    -- billing_email (~30 chars)
    lower(
        (ARRAY['james','maria','robert','sarah','michael','jennifer','william','linda',
               'david','elizabeth'])[1 + (i % 10)]
        || '.' ||
        (ARRAY['smith','johnson','williams','brown','jones','garcia','miller','davis',
               'rodriguez','martinez'])[1 + ((i / 10) % 10)]
    ) || '@' ||
    (ARRAY['acme.com','globex.com','initech.com','umbrella.co','wayne-ent.com',
           'stark-ind.com','oscorp.net','lexcorp.io'])[1 + (i % 8)],

    -- notes (nullable ~60%)
    CASE WHEN i % 10 < 6 THEN
        'Order note: ' ||
        (ARRAY['Please handle with care','Deliver to back door','Gift wrap requested',
               'Leave at front desk','Call before delivery','Signature required',
               'Expedited shipping requested','No substitutions please'])[1 + (i % 8)]
        || '. Ref #' || (i % 10000)::text
    ELSE NULL END,

    -- tags (3-5 comma-separated)
    (ARRAY['wholesale','retail','premium','standard','bulk'])[1 + (i % 5)] || ',' ||
    (ARRAY['electronics','clothing','food','furniture','software'])[1 + ((i / 5) % 5)] || ',' ||
    (ARRAY['domestic','international','express','economy','freight'])[1 + ((i / 25) % 5)] ||
    CASE WHEN i % 3 = 0 THEN ',' || (ARRAY['priority','fragile','oversized','hazmat'])[1 + (i % 4)] ELSE '' END,

    -- metadata (~1.5 KB JSONB blob)
    jsonb_build_object(
        'plan', (ARRAY['starter','professional','enterprise','unlimited'])[1 + (i % 4)],
        'features', jsonb_build_array(
            'analytics', 'sso', 'audit_log', 'api_access',
            (ARRAY['custom_branding','advanced_reporting','dedicated_support','sandbox'])[1 + (i % 4)]
        ),
        'preferences', jsonb_build_object(
            'theme', (ARRAY['dark','light','auto','system'])[1 + (i % 4)],
            'timezone', (ARRAY['America/New_York','America/Chicago','America/Denver',
                               'America/Los_Angeles','Europe/London','Europe/Berlin',
                               'Asia/Tokyo','Australia/Sydney'])[1 + (i % 8)],
            'locale', (ARRAY['en-US','en-GB','de-DE','fr-FR','ja-JP','es-ES'])[1 + (i % 6)],
            'notifications', jsonb_build_object(
                'email', (i % 2 = 0),
                'slack', (i % 3 = 0),
                'sms', (i % 7 = 0),
                'webhook_url', 'https://hooks.example.com/notify/' || (i % 1000)::text
            )
        ),
        'billing_history', jsonb_build_array(
            jsonb_build_object('date', '2024-01-15', 'amount', '149.99', 'status', 'paid',
                               'invoice', 'INV-' || lpad((i * 3)::text, 5, '0')),
            jsonb_build_object('date', '2024-02-15', 'amount', '149.99', 'status', 'paid',
                               'invoice', 'INV-' || lpad((i * 3 + 1)::text, 5, '0')),
            jsonb_build_object('date', '2024-03-15', 'amount', '149.99', 'status', 'paid',
                               'invoice', 'INV-' || lpad((i * 3 + 2)::text, 5, '0'))
        ),
        'custom_fields', jsonb_build_object(
            'department', (ARRAY['Engineering','Marketing','Sales','Support','Finance',
                                 'Operations','Legal','HR'])[1 + (i % 8)],
            'cost_center', 'CC-' || lpad((i % 9999 + 1)::text, 4, '0'),
            'approved_by', lower(
                (ARRAY['jane','john','alice','bob','carol'])[1 + (i % 5)]
                || '.' ||
                (ARRAY['doe','smith','chen','patel','kim'])[1 + ((i / 5) % 5)]
            ) || '@acme.com',
            'project_code', 'PRJ-' || lpad((i % 9999 + 1)::text, 4, '0')
        ),
        'integration_data', jsonb_build_object(
            'stripe_customer_id', 'cus_' || lpad(i::text, 8, '0'),
            'salesforce_account', 'SF-' || lpad((i % 50000)::text, 6, '0'),
            'hubspot_deal_id', i % 100000
        )
    ),

    NOW() - make_interval(secs => (i * 37) % 2592000),
    NOW() - make_interval(secs => (i * 13) % 864000)
FROM generate_series(1, :'bench_rows') AS s(i);
