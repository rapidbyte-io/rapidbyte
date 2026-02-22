-- All PG types fixture for type correctness testing.
-- Tests that every supported PG type round-trips correctly through
-- source-postgres (PG -> Arrow IPC) and dest-postgres (Arrow IPC -> PG).
CREATE TABLE IF NOT EXISTS all_types (
    id              SERIAL PRIMARY KEY,
    -- Integer types
    col_smallint    SMALLINT,
    col_int         INTEGER,
    col_bigint      BIGINT,
    -- Float types
    col_real        REAL,
    col_double      DOUBLE PRECISION,
    -- Boolean
    col_bool        BOOLEAN,
    -- Text types
    col_text        TEXT,
    col_varchar     VARCHAR(255),
    col_char        CHAR(10),
    -- Timestamp types
    col_timestamp   TIMESTAMP WITHOUT TIME ZONE,
    col_timestamptz TIMESTAMP WITH TIME ZONE,
    -- Date
    col_date        DATE,
    -- Binary
    col_bytea       BYTEA,
    -- JSON
    col_json        JSON,
    col_jsonb       JSONB,
    -- Text-cast types (extracted via ::text)
    col_numeric     NUMERIC(18,6),
    col_uuid        UUID,
    col_time        TIME WITHOUT TIME ZONE,
    col_timetz      TIME WITH TIME ZONE,
    col_interval    INTERVAL,
    col_inet        INET,
    col_cidr        CIDR,
    col_macaddr     MACADDR
);

INSERT INTO all_types (
    col_smallint, col_int, col_bigint,
    col_real, col_double,
    col_bool,
    col_text, col_varchar, col_char,
    col_timestamp, col_timestamptz,
    col_date,
    col_bytea,
    col_json, col_jsonb,
    col_numeric, col_uuid,
    col_time, col_timetz, col_interval,
    col_inet, col_cidr, col_macaddr
) VALUES
(
    1, 100, 1000000000000,
    3.14, 2.718281828459045,
    true,
    'hello world', 'varchar_val', 'char_val  ',
    '2024-01-15 10:30:00', '2024-01-15 10:30:00+00',
    '2024-01-15',
    E'\\xDEADBEEF',
    '{"key": "value"}', '{"nested": {"a": 1}}',
    123456.789012, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '10:30:00', '10:30:00+02', '1 year 2 months 3 days',
    '192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03'
),
-- NULL row to test NULL handling
(
    NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL, NULL, NULL,
    NULL, NULL,
    NULL,
    NULL,
    NULL, NULL,
    NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL
),
-- Edge-case row 3: integer min boundaries, float specials (NaN/Inf), epoch dates, empty strings
(
    -32768, -2147483648, -9223372036854775808,
    'NaN'::real, 'Infinity'::double precision,
    false,
    '', '', '          ',
    '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
    '1970-01-01',
    E'\\x',
    '{}', '[]',
    0.000000, '00000000-0000-0000-0000-000000000000',
    '00:00:00', '00:00:00+00', '0 seconds',
    '0.0.0.0', '0.0.0.0/0', '00:00:00:00:00:00'
),
-- Edge-case row 4: integer max boundaries, float specials (-Inf/NaN), max dates, Unicode
(
    32767, 2147483647, 9223372036854775807,
    '-Infinity'::real, 'NaN'::double precision,
    true,
    E'\U0001F680 rocket \u4E2D\u6587', repeat('x', 255), 'abcdefghij',
    '9999-12-31 23:59:59', '9999-12-31 23:59:59+00',
    '9999-12-31',
    E'\\x00',
    '[1,2,3]', '{"a":{"b":{"c":[1,null,"x"]}}}',
    999999999999.999999, 'ffffffff-ffff-ffff-ffff-ffffffffffff',
    '23:59:59', '23:59:59+00', '100 years',
    '255.255.255.255', '255.255.255.255/32', 'ff:ff:ff:ff:ff:ff'
);
