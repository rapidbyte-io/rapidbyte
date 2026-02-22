-- Edge case values for boundary testing.
CREATE TABLE IF NOT EXISTS edge_cases (
    id                SERIAL PRIMARY KEY,
    -- Integer boundaries
    col_smallint_min  SMALLINT,
    col_smallint_max  SMALLINT,
    col_int_min       INTEGER,
    col_int_max       INTEGER,
    col_bigint_min    BIGINT,
    col_bigint_max    BIGINT,
    -- Float special values
    col_real_nan      REAL,
    col_real_inf      REAL,
    col_real_neginf   REAL,
    col_real_tiny     REAL,
    col_double_nan    DOUBLE PRECISION,
    col_double_inf    DOUBLE PRECISION,
    col_double_neginf DOUBLE PRECISION,
    -- Text edge cases
    col_empty_text    TEXT,
    col_unicode       TEXT,
    col_long_text     TEXT,
    col_newlines      TEXT,
    -- Timestamp edge cases
    col_ts_epoch      TIMESTAMP,
    col_ts_far_future TIMESTAMP,
    -- Date edge cases
    col_date_epoch    DATE,
    col_date_max      DATE,
    -- JSON edge cases
    col_json_empty    JSON,
    col_json_array    JSON,
    col_json_nested   JSON,
    col_json_null_val JSON,
    -- Numeric edge cases
    col_numeric_zero  NUMERIC(18,6),
    col_numeric_neg   NUMERIC(18,6),
    col_numeric_large NUMERIC(18,6),
    -- Binary edge cases
    col_bytea_empty   BYTEA,
    col_bytea_zero    BYTEA
);

INSERT INTO edge_cases (
    col_smallint_min, col_smallint_max,
    col_int_min, col_int_max,
    col_bigint_min, col_bigint_max,
    col_real_nan, col_real_inf, col_real_neginf, col_real_tiny,
    col_double_nan, col_double_inf, col_double_neginf,
    col_empty_text, col_unicode, col_long_text, col_newlines,
    col_ts_epoch, col_ts_far_future,
    col_date_epoch, col_date_max,
    col_json_empty, col_json_array, col_json_nested, col_json_null_val,
    col_numeric_zero, col_numeric_neg, col_numeric_large,
    col_bytea_empty, col_bytea_zero
) VALUES (
    -32768, 32767,
    -2147483648, 2147483647,
    -9223372036854775808, 9223372036854775807,
    'NaN'::real, 'Infinity'::real, '-Infinity'::real, 1e-38::real,
    'NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision,
    '', E'\U0001F389\U0001F680 \u4E2D\u6587', repeat('x', 10000), E'line1\nline2\ttab',
    '1970-01-01 00:00:00', '9999-12-31 23:59:59',
    '1970-01-01', '9999-12-31',
    '{}', '[1,2,3]', '{"a":{"b":{"c":[1,null,"x"]}}}', '{"key":null}',
    0.000000, -999999999999.999999, 999999999999.999999,
    E'\\x', E'\\x00'
);
