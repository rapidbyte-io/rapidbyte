-- Verify destination tables exist and have correct row counts
SELECT COUNT(*) as user_count FROM raw.users;
SELECT COUNT(*) as order_count FROM raw.orders;

-- Verify data integrity
SELECT name, email FROM raw.users ORDER BY name;
SELECT amount_cents, status FROM raw.orders ORDER BY amount_cents;
