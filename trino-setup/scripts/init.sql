-- Run this after the stack is up to create sample Iceberg tables.
-- Usage: ./scripts/run-init.sh
-- Or manually: docker exec -it trino trino < /scripts/init.sql

-- ── Schema ────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS iceberg.sales;

-- ── Orders table (Iceberg / Parquet on MinIO) ─────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.sales.orders (
    order_id    BIGINT,
    region      VARCHAR,
    product     VARCHAR,
    amount      DOUBLE,
    order_date  DATE
)
WITH (format = 'PARQUET');

-- ── Idempotent seed: clear before inserting so re-runs don't duplicate rows ─
DELETE FROM iceberg.sales.orders;

-- ── Sample data ───────────────────────────────────────────────────────────
INSERT INTO iceberg.sales.orders VALUES
    (1,  'APAC',   'Widget A', 1500.00, DATE '2024-01-15'),
    (2,  'EMEA',   'Widget B',  800.50, DATE '2024-01-16'),
    (3,  'AMER',   'Widget A', 2200.00, DATE '2024-01-17'),
    (4,  'APAC',   'Widget C',  350.75, DATE '2024-01-18'),
    (5,  'EMEA',   'Widget A', 1100.00, DATE '2024-01-19'),
    (6,  'AMER',   'Widget B',  950.25, DATE '2024-01-20'),
    (7,  'APAC',   'Widget B', 1750.00, DATE '2024-01-21'),
    (8,  'EMEA',   'Widget C',  420.00, DATE '2024-01-22'),
    (9,  'AMER',   'Widget C',  680.50, DATE '2024-01-23'),
    (10, 'APAC',   'Widget A', 3000.00, DATE '2024-01-24');

-- ── Verify ────────────────────────────────────────────────────────────────
SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM iceberg.sales.orders
GROUP BY region
ORDER BY total_amount DESC;
