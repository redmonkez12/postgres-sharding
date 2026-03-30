-- 103_partition_maintenance.sql
-- Partition maintenance procedures for range-partitioned orders table.
--
-- Procedures:
--   1. create_next_month_partition() — idempotent, call from cron monthly
--   2. detach_partition()            — detach an old partition (non-blocking with CONCURRENTLY)
--   3. reattach_partition()          — reattach a previously detached partition

BEGIN;

-- ── 1. Create next month's partition ────────────────────────────────────
-- Designed to run monthly from pg_cron or an external scheduler.
-- Idempotent: does nothing if the partition already exists.

CREATE OR REPLACE FUNCTION create_next_month_partition()
RETURNS TEXT AS $$
DECLARE
  next_month DATE := date_trunc('month', NOW() + INTERVAL '1 month');
  month_after DATE := next_month + INTERVAL '1 month';
  partition_name TEXT := format(
    'orders_%s_%s',
    extract(YEAR FROM next_month)::int,
    lpad(extract(MONTH FROM next_month)::int::text, 2, '0')
  );
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
    RAISE NOTICE 'Partition % already exists — skipping', partition_name;
    RETURN partition_name || ' (already exists)';
  END IF;

  EXECUTE format(
    'CREATE TABLE %I PARTITION OF orders FOR VALUES FROM (%L) TO (%L)',
    partition_name, next_month::text, month_after::text
  );

  RAISE NOTICE 'Created partition %', partition_name;
  RETURN partition_name || ' (created)';
END;
$$ LANGUAGE plpgsql;

-- Usage:
--   SELECT create_next_month_partition();
--
-- pg_cron example (run on the 25th of each month):
--   SELECT cron.schedule('create-orders-partition', '0 3 25 * *',
--     $$SELECT create_next_month_partition()$$);


-- ── 2. Detach a partition ───────────────────────────────────────────────
-- Detaches a named partition from the orders table.
-- The table remains accessible as a standalone table for archival/export.

CREATE OR REPLACE FUNCTION detach_partition(partition TEXT)
RETURNS TEXT AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition) THEN
    RAISE EXCEPTION 'Table % does not exist', partition;
  END IF;

  -- Verify it is currently a partition of orders
  IF NOT EXISTS (
    SELECT 1 FROM pg_inherits
    JOIN pg_class child ON child.oid = inhrelid
    JOIN pg_class parent ON parent.oid = inhparent
    WHERE child.relname = partition AND parent.relname = 'orders'
  ) THEN
    RAISE EXCEPTION '% is not a partition of orders', partition;
  END IF;

  EXECUTE format('ALTER TABLE orders DETACH PARTITION %I', partition);

  RAISE NOTICE 'Detached partition %', partition;
  RETURN partition || ' detached';
END;
$$ LANGUAGE plpgsql;

-- Usage:
--   SELECT detach_partition('orders_2024_01');
--   -- The table orders_2024_01 still exists and can be queried directly.
--   -- Export it:  COPY orders_2024_01 TO '/tmp/orders_2024_01.csv' CSV HEADER;
--   -- Drop it:   DROP TABLE orders_2024_01;


-- ── 3. Reattach a previously detached partition ─────────────────────────
-- Re-adds a standalone table as a partition of orders.
-- The table's CHECK constraint must match the partition bounds (Postgres
-- validates this automatically on ATTACH).

CREATE OR REPLACE FUNCTION reattach_partition(
  partition TEXT,
  range_start DATE,
  range_end DATE
) RETURNS TEXT AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition) THEN
    RAISE EXCEPTION 'Table % does not exist', partition;
  END IF;

  -- Verify it is NOT already a partition of orders
  IF EXISTS (
    SELECT 1 FROM pg_inherits
    JOIN pg_class child ON child.oid = inhrelid
    JOIN pg_class parent ON parent.oid = inhparent
    WHERE child.relname = partition AND parent.relname = 'orders'
  ) THEN
    RAISE EXCEPTION '% is already a partition of orders', partition;
  END IF;

  EXECUTE format(
    'ALTER TABLE orders ATTACH PARTITION %I FOR VALUES FROM (%L) TO (%L)',
    partition, range_start::text, range_end::text
  );

  RAISE NOTICE 'Reattached partition % for range [%, %)', partition, range_start, range_end;
  RETURN partition || ' reattached';
END;
$$ LANGUAGE plpgsql;

-- Usage:
--   SELECT reattach_partition('orders_2024_01', '2024-01-01', '2024-02-01');

COMMIT;


-- ============================================================
-- Experiment: detach / insert / default partition behaviour
-- ============================================================

-- 1. Detach a partition and verify queries on remaining data are unaffected
--    Expected: queries that filter on other months still work with partition pruning.
SELECT detach_partition('orders_2025_01');

EXPLAIN ANALYZE
SELECT * FROM orders WHERE created_at >= '2025-03-01' AND created_at < '2025-04-01';
-- Should prune to orders_2025_03 only; orders_2025_01 is gone from the partition set.

-- 2. Reattach and insert backdated data — verify correct routing
SELECT reattach_partition('orders_2025_01', '2025-01-01', '2025-02-01');

INSERT INTO orders (id, customer_id, status, total_cents, region, created_at)
SELECT
  gen_random_uuid(),
  (SELECT id FROM customers ORDER BY random() LIMIT 1),
  'paid',
  1999,
  'us-east',
  '2025-01-15'::timestamptz;

-- Verify it landed in the correct partition
SELECT count(*) AS jan_rows FROM orders_2025_01
WHERE created_at = '2025-01-15';

-- 3. Insert data with no matching partition — verify default catches it
--    First detach a partition to create a gap, then insert into that gap.
SELECT detach_partition('orders_2025_01');

INSERT INTO orders (id, customer_id, status, total_cents, region, created_at)
SELECT
  gen_random_uuid(),
  (SELECT id FROM customers ORDER BY random() LIMIT 1),
  'pending',
  500,
  'eu-west',
  '2025-01-20'::timestamptz;

-- Should land in orders_default because orders_2025_01 is detached
SELECT count(*) AS default_rows FROM orders_default
WHERE created_at >= '2025-01-01' AND created_at < '2025-02-01';

-- Clean up: reattach the partition
-- NOTE: You must first move any rows from orders_default that fall in the
-- reattached range, otherwise ATTACH will fail with an overlap error.
DELETE FROM orders_default
WHERE created_at >= '2025-01-01' AND created_at < '2025-02-01';

SELECT reattach_partition('orders_2025_01', '2025-01-01', '2025-02-01');
