-- 101_partition_range.sql
-- Range-partition the orders table by month on created_at.
--
-- Key decisions:
--   1. created_at must be part of the PRIMARY KEY (Postgres requirement for partitioned tables)
--   2. order_items FK cannot reference a partitioned PK unless it includes the partition key,
--      so we drop the FK and rely on application-level integrity
--   3. A default partition catches rows that don't match any defined range

BEGIN;

-- Drop existing tables that depend on orders
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;

-- Partitioned orders table
CREATE TABLE orders (
  id UUID NOT NULL,
  customer_id UUID NOT NULL REFERENCES customers (id),
  status TEXT NOT NULL,
  total_cents INTEGER NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, created_at)  -- partition key must be in PK
) PARTITION BY RANGE (created_at);

-- Monthly partitions for 2025
CREATE TABLE orders_2025_01 PARTITION OF orders
  FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE orders_2025_02 PARTITION OF orders
  FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE orders_2025_03 PARTITION OF orders
  FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE orders_2025_04 PARTITION OF orders
  FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE orders_2025_05 PARTITION OF orders
  FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE orders_2025_06 PARTITION OF orders
  FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE orders_2025_07 PARTITION OF orders
  FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE orders_2025_08 PARTITION OF orders
  FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE orders_2025_09 PARTITION OF orders
  FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE orders_2025_10 PARTITION OF orders
  FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE orders_2025_11 PARTITION OF orders
  FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE orders_2025_12 PARTITION OF orders
  FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Default partition catches anything outside defined ranges
CREATE TABLE orders_default PARTITION OF orders DEFAULT;

-- Recreate indexes on the partitioned table
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_created_at_idx ON orders (created_at DESC);
CREATE INDEX orders_status_idx ON orders (status);
CREATE INDEX orders_region_idx ON orders (region);

-- Recreate order_items WITHOUT the FK to orders
-- (Postgres cannot enforce FKs pointing into a partitioned table unless
--  the reference includes the full partition key, which is impractical here)
CREATE TABLE order_items (
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL,
  product_id UUID NOT NULL REFERENCES products (id),
  quantity INTEGER NOT NULL,
  unit_price_cents INTEGER NOT NULL
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);
CREATE INDEX order_items_product_id_idx ON order_items (product_id);

-- Auto-create monthly partitions.
-- Call: SELECT create_monthly_partitions('orders', 2026, 2027);
-- This detaches matching rows from the default partition before attaching the new one.
CREATE OR REPLACE FUNCTION create_monthly_partitions(
  table_name TEXT,
  start_year INT,
  end_year INT  -- inclusive
) RETURNS void AS $$
DECLARE
  y INT;
  m INT;
  partition_name TEXT;
  start_date TEXT;
  end_date TEXT;
BEGIN
  FOR y IN start_year..end_year LOOP
    FOR m IN 1..12 LOOP
      partition_name := format('%s_%s_%s', table_name, y, lpad(m::text, 2, '0'));
      start_date := format('%s-%s-01', y, lpad(m::text, 2, '0'));

      IF m = 12 THEN
        end_date := format('%s-01-01', y + 1);
      ELSE
        end_date := format('%s-%s-01', y, lpad((m + 1)::text, 2, '0'));
      END IF;

      -- Skip if partition already exists
      IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
      ) THEN
        EXECUTE format(
          'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
          partition_name, table_name, start_date, end_date
        );
        RAISE NOTICE 'Created partition %', partition_name;
      END IF;
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMIT;

-- ============================================================
-- Verification: run these after inserting sample data
-- ============================================================

-- Should prune to orders_2025_03 only
EXPLAIN ANALYZE
SELECT * FROM orders WHERE created_at = '2025-03-15';

-- No pruning possible: scans all partitions
EXPLAIN ANALYZE
SELECT * FROM orders WHERE status = 'paid';
