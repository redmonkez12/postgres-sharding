-- 101_partition_range.sql
-- Range-partition the orders table by month on created_at.
--
-- Key decisions:
--   1. created_at must be part of the PRIMARY KEY (Postgres requirement for partitioned tables).
--      The enforceable unique key becomes (id, created_at), not id alone.
--   2. order_items FK: a FK on order_items(order_id) cannot reference orders(id) because the
--      unique constraint is on (id, created_at). Options:
--        a) Add created_at to order_items and reference (id, created_at)  -- full RI, wider join key
--        b) Drop the FK and rely on application-level integrity           -- simpler schema
--      We choose (b) here for simplicity.
--   3. A default partition exists as a safety net only. All expected date ranges must have
--      explicit partitions. Growth in orders_default signals a misconfiguration.

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

-- Monthly partitions: cover all expected data ranges so orders_default stays empty.
-- Pre-creating 2024-2026 ensures no data lands in the default partition.
CREATE TABLE orders_2024_01 PARTITION OF orders FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE orders_2024_02 PARTITION OF orders FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE orders_2024_03 PARTITION OF orders FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE orders_2024_04 PARTITION OF orders FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE orders_2024_05 PARTITION OF orders FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE orders_2024_06 PARTITION OF orders FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE orders_2024_07 PARTITION OF orders FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE orders_2024_08 PARTITION OF orders FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE orders_2024_09 PARTITION OF orders FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE orders_2024_10 PARTITION OF orders FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE orders_2024_11 PARTITION OF orders FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE orders_2024_12 PARTITION OF orders FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE orders_2025_01 PARTITION OF orders FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE orders_2025_02 PARTITION OF orders FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE orders_2025_03 PARTITION OF orders FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE orders_2025_04 PARTITION OF orders FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE orders_2025_05 PARTITION OF orders FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE orders_2025_06 PARTITION OF orders FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE orders_2025_07 PARTITION OF orders FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE orders_2025_08 PARTITION OF orders FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE orders_2025_09 PARTITION OF orders FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE orders_2025_10 PARTITION OF orders FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE orders_2025_11 PARTITION OF orders FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE orders_2025_12 PARTITION OF orders FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE orders_2026_01 PARTITION OF orders FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE orders_2026_02 PARTITION OF orders FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE orders_2026_03 PARTITION OF orders FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE orders_2026_04 PARTITION OF orders FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE orders_2026_05 PARTITION OF orders FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE orders_2026_06 PARTITION OF orders FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE orders_2026_07 PARTITION OF orders FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE orders_2026_08 PARTITION OF orders FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE orders_2026_09 PARTITION OF orders FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE orders_2026_10 PARTITION OF orders FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE orders_2026_11 PARTITION OF orders FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE orders_2026_12 PARTITION OF orders FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Default partition: safety net only. If rows appear here, it means partitions
-- are missing for that date range. Monitor with:
--   SELECT count(*) FROM orders_default;  -- should be 0
CREATE TABLE orders_default PARTITION OF orders DEFAULT;

-- Recreate indexes on the partitioned table
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_created_at_idx ON orders (created_at DESC);
CREATE INDEX orders_status_idx ON orders (status);
CREATE INDEX orders_region_idx ON orders (region);

-- Recreate order_items WITHOUT the FK to orders.
-- The partitioned PK is (id, created_at), so a FK on order_items(order_id)
-- cannot reference orders(id) alone — Postgres requires the FK to match
-- the full unique key. Alternatives:
--   a) Add created_at to order_items and FK to (id, created_at)
--   b) Accept application-level integrity (chosen here for simplicity)
CREATE TABLE order_items (
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL,
  product_id UUID NOT NULL REFERENCES products (id),
  quantity INTEGER NOT NULL,
  unit_price_cents INTEGER NOT NULL
);

CREATE INDEX order_items_order_id_idx ON order_items (order_id);
CREATE INDEX order_items_product_id_idx ON order_items (product_id);

-- Auto-create monthly partitions for future date ranges.
-- Call: SELECT create_monthly_partitions('orders', 2027, 2028);
--
-- IMPORTANT: PostgreSQL does NOT automatically move rows from the default
-- partition into newly created partitions. If orders_default contains rows
-- for the new range, you must migrate them explicitly:
--   1. CREATE the new partition
--   2. DELETE rows from orders_default that belong to the new range
--   3. INSERT those rows into the parent (they will route to the new partition)
-- Alternatively, detach orders_default, create the partition, then migrate and reattach.
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

-- Should prune to orders_2025_03 only (1 partition scanned)
EXPLAIN ANALYZE
SELECT * FROM orders WHERE created_at = '2025-03-15';

-- No pruning possible: scans all partitions (expected for non-partition-key filters)
EXPLAIN ANALYZE
SELECT * FROM orders WHERE status = 'paid';

-- Confirm default partition is empty (non-zero = missing partition coverage)
SELECT count(*) AS default_partition_rows FROM orders_default;
