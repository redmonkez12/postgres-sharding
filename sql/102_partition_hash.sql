-- 102_partition_hash.sql
-- Hash-partition the orders table by customer_id.
--
-- Side experiment comparing hash vs range (101_partition_range.sql):
--   - Hash gives even write distribution across partitions
--   - Range concentrates writes in the current-month partition (hot partition)
--   - Hash prunes on customer_id queries
--   - Range prunes on created_at queries
--
-- NOTE: This creates orders_hash (not orders) so both strategies can coexist
--       for benchmarking. Run 101 first if you need the range-partitioned table.

BEGIN;

-- Drop previous experiment tables if re-running
DROP TABLE IF EXISTS order_items_hash;
DROP TABLE IF EXISTS orders_hash;

-- Hash-partitioned orders table (4 buckets by customer_id)
CREATE TABLE orders_hash (
  id UUID NOT NULL,
  customer_id UUID NOT NULL REFERENCES customers (id),
  status TEXT NOT NULL,
  total_cents INTEGER NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, customer_id)  -- partition key must be in PK
) PARTITION BY HASH (customer_id);

CREATE TABLE orders_hash_p0 PARTITION OF orders_hash
  FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE orders_hash_p1 PARTITION OF orders_hash
  FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE orders_hash_p2 PARTITION OF orders_hash
  FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE orders_hash_p3 PARTITION OF orders_hash
  FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Indexes on the hash-partitioned table
CREATE INDEX orders_hash_customer_id_idx ON orders_hash (customer_id);
CREATE INDEX orders_hash_created_at_idx ON orders_hash (created_at DESC);
CREATE INDEX orders_hash_status_idx ON orders_hash (status);
CREATE INDEX orders_hash_region_idx ON orders_hash (region);

-- order_items mirror for hash-partitioned orders (no FK into partitioned table)
CREATE TABLE order_items_hash (
  id UUID PRIMARY KEY,
  order_id UUID NOT NULL,
  product_id UUID NOT NULL REFERENCES products (id),
  quantity INTEGER NOT NULL,
  unit_price_cents INTEGER NOT NULL
);

CREATE INDEX order_items_hash_order_id_idx ON order_items_hash (order_id);
CREATE INDEX order_items_hash_product_id_idx ON order_items_hash (product_id);

COMMIT;

-- ============================================================
-- Verification: compare partition pruning behaviour
-- ============================================================

-- HASH WINS: prunes to a single partition (1 of 4)
EXPLAIN ANALYZE
SELECT * FROM orders_hash WHERE customer_id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';

-- HASH LOSES: no pruning possible on created_at, scans all 4 partitions
EXPLAIN ANALYZE
SELECT * FROM orders_hash WHERE created_at = '2025-03-15';

-- HASH LOSES: no pruning on status, scans all 4 partitions
EXPLAIN ANALYZE
SELECT * FROM orders_hash WHERE status = 'paid';

-- ============================================================
-- Distribution check: verify even spread across partitions
-- ============================================================
SELECT
  tableoid::regclass AS partition,
  count(*) AS row_count,
  round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM orders_hash
GROUP BY tableoid
ORDER BY partition;
