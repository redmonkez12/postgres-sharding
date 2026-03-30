-- 104_partition_pruning_benchmarks.sql
-- Creates a non-partitioned baseline copy of the orders table for A/B comparison.
--
-- The partitioned `orders` table (101_partition_range.sql) is the experiment;
-- `orders_baseline` is the Stage 1 equivalent — same data, same indexes,
-- no partitioning overhead. Run this AFTER seeding data.

BEGIN;

DROP TABLE IF EXISTS orders_baseline;

-- Exact same columns as the partitioned orders table, but plain heap table
CREATE TABLE orders_baseline (
  id UUID PRIMARY KEY,
  customer_id UUID NOT NULL,
  status TEXT NOT NULL,
  total_cents INTEGER NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Copy all rows from the partitioned table
INSERT INTO orders_baseline
SELECT id, customer_id, status, total_cents, region, created_at, updated_at
FROM orders;

-- Recreate the same indexes as the partitioned table
CREATE INDEX orders_baseline_customer_id_idx ON orders_baseline (customer_id);
CREATE INDEX orders_baseline_created_at_idx ON orders_baseline (created_at DESC);
CREATE INDEX orders_baseline_status_idx ON orders_baseline (status);
CREATE INDEX orders_baseline_region_idx ON orders_baseline (region);

-- Ensure planner statistics are up to date
ANALYZE orders_baseline;
ANALYZE orders;

COMMIT;
