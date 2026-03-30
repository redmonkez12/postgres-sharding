import { closePool, getPool } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

type LatencyBucket = {
  label: string;
  durations: number[];
};

type BenchmarkResult = {
  label: string;
  count: number;
  errors: number;
  p50: number;
  p95: number;
  p99: number;
  qps: number;
};

function parseFlag(argv: string[], name: string, fallback: number): number {
  const eqFlag = argv.find((arg) => arg.startsWith(`--${name}=`));
  if (eqFlag) {
    const parsed = Number(eqFlag.split("=")[1]);
    if (!Number.isNaN(parsed) && parsed > 0) return parsed;
  }

  const idx = argv.indexOf(`--${name}`);
  if (idx >= 0 && idx + 1 < argv.length) {
    const parsed = Number(argv[idx + 1]);
    if (!Number.isNaN(parsed) && parsed > 0) return parsed;
  }

  return fallback;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function summarize(bucket: LatencyBucket, durationSec: number): BenchmarkResult {
  const sorted = bucket.durations.slice().sort((a, b) => a - b);
  return {
    label: bucket.label,
    count: sorted.length,
    errors: 0,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    qps: Number((sorted.length / durationSec).toFixed(2)),
  };
}

// ── OLTP queries ──────────────────────────────────────────────────────

async function placeOrder(): Promise<void> {
  const pool = getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const custResult = await client.query(
      "SELECT id, region FROM customers ORDER BY RANDOM() LIMIT 1",
    );
    const customer = custResult.rows[0];

    const prodResult = await client.query(
      "SELECT id, price_cents FROM products ORDER BY RANDOM() LIMIT 2",
    );
    const products = prodResult.rows;

    let totalCents = 0;
    for (const p of products) totalCents += p.price_cents;

    const orderId = crypto.randomUUID();
    await client.query(
      `INSERT INTO orders (id, customer_id, status, total_cents, region, created_at, updated_at)
       VALUES ($1, $2, 'pending', $3, $4, NOW(), NOW())`,
      [orderId, customer.id, totalCents, customer.region],
    );

    for (const p of products) {
      await client.query(
        `INSERT INTO order_items (id, order_id, product_id, quantity, unit_price_cents)
         VALUES ($1, $2, $3, 1, $4)`,
        [crypto.randomUUID(), orderId, p.id, p.price_cents],
      );
    }

    await client.query("COMMIT");
  } catch {
    await client.query("ROLLBACK");
    throw new Error("placeOrder failed");
  } finally {
    client.release();
  }
}

async function getOrderById(): Promise<void> {
  const pool = getPool();
  await pool.query(
    `SELECT o.*, c.name AS customer_name
     FROM orders o
     JOIN customers c ON c.id = o.customer_id
     ORDER BY RANDOM()
     LIMIT 1`,
  );
}

async function listCustomerOrders(): Promise<void> {
  const pool = getPool();
  const custResult = await pool.query(
    "SELECT id FROM customers ORDER BY RANDOM() LIMIT 1",
  );
  const customerId = custResult.rows[0]?.id;
  if (!customerId) return;

  await pool.query(
    `SELECT id, status, total_cents, created_at
     FROM orders
     WHERE customer_id = $1
     ORDER BY created_at DESC
     LIMIT 20`,
    [customerId],
  );
}

// ── Analytics queries ─────────────────────────────────────────────────

async function ordersByMonth(): Promise<void> {
  const pool = getPool();
  await pool.query(
    `SELECT DATE_TRUNC('month', created_at) AS month, COUNT(*) AS order_count
     FROM orders
     GROUP BY month
     ORDER BY month DESC`,
  );
}

async function topProductsByRevenue(): Promise<void> {
  const pool = getPool();
  await pool.query(
    `SELECT p.name, SUM(oi.quantity * oi.unit_price_cents) AS revenue_cents
     FROM order_items oi
     JOIN products p ON p.id = oi.product_id
     GROUP BY p.id, p.name
     ORDER BY revenue_cents DESC
     LIMIT 10`,
  );
}

async function revenueByRegion(): Promise<void> {
  const pool = getPool();
  await pool.query(
    `SELECT region, SUM(total_cents) AS revenue_cents, COUNT(*) AS order_count
     FROM orders
     GROUP BY region
     ORDER BY revenue_cents DESC`,
  );
}

// ── Runner ────────────────────────────────────────────────────────────

type WorkloadEntry = {
  label: string;
  fn: () => Promise<void>;
  weight: number;
};

const OLTP_WORKLOAD: WorkloadEntry[] = [
  { label: "place_order", fn: placeOrder, weight: 40 },
  { label: "get_order_by_id", fn: getOrderById, weight: 35 },
  { label: "list_customer_orders", fn: listCustomerOrders, weight: 25 },
];

const ANALYTICS_WORKLOAD: WorkloadEntry[] = [
  { label: "orders_by_month", fn: ordersByMonth, weight: 34 },
  { label: "top_products_revenue", fn: topProductsByRevenue, weight: 33 },
  { label: "revenue_by_region", fn: revenueByRegion, weight: 33 },
];

function pickWeighted(entries: WorkloadEntry[]): WorkloadEntry {
  const total = entries.reduce((s, e) => s + e.weight, 0);
  let cursor = Math.random() * total;

  for (const entry of entries) {
    cursor -= entry.weight;
    if (cursor <= 0) return entry;
  }

  return entries[entries.length - 1];
}

async function runWorker(
  workload: WorkloadEntry[],
  buckets: Map<string, LatencyBucket>,
  errorCounts: Map<string, number>,
  deadlineMs: number,
): Promise<void> {
  while (Date.now() < deadlineMs) {
    const entry = pickWeighted(workload);
    const start = performance.now();

    try {
      await entry.fn();
      const elapsed = Number((performance.now() - start).toFixed(3));
      buckets.get(entry.label)!.durations.push(elapsed);
    } catch {
      errorCounts.set(entry.label, (errorCounts.get(entry.label) ?? 0) + 1);
    }
  }
}

async function runWorkload(
  name: string,
  workload: WorkloadEntry[],
  concurrency: number,
  durationSec: number,
): Promise<void> {
  const buckets = new Map<string, LatencyBucket>();
  const errorCounts = new Map<string, number>();

  for (const entry of workload) {
    buckets.set(entry.label, { label: entry.label, durations: [] });
    errorCounts.set(entry.label, 0);
  }

  const deadlineMs = Date.now() + durationSec * 1000;
  const workers = Array.from({ length: concurrency }, () =>
    runWorker(workload, buckets, errorCounts, deadlineMs),
  );

  await Promise.all(workers);

  console.log(`\n═══ ${name} (concurrency=${concurrency}, duration=${durationSec}s) ═══\n`);
  console.log(
    "Query".padEnd(25),
    "Count".padStart(8),
    "Errors".padStart(8),
    "p50 ms".padStart(10),
    "p95 ms".padStart(10),
    "p99 ms".padStart(10),
    "QPS".padStart(10),
  );
  console.log("─".repeat(81));

  for (const entry of workload) {
    const bucket = buckets.get(entry.label)!;
    const result = summarize(bucket, durationSec);
    result.errors = errorCounts.get(entry.label) ?? 0;

    console.log(
      result.label.padEnd(25),
      String(result.count).padStart(8),
      String(result.errors).padStart(8),
      result.p50.toFixed(2).padStart(10),
      result.p95.toFixed(2).padStart(10),
      result.p99.toFixed(2).padStart(10),
      result.qps.toFixed(2).padStart(10),
    );
  }
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const concurrency = parseFlag(argv, "concurrency", 4);
  const duration = parseFlag(argv, "duration", 10);

  logger.info("Starting benchmark", { concurrency, duration });

  await runWorkload("OLTP Workload", OLTP_WORKLOAD, concurrency, duration);
  await runWorkload("Analytics Workload", ANALYTICS_WORKLOAD, concurrency, duration);

  console.log();
  logger.info("Benchmark complete");
}

main()
  .catch((error) => {
    logger.error("Benchmark failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
