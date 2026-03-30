import { logger } from "../src/utils/logger.js";
import { ShardRouter } from "../src/db/shard-router.js";

type RevenueRow = { total_revenue: string };
type TopProductRow = { product_name: string; total_qty: string; total_revenue: string };
type OrderRow = { id: string; status: string; total_cents: string; created_at: string };
type CountRow = { cnt: string };

function formatCents(cents: number): string {
  return `$${(cents / 100).toLocaleString("en-US", { minimumFractionDigits: 2 })}`;
}

function elapsed(start: number): number {
  return Number((performance.now() - start).toFixed(2));
}

// ── Total revenue across all regions (scatter-gather) ─────────────

async function totalRevenue(router: ShardRouter): Promise<void> {
  console.log("\n═══ Total Revenue — scatter-gather across all shards ═══\n");

  const start = performance.now();
  const results = await router.queryAll<RevenueRow>(
    `SELECT COALESCE(SUM(total_cents), 0)::text AS total_revenue FROM orders`,
  );

  let globalRevenue = 0;
  for (const result of results) {
    globalRevenue += Number(result.rows[0]?.total_revenue ?? 0);
  }

  const ms = elapsed(start);
  console.log(`  Global revenue: ${formatCents(globalRevenue)}`);
  console.log(`  Latency:        ${ms} ms (scatter-gather, ${results.length} shards)\n`);

  return;
}

// ── Top 10 products globally (merge + re-sort) ────────────────────

async function topProducts(router: ShardRouter): Promise<void> {
  console.log("═══ Top 10 Products Globally — merge + re-sort ═══\n");

  const sql = `
    SELECT p.name AS product_name,
           SUM(oi.quantity)::text AS total_qty,
           SUM(oi.quantity * oi.unit_price_cents)::text AS total_revenue
    FROM order_items oi
    JOIN products p ON p.id = oi.product_id
    GROUP BY p.name
    ORDER BY SUM(oi.quantity * oi.unit_price_cents) DESC
    LIMIT 20`;

  const start = performance.now();
  const results = await router.queryAll<TopProductRow>(sql);

  // merge results from all shards
  const merged = new Map<string, { qty: number; revenue: number }>();
  for (const result of results) {
    for (const row of result.rows) {
      const existing = merged.get(row.product_name);
      const qty = Number(row.total_qty);
      const revenue = Number(row.total_revenue);

      if (existing) {
        existing.qty += qty;
        existing.revenue += revenue;
      } else {
        merged.set(row.product_name, { qty, revenue });
      }
    }
  }

  // re-sort and take top 10
  const sorted = [...merged.entries()]
    .sort((a, b) => b[1].revenue - a[1].revenue)
    .slice(0, 10);

  const ms = elapsed(start);

  const cols = [
    { label: "#", width: 4 },
    { label: "Product", width: 40 },
    { label: "Qty", width: 10 },
    { label: "Revenue", width: 16 },
  ];

  console.log(cols.map((c) => c.label.padStart(c.width)).join(""));
  console.log("─".repeat(cols.reduce((sum, c) => sum + c.width, 0)));

  for (const [index, [name, data]] of sorted.entries()) {
    console.log(
      [
        String(index + 1).padStart(cols[0].width),
        name.padStart(cols[1].width),
        data.qty.toLocaleString().padStart(cols[2].width),
        formatCents(data.revenue).padStart(cols[3].width),
      ].join(""),
    );
  }

  console.log(`\n  Latency: ${ms} ms (scatter-gather + merge)\n`);
}

// ── Single-shard query: orders for a customer in eu ───────────────

async function customerOrders(router: ShardRouter): Promise<void> {
  console.log("═══ Single-Shard Query — orders for a customer in eu ═══\n");

  // pick a random customer from eu shard
  const pickStart = performance.now();
  const customerResult = await router.query<{ id: string; name: string }>(
    "eu",
    `SELECT id, name FROM customers ORDER BY RANDOM() LIMIT 1`,
  );

  if (customerResult.rowCount === 0) {
    console.log("  No customers found on eu shard.\n");
    return;
  }

  const customer = customerResult.rows[0];
  const pickMs = elapsed(pickStart);

  const queryStart = performance.now();
  const ordersResult = await router.query<OrderRow>(
    "eu",
    `SELECT id, status, total_cents::text, created_at::text
     FROM orders
     WHERE customer_id = $1
     ORDER BY created_at DESC
     LIMIT 10`,
    [customer.id],
  );
  const queryMs = elapsed(queryStart);

  console.log(`  Customer: ${customer.name} (${customer.id})`);
  console.log(`  Orders found: ${ordersResult.rowCount}\n`);

  if (ordersResult.rowCount && ordersResult.rowCount > 0) {
    const cols = [
      { label: "Order ID", width: 38 },
      { label: "Status", width: 14 },
      { label: "Total", width: 14 },
      { label: "Created", width: 22 },
    ];

    console.log(cols.map((c) => c.label.padStart(c.width)).join(""));
    console.log("─".repeat(cols.reduce((sum, c) => sum + c.width, 0)));

    for (const row of ordersResult.rows) {
      console.log(
        [
          row.id.padStart(cols[0].width),
          row.status.padStart(cols[1].width),
          formatCents(Number(row.total_cents)).padStart(cols[2].width),
          row.created_at.slice(0, 19).padStart(cols[3].width),
        ].join(""),
      );
    }
  }

  console.log(`\n  Latency: ${pickMs} ms (pick customer) + ${queryMs} ms (fetch orders)\n`);
}

// ── Latency comparison: shard-local vs scatter-gather ─────────────

async function latencyComparison(router: ShardRouter): Promise<void> {
  console.log("═══ Latency Comparison — shard-local vs scatter-gather ═══\n");

  const sql = `SELECT COUNT(*)::text AS cnt FROM orders WHERE status = 'delivered'`;

  // single-shard (eu only)
  const localStart = performance.now();
  const localResult = await router.query<CountRow>("eu", sql);
  const localMs = elapsed(localStart);

  // scatter-gather (all shards)
  const scatterStart = performance.now();
  const scatterResults = await router.queryAll<CountRow>(sql);
  const scatterMs = elapsed(scatterStart);

  let scatterTotal = 0;
  for (const r of scatterResults) {
    scatterTotal += Number(r.rows[0]?.cnt ?? 0);
  }

  console.log(`  Query: COUNT delivered orders\n`);
  console.log(`  Shard-local (eu):   ${Number(localResult.rows[0]?.cnt).toLocaleString()} rows — ${localMs} ms`);
  console.log(`  Scatter-gather:     ${scatterTotal.toLocaleString()} rows — ${scatterMs} ms`);
  console.log(`  Overhead:           ${(scatterMs - localMs).toFixed(2)} ms (+${((scatterMs / localMs - 1) * 100).toFixed(1)}%)\n`);
}

// ── Main ──────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const router = new ShardRouter();

  const health = await router.healthCheck();
  const downShards = [...health.entries()].filter(([, ok]) => !ok);
  if (downShards.length > 0) {
    logger.warn("Some shards are down", { down: downShards.map(([r]) => r) });
  }

  await totalRevenue(router);
  await topProducts(router);
  await customerOrders(router);
  await latencyComparison(router);

  await router.close();
}

main().catch((error) => {
  logger.error("Cross-shard query failed", {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exitCode = 1;
});
