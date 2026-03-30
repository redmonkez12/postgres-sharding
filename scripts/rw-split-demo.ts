import {
  query,
  withPrimaryClient,
  getPrimaryPool,
  getReplicaPool,
  closePools,
} from "../src/db/read-write-split.js";
import { logger } from "../src/utils/logger.js";

// ── CLI flags ───────────────────────────────────────────────────────

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

// ── Helpers ─────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// ── 1. Stale-read demo ─────────────────────────────────────────────

async function staleReadDemo(): Promise<void> {
  console.log("\n═══ Stale-Read Demo ═══\n");

  // Pick a random customer for the order
  const custResult = await query(
    "SELECT id, region FROM customers ORDER BY RANDOM() LIMIT 1",
    [],
    { readonly: true },
  );
  const customer = custResult.rows[0];
  if (!customer) {
    logger.warn("No customers found — run the seed script first");
    return;
  }

  // Insert an order on PRIMARY
  const orderId = crypto.randomUUID();
  const now = new Date().toISOString();

  await withPrimaryClient(async (client) => {
    await client.query(
      `INSERT INTO orders (id, customer_id, status, total_cents, region, created_at, updated_at)
       VALUES ($1, $2, 'pending', 4200, $3, $4, $4)`,
      [orderId, customer.id, customer.region, now],
    );
  });

  logger.info("Inserted order on PRIMARY", { orderId });

  // Immediately read from REPLICA — likely stale
  const immediateRead = await query(
    "SELECT id, status FROM orders WHERE id = $1",
    [orderId],
    { readonly: true },
  );

  if (immediateRead.rows.length === 0) {
    console.log("  ⏳ Immediate replica read: ORDER NOT YET VISIBLE (expected stale read)");
  } else {
    console.log("  ✅ Immediate replica read: order already visible (replica caught up fast)");
  }

  // Wait for replication to catch up
  const catchUpMs = 2000;
  console.log(`  … waiting ${catchUpMs}ms for replication catch-up …`);
  await sleep(catchUpMs);

  const delayedRead = await query(
    "SELECT id, status FROM orders WHERE id = $1",
    [orderId],
    { readonly: true },
  );

  if (delayedRead.rows.length === 0) {
    console.log("  ⚠️  Delayed replica read: still not visible (high replica lag)");
  } else {
    console.log("  ✅ Delayed replica read: order visible — replica caught up");
  }

  // Clean up
  await query("DELETE FROM orders WHERE id = $1", [orderId]);
  logger.info("Cleaned up demo order", { orderId });
}

// ── 2. Benchmark: split vs all-on-primary ───────────────────────────

type BenchmarkResult = {
  label: string;
  queries: number;
  durationMs: number;
  qps: number;
  p50: number;
  p95: number;
  p99: number;
};

async function runBenchmark(
  label: string,
  queryFn: () => Promise<void>,
  concurrency: number,
  durationSec: number,
): Promise<BenchmarkResult> {
  const durations: number[] = [];
  const deadlineMs = Date.now() + durationSec * 1000;

  async function worker(): Promise<void> {
    while (Date.now() < deadlineMs) {
      const start = performance.now();
      await queryFn();
      durations.push(performance.now() - start);
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  const sorted = durations.slice().sort((a, b) => a - b);
  return {
    label,
    queries: sorted.length,
    durationMs: durationSec * 1000,
    qps: Number((sorted.length / durationSec).toFixed(2)),
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
  };
}

function printResults(results: BenchmarkResult[]): void {
  console.log(
    "Mode".padEnd(25),
    "Queries".padStart(10),
    "QPS".padStart(10),
    "p50 ms".padStart(10),
    "p95 ms".padStart(10),
    "p99 ms".padStart(10),
  );
  console.log("─".repeat(75));

  for (const r of results) {
    console.log(
      r.label.padEnd(25),
      String(r.queries).padStart(10),
      r.qps.toFixed(2).padStart(10),
      r.p50.toFixed(2).padStart(10),
      r.p95.toFixed(2).padStart(10),
      r.p99.toFixed(2).padStart(10),
    );
  }
}

async function benchmarkDemo(concurrency: number, durationSec: number): Promise<void> {
  console.log(`\n═══ Benchmark: Split vs All-on-Primary (c=${concurrency}, ${durationSec}s) ═══\n`);

  const readSql = `SELECT o.id, o.status, o.total_cents
     FROM orders o
     ORDER BY RANDOM()
     LIMIT 1`;

  // All-on-primary: reads go to primary pool directly
  const allOnPrimary = await runBenchmark(
    "All-on-primary",
    async () => {
      await getPrimaryPool().query(readSql);
    },
    concurrency,
    durationSec,
  );

  // Check if replica can serve the benchmark query before starting
  let replicaAvailable = false;
  try {
    await getReplicaPool().query("SELECT 1 FROM orders LIMIT 1");
    replicaAvailable = true;
  } catch {
    // replica missing schema or unreachable
  }

  const results: BenchmarkResult[] = [allOnPrimary];

  if (replicaAvailable) {
    // Split: reads go to replica via query(..., { readonly: true })
    const split = await runBenchmark(
      "Read/write split",
      async () => {
        await getReplicaPool().query(readSql);
      },
      concurrency,
      durationSec,
    );
    results.push(split);
  } else {
    console.log("  ⚠️  Replica unavailable — skipping read/write split benchmark");
    console.log("     Ensure streaming replication is running and schema is replicated\n");
  }

  printResults(results);

  if (results.length === 2) {
    const improvement = ((results[1].qps - allOnPrimary.qps) / allOnPrimary.qps) * 100;
    console.log(
      `\n  Throughput delta: ${improvement >= 0 ? "+" : ""}${improvement.toFixed(1)}% with read/write split`,
    );
  }
}

// ── Main ────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const concurrency = parseFlag(argv, "concurrency", 4);
  const duration = parseFlag(argv, "duration", 5);

  logger.info("Starting read/write split demo", { concurrency, duration });

  await staleReadDemo();
  await benchmarkDemo(concurrency, duration);

  console.log();
  logger.info("Demo complete");
}

main()
  .catch((error) => {
    logger.error("Demo failed", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePools();
  });
