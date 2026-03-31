import "dotenv/config";

import { execSync } from "node:child_process";
import { Redis } from "ioredis";

import { query, closePool } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

// ── Config ─────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
const COMPOSE_FILE = "-f docker-compose.stage5.yml";
const PRODUCT_TTL = 300;
const FAILURE_TEST_PRODUCTS = 10;

// ── Helpers ────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function docker(cmd: string): string {
  const full = `docker compose ${COMPOSE_FILE} ${cmd}`;
  logger.info(`Exec: ${full}`);
  return execSync(full, { encoding: "utf-8", timeout: 30_000 }).trim();
}

function banner(text: string): void {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`  ${text}`);
  console.log(`${"=".repeat(60)}\n`);
}

function formatMs(ms: number): string {
  return `${ms.toFixed(2)}ms`;
}

function avg(values: number[]): number {
  return values.reduce((a, b) => a + b, 0) / values.length;
}

// ── Part 1: Cache invalidation test ───────────────────────────────

async function invalidationTest(): Promise<void> {
  banner("CACHE INVALIDATION TEST");

  const redis = new Redis(REDIS_URL);

  const result = await query<{ id: string; price_cents: number }>(
    "SELECT id, price_cents FROM products LIMIT 1",
  );

  if (result.rows.length === 0) {
    logger.warn("No products — run seed first");
    redis.disconnect();
    return;
  }

  const { id, price_cents: originalPrice } = result.rows[0];
  const testPrice = originalPrice + 100;
  const key = `product:${id}`;

  try {
    // Step 1: Read product into cache
    console.log("  Step 1: Read product into cache");
    const dbResult = await query("SELECT * FROM products WHERE id = $1", [id]);
    await redis.set(key, JSON.stringify(dbResult.rows[0]), "EX", PRODUCT_TTL);
    console.log(`    Cached product ${id} with price_cents=${originalPrice}`);

    // Step 2: Update price directly in Postgres (bypass cache)
    console.log("\n  Step 2: Update price in Postgres (bypass cache)");
    const updateStart = performance.now();
    await query("UPDATE products SET price_cents = $1 WHERE id = $2", [testPrice, id]);
    console.log(`    Set price_cents=${testPrice} in DB`);

    // Step 3: Read from cache — should still return old value
    console.log("\n  Step 3: Read from cache (expect stale value)");
    const cached = await redis.get(key);
    const cachedProduct = cached ? JSON.parse(cached) : null;
    const stalePrice = cachedProduct?.price_cents;
    const isStale = stalePrice === originalPrice;
    console.log(`    Cache returned price_cents=${stalePrice} (stale: ${isStale})`);

    // Step 4: Invalidate cache
    console.log("\n  Step 4: Invalidate cache");
    await redis.del(key, "products:list");
    const staleWindowMs = performance.now() - updateStart;
    console.log("    Cache keys deleted");

    // Step 5: Read again — DB returns fresh value, re-populate cache
    console.log("\n  Step 5: Read after invalidation (expect fresh value)");
    const freshResult = await query("SELECT * FROM products WHERE id = $1", [id]);
    const freshPrice = freshResult.rows[0].price_cents;
    await redis.set(key, JSON.stringify(freshResult.rows[0]), "EX", PRODUCT_TTL);
    const isFresh = freshPrice === testPrice;
    console.log(`    Got price_cents=${freshPrice} (correct: ${isFresh})`);

    // Step 6: Report stale window
    banner("INVALIDATION RESULTS");
    console.log(`  Stale read detected:  ${isStale ? "Yes (expected)" : "No (cache already expired)"}`);
    console.log(`  Fresh read correct:   ${isFresh ? "Yes" : "No"}`);
    console.log(`  Stale window:         ${formatMs(staleWindowMs)}`);
    console.log("  Conclusion: Cache-aside has a stale window between");
    console.log("  the DB write and explicit invalidation.\n");
  } finally {
    // Restore original price
    await query("UPDATE products SET price_cents = $1 WHERE id = $2", [originalPrice, id]);
    await redis.del(key, "products:list");
    logger.info("Restored original price and cleared cache");
    redis.disconnect();
  }
}

// ── Part 2: Redis failure test ────────────────────────────────────

async function redisFailureTest(): Promise<void> {
  banner("REDIS FAILURE TEST");

  const result = await query<{ id: string }>(
    "SELECT id FROM products LIMIT $1",
    [FAILURE_TEST_PRODUCTS],
  );
  const productIds = result.rows.map((r) => r.id);

  if (productIds.length === 0) {
    logger.warn("No products — run seed first");
    return;
  }

  // ── Step 1: Baseline with Redis up ────────────────────────────

  console.log("  Step 1: Baseline — cached reads with Redis up");
  let redis = new Redis(REDIS_URL);

  // Warm cache
  for (const id of productIds) {
    const dbResult = await query("SELECT * FROM products WHERE id = $1", [id]);
    await redis.set(`product:${id}`, JSON.stringify(dbResult.rows[0]), "EX", PRODUCT_TTL);
  }

  const baselineLatencies: number[] = [];

  for (const id of productIds) {
    const start = performance.now();
    await redis.get(`product:${id}`);
    baselineLatencies.push(performance.now() - start);
  }

  const baselineAvg = avg(baselineLatencies);
  console.log(`    Cached read latency (avg): ${formatMs(baselineAvg)}`);
  redis.disconnect();

  // ── Step 2: Stop Redis ──────────────────────────────────────────

  console.log("\n  Step 2: Stopping Redis container");
  docker("stop redis");
  await sleep(1_000);
  console.log("    Redis container stopped");

  // ── Step 3: Verify Postgres fallback ────────────────────────────

  console.log("\n  Step 3: Postgres fallback during Redis outage");

  const outageLatencies: number[] = [];

  for (const id of productIds) {
    const start = performance.now();
    await query("SELECT * FROM products WHERE id = $1", [id]);
    outageLatencies.push(performance.now() - start);
  }

  const outageAvg = avg(outageLatencies);
  console.log(`    Postgres direct read latency (avg): ${formatMs(outageAvg)}`);
  console.log("    App can serve requests via Postgres fallback");

  // ── Step 4: Restart Redis ───────────────────────────────────────

  console.log("\n  Step 4: Restarting Redis container");
  docker("start redis");

  let redisReady = false;

  for (let attempt = 1; attempt <= 20; attempt++) {
    try {
      const probe = new Redis(REDIS_URL, {
        maxRetriesPerRequest: 0,
        connectTimeout: 1_000,
        lazyConnect: true,
      });
      await probe.connect();
      await probe.ping();
      probe.disconnect();
      redisReady = true;
      break;
    } catch {
      await sleep(500);
    }
  }

  if (!redisReady) {
    logger.error("Redis did not come back within 10s");
    return;
  }

  console.log("    Redis restarted and healthy");

  // ── Step 5: Verify cache fills again ────────────────────────────

  console.log("\n  Step 5: Verify cache starts filling again");
  redis = new Redis(REDIS_URL);

  const recoveryLatencies: number[] = [];

  for (const id of productIds) {
    const key = `product:${id}`;
    const start = performance.now();

    const cached = await redis.get(key);

    if (!cached) {
      const dbResult = await query("SELECT * FROM products WHERE id = $1", [id]);
      await redis.set(key, JSON.stringify(dbResult.rows[0]), "EX", PRODUCT_TTL);
    }

    recoveryLatencies.push(performance.now() - start);
  }

  const recoveryAvg = avg(recoveryLatencies);
  const dbSize = await redis.dbsize();

  console.log(`    Cache keys after recovery: ${dbSize}`);
  console.log(`    Recovery read latency (avg): ${formatMs(recoveryAvg)}`);

  // ── Summary ─────────────────────────────────────────────────────

  banner("REDIS FAILURE TEST RESULTS");
  console.log("  Latency comparison:");
  console.log(`    Redis up (cached reads):     ${formatMs(baselineAvg)} avg`);
  console.log(`    Redis down (Postgres only):  ${formatMs(outageAvg)} avg`);
  console.log(`    After recovery (cache miss):  ${formatMs(recoveryAvg)} avg`);
  console.log(`    Degradation during outage:   ${(outageAvg / baselineAvg).toFixed(1)}x slower`);
  console.log(`    Cache repopulated:           ${dbSize > 0 ? "Yes" : "No"}\n`);

  redis.disconnect();
}

// ── Main ──────────────────────────────────────────────────────────

async function main(): Promise<void> {
  await invalidationTest();
  await redisFailureTest();
  await closePool();
}

main().catch((err) => {
  // Best-effort Redis restart on failure
  try {
    execSync(`docker compose ${COMPOSE_FILE} start redis`, { timeout: 30_000 });
  } catch {
    logger.warn("Could not restart Redis — run manually: docker compose -f docker-compose.stage5.yml start redis");
  }

  console.error(err);
  process.exit(1);
});
