import "dotenv/config";

import { Redis } from "ioredis";

import { query, closePool } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

// ── Config ─────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
const SAMPLE_SIZE = 1_000;
const PRODUCT_TTL = 300;

// ── Helpers ────────────────────────────────────────────────────────

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function banner(text: string): void {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`  ${text}`);
  console.log(`${"=".repeat(60)}\n`);
}

function formatMs(ms: number): string {
  return `${ms.toFixed(2)}ms`;
}

// ── Benchmark phases ──────────────────────────────────────────────

type PhaseResult = {
  latencies: number[];
  hits: number;
  misses: number;
};

async function cachePhase(
  redis: Redis,
  productIds: string[],
): Promise<PhaseResult> {
  const latencies: number[] = [];
  let hits = 0;
  let misses = 0;

  for (const id of productIds) {
    const key = `product:${id}`;
    const start = performance.now();

    const cached = await redis.get(key);

    if (cached) {
      hits++;
    } else {
      misses++;
      const result = await query("SELECT * FROM products WHERE id = $1", [id]);

      if (result.rows.length > 0) {
        await redis.set(key, JSON.stringify(result.rows[0]), "EX", PRODUCT_TTL);
      }
    }

    latencies.push(performance.now() - start);
  }

  return { latencies, hits, misses };
}

async function directPgPhase(productIds: string[]): Promise<number[]> {
  const latencies: number[] = [];

  for (const id of productIds) {
    const start = performance.now();
    await query("SELECT * FROM products WHERE id = $1", [id]);
    latencies.push(performance.now() - start);
  }

  return latencies;
}

function printPhaseStats(result: PhaseResult): void {
  const sorted = [...result.latencies].sort((a, b) => a - b);
  const total = result.hits + result.misses;

  console.log(`  Lookups:   ${total}`);
  console.log(`  Hits:      ${result.hits}   Misses: ${result.misses}`);
  console.log(`  Hit ratio: ${((result.hits / total) * 100).toFixed(1)}%`);
  console.log(`  p50:       ${formatMs(percentile(sorted, 50))}`);
  console.log(`  p95:       ${formatMs(percentile(sorted, 95))}`);
}

// ── Main ──────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const redis = new Redis(REDIS_URL);

  banner("CACHE BENCHMARK");

  // Fetch product IDs
  const result = await query<{ id: string }>(
    "SELECT id FROM products LIMIT $1",
    [SAMPLE_SIZE],
  );
  const productIds = result.rows.map((r) => r.id);

  if (productIds.length === 0) {
    logger.warn("No products in database — run seed first");
    redis.disconnect();
    await closePool();
    return;
  }

  logger.info("Benchmark config", { products: productIds.length });

  // Capture Redis stats before the run
  const infoBefore = await redis.info("stats");
  const hitsBefore = Number(infoBefore.match(/keyspace_hits:(\d+)/)?.[1] ?? 0);
  const missesBefore = Number(infoBefore.match(/keyspace_misses:(\d+)/)?.[1] ?? 0);

  // ── Phase 1: Cold start (empty cache) ───────────────────────────

  banner("PHASE 1 — Cold start (empty cache)");

  const keys = await redis.keys("product:*");
  if (keys.length > 0) await redis.del(...keys);
  await redis.del("products:list");
  logger.info("Flushed all product cache keys");

  const cold = await cachePhase(redis, productIds);
  printPhaseStats(cold);

  // ── Phase 2: Warm cache ─────────────────────────────────────────

  banner("PHASE 2 — Warm cache (all keys populated)");

  const warm = await cachePhase(redis, productIds);
  printPhaseStats(warm);

  // ── Phase 3: Direct Postgres (no cache) ─────────────────────────

  banner("PHASE 3 — Direct Postgres (no cache)");

  const pgLatencies = await directPgPhase(productIds);
  const pgSorted = [...pgLatencies].sort((a, b) => a - b);

  console.log(`  Lookups: ${productIds.length}`);
  console.log(`  p50:     ${formatMs(percentile(pgSorted, 50))}`);
  console.log(`  p95:     ${formatMs(percentile(pgSorted, 95))}`);

  // ── Redis INFO stats ────────────────────────────────────────────

  banner("REDIS INFO STATS");

  const infoAfter = await redis.info("stats");
  const hitsAfter = Number(infoAfter.match(/keyspace_hits:(\d+)/)?.[1] ?? 0);
  const missesAfter = Number(infoAfter.match(/keyspace_misses:(\d+)/)?.[1] ?? 0);

  const totalHits = hitsAfter - hitsBefore;
  const totalMisses = missesAfter - missesBefore;
  const totalOps = totalHits + totalMisses;

  console.log(`  keyspace_hits (delta):   ${totalHits}`);
  console.log(`  keyspace_misses (delta): ${totalMisses}`);
  console.log(`  Server hit ratio:        ${totalOps > 0 ? ((totalHits / totalOps) * 100).toFixed(1) : "N/A"}%`);

  // ── Comparison ──────────────────────────────────────────────────

  banner("COMPARISON");

  const coldSorted = [...cold.latencies].sort((a, b) => a - b);
  const warmSorted = [...warm.latencies].sort((a, b) => a - b);

  const coldP50 = percentile(coldSorted, 50);
  const warmP50 = percentile(warmSorted, 50);
  const pgP50 = percentile(pgSorted, 50);

  const coldP95 = percentile(coldSorted, 95);
  const warmP95 = percentile(warmSorted, 95);
  const pgP95 = percentile(pgSorted, 95);

  console.log("  p50 latency:");
  console.log(`    Cold cache (miss):  ${formatMs(coldP50)}`);
  console.log(`    Warm cache (hit):   ${formatMs(warmP50)}`);
  console.log(`    Direct Postgres:    ${formatMs(pgP50)}`);
  console.log(`    Speedup (warm/PG):  ${(pgP50 / warmP50).toFixed(1)}x`);

  console.log("\n  p95 latency:");
  console.log(`    Cold cache (miss):  ${formatMs(coldP95)}`);
  console.log(`    Warm cache (hit):   ${formatMs(warmP95)}`);
  console.log(`    Direct Postgres:    ${formatMs(pgP95)}`);
  console.log(`    Speedup (warm/PG):  ${(pgP95 / warmP95).toFixed(1)}x`);
  console.log();

  redis.disconnect();
  await closePool();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
