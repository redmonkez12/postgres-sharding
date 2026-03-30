import { logger } from "../src/utils/logger.js";
import { ShardRouter } from "../src/db/shard-router.js";

async function main(): Promise<void> {
  const router = new ShardRouter();

  // ── Health check ──────────────────────────────────────────────
  logger.info("Running health check…");
  const health = await router.healthCheck();
  for (const [region, ok] of health) {
    logger.info(`  ${region}: ${ok ? "UP" : "DOWN"}`);
  }

  // ── Single-shard query ────────────────────────────────────────
  for (const region of router.regions) {
    const result = await router.query(region, "SELECT COUNT(*) AS cnt FROM orders");
    logger.info(`Orders on ${region}`, { count: result.rows[0]?.cnt });
  }

  // ── Scatter-gather ────────────────────────────────────────────
  logger.info("Scatter-gather: order count across all shards");
  const results = await router.queryAll("SELECT COUNT(*) AS cnt FROM orders");
  let total = 0;
  for (const r of results) {
    total += Number(r.rows[0]?.cnt ?? 0);
  }
  logger.info("Total orders across all shards", { total });

  await router.close();
}

main().catch((error) => {
  logger.error("Shard router demo failed", {
    error: error instanceof Error ? error.message : String(error),
  });
  process.exitCode = 1;
});
