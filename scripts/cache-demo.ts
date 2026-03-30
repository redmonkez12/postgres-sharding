import { ProductCache } from "../src/cache/product-cache.js";
import { query, closePool } from "../src/db/connection.js";
import { logger } from "../src/utils/logger.js";

async function main() {
  const cache = new ProductCache();

  // ── Warm-up ───────────────────────────────────────────────────────
  logger.info("=== Bulk warm-up ===");
  const count = await cache.warmUp();
  logger.info(`Warmed ${count} products into cache`);

  // ── Fetch a product (should be HIT after warm-up) ─────────────────
  const listResult = await query("SELECT id FROM products LIMIT 1");

  if (listResult.rows.length === 0) {
    logger.warn("No products in database — run seed first");
    await cache.close();
    await closePool();
    return;
  }

  const sampleId = listResult.rows[0].id as string;

  logger.info("=== Single product read (expect HIT) ===");
  const p1 = await cache.getProduct(sampleId);
  logger.info("Product fetched", { id: p1?.id, name: p1?.name });

  // ── Invalidate and re-read (should be MISS → SET) ─────────────────
  logger.info("=== Invalidate & re-read (expect MISS) ===");
  await cache.invalidateProduct(sampleId);
  const p2 = await cache.getProduct(sampleId);
  logger.info("Product re-fetched", { id: p2?.id, name: p2?.name });

  // ── List products (expect HIT from warm-up) ───────────────────────
  logger.info("=== List products (expect HIT) ===");
  const products = await cache.listProducts();
  logger.info(`Listed ${products.length} products`);

  // ── Update a product ──────────────────────────────────────────────
  logger.info("=== Update product ===");
  const updated = await cache.updateProduct(sampleId, { stock_qty: 999 });
  logger.info("Product updated", { id: updated?.id, stock_qty: updated?.stock_qty });

  // ── Read again (should be MISS after update invalidation) ─────────
  logger.info("=== Read after update (expect MISS) ===");
  const p3 = await cache.getProduct(sampleId);
  logger.info("Product fetched", { id: p3?.id, stock_qty: p3?.stock_qty });

  // ── Cache stats ───────────────────────────────────────────────────
  const stats = await cache.cacheStats();
  logger.info("Cache stats", stats);

  await cache.close();
  await closePool();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
