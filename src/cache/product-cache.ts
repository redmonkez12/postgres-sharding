import "dotenv/config";

import { Redis } from "ioredis";

import { query } from "../db/connection.js";
import { logger } from "../utils/logger.js";

// ── Types ──────────────────────────────────────────────────────────

type Product = {
  id: string;
  name: string;
  sku: string;
  price_cents: number;
  category_id: number;
  stock_qty: number;
  created_at: string;
};

// ── Config ─────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
const PRODUCT_TTL = Number(process.env.PRODUCT_CACHE_TTL ?? 300);
const LIST_TTL = Number(process.env.PRODUCT_LIST_CACHE_TTL ?? 60);

// ── Keys ───────────────────────────────────────────────────────────

function productKey(id: string): string {
  return `product:${id}`;
}

const PRODUCTS_LIST_KEY = "products:list";

// ── ProductCache ───────────────────────────────────────────────────

export class ProductCache {
  private readonly redis: Redis;

  constructor(redisUrl?: string) {
    this.redis = new Redis(redisUrl ?? REDIS_URL);
    logger.info("Redis client created", { url: (redisUrl ?? REDIS_URL).replace(/\/\/.*@/, "//<credentials>@") });
  }

  // ── Read (cache-aside) ──────────────────────────────────────────

  async getProduct(id: string): Promise<Product | null> {
    const key = productKey(id);
    const cached = await this.redis.get(key);

    if (cached) {
      logger.info("Cache HIT", { key });
      return JSON.parse(cached) as Product;
    }

    logger.info("Cache MISS", { key });

    const result = await query<Product>("SELECT * FROM products WHERE id = $1", [id]);

    if (result.rows.length === 0) {
      return null;
    }

    const product = result.rows[0];
    await this.redis.set(key, JSON.stringify(product), "EX", PRODUCT_TTL);
    logger.info("Cache SET", { key, ttl: PRODUCT_TTL });

    return product;
  }

  // ── List (cache-aside, shorter TTL) ─────────────────────────────

  async listProducts(): Promise<Product[]> {
    const cached = await this.redis.get(PRODUCTS_LIST_KEY);

    if (cached) {
      logger.info("Cache HIT", { key: PRODUCTS_LIST_KEY });
      return JSON.parse(cached) as Product[];
    }

    logger.info("Cache MISS", { key: PRODUCTS_LIST_KEY });

    const result = await query<Product>("SELECT * FROM products ORDER BY created_at DESC");
    const products = result.rows;

    await this.redis.set(PRODUCTS_LIST_KEY, JSON.stringify(products), "EX", LIST_TTL);
    logger.info("Cache SET", { key: PRODUCTS_LIST_KEY, ttl: LIST_TTL });

    return products;
  }

  // ── Invalidation ────────────────────────────────────────────────

  async invalidateProduct(id: string): Promise<void> {
    const key = productKey(id);
    await this.redis.del(key, PRODUCTS_LIST_KEY);
    logger.info("Cache INVALIDATED", { keys: [key, PRODUCTS_LIST_KEY] });
  }

  // ── Write-through with invalidation ─────────────────────────────

  async updateProduct(id: string, fields: Partial<Pick<Product, "name" | "price_cents" | "stock_qty">>): Promise<Product | null> {
    const setClauses: string[] = [];
    const params: unknown[] = [];
    let paramIndex = 1;

    for (const [col, value] of Object.entries(fields)) {
      setClauses.push(`${col} = $${paramIndex}`);
      params.push(value);
      paramIndex++;
    }

    if (setClauses.length === 0) {
      return this.getProduct(id);
    }

    params.push(id);
    const sql = `UPDATE products SET ${setClauses.join(", ")} WHERE id = $${paramIndex} RETURNING *`;
    const result = await query<Product>(sql, params);

    if (result.rows.length === 0) {
      return null;
    }

    await this.invalidateProduct(id);
    logger.info("Product updated & cache invalidated", { id });

    return result.rows[0];
  }

  // ── Bulk warm-up ────────────────────────────────────────────────

  async warmUp(): Promise<number> {
    logger.info("Cache warm-up started");

    const result = await query<Product>("SELECT * FROM products");
    const pipeline = this.redis.pipeline();

    for (const product of result.rows) {
      pipeline.set(productKey(product.id), JSON.stringify(product), "EX", PRODUCT_TTL);
    }

    pipeline.set(PRODUCTS_LIST_KEY, JSON.stringify(result.rows), "EX", LIST_TTL);
    await pipeline.exec();

    logger.info("Cache warm-up complete", { productCount: result.rows.length });
    return result.rows.length;
  }

  // ── Stats ───────────────────────────────────────────────────────

  async cacheStats(): Promise<{ keys: number; memoryUsed: string }> {
    const info = await this.redis.info("memory");
    const memoryMatch = info.match(/used_memory_human:(.+)/);
    const dbSize = await this.redis.dbsize();

    return {
      keys: dbSize,
      memoryUsed: memoryMatch ? memoryMatch[1].trim() : "unknown",
    };
  }

  // ── Cleanup ─────────────────────────────────────────────────────

  async close(): Promise<void> {
    this.redis.disconnect();
    logger.info("Redis client disconnected");
  }
}
