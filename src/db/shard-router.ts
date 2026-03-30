import "dotenv/config";

import { Pool, type QueryResult, type QueryResultRow } from "pg";

import { logger } from "../utils/logger.js";

// ── Types ──────────────────────────────────────────────────────────

type Region = "eu" | "us" | "ap";

type ShardConfig = {
  region: Region;
  connectionString: string;
};

// ── Defaults ───────────────────────────────────────────────────────

const DEFAULT_URLS: Record<Region, string> = {
  eu: "postgresql://postgres:postgres@localhost:5435/postgres",
  us: "postgresql://postgres:postgres@localhost:5436/postgres",
  ap: "postgresql://postgres:postgres@localhost:5437/postgres",
};

const POOL_MAX = 10;

// ── ShardRouter ────────────────────────────────────────────────────

export class ShardRouter {
  private readonly pools: Map<Region, Pool>;

  constructor(configs?: ShardConfig[]) {
    const shards = configs ?? ShardRouter.configsFromEnv();
    this.pools = new Map();

    for (const { region, connectionString } of shards) {
      this.pools.set(
        region,
        new Pool({ connectionString, max: POOL_MAX }),
      );
      logger.info(`Shard pool created`, { region, connectionString: connectionString.replace(/\/\/.*@/, "//<credentials>@") });
    }
  }

  // ── Pool access ────────────────────────────────────────────────

  getPool(region: string): Pool {
    const pool = this.pools.get(region as Region);
    if (!pool) {
      throw new Error(`Unknown shard region: ${region}`);
    }
    return pool;
  }

  // ── Query a single shard ───────────────────────────────────────

  async query<T extends QueryResultRow = QueryResultRow>(
    region: string,
    sql: string,
    params?: unknown[],
  ): Promise<QueryResult<T>> {
    const pool = this.getPool(region);
    logger.info("Shard query", { region, sql: sql.slice(0, 80) });
    return pool.query<T>(sql, params);
  }

  // ── Scatter-gather across all shards ───────────────────────────

  async queryAll<T extends QueryResultRow = QueryResultRow>(
    sql: string,
    params?: unknown[],
  ): Promise<QueryResult<T>[]> {
    logger.info("Scatter-gather query", { sql: sql.slice(0, 80), shards: [...this.pools.keys()] });

    const results = await Promise.allSettled(
      [...this.pools.entries()].map(async ([region, pool]) => {
        const result = await pool.query<T>(sql, params);
        logger.info("Shard responded", { region, rowCount: result.rowCount });
        return result;
      }),
    );

    const output: QueryResult<T>[] = [];
    for (const [index, result] of results.entries()) {
      const region = [...this.pools.keys()][index];
      if (result.status === "fulfilled") {
        output.push(result.value);
      } else {
        logger.error("Shard failed during scatter-gather", {
          region,
          error: result.reason instanceof Error ? result.reason.message : String(result.reason),
        });
      }
    }

    return output;
  }

  // ── Health check ───────────────────────────────────────────────

  async healthCheck(): Promise<Map<string, boolean>> {
    const health = new Map<string, boolean>();
    const regions = [...this.pools.keys()];

    const checks = await Promise.allSettled(
      [...this.pools.entries()].map(async ([, pool]) => pool.query("SELECT 1")),
    );

    for (const [index, check] of checks.entries()) {
      const region = regions[index];
      health.set(region, check.status === "fulfilled");

      if (check.status === "rejected") {
        logger.error("Shard health check failed", {
          region,
          error: check.reason instanceof Error ? check.reason.message : String(check.reason),
        });
      }
    }

    return health;
  }

  // ── Cleanup ────────────────────────────────────────────────────

  async close(): Promise<void> {
    await Promise.all(
      [...this.pools.entries()].map(async ([region, pool]) => {
        await pool.end();
        logger.info("Shard pool closed", { region });
      }),
    );
  }

  // ── Helpers ────────────────────────────────────────────────────

  get regions(): Region[] {
    return [...this.pools.keys()];
  }

  private static configsFromEnv(): ShardConfig[] {
    return (["eu", "us", "ap"] as const).map((region) => {
      const envKey = `SHARD_${region.toUpperCase()}_URL`;
      const connectionString = process.env[envKey] ?? DEFAULT_URLS[region];
      return { region, connectionString };
    });
  }
}
