import "dotenv/config";

import { Pool, type PoolClient, type PoolConfig, type QueryResult, type QueryResultRow } from "pg";
import { logger } from "../utils/logger.js";

// ── Config ──────────────────────────────────────────────────────────

const DEFAULTS = {
  host: "localhost",
  primaryPort: 5432,
  replicaPort: 5433,
  user: "postgres",
  password: "postgres",
  database: "postgres",
  max: 10,
};

function toOptionalNumber(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? undefined : parsed;
}

function buildPoolConfig(port: number): PoolConfig {
  return {
    host: process.env.PGHOST ?? DEFAULTS.host,
    port,
    user: process.env.PGUSER ?? DEFAULTS.user,
    password: process.env.PGPASSWORD ?? DEFAULTS.password,
    database: process.env.PGDATABASE ?? DEFAULTS.database,
    max: toOptionalNumber(process.env.PGPOOLMAX) ?? DEFAULTS.max,
  };
}

// ── Pools ───────────────────────────────────────────────────────────

let primaryPool: Pool | undefined;
let replicaPool: Pool | undefined;
let replicaDown = false;

function getPrimaryPool(): Pool {
  if (!primaryPool) {
    const port = toOptionalNumber(process.env.PRIMARY_PORT) ?? DEFAULTS.primaryPort;
    primaryPool = new Pool(buildPoolConfig(port));
    logger.info("Primary pool created", { port });
  }
  return primaryPool;
}

function getReplicaPool(): Pool {
  if (!replicaPool) {
    const port = toOptionalNumber(process.env.REPLICA_PORT) ?? DEFAULTS.replicaPort;
    replicaPool = new Pool(buildPoolConfig(port));
    logger.info("Replica pool created", { port });
  }
  return replicaPool;
}

// ── Query options ───────────────────────────────────────────────────

export type QueryOptions = {
  readonly?: boolean;
};

// ── Exported query function ─────────────────────────────────────────

export async function query<T extends QueryResultRow = QueryResultRow>(
  sql: string,
  params: unknown[] = [],
  opts: QueryOptions = {},
): Promise<QueryResult<T>> {
  const wantReplica = opts.readonly === true;

  if (wantReplica && !replicaDown) {
    try {
      const result = await getReplicaPool().query<T>(sql, params);
      logger.info("Query routed to REPLICA", { sql: sql.slice(0, 80) });
      return result;
    } catch (err) {
      replicaDown = true;
      logger.warn("Replica unavailable — falling back to primary", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  const pool = getPrimaryPool();
  const label = wantReplica ? "PRIMARY (fallback)" : "PRIMARY";
  const result = await pool.query<T>(sql, params);
  logger.info(`Query routed to ${label}`, { sql: sql.slice(0, 80) });
  return result;
}

// ── Direct pool access (for benchmarks / advanced usage) ────────────

export { getPrimaryPool, getReplicaPool };

// ── withClient on a specific pool ───────────────────────────────────

export async function withPrimaryClient<T>(
  callback: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await getPrimaryPool().connect();
  try {
    return await callback(client);
  } finally {
    client.release();
  }
}

// ── Cleanup ─────────────────────────────────────────────────────────

export async function closePools(): Promise<void> {
  const tasks: Promise<void>[] = [];

  if (primaryPool) {
    tasks.push(primaryPool.end());
    primaryPool = undefined;
  }
  if (replicaPool) {
    tasks.push(replicaPool.end());
    replicaPool = undefined;
  }

  replicaDown = false;
  await Promise.all(tasks);
}

// ── Health check (re-enable replica after transient failure) ────────

export async function resetReplicaFlag(): Promise<void> {
  replicaDown = false;
}
