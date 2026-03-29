import "dotenv/config";

import { Pool, type PoolClient, type PoolConfig, type QueryResult, type QueryResultRow } from "pg";

const DEFAULTS = {
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "postgres",
  database: "postgres",
  max: 10,
};

let pool: Pool | undefined;

function toOptionalNumber(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isNaN(parsed) ? undefined : parsed;
}

function toBoolean(value: string | undefined): boolean | undefined {
  if (value === undefined) {
    return undefined;
  }

  return value === "true";
}

function getPoolConfig(): PoolConfig {
  const connectionString = process.env.DATABASE_URL;

  if (connectionString) {
    return {
      connectionString,
      max: toOptionalNumber(process.env.PGPOOLMAX) ?? DEFAULTS.max,
      ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : undefined,
    };
  }

  return {
    host: process.env.PGHOST ?? DEFAULTS.host,
    port: toOptionalNumber(process.env.PGPORT) ?? DEFAULTS.port,
    user: process.env.PGUSER ?? DEFAULTS.user,
    password: process.env.PGPASSWORD ?? DEFAULTS.password,
    database: process.env.PGDATABASE ?? DEFAULTS.database,
    max: toOptionalNumber(process.env.PGPOOLMAX) ?? DEFAULTS.max,
    ssl: toBoolean(process.env.PGSSL) ? { rejectUnauthorized: false } : undefined,
  };
}

export function getPool(): Pool {
  if (!pool) {
    pool = new Pool(getPoolConfig());
  }

  return pool;
}

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params: unknown[] = [],
): Promise<QueryResult<T>> {
  return getPool().query<T>(text, params);
}

export async function withClient<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await getPool().connect();

  try {
    return await callback(client);
  } finally {
    client.release();
  }
}

export async function closePool(): Promise<void> {
  if (!pool) {
    return;
  }

  await pool.end();
  pool = undefined;
}
